"""Custom simulator handlers for the Notion API.

Notion does not fit the simulator's declarative param-role pipeline for two
reasons:

1. **Opaque cursor pagination.** Every Notion list endpoint takes
   ``start_cursor`` / ``page_size`` and answers with ``next_cursor`` /
   ``has_more``. The built-in pagination styles are page-number and
   offset-limit only, so cursor handling lives here (``_paginate``).

2. **The API is a graph.** ``POST /v1/search`` picks its corpus from a
   filter *inside the request body*; block children, comments, views and
   page properties are all keyed by a parent id taken from the path or the
   query string. Those are joins, not filters.

Every handler is a pure function of (request, corpus) so responses stay
deterministic across runs.
"""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_DEFAULT_PAGE_SIZE = 100
_CURSOR_PREFIX = "cursor-"

# Clones appended to the search corpus with ``last_edited_time`` set well
# into the future. A connector that caps its offset at init time never
# returns them; one that does not will chase them forever and
# ``test_read_terminates`` catches the non-convergence.
_FUTURE_RECORDS = 3


# ---------------------------------------------------------------------------
# Endpoint handlers
# ---------------------------------------------------------------------------


def search(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``POST /v1/search`` — pages and data sources.

    The corpus is selected by ``filter.value`` in the request body, and the
    result set is ordered by the requested ``sort.direction``. Notion's
    search has no time filter, which is exactly why the connector walks
    ascending and skips client-side.
    """
    body = _parse_body(prep.body)
    filt = body.get("filter") or {}
    object_type = filt.get("value")

    corpus_name = {"page": "pages", "data_source": "data_sources"}.get(object_type)
    if corpus_name is None:
        return _respond(
            prep,
            400,
            {
                "object": "error",
                "status": 400,
                "code": "validation_error",
                "message": (
                    "filter.value must be 'page' or 'data_source', "
                    f"got {object_type!r}"
                ),
            },
        )

    records = _augment_with_future(_records(corpus, corpus_name))

    sort = body.get("sort") or {}
    descending = sort.get("direction") == "descending"
    records = sorted(
        records, key=lambda r: r.get("last_edited_time") or "", reverse=descending
    )

    return _paginate(prep, records, body.get("start_cursor"), body.get("page_size"))


def users(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/users`` — workspace members and bots."""
    query = _query(prep)
    return _paginate(
        prep,
        _records(corpus, "users"),
        _first(query, "start_cursor"),
        _first(query, "page_size"),
    )


def file_uploads(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/file_uploads`` — enumerates every upload the integration owns.

    Notion documents no sort order here, so the corpus is served in a
    deliberately shuffled order: the connector is responsible for sorting by
    ``last_edited_time`` before applying its watermark, and this keeps it
    honest.
    """
    query = _query(prep)
    records = _records(corpus, "file_uploads")

    status = _first(query, "status")
    if status:
        records = [r for r in records if r.get("status") == status]

    # Deterministic non-chronological order: reverse the corpus.
    records = list(reversed(records))

    return _paginate(
        prep, records, _first(query, "start_cursor"), _first(query, "page_size")
    )


def retrieve_database(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/databases/{database_id}`` — single container object."""
    database_id = _path_tail(prep, r"/v1/databases/([^/?]+)")
    for record in _records(corpus, "databases"):
        if record.get("id") == database_id:
            return _respond(prep, 200, record)
    return _not_found(prep, "database", database_id)


def list_views(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/views?data_source_id=…`` — *partial* view references.

    Mirrors the real API: the listing returns only ``object``/``id``/
    ``parent``/``type``, forcing the connector through
    ``GET /v1/views/{view_id}`` to hydrate. If the connector ever stops
    hydrating, the column-population test will notice the empty columns.
    """
    query = _query(prep)
    data_source_id = _first(query, "data_source_id")
    database_id = _first(query, "database_id")

    if not data_source_id and not database_id:
        return _respond(
            prep,
            400,
            {
                "object": "error",
                "status": 400,
                "code": "validation_error",
                "message": "either data_source_id or database_id is required",
            },
        )

    matched = []
    for view in _records(corpus, "views"):
        if data_source_id and view.get("data_source_id") != data_source_id:
            continue
        if database_id and (view.get("parent") or {}).get("database_id") != database_id:
            continue
        matched.append(
            {
                "object": "view",
                "id": view.get("id"),
                "parent": view.get("parent"),
                "type": view.get("type"),
            }
        )

    return _paginate(
        prep,
        matched,
        _first(query, "start_cursor"),
        _first(query, "page_size"),
        result_type="view",
    )


def retrieve_view(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/views/{view_id}`` — the complete view object."""
    view_id = _path_tail(prep, r"/v1/views/([^/?]+)")
    for record in _records(corpus, "views"):
        if record.get("id") == view_id:
            return _respond(prep, 200, record)
    return _not_found(prep, "view", view_id)


def block_children(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/blocks/{block_id}/children``.

    Blocks in the corpus carry a real ``parent`` pointer, so this is a join
    on parent id — which is what makes the connector's recursive tree crawl
    actually get exercised rather than short-circuiting on a flat list.
    """
    block_id = _path_tail(prep, r"/v1/blocks/([^/?]+)/children")
    query = _query(prep)

    children = [
        block
        for block in _records(corpus, "blocks")
        if _parent_id(block.get("parent")) == block_id
    ]

    return _paginate(
        prep,
        children,
        _first(query, "start_cursor"),
        _first(query, "page_size"),
        result_type="block",
    )


def comments(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/comments?block_id=…`` — the comment thread on one block."""
    query = _query(prep)
    block_id = _first(query, "block_id")
    if not block_id:
        return _respond(
            prep,
            400,
            {
                "object": "error",
                "status": 400,
                "code": "validation_error",
                "message": "block_id is required",
            },
        )

    thread = [
        comment
        for comment in _records(corpus, "comments")
        if _parent_id(comment.get("parent")) == block_id
    ]

    return _paginate(
        prep,
        thread,
        _first(query, "start_cursor"),
        _first(query, "page_size"),
        result_type="comment",
    )


def page_property(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """``GET /v1/pages/{page_id}/properties/{property_id}``.

    The corpus keys overflow items by ``"{page_id}:{property_id}"``. Served
    in small pages (5 at a time) so the connector's own pagination through
    the property-item list is exercised, not just the first response.
    """
    match = re.search(r"/v1/pages/([^/?]+)/properties/([^/?]+)", urlparse(prep.url).path)
    if not match:
        return _not_found(prep, "property_item", prep.url)

    page_id, property_id = match.group(1), match.group(2)
    overflow = _records(corpus, "page_properties", as_dict=True)
    items = overflow.get(f"{page_id}:{property_id}")

    if items is None:
        return _not_found(prep, "property_item", f"{page_id}:{property_id}")

    query = _query(prep)
    offset = _decode_cursor(_first(query, "start_cursor"))
    page = items[offset : offset + 5]
    next_offset = offset + len(page)
    has_more = next_offset < len(items)

    return _respond(
        prep,
        200,
        {
            "object": "list",
            "type": "property_item",
            "results": page,
            "property_item": {
                "id": property_id,
                "type": page[0].get("type") if page else None,
                "next_url": None,
            },
            "next_cursor": _encode_cursor(next_offset) if has_more else None,
            "has_more": has_more,
        },
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _records(corpus: Any, name: str, *, as_dict: bool = False) -> Any:
    value = corpus.get(name)
    if as_dict:
        return value if isinstance(value, dict) else {}
    if isinstance(value, list):
        return value
    return []


def _parse_body(body: Any) -> Dict[str, Any]:
    if body is None:
        return {}
    if isinstance(body, (bytes, bytearray)):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    else:
        text = str(body)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _query(prep: PreparedRequest) -> Dict[str, List[str]]:
    return parse_qs(urlparse(prep.url).query)


def _first(query: Dict[str, List[str]], key: str) -> Optional[str]:
    values = query.get(key)
    return values[0] if values else None


def _path_tail(prep: PreparedRequest, pattern: str) -> Optional[str]:
    match = re.search(pattern, urlparse(prep.url).path)
    return match.group(1) if match else None


def _parent_id(parent: Any) -> Optional[str]:
    """Pull the referenced id out of a Notion ``parent`` object.

    ``{"type": "page_id", "page_id": "..."}`` — the type names the key that
    holds the id.
    """
    if not isinstance(parent, dict):
        return None
    parent_type = parent.get("type")
    if parent_type and parent_type in parent:
        value = parent[parent_type]
        return value if isinstance(value, str) else None
    for key in ("page_id", "block_id", "database_id", "data_source_id"):
        if isinstance(parent.get(key), str):
            return parent[key]
    return None


def _augment_with_future(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Append future-dated clones so the connector's init-time cap is tested."""
    if not records:
        return records
    template = records[-1]
    base = datetime.now(timezone.utc) + timedelta(days=365)
    future = []
    for i in range(_FUTURE_RECORDS):
        clone = copy.deepcopy(template)
        clone["id"] = f"{clone.get('id', 'future')}-future-{i}"
        stamp = (base + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        clone["last_edited_time"] = stamp
        clone["created_time"] = stamp
        future.append(clone)
    return list(records) + future


def _paginate(
    prep: PreparedRequest,
    records: List[Dict[str, Any]],
    start_cursor: Any,
    page_size: Any,
    *,
    result_type: str = "page_or_data_source",
) -> Response:
    """Serve one cursor-delimited page in Notion's list envelope."""
    offset = _decode_cursor(start_cursor)
    try:
        size = int(page_size) if page_size else _DEFAULT_PAGE_SIZE
    except (TypeError, ValueError):
        size = _DEFAULT_PAGE_SIZE
    size = max(1, min(size, _DEFAULT_PAGE_SIZE))

    page = records[offset : offset + size]
    next_offset = offset + len(page)
    has_more = next_offset < len(records)

    return _respond(
        prep,
        200,
        {
            "object": "list",
            "results": page,
            "next_cursor": _encode_cursor(next_offset) if has_more else None,
            "has_more": has_more,
            "type": result_type,
            "request_id": "simulated-request-id",
        },
    )


def _decode_cursor(cursor: Any) -> int:
    if not cursor:
        return 0
    text = str(cursor)
    if text.startswith(_CURSOR_PREFIX):
        text = text[len(_CURSOR_PREFIX) :]
    try:
        return max(0, int(text))
    except (TypeError, ValueError):
        return 0


def _encode_cursor(offset: int) -> str:
    return f"{_CURSOR_PREFIX}{offset}"


def _not_found(prep: PreparedRequest, kind: str, identifier: Any) -> Response:
    return _respond(
        prep,
        404,
        {
            "object": "error",
            "status": 404,
            "code": "object_not_found",
            "message": (
                f"Could not find {kind} with ID {identifier}. Make sure the "
                "relevant pages and databases are shared with your integration."
            ),
        },
    )


def _respond(prep: PreparedRequest, status: int, payload: Dict[str, Any]) -> Response:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    record = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)
