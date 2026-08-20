"""Notion Community Connector implementation.

Notion's REST API is a *graph*, not a set of flat collections. Only three
things can be enumerated directly:

* ``POST /v1/search``  — pages and data sources shared with the integration
* ``GET  /v1/users``   — workspace members and bots
* ``GET  /v1/file_uploads`` — uploaded files

Everything else (databases, views, blocks, comments) is reachable only by
walking outward from those roots. This connector does that walk explicitly;
see ``_iter_search`` and the per-table ``_collect_*`` helpers.

API version
-----------
Pinned to ``2026-03-11`` (override with the ``notion_version`` option).
That version is required for the Views API, ``GET /v1/file_uploads`` and the
``meeting_notes`` block type. It also renames ``archived`` to ``in_trash``
on pages / blocks / databases / data sources, which this connector's schemas
reflect.
"""

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

# Notion truncates paginated page properties at 25 items. A property that
# comes back with exactly this many entries is *probably* truncated even
# when the API omits ``has_more``, so we re-resolve it to be sure.
_TRUNCATION_THRESHOLD = 25

# Property types Notion paginates on the page object.
_PAGINATED_PROPERTY_TYPES = ("title", "rich_text", "relation", "people", "rollup")

_DEFAULT_NOTION_VERSION = "2026-03-11"
_DEFAULT_PAGE_SIZE = 100
_DEFAULT_MAX_RECORDS_PER_BATCH = 200
# Hard ceiling on how many items we will pull back for a single truncated
# property, so one pathological relation cannot blow up a microbatch.
_DEFAULT_MAX_PROPERTY_ITEMS = 1000
# Depth limit for the recursive block-tree crawl.
_DEFAULT_MAX_BLOCK_DEPTH = 5

_MAX_RETRIES = 5
_RETRIABLE_STATUS_CODES = (429, 500, 502, 503, 504)


def _json_str(value: Any) -> Optional[str]:
    """Render a value as a string suitable for a ``MapType(String, String)`` cell.

    Notion nests objects arbitrarily (``cover.external.url``,
    ``parent.database_id``, …). Spark's string map cannot hold those, so
    non-scalar values are JSON-encoded and scalars are passed through as
    plain strings — no quotes, so ``parent["type"]`` reads as ``database_id``
    rather than ``"database_id"``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _string_map(value: Any) -> Optional[Dict[str, Optional[str]]]:
    """Coerce a Notion sub-object into a flat ``{str: str}`` map."""
    if value is None:
        return None
    if not isinstance(value, dict):
        return {"value": _json_str(value)}
    return {k: _json_str(v) for k, v in value.items()}


def _string_map_list(value: Any) -> Optional[List[Dict[str, Optional[str]]]]:
    """Coerce a list of Notion sub-objects into a list of flat string maps."""
    if value is None:
        return None
    if not isinstance(value, list):
        value = [value]
    return [_string_map(item) or {} for item in value]


class NotionLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Notion.

    Uses the plain (non-partitioned) ``LakeflowConnect`` path deliberately.
    Notion exposes no range-query filter — ``POST /v1/search`` can only be
    *sorted* by ``last_edited_time``, never bounded by it — so there is no
    way to split a read into independent, self-contained time windows for
    executors. Partitioning would degenerate into every executor replaying
    the same cursor walk.
    """

    def __init__(self, options: dict) -> None:
        super().__init__(options)
        self.api_token = options["api_token"]
        self.base_url = "https://api.notion.com/v1"
        self.notion_version = options.get("notion_version", _DEFAULT_NOTION_VERSION)
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Notion-Version": self.notion_version,
            "Content-Type": "application/json",
        }
        # Cap cursors at init time so a Trigger.AvailableNow run never chases
        # data that arrives mid-run. The next trigger builds a fresh instance
        # with a newer cap and picks up the remainder.
        self._init_ts = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Interface: tables, schemas, metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> List[str]:
        return [
            "pages",
            "databases",
            "data_sources",
            "views",
            "blocks",
            "comments",
            "users",
            "file_uploads",
        ]

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return _TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(_TABLE_METADATA[table_name])

    def _validate_table(self, table_name: str) -> None:
        if table_name not in _TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {sorted(_TABLE_SCHEMAS)}"
            )

    # ------------------------------------------------------------------
    # Interface: reads
    # ------------------------------------------------------------------

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        self._validate_table(table_name)

        if table_name == "users":
            return self._read_users(table_options)

        return self._read_incremental(table_name, start_offset or {}, table_options)

    def _read_incremental(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Shared incremental driver for every CDC table.

        Notion has no server-side ``since`` filter, so the contract each
        ``_collect_*`` helper implements is: walk the source in *ascending*
        watermark order, drop anything at-or-below ``since``, drop anything
        above ``self._init_ts``, and stop once ``max_records`` rows are in
        hand. This method then derives the new offset from the last
        watermark actually consumed.

        Returning ``start_offset`` unchanged when nothing new was found is
        what terminates ``Trigger.AvailableNow``.
        """
        since = start_offset.get("cursor")
        if since and since >= self._init_ts:
            # Already caught up to the cap — skip the API calls entirely.
            return iter([]), start_offset

        max_records = int(
            table_options.get("max_records_per_batch", _DEFAULT_MAX_RECORDS_PER_BATCH)
        )

        collector = getattr(self, f"_collect_{table_name}")
        records, watermark = collector(since, max_records, table_options)

        if not records:
            return iter([]), start_offset

        end_offset = {"cursor": watermark}
        if end_offset == start_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Issue an API request, retrying on 429, 5xx, and connection-level
        failures (timeouts, dropped connections) with backoff.

        A read/connect timeout raises inside ``requests`` before a response
        ever exists, so it can't be handled by checking ``resp.status_code``
        like the retriable-status path below -- it has to be caught
        separately, or one transient network blip kills the whole streaming
        query instead of just costing a retry.
        """
        kwargs.setdefault("timeout", 30)
        backoff = 1.0
        resp = None
        last_error: Optional[requests.exceptions.RequestException] = None

        for attempt in range(_MAX_RETRIES):
            url = f"{self.base_url}/{path.lstrip('/')}"
            try:
                if method == "GET":
                    resp = requests.get(url, headers=self.headers, **kwargs)
                elif method == "POST":
                    resp = requests.post(url, headers=self.headers, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
            except requests.exceptions.RequestException as exc:
                last_error = exc
                resp = None
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff *= 2
                continue

            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                return resp

            if attempt < _MAX_RETRIES - 1:
                # Notion sends Retry-After on 429; fall back to exponential
                # backoff for 5xx.
                delay = resp.headers.get("retry-after")
                time.sleep(float(delay) if delay else backoff)
                backoff *= 2

        if resp is None and last_error is not None:
            raise last_error
        return resp

    def _get_json(
        self, path: str, params: Optional[dict] = None, *, ignore_statuses: Tuple[int, ...] = ()
    ) -> Optional[dict]:
        """GET one object. Returns ``None`` for 403/404 (not shared with the
        integration) or any status in ``ignore_statuses`` rather than aborting
        the whole read."""
        resp = self._request("GET", path, params=params or {})
        if resp.status_code in (403, 404) or resp.status_code in ignore_statuses:
            return None
        if resp.status_code != 200:
            raise RuntimeError(
                f"Notion API error on GET /{path}: {resp.status_code} {resp.text}"
            )
        return resp.json()

    def _paginate(
        self, method: str, path: str, *, params: Optional[dict] = None,
        body: Optional[dict] = None, page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> Iterator[dict]:
        """Walk a Notion cursor-paginated list endpoint, yielding results.

        A 403/404 mid-walk means the object stopped being shared with the
        integration; treat it as end-of-list rather than failing the read.
        """
        start_cursor: Optional[str] = None
        while True:
            if method == "GET":
                call_params = dict(params or {})
                call_params["page_size"] = page_size
                if start_cursor:
                    call_params["start_cursor"] = start_cursor
                resp = self._request("GET", path, params=call_params)
            else:
                call_body = dict(body or {})
                call_body["page_size"] = page_size
                if start_cursor:
                    call_body["start_cursor"] = start_cursor
                resp = self._request("POST", path, json=call_body)

            if resp.status_code in (403, 404):
                return
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Notion API error on {method} /{path}: "
                    f"{resp.status_code} {resp.text}"
                )

            payload = resp.json()
            results = payload.get("results") or []
            for item in results:
                yield item

            start_cursor = payload.get("next_cursor")
            if not payload.get("has_more") or not start_cursor:
                return

    # ------------------------------------------------------------------
    # Root enumeration
    # ------------------------------------------------------------------

    def _iter_search(self, object_type: str) -> Iterator[dict]:
        """Enumerate ``page`` or ``data_source`` objects, oldest edit first.

        ``POST /v1/search`` accepts no time filter, only a sort direction, so
        an incremental read has to re-walk from the start of the sort and
        skip client-side. Ascending order is what makes that skip safe: once
        we pass the previous watermark, every remaining record is new.
        """
        body = {
            "filter": {"property": "object", "value": object_type},
            "sort": {"direction": "ascending", "timestamp": "last_edited_time"},
        }
        yield from self._paginate("POST", "search", body=body)

    def _iter_new(
        self, records: Iterable[dict], since: Optional[str], cursor_field: str
    ) -> Iterator[Tuple[dict, str]]:
        """Yield ``(record, watermark)`` for records inside ``(since, init_ts]``.

        Ascending input is assumed, so the first record past ``_init_ts``
        ends the walk.
        """
        for record in records:
            watermark = record.get(cursor_field)
            if not watermark:
                continue
            if watermark > self._init_ts:
                return
            if since and watermark <= since:
                continue
            yield record, watermark

    # ------------------------------------------------------------------
    # Per-table collectors
    #
    # Each returns ``(records, watermark)``. The watermark is the offset
    # value to checkpoint; it is not necessarily the emitted rows' cursor
    # field (see ``_collect_blocks``).
    # ------------------------------------------------------------------

    def _collect_pages(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        resolve = table_options.get("resolve_truncated_properties", "true").lower() != "false"
        max_items = int(table_options.get("max_property_items", _DEFAULT_MAX_PROPERTY_ITEMS))

        records: List[dict] = []
        watermark: Optional[str] = None
        for page, mark in self._iter_new(
            self._iter_search("page"), since, "last_edited_time"
        ):
            records.append(self._flatten_page(page, resolve=resolve, max_items=max_items))
            watermark = mark
            if len(records) >= max_records:
                break
        return records, watermark

    def _collect_data_sources(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        records: List[dict] = []
        watermark: Optional[str] = None
        for source, mark in self._iter_new(
            self._iter_search("data_source"), since, "last_edited_time"
        ):
            records.append(self._flatten_data_source(source))
            watermark = mark
            if len(records) >= max_records:
                break
        return records, watermark

    def _collect_databases(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        """Databases are not searchable — ``POST /v1/search`` only accepts
        ``page`` and ``data_source``. Reach them through the data sources
        they own (``data_source.parent.database_id``), then hydrate each with
        ``GET /v1/databases/{id}``.

        The offset watermark stays on the *data source* edit time, since
        that is the ordered sequence we walk.
        """
        records: List[dict] = []
        watermark: Optional[str] = None
        seen: set = set()

        for source, mark in self._iter_new(
            self._iter_search("data_source"), since, "last_edited_time"
        ):
            parent = source.get("parent") or {}
            database_id = parent.get("database_id")
            watermark = mark
            if not database_id or database_id in seen:
                continue
            seen.add(database_id)

            database = self._get_json(f"databases/{database_id}")
            if database:
                records.append(self._flatten_database(database))
            if len(records) >= max_records:
                break
        return records, watermark

    def _collect_views(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        """``GET /v1/views`` requires a ``data_source_id`` (or ``database_id``)
        and returns *partial* view refs — id and type only. Each ref is then
        hydrated with ``GET /v1/views/{view_id}``.

        Watermark is the owning data source's edit time: Notion bumps that
        whenever a view on it is added or reconfigured.
        """
        records: List[dict] = []
        watermark: Optional[str] = None
        seen: set = set()

        for source, mark in self._iter_new(
            self._iter_search("data_source"), since, "last_edited_time"
        ):
            watermark = mark
            data_source_id = source.get("id")
            if not data_source_id:
                continue

            for ref in self._paginate(
                "GET", "views", params={"data_source_id": data_source_id}
            ):
                view_id = ref.get("id")
                if not view_id or view_id in seen:
                    continue
                seen.add(view_id)

                view = ref
                # A partial ref has no name/created_time — hydrate it. Some
                # view types (e.g. "feed") 400 on the single-view GET even
                # though they list fine — Notion just doesn't support
                # retrieving them individually. The id came straight from the
                # list endpoint, so a 400 here means an unsupported view
                # type, not a malformed request; fall back to the partial ref
                # rather than failing the whole read over one view.
                if "name" not in ref or "created_time" not in ref:
                    hydrated = self._get_json(f"views/{view_id}", ignore_statuses=(400,))
                    if hydrated:
                        view = hydrated
                records.append(self._flatten_view(view, data_source_id))

            if len(records) >= max_records:
                break
        return records, watermark

    def _collect_blocks(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        """Crawl the block tree beneath every page whose edit time is new.

        The offset watermark is the *parent page's* ``last_edited_time``, not
        the block's. Notion bumps a page's edit time whenever any block on it
        changes, so gating the crawl on the page watermark is both complete
        and monotonic — whereas individual block timestamps are not ordered
        with respect to each other across pages, and would make the offset
        jump backwards. ``cursor_field`` in table metadata remains the
        block's own ``last_edited_time``, which is the right key for the
        downstream CDC merge.
        """
        max_depth = int(table_options.get("max_block_depth", _DEFAULT_MAX_BLOCK_DEPTH))

        records: List[dict] = []
        watermark: Optional[str] = None
        for page, mark in self._iter_new(
            self._iter_search("page"), since, "last_edited_time"
        ):
            watermark = mark
            page_id = page.get("id")
            if not page_id:
                continue
            for block in self._crawl_blocks(page_id, depth=max_depth):
                records.append(self._flatten_block(block))
            if len(records) >= max_records:
                break
        return records, watermark

    def _crawl_blocks(self, block_id: str, depth: int) -> Iterator[dict]:
        """Depth-first walk of ``GET /v1/blocks/{id}/children``."""
        if depth <= 0:
            return
        for block in self._paginate("GET", f"blocks/{block_id}/children"):
            yield block
            if block.get("has_children") and block.get("id"):
                yield from self._crawl_blocks(block["id"], depth - 1)

    def _collect_comments(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        """Comments are only reachable per-block. Walk new pages and read the
        comment thread hanging off each. Watermark is the page edit time, for
        the same reason as ``_collect_blocks``.
        """
        records: List[dict] = []
        watermark: Optional[str] = None
        for page, mark in self._iter_new(
            self._iter_search("page"), since, "last_edited_time"
        ):
            watermark = mark
            page_id = page.get("id")
            if not page_id:
                continue
            for comment in self._paginate(
                "GET", "comments", params={"block_id": page_id}
            ):
                records.append(self._flatten_comment(comment))
            if len(records) >= max_records:
                break
        return records, watermark

    def _collect_file_uploads(
        self, since: Optional[str], max_records: int, table_options: Dict[str, str]
    ) -> Tuple[List[dict], Optional[str]]:
        """``GET /v1/file_uploads`` enumerates every upload the integration
        owns, so unlike most "attachment" APIs there is no need to hydrate
        ids scraped out of blocks one at a time.

        The endpoint has no time filter and no documented sort order, so we
        buffer the listing and sort by ``last_edited_time`` ourselves before
        applying the watermark.
        """
        params = {}
        status = table_options.get("file_upload_status")
        if status:
            params["status"] = status

        uploads = list(self._paginate("GET", "file_uploads", params=params))
        uploads.sort(key=lambda u: u.get("last_edited_time") or "")

        records: List[dict] = []
        watermark: Optional[str] = None
        for upload, mark in self._iter_new(uploads, since, "last_edited_time"):
            records.append(self._flatten_file_upload(upload))
            watermark = mark
            if len(records) >= max_records:
                break
        return records, watermark

    def _read_users(self, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        """Full snapshot of ``GET /v1/users``."""
        records = [self._flatten_user(u) for u in self._paginate("GET", "users")]
        return iter(records), {}

    # ------------------------------------------------------------------
    # Page property truncation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_truncated(prop: Any) -> bool:
        """Detect a page property that the page object returned incomplete.

        Notion signals this three different ways depending on type, and
        sometimes not at all:

        * ``has_more: true`` on the property (relations, people, rich text)
        * ``rollup.type == "incomplete"`` for rollups needing more
          aggregation pages
        * silently, by capping the array at 25 entries

        The last case is why we also treat "exactly 25 items" as suspect and
        re-resolve; a false positive costs one extra API call, a false
        negative silently drops data.
        """
        if not isinstance(prop, dict):
            return False
        if prop.get("has_more") is True:
            return True

        prop_type = prop.get("type")
        if prop_type == "rollup":
            rollup = prop.get("rollup")
            if isinstance(rollup, dict) and rollup.get("type") == "incomplete":
                return True

        if prop_type in _PAGINATED_PROPERTY_TYPES:
            value = prop.get(prop_type)
            if isinstance(value, list) and len(value) >= _TRUNCATION_THRESHOLD:
                return True
        return False

    def _resolve_property(
        self, page_id: str, property_id: str, prop: dict, max_items: int
    ) -> dict:
        """Re-fetch a truncated property via
        ``GET /v1/pages/{page_id}/properties/{property_id}``.

        The endpoint returns a paginated list of ``property_item`` objects;
        we unwrap each one back into the shape the page object *would* have
        had, so downstream consumers see a single consistent representation
        rather than two.
        """
        prop_type = prop.get("type")
        items: List[Any] = []
        rollup_aggregate: Optional[dict] = None
        truncated = False

        for item in self._paginate(
            "GET", f"pages/{page_id}/properties/{property_id}"
        ):
            if len(items) >= max_items:
                truncated = True
                break
            item_type = item.get("type")
            if item_type == "rollup":
                # The aggregate lives on the wrapper, not the items; the
                # items themselves are the rolled-up values.
                rollup_aggregate = item.get("rollup")
                continue
            items.append(item.get(item_type, item) if item_type else item)

        if not items and rollup_aggregate is None:
            # Nothing came back (403/404, or an unsupported property type) —
            # keep the truncated page-object value rather than blanking it.
            return prop

        resolved = dict(prop)
        if prop_type == "rollup" and rollup_aggregate is not None:
            resolved["rollup"] = rollup_aggregate
            resolved["results"] = items
        elif prop_type:
            resolved[prop_type] = items
        resolved["has_more"] = truncated
        return resolved

    def _resolve_properties(
        self, page_id: str, properties: dict, max_items: int
    ) -> Tuple[Dict[str, Any], List[str]]:
        """Resolve every truncated property on a page.

        Returns ``(properties, names_of_resolved_properties)``.
        """
        resolved: Dict[str, Any] = {}
        touched: List[str] = []
        for name, prop in (properties or {}).items():
            if self._is_truncated(prop) and isinstance(prop, dict) and prop.get("id"):
                resolved[name] = self._resolve_property(
                    page_id, prop["id"], prop, max_items
                )
                touched.append(name)
            else:
                resolved[name] = prop
        return resolved, touched

    # ------------------------------------------------------------------
    # Flatteners — map a raw Notion object onto its table schema
    # ------------------------------------------------------------------

    def _flatten_page(self, page: dict, *, resolve: bool, max_items: int) -> dict:
        properties = page.get("properties") or {}
        truncated: List[str] = []
        if resolve and page.get("id"):
            properties, truncated = self._resolve_properties(
                page["id"], properties, max_items
            )

        return {
            "id": page.get("id"),
            "url": page.get("url"),
            "public_url": page.get("public_url"),
            "in_trash": _coalesce_trash(page),
            "is_locked": page.get("is_locked"),
            "created_time": page.get("created_time"),
            "last_edited_time": page.get("last_edited_time"),
            "created_by": _string_map(page.get("created_by")),
            "last_edited_by": _string_map(page.get("last_edited_by")),
            "cover": _string_map(page.get("cover")),
            "icon": _string_map(page.get("icon")),
            "parent": _string_map(page.get("parent")),
            "properties": {k: _json_str(v) for k, v in properties.items()},
            "truncated_properties": truncated,
            "request_id": page.get("request_id"),
        }

    def _flatten_data_source(self, source: dict) -> dict:
        return {
            "id": source.get("id"),
            "url": source.get("url"),
            "name": source.get("name"),
            "title": _string_map_list(source.get("title")),
            "description": _string_map_list(source.get("description")),
            "is_inline": source.get("is_inline"),
            "in_trash": _coalesce_trash(source),
            "database_parent": _string_map(source.get("database_parent")),
            "created_time": source.get("created_time"),
            "last_edited_time": source.get("last_edited_time"),
            "created_by": _string_map(source.get("created_by")),
            "last_edited_by": _string_map(source.get("last_edited_by")),
            "parent": _string_map(source.get("parent")),
            "properties": {
                k: _json_str(v) for k, v in (source.get("properties") or {}).items()
            },
            "request_id": source.get("request_id"),
        }

    def _flatten_database(self, database: dict) -> dict:
        data_sources = database.get("data_sources") or []
        return {
            "id": database.get("id"),
            "url": database.get("url"),
            "public_url": database.get("public_url"),
            "title": _string_map_list(database.get("title")),
            "description": _string_map_list(database.get("description")),
            "is_inline": database.get("is_inline"),
            "is_locked": database.get("is_locked"),
            "in_trash": _coalesce_trash(database),
            "created_time": database.get("created_time"),
            "last_edited_time": database.get("last_edited_time"),
            "created_by": _string_map(database.get("created_by")),
            "last_edited_by": _string_map(database.get("last_edited_by")),
            "cover": _string_map(database.get("cover")),
            "icon": _string_map(database.get("icon")),
            "parent": _string_map(database.get("parent")),
            "data_sources": _string_map_list(data_sources),
            "data_source_ids": [
                ds.get("id") for ds in data_sources if isinstance(ds, dict) and ds.get("id")
            ],
            "request_id": database.get("request_id"),
        }

    def _flatten_view(self, view: dict, data_source_id: Optional[str]) -> dict:
        return {
            "id": view.get("id"),
            "name": view.get("name"),
            "type": view.get("type"),
            "url": view.get("url"),
            "data_source_id": view.get("data_source_id") or data_source_id,
            "parent": _string_map(view.get("parent")),
            "created_time": view.get("created_time"),
            "last_edited_time": view.get("last_edited_time"),
            "created_by": _string_map(view.get("created_by")),
            "last_edited_by": _string_map(view.get("last_edited_by")),
            "filter": _json_str(view.get("filter")),
            "sorts": _json_str(view.get("sorts")),
            "quick_filters": _json_str(view.get("quick_filters")),
            "configuration": _json_str(view.get("configuration")),
            "dashboard_view_id": view.get("dashboard_view_id"),
        }

    def _flatten_block(self, block: dict) -> dict:
        row = {
            "id": block.get("id"),
            "type": block.get("type"),
            "created_time": block.get("created_time"),
            "last_edited_time": block.get("last_edited_time"),
            "created_by": _string_map(block.get("created_by")),
            "last_edited_by": _string_map(block.get("last_edited_by")),
            "has_children": block.get("has_children"),
            "in_trash": _coalesce_trash(block),
            "parent": _string_map(block.get("parent")),
        }
        for block_type in _BLOCK_TYPE_FIELDS:
            row[block_type] = _string_map(block.get(block_type))
        return row

    def _flatten_comment(self, comment: dict) -> dict:
        return {
            "id": comment.get("id"),
            "parent": _string_map(comment.get("parent")),
            "discussion_id": comment.get("discussion_id"),
            "created_by": _string_map(comment.get("created_by")),
            "created_time": comment.get("created_time"),
            "last_edited_time": comment.get("last_edited_time"),
            "display_name": _string_map(comment.get("display_name")),
            "rich_text": _string_map_list(comment.get("rich_text")),
            "attachments": _string_map_list(comment.get("attachments")),
        }

    def _flatten_user(self, user: dict) -> dict:
        return {
            "id": user.get("id"),
            "name": user.get("name"),
            "avatar_url": user.get("avatar_url"),
            "type": user.get("type"),
            "person": _string_map(user.get("person")),
            "bot": _string_map(user.get("bot")),
        }

    def _flatten_file_upload(self, upload: dict) -> dict:
        content_length = upload.get("content_length")
        return {
            "id": upload.get("id"),
            "filename": upload.get("filename"),
            "content_type": upload.get("content_type"),
            "content_length": int(content_length) if content_length is not None else None,
            "status": upload.get("status"),
            "mode": upload.get("mode"),
            "expiry_time": upload.get("expiry_time"),
            "created_time": upload.get("created_time"),
            "last_edited_time": upload.get("last_edited_time"),
            "created_by": _string_map(upload.get("created_by")),
            "file_import_result": _string_map(upload.get("file_import_result")),
            "number_of_parts": _string_map(upload.get("number_of_parts")),
        }


def _coalesce_trash(obj: dict) -> Optional[bool]:
    """Read the trash flag under either name.

    ``2026-03-11`` renamed ``archived`` to ``in_trash``. Reading both keeps
    the connector working if a user pins ``notion_version`` back to
    ``2025-09-03``.
    """
    if "in_trash" in obj:
        return obj.get("in_trash")
    return obj.get("archived")


# ----------------------------------------------------------------------
# Block-type union
#
# One column per Notion block type, each holding that type's content
# object. ``meeting_notes`` is the 2026-03-11 name for what earlier
# versions called ``transcription``; both are listed so a pinned-back
# ``notion_version`` still populates a column.
# ----------------------------------------------------------------------

_BLOCK_TYPE_FIELDS = (
    "audio",
    "bookmark",
    "breadcrumb",
    "bulleted_list_item",
    "callout",
    "child_database",
    "child_page",
    "code",
    "column",
    "column_list",
    "divider",
    "embed",
    "equation",
    "file",
    "heading_1",
    "heading_2",
    "heading_3",
    "heading_4",
    "image",
    "link_preview",
    "link_to_page",
    "meeting_notes",
    "numbered_list_item",
    "paragraph",
    "pdf",
    "quote",
    "synced_block",
    "table",
    "table_of_contents",
    "table_row",
    "tab",
    "template",
    "to_do",
    "toggle",
    "transcription",
    "unsupported",
    "video",
)


def _string_map_field(name: str) -> StructField:
    return StructField(name, MapType(StringType(), StringType()))


def _string_map_list_field(name: str) -> StructField:
    return StructField(name, ArrayType(MapType(StringType(), StringType())))


_TABLE_SCHEMAS: Dict[str, StructType] = {
    "pages": StructType(
        [
            StructField("id", StringType()),
            StructField("url", StringType()),
            StructField("public_url", StringType()),
            StructField("in_trash", BooleanType()),
            StructField("is_locked", BooleanType()),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("created_by"),
            _string_map_field("last_edited_by"),
            _string_map_field("cover"),
            _string_map_field("icon"),
            _string_map_field("parent"),
            # Property values are JSON-encoded: Notion property values are
            # arbitrarily nested (a relation is an array of objects, a rollup
            # wraps an aggregate) and no flat Spark map can hold them.
            # Truncated properties are resolved before encoding, so what
            # lands here is always the *complete* value.
            StructField("properties", MapType(StringType(), StringType())),
            # Names of the properties that came back truncated on the page
            # object and were re-fetched. Empty for the common case.
            StructField("truncated_properties", ArrayType(StringType())),
            StructField("request_id", StringType()),
        ]
    ),
    "databases": StructType(
        [
            StructField("id", StringType()),
            StructField("url", StringType()),
            StructField("public_url", StringType()),
            _string_map_list_field("title"),
            _string_map_list_field("description"),
            StructField("is_inline", BooleanType()),
            StructField("is_locked", BooleanType()),
            StructField("in_trash", BooleanType()),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("created_by"),
            _string_map_field("last_edited_by"),
            _string_map_field("cover"),
            _string_map_field("icon"),
            _string_map_field("parent"),
            _string_map_list_field("data_sources"),
            StructField("data_source_ids", ArrayType(StringType())),
            StructField("request_id", StringType()),
        ]
    ),
    "data_sources": StructType(
        [
            StructField("id", StringType()),
            StructField("url", StringType()),
            StructField("name", StringType()),
            _string_map_list_field("title"),
            _string_map_list_field("description"),
            StructField("is_inline", BooleanType()),
            StructField("in_trash", BooleanType()),
            _string_map_field("database_parent"),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("created_by"),
            _string_map_field("last_edited_by"),
            _string_map_field("parent"),
            StructField("properties", MapType(StringType(), StringType())),
            StructField("request_id", StringType()),
        ]
    ),
    "views": StructType(
        [
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("type", StringType()),
            StructField("url", StringType()),
            StructField("data_source_id", StringType()),
            _string_map_field("parent"),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("created_by"),
            _string_map_field("last_edited_by"),
            StructField("filter", StringType()),
            StructField("sorts", StringType()),
            StructField("quick_filters", StringType()),
            StructField("configuration", StringType()),
            StructField("dashboard_view_id", StringType()),
        ]
    ),
    "blocks": StructType(
        [
            StructField("id", StringType()),
            StructField("type", StringType()),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("created_by"),
            _string_map_field("last_edited_by"),
            StructField("has_children", BooleanType()),
            StructField("in_trash", BooleanType()),
            _string_map_field("parent"),
        ]
        + [_string_map_field(name) for name in _BLOCK_TYPE_FIELDS]
    ),
    "comments": StructType(
        [
            StructField("id", StringType()),
            _string_map_field("parent"),
            StructField("discussion_id", StringType()),
            _string_map_field("created_by"),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("display_name"),
            _string_map_list_field("rich_text"),
            _string_map_list_field("attachments"),
        ]
    ),
    "users": StructType(
        [
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("avatar_url", StringType()),
            StructField("type", StringType()),
            _string_map_field("person"),
            _string_map_field("bot"),
        ]
    ),
    "file_uploads": StructType(
        [
            StructField("id", StringType()),
            StructField("filename", StringType()),
            StructField("content_type", StringType()),
            StructField("content_length", LongType()),
            StructField("status", StringType()),
            StructField("mode", StringType()),
            StructField("expiry_time", StringType()),
            StructField("created_time", StringType()),
            StructField("last_edited_time", StringType()),
            _string_map_field("created_by"),
            _string_map_field("file_import_result"),
            _string_map_field("number_of_parts"),
        ]
    ),
}


_TABLE_METADATA: Dict[str, dict] = {
    "pages": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "databases": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "data_sources": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "views": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "blocks": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "comments": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "users": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "file_uploads": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
}
