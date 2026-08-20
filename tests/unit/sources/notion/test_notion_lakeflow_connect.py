"""Unit tests for the Notion Lakeflow Connect connector.

``LakeflowConnectTests`` supplies the generic contract suite (schemas,
metadata, offsets, termination, column population). The tests added here
cover the Notion-specific behaviour that the generic suite cannot know
about: the graph walk out from ``/v1/search``, view hydration, and page
property truncation.

Simulate mode only — no credentials required.
"""

import json

import pytest

from databricks.labs.community_connector.sources.notion.notion import (
    NotionLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

# Fixture ids, mirrored from the simulator corpus.
PAGE_ONE = "aaaaaaaa-0001-4000-8000-000000000001"
RELATION_PROPERTY = "Related tasks"
ROLLUP_PROPERTY = "Total estimate"


class TestNotionConnector(LakeflowConnectTests):
    """Test suite for the Notion connector."""

    connector_class = NotionLakeflowConnect
    simulator_source = "notion"
    replay_config = {
        "api_token": "simulator-fake-token",
    }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _read_all(self, table, table_options=None):
        """Drain a table across microbatches until the offset converges."""
        offset = {}
        records = []
        for _ in range(self.read_termination_max_iterations):
            batch, new_offset = self.connector.read_table(
                table, offset, table_options or {}
            )
            records.extend(batch)
            if new_offset == offset:
                break
            offset = new_offset
        return records

    # ------------------------------------------------------------------
    # table surface
    # ------------------------------------------------------------------

    def test_expected_tables_present(self):
        """Every table the connector advertises is one we intend to ship."""
        assert set(self.connector.list_tables()) == {
            "pages",
            "databases",
            "data_sources",
            "views",
            "blocks",
            "comments",
            "users",
            "file_uploads",
        }

    # ------------------------------------------------------------------
    # databases — reached via data_source.parent, not search
    # ------------------------------------------------------------------

    def test_databases_hydrated_from_data_source_parents(self):
        """`databases` is populated even though search cannot return them.

        Notion's search filter accepts only ``page`` and ``data_source``, so
        the connector has to walk ``data_source.parent.database_id`` and
        hydrate each container. A regression here shows up as an empty table.
        """
        records = self._read_all("databases")
        assert records, "no database rows produced"

        ids = {r["id"] for r in records}
        assert len(ids) == len(records), "database rows are not deduplicated"

        first = records[0]
        # Container-only fields — the whole reason this table exists
        # separately from data_sources.
        assert first["is_inline"] is not None
        assert first["icon"], "icon not carried through"
        assert first["data_source_ids"], "database is not linked to its data sources"

    def test_database_data_source_ids_match_data_sources_table(self):
        """The container's data_source_ids join to real data_sources rows."""
        databases = self._read_all("databases")
        data_sources = self._read_all("data_sources")

        known = {r["id"] for r in data_sources}
        referenced = {ds_id for r in databases for ds_id in r["data_source_ids"]}
        assert referenced, "no data source references found on any database"
        assert referenced <= known, (
            f"databases reference unknown data sources: {referenced - known}"
        )

    # ------------------------------------------------------------------
    # views
    # ------------------------------------------------------------------

    def test_views_are_hydrated_not_partial(self):
        """`GET /v1/views` returns partial refs; the connector must hydrate.

        If hydration is skipped, ``name``/``filter``/``sorts`` stay null and
        the table is useless for understanding how a data source is
        segmented.
        """
        records = self._read_all("views")
        assert records, "no view rows produced"

        for record in records:
            assert record["name"], f"view {record['id']} was not hydrated (no name)"
            assert record["type"], f"view {record['id']} has no layout type"
            assert record["data_source_id"], "view is not linked to a data source"

        # filter / sorts are JSON-encoded because their shapes are recursive.
        with_filter = [r for r in records if r["filter"]]
        assert with_filter, "no view carried a saved filter"
        assert isinstance(json.loads(with_filter[0]["filter"]), dict)
        assert isinstance(json.loads(with_filter[0]["sorts"]), list)

    # ------------------------------------------------------------------
    # blocks
    # ------------------------------------------------------------------

    def test_blocks_crawl_is_recursive(self):
        """Nested blocks are reached, not just a page's direct children."""
        records = self._read_all("blocks")
        assert records, "no block rows produced"

        nested = [
            r for r in records
            if (r.get("parent") or {}).get("type") == "block_id"
        ]
        assert nested, (
            "no block with a block_id parent — the crawl stopped at "
            "the page's direct children instead of descending"
        )

    def test_meeting_notes_block_type_is_captured(self):
        """The 2026-03-11 meeting_notes block type lands in its own column."""
        records = self._read_all("blocks")
        meeting_notes = [r for r in records if r["type"] == "meeting_notes"]
        assert meeting_notes, "no meeting_notes block ingested"

        content = meeting_notes[0]["meeting_notes"]
        assert content, "meeting_notes column is empty for a meeting_notes block"
        assert "status" in content
        assert "calendar_event" in content

    def test_block_type_column_matches_type_field(self):
        """For each block, the column named by `type` is the populated one.

        Some types (`divider`, `column_list`, …) carry a legitimately empty
        content object, so the invariant is presence, not truthiness: the
        column must not be null for a block of that type.
        """
        records = self._read_all("blocks")
        for record in records:
            block_type = record["type"]
            assert record.get(block_type) is not None, (
                f"block {record['id']} has type={block_type!r} but that "
                "column is null"
            )

        # Types that do carry content must actually carry it.
        by_type = {r["type"]: r for r in records}
        for block_type in ("paragraph", "code", "to_do", "image", "table_row"):
            assert by_type[block_type][block_type], (
                f"{block_type} block has an empty content object"
            )

    # ------------------------------------------------------------------
    # comments
    # ------------------------------------------------------------------

    def test_comments_are_linked_to_their_page(self):
        records = self._read_all("comments")
        assert records, "no comment rows produced"
        for record in records:
            assert record["discussion_id"]
            assert (record.get("parent") or {}).get("type") == "page_id"
            assert record["rich_text"], "comment body was dropped"

    # ------------------------------------------------------------------
    # file_uploads
    # ------------------------------------------------------------------

    def test_file_uploads_are_hydrated(self):
        """File metadata is resolved, not left as a bare id reference."""
        records = self._read_all("file_uploads")
        assert records, "no file_upload rows produced"
        for record in records:
            assert record["filename"], "filename not hydrated"
            assert record["content_type"], "content_type not hydrated"
            assert record["status"], "status not hydrated"
            assert isinstance(record["content_length"], int)

    def test_file_uploads_sorted_despite_unordered_api(self):
        """The connector sorts before watermarking.

        ``GET /v1/file_uploads`` documents no order, and the simulator
        deliberately serves the corpus reversed. If the connector trusted
        the API order its watermark would skip rows.
        """
        records = self._read_all("file_uploads")
        stamps = [r["last_edited_time"] for r in records]
        assert stamps == sorted(stamps), (
            f"file_uploads emitted out of watermark order: {stamps}"
        )
        assert len(records) == 3, "rows were skipped by a bad watermark"

    def test_file_upload_status_filter_option(self):
        records = self._read_all("file_uploads", {"file_upload_status": "uploaded"})
        assert records
        assert {r["status"] for r in records} == {"uploaded"}

    # ------------------------------------------------------------------
    # page property truncation
    # ------------------------------------------------------------------

    def test_truncated_relation_property_is_resolved(self):
        """A >25-item relation is completed from the property-item endpoint.

        The page object caps the relation at 25 entries with
        ``has_more: true``. Without resolution the extra 7 are silently
        lost — the exact bug this fix exists for.
        """
        pages = {r["id"]: r for r in self._read_all("pages")}
        page = pages[PAGE_ONE]

        assert RELATION_PROPERTY in page["truncated_properties"], (
            "the truncated relation was not flagged as resolved"
        )

        resolved = json.loads(page["properties"][RELATION_PROPERTY])
        assert len(resolved["relation"]) == 32, (
            f"expected the full 32-item relation, got "
            f"{len(resolved['relation'])} — page-object truncation was not "
            "repaired"
        )
        assert resolved["has_more"] is False

    def test_incomplete_rollup_property_is_resolved(self):
        """A rollup returned as `type: incomplete` gets its aggregate filled in."""
        pages = {r["id"]: r for r in self._read_all("pages")}
        page = pages[PAGE_ONE]

        assert ROLLUP_PROPERTY in page["truncated_properties"]
        resolved = json.loads(page["properties"][ROLLUP_PROPERTY])
        assert resolved["rollup"]["type"] != "incomplete", (
            "rollup left in its incomplete state"
        )
        assert resolved["rollup"]["number"] == 132
        assert len(resolved["results"]) == 12

    def test_untruncated_properties_are_left_alone(self):
        """Only paginated properties are re-fetched — no needless API calls."""
        pages = {r["id"]: r for r in self._read_all("pages")}
        page = pages[PAGE_ONE]

        assert set(page["truncated_properties"]) == {
            RELATION_PROPERTY,
            ROLLUP_PROPERTY,
        }
        # A plain select survives untouched and stays queryable as JSON.
        status = json.loads(page["properties"]["Status"])
        assert status["select"]["name"] == "In progress"

    def test_property_resolution_can_be_disabled(self):
        """`resolve_truncated_properties=false` skips the extra API calls."""
        pages = {
            r["id"]: r
            for r in self._read_all("pages", {"resolve_truncated_properties": "false"})
        }
        page = pages[PAGE_ONE]

        assert page["truncated_properties"] == []
        resolved = json.loads(page["properties"][RELATION_PROPERTY])
        assert len(resolved["relation"]) == 25, (
            "properties were resolved despite resolution being disabled"
        )

    def test_property_resolution_respects_item_cap(self):
        """`max_property_items` bounds one pathological property."""
        pages = {
            r["id"]: r for r in self._read_all("pages", {"max_property_items": "10"})
        }
        page = pages[PAGE_ONE]

        resolved = json.loads(page["properties"][RELATION_PROPERTY])
        assert len(resolved["relation"]) == 10
        assert resolved["has_more"] is True, (
            "capped property must report has_more so truncation stays visible"
        )

    # ------------------------------------------------------------------
    # incremental behaviour
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "table", ["pages", "databases", "data_sources", "views", "blocks", "comments"]
    )
    def test_second_read_is_empty(self, table):
        """Re-reading from a converged offset returns nothing.

        The search corpus carries future-dated records; a connector that
        did not cap at init time would keep returning them.
        """
        batch, offset = self.connector.read_table(table, {}, {})
        first = list(batch)
        assert first, f"[{table}] first read returned nothing"

        batch2, offset2 = self.connector.read_table(table, offset, {})
        assert list(batch2) == []
        assert offset2 == offset

    def test_future_records_are_not_ingested(self):
        """Records edited after connector init are left for the next trigger."""
        records = self._read_all("pages")
        init_ts = self.connector._init_ts  # pylint: disable=protected-access
        late = [r for r in records if r["last_edited_time"] > init_ts]
        assert not late, (
            f"{len(late)} record(s) newer than init time leaked into the "
            "batch; the offset cap is not holding"
        )
