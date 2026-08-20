# Notion API Documentation

Source reference for the Notion Lakeflow community connector.

**API version: `2026-03-11`** (sent as the `Notion-Version` header; override with
the `notion_version` connector option).

That version is required for three things this connector depends on:

- the **Views API** (`/v1/views`)
- **`GET /v1/file_uploads`** (the list endpoint)
- the **`meeting_notes`** block type (renamed from `transcription`)

It also renames `archived` to `in_trash` on pages, blocks, databases and data
sources. The connector reads both names, so pinning `notion_version` back to
`2025-09-03` still works.

---

## Authorization

### Supported Methods

| Method | Used by connector | Notes |
|---|---|---|
| Internal integration token | Yes | Simplest; token is scoped to one workspace |
| OAuth 2.0 (public integration) | Compatible | The connector only needs the resulting bearer token |

### API Token Authentication

```
Authorization: Bearer secret_xxxxxxxxxxxxxxxxxxxx
Notion-Version: 2026-03-11
Content-Type: application/json
```

Tokens come from <https://www.notion.so/my-integrations>. **An integration sees
only what has been explicitly shared with it** — an unshared page is not
"forbidden", it is simply absent from search results. This is the single most
common cause of an unexpectedly empty table.

### Example API Request

```bash
curl -X POST https://api.notion.com/v1/search \
  -H "Authorization: Bearer $NOTION_TOKEN" \
  -H "Notion-Version: 2026-03-11" \
  -H "Content-Type: application/json" \
  -d '{"filter":{"property":"object","value":"page"},
       "sort":{"direction":"ascending","timestamp":"last_edited_time"}}'
```

---

## The shape of the API: a graph, not a set of collections

This is the fact that drives the entire connector design. Notion lets you
**enumerate** exactly three things:

| Endpoint | Returns |
|---|---|
| `POST /v1/search` | pages and data sources shared with the integration |
| `GET /v1/users` | workspace members and bots |
| `GET /v1/file_uploads` | files uploaded via the integration |

Everything else is reachable **only by walking outward from one of those
roots**:

```
POST /v1/search (data_source)
        │
        ├─ .parent.database_id ──► GET /v1/databases/{id}        → databases
        ├─ .id ─────────────────► GET /v1/views?data_source_id=  → views
        │                              └─► GET /v1/views/{id}      (hydrate)
        │
POST /v1/search (page)
        │
        ├─ .id ─► GET /v1/blocks/{page_id}/children ──┐          → blocks
        │              └─ recurse while has_children ─┘
        ├─ .id ─► GET /v1/comments?block_id={page_id}            → comments
        └─ .properties[*] ─► GET /v1/pages/{id}/properties/{pid}   (repair)
```

### Databases are *not* searchable

`POST /v1/search`'s `filter.value` accepts **only `"page"` and
`"data_source"`**. There is no `"database"` value, and `GET /v1/databases`
(the old list endpoint) was deprecated in `2025-09-03`.

So the only supported route to a database container is through the data
sources it owns: `data_source.parent.database_id` → `GET /v1/databases/{id}`.
The connector dedupes ids, since several data sources can share one container.

---

## Object List

| Table | Source endpoints |
|---|---|
| `pages` | `POST /v1/search` + `GET /v1/pages/{id}/properties/{pid}` |
| `databases` | `POST /v1/search` → `GET /v1/databases/{id}` |
| `data_sources` | `POST /v1/search` |
| `views` | `POST /v1/search` → `GET /v1/views` → `GET /v1/views/{id}` |
| `blocks` | `POST /v1/search` → `GET /v1/blocks/{id}/children` (recursive) |
| `comments` | `POST /v1/search` → `GET /v1/comments` |
| `users` | `GET /v1/users` |
| `file_uploads` | `GET /v1/file_uploads` |

---

## Read endpoints

### `POST /v1/search` — pages and data sources

```json
{
  "filter": {"property": "object", "value": "page"},
  "sort": {"direction": "ascending", "timestamp": "last_edited_time"},
  "page_size": 100,
  "start_cursor": "<opaque>"
}
```

| Field | Values |
|---|---|
| `filter.value` | `page`, `data_source` |
| `sort.timestamp` | `last_edited_time` |
| `sort.direction` | `ascending`, `descending` |
| `page_size` | 1–100 (default 100) |

**Critical limitation: search has no time filter.** You can *sort* by
`last_edited_time` but you cannot bound it. There is no `since`/`until`, no
`filter` on timestamps, nothing. Consequences:

1. Incremental reads must re-walk the sort from the beginning and skip
   client-side. The connector sorts **ascending** so that once it passes the
   previous watermark, every remaining record is new.
2. The connector cannot be partitioned across executors — there is no way to
   hand each one a self-contained slice of the range.

### `GET /v1/databases/{database_id}`

Container object. Post-`2025-09-03` it holds presentation and structure only —
the property schema and the rows live on the data sources it points at.

```json
{
  "object": "database", "id": "…",
  "title": [...], "description": [...],
  "icon": {...}, "cover": {...},
  "is_inline": false, "is_locked": false, "in_trash": false,
  "parent": {"type": "workspace", "workspace": true},
  "data_sources": [{"id": "…", "name": "…"}],
  "url": "…", "public_url": "…",
  "created_time": "…", "last_edited_time": "…"
}
```

### `GET /v1/data_sources/…` and search results

The data source carries `properties` (the column schema), `title`,
`description`, `is_inline`, and `parent` / `database_parent` pointing back at
its container.

### `GET /v1/views` — the Views API

**The path is `/v1/views`, not `/v1/data_sources/{id}/views`.**

| Param | Notes |
|---|---|
| `data_source_id` | one of these two is **required** |
| `database_id` | " |
| `start_cursor`, `page_size` | standard pagination |

The listing returns **partial** objects — `object`, `id`, `parent`, `type` and
nothing else. Each must be hydrated with `GET /v1/views/{view_id}` to get
`name`, `filter`, `sorts`, `quick_filters`, `configuration`, timestamps.

Full view object:

```json
{
  "object": "view", "id": "…",
  "name": "By status",
  "type": "table|board|list|calendar|timeline|gallery|form|chart|map|dashboard",
  "parent": {"type": "database_id", "database_id": "…"},
  "data_source_id": "…",
  "filter": {...}, "sorts": [...],
  "quick_filters": {...}, "configuration": {...},
  "dashboard_view_id": "…",
  "url": "…", "created_time": "…", "last_edited_time": "…",
  "created_by": {...}, "last_edited_by": {...}
}
```

Views are the closest analogue Notion has to a "pipeline" in a CRM: they are
the saved filter/sort definitions describing how a data source is segmented.

### `GET /v1/blocks/{block_id}/children`

Returns the direct children of a page or block. **Not recursive** — any child
with `has_children: true` needs its own call. The connector walks depth-first
with a configurable depth limit (`max_block_depth`, default 5) so a
pathologically nested page cannot stall a microbatch.

Block types (one column each in the `blocks` table):

```
audio, bookmark, breadcrumb, bulleted_list_item, callout, child_database,
child_page, code, column, column_list, divider, embed, equation, file,
heading_1, heading_2, heading_3, heading_4, image, link_preview,
link_to_page, meeting_notes, numbered_list_item, paragraph, pdf, quote,
synced_block, table, table_of_contents, table_row, tab, template, to_do,
toggle, transcription, unsupported, video
```

**`meeting_notes`** (new in `2026-03-11`, previously `transcription`) carries
AI meeting-note metadata:

```json
{
  "title": [...rich text...],
  "status": "transcription_not_started|transcription_in_progress|notes_ready",
  "children": {"summary_block_id": "…", "notes_block_id": "…",
               "transcript_block_id": "…"},
  "calendar_event": {"start_time": "…", "end_time": "…", "attendees": [...]},
  "recording": {"start_time": "…", "end_time": "…"}
}
```

### `GET /v1/comments`

Requires `block_id` (a page id works — a page *is* a block). Returns the
comment threads on that block. There is no workspace-wide comment listing.

### `GET /v1/users`

Flat list of members and bots. No timestamps at all, hence snapshot ingestion.

### `GET /v1/file_uploads`

| Param | Notes |
|---|---|
| `status` | `pending`, `uploaded`, `expired`, `failed` |
| `start_cursor`, `page_size` | 1–100 |

**This endpoint really does enumerate.** Unlike most attachment APIs (and
unlike HubSpot's engagement attachments, which have no batch endpoint), you do
not have to scrape ids out of blocks and resolve them one at a time. One list
walk gets every upload the integration owns, with `filename`, `content_type`,
`content_length`, `status`, `expiry_time` and timestamps already hydrated.

Caveat: the endpoint covers files *uploaded through the API*. Files attached
by external URL, and Notion-hosted files predating the File Upload API, appear
in block/property payloads but not in this list.

No documented sort order, so the connector buffers and sorts by
`last_edited_time` itself before applying its watermark.

### `GET /v1/pages/{page_id}/properties/{property_id}`

The repair endpoint for truncated page properties — see below.

---

## Page property truncation

**This is the most important correctness hazard in the Notion API.**

A page object does not always contain its complete property values. Five
property types are paginated:

`title`, `rich_text`, `relation`, `people`, `rollup`

For these, the page object returns **at most 25 entries**, and rollups that
need more than one aggregation pass return `{"type": "incomplete"}` instead of
a value. A consumer that reads `page.properties` directly and stops there
silently loses data on every heavily-linked page.

### Detecting truncation

Notion signals it three different ways, and sometimes not at all:

| Signal | Applies to |
|---|---|
| `has_more: true` on the property | relation, people, rich_text |
| `rollup.type == "incomplete"` | rollup |
| *(nothing)* — array is just capped at 25 | any paginated type |

Because of the third case the connector also treats "exactly 25 items" as
suspect and re-resolves. A false positive costs one extra API call; a false
negative loses data.

### Repairing it

`GET /v1/pages/{page_id}/properties/{property_id}` returns a paginated list of
`property_item` objects:

```json
{
  "object": "list",
  "type": "property_item",
  "results": [{"object": "property_item", "type": "relation",
               "relation": {"id": "…"}}, ...],
  "property_item": {"id": "…", "type": "relation", "next_url": null},
  "next_cursor": "…",
  "has_more": true
}
```

For rollups the aggregate arrives on a trailing `property_item` of type
`rollup`; the other items are the rolled-up values.

### Design decision: resolve inline

The connector **resolves truncated properties in place** before emitting the
page row, rather than emitting an overflow table.

Why inline:

- One row per page. No join is needed to read a relation, which is what
  makes the difference between "the data is there" and "the data is usable".
- The alternative — a `page_properties` child table — would force every
  consumer to join for correctness, and would make a *partially* truncated
  page (2 of 40 properties overflowing) split across two tables with no
  obvious signal about which properties to look for where.
- Resolution is opportunistic: the API calls only fire for properties that
  actually came back truncated, which is a small minority of pages.

The costs, accepted knowingly:

- **Extra API calls.** One paginated walk per truncated property per page.
  Disable with `resolve_truncated_properties: "false"` if you do not need
  complete relations and are hitting rate limits.
- **Unbounded row width.** A relation with 50,000 entries would produce a
  huge cell. Bounded by `max_property_items` (default 1000); when the cap
  bites, the emitted property keeps `has_more: true` so the truncation stays
  visible rather than looking complete.

Two schema consequences:

1. `pages.properties` is `MAP<STRING, STRING>` where each **value is JSON**.
   Property values are recursive (a relation is an array of objects, a rollup
   wraps an aggregate); no flat Spark map can hold them, and the previous
   `MAP<STRING, MAP<STRING, STRING>>` typing silently mangled them.
2. `pages.truncated_properties` is `ARRAY<STRING>` listing the property names
   that were re-fetched — an audit trail for which rows took the slow path.

---

## Incremental sync strategy

Because search has no time filter, every CDC table works the same way:

1. Walk the source in **ascending** watermark order.
2. Skip records at or below the offset's `cursor`.
3. Stop at the first record newer than `self._init_ts` (the connector's
   construction time). This cap is what terminates `Trigger.AvailableNow`:
   without it a busy workspace would keep producing new records forever.
4. Stop once `max_records_per_batch` rows are collected.
5. Return the last watermark consumed. Returning the *same* offset that came
   in is the "no more data" signal.

### Watermark vs. cursor field for derived tables

`blocks` and `comments` are checkpointed on the **parent page's**
`last_edited_time`, not the block's or comment's own.

This is deliberate. Notion bumps a page's `last_edited_time` whenever any
block on it changes, so the page timestamp is a complete and monotonically
ordered gate for the crawl. Individual block timestamps are *not* ordered with
respect to each other across pages — using them as the offset would make the
watermark jump backwards and re-read or skip arbitrarily.

The table metadata still declares `cursor_field: last_edited_time` (the
block's own), because that is the correct key for the downstream CDC merge.
The offset and the merge cursor are answering different questions.

`databases` and `views` are similarly checkpointed on the owning **data
source's** `last_edited_time`.

---

## Field type mapping

| Notion type | Spark type |
|---|---|
| id / url / ISO 8601 timestamp | `StringType` |
| boolean | `BooleanType` |
| `content_length` (bytes) | `LongType` |
| nested object (`parent`, `icon`, `cover`, `created_by`, block content) | `MapType(StringType, StringType)` |
| array of objects (`title`, `rich_text`, `data_sources`) | `ArrayType(MapType(StringType, StringType))` |
| page/data-source `properties` | `MapType(StringType, StringType)`, values JSON |
| view `filter` / `sorts` / `configuration` | `StringType`, JSON |

### Special field behaviours

- **Nested map values are JSON-encoded.** `cover` arrives as
  `{"type": "external", "external": {"url": "…"}}` — a two-level structure a
  string map cannot hold. Scalars pass through unquoted (`parent["type"]`
  reads as `database_id`, not `"database_id"`), objects and arrays are
  JSON-encoded.
- **`archived` / `in_trash`.** Read under both names for version portability;
  exposed as `in_trash`.
- **Block type union.** Every block type gets its own column; only the one
  matching the row's `type` is non-null.

---

## Rate limits

- ~3 requests/second average, with bursts tolerated.
- `429` responses carry `Retry-After` (seconds).
- The connector retries `429`, `500`, `502`, `503`, `504` up to 5 times,
  honouring `Retry-After` and otherwise backing off exponentially.
- `403`/`404` are treated as "not shared with this integration" and skipped
  rather than failing the read — one unshared page should not abort a sync.

The graph walk multiplies request counts: `blocks` costs roughly one request
per page plus one per nested container. Tune `max_records_per_batch` and
`max_block_depth` if you approach the limit.

---

## Known quirks

1. **Search cannot filter by time.** Sort only. Every incremental read
   re-walks and skips client-side.
2. **Search cannot return databases.** Only `page` and `data_source`.
3. **`GET /v1/views` returns partial objects.** Hydration is mandatory.
4. **Page properties truncate silently at 25 items.** See above.
5. **Users have no timestamps**, so `users` can only be a snapshot.
6. **Comments need a parent block id**; there is no global comment list.
7. **Block children are one level deep** per call.
8. **Unshared content is invisible, not forbidden.** An empty table usually
   means a sharing problem, not an auth problem.

---

## Sources and references

- Notion API reference — <https://developers.notion.com/reference/intro>
- Upgrade guide, `2025-09-03` (database / data source split) —
  <https://developers.notion.com/docs/upgrade-guide-2025-09-03>
- Upgrade guide, `2026-03-11` (`in_trash`, `meeting_notes`) —
  <https://developers.notion.com/docs/upgrade-guide-2026-03-11>
- Working with views — <https://developers.notion.com/guides/data-apis/working-with-views>
- List views — <https://developers.notion.com/reference/list-views>
- Retrieve a view — <https://developers.notion.com/reference/retrieve-a-view>
- List file uploads — <https://developers.notion.com/reference/list-file-uploads>
- File upload object — <https://developers.notion.com/reference/file-upload>
- Retrieve a page property item — <https://developers.notion.com/reference/retrieve-a-page-property>
- Search by title — <https://developers.notion.com/reference/post-search>
- Block object — <https://developers.notion.com/reference/block>
