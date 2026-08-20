# Lakeflow Notion Community Connector

This documentation provides setup instructions and reference information for the Notion source connector.

## Prerequisites

- A Notion account with access to the pages, databases, and blocks you want to sync
- A Notion Integration (API token) with **read content** capabilities, which covers
  pages, databases, data sources, views, blocks, users, comments and file uploads
- The integration must be shared with every page and database you want to ingest.
  Notion makes unshared content *invisible* rather than forbidden, so anything
  you forget to share shows up as missing rows, not as an error.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_token` | string | Yes | Your Notion Integration token (starts with `secret_`) | `secret_xxxxxxxxxxxxxxxxxxxxx` |
| `notion_version` | string | No | `Notion-Version` header. Defaults to `2026-03-11`, which is the minimum for the Views API, `GET /v1/file_uploads`, and the `meeting_notes` block type. | `2025-09-03` |

**Note:** the table-specific options listed under [Source-specific `table_configuration` options](#source-specific-table_configuration-options) must be added to `externalOptionsAllowList` if you use them.

### How to Obtain Required Parameters

#### 1. Create a Notion Integration

1. Go to the [Notion Developer Portal](https://www.notion.so/my-integrations)
2. Click **"+ New integration"**
3. Fill in the required fields:
   - **Name**: A descriptive name for your integration
   - **Logo**: (Optional) Upload a logo
   - **Associated workspace**: Select your workspace
4. Click **Submit**
5. Copy the **Internal Integration Token** (starts with `secret_`) - you'll need this for the `api_token` parameter

#### 2. Share Pages/Data sources with Your Integration

For each page or database you want to sync:

1. Open the page/database in Notion
2. Click the **Share** button (top right)
3. Click **Invite**
4. Search for and select your integration name
5. Grant **Can read** permissions (or higher if needed)

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide your Notion Integration token in the `api_token` field.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Notion connector supports the following tables:

| Table | Ingestion Type | Primary Key | Cursor Field | Description |
|-------|---------------|-------------|--------------|-------------|
| `pages` | CDC | `id` | `last_edited_time` | Pages, with truncated properties repaired (see below) |
| `databases` | CDC | `id` | `last_edited_time` | Database **containers** — icon, cover, `is_inline`, and the data sources they own |
| `data_sources` | CDC | `id` | `last_edited_time` | The tables inside a database, with their property schema |
| `views` | CDC | `id` | `last_edited_time` | Saved views on a data source — layout, filter, sorts, quick filters |
| `blocks` | CDC | `id` | `last_edited_time` | Content blocks within pages, crawled recursively |
| `comments` | CDC | `id` | `last_edited_time` | Comments on pages and blocks |
| `users` | Snapshot | `id` | N/A | Workspace users and bots |
| `file_uploads` | CDC | `id` | `last_edited_time` | Uploaded files with `filename`, `content_type`, `content_length`, `status` |

### `databases` vs `data_sources`

API version `2025-09-03` split what used to be one object in two:

- a **database** is the container — icon, cover, title, `is_inline`, and a list of the data sources it owns;
- a **data source** is the actual table — the property schema and the rows.

One database can own several data sources. Join them on
`databases.data_source_ids` → `data_sources.id`.

### Incremental Ingestion

Notion's search API can **sort** by `last_edited_time` but cannot **filter** by
it — there is no `since`/`until` parameter anywhere. So every incremental read
walks the source in ascending order and skips client-side up to the last
watermark. Records edited after the sync started are left for the next run.

- **CDC tables**: only records newer than the checkpointed watermark are emitted.
- **Snapshot table** (`users`): the full list is retrieved each sync, because
  user objects carry no timestamps at all.

`blocks`, `comments`, `databases` and `views` are not directly enumerable — the
connector reaches them by walking out from pages and data sources. They are
therefore checkpointed on their **parent's** `last_edited_time` (Notion bumps a
page's timestamp whenever a block on it changes), while `cursor_field` stays on
the record's own timestamp for the destination merge.

### Block Types

The `blocks` table has one column per Notion block type; only the column
matching the row's `type` is populated:

`audio`, `bookmark`, `breadcrumb`, `bulleted_list_item`, `callout`,
`child_database`, `child_page`, `code`, `column`, `column_list`, `divider`,
`embed`, `equation`, `file`, `heading_1`–`heading_4`, `image`, `link_preview`,
`link_to_page`, `meeting_notes`, `numbered_list_item`, `paragraph`, `pdf`,
`quote`, `synced_block`, `table`, `table_of_contents`, `table_row`, `tab`,
`template`, `to_do`, `toggle`, `transcription`, `unsupported`, `video`

**`meeting_notes`** carries Notion AI meeting notes — title, lifecycle status,
calendar event (start/end/attendees), recording window, and pointers to the
summary / notes / transcript blocks. It is called `transcription` on API
versions before `2026-03-11`; both columns exist so a pinned-back
`notion_version` still populates one.

The crawl is recursive: blocks with `has_children` are descended into, up to
`max_block_depth` levels (default 5).

### Page property truncation (important)

Notion page objects **silently truncate** `relation`, `people`, `rich_text`,
`title` and `rollup` properties at 25 entries; rollups needing multiple
aggregation passes come back as `type: "incomplete"`. Reading
`page.properties` naively loses data on every heavily-linked page.

This connector detects truncated properties and re-fetches them from
`GET /v1/pages/{page_id}/properties/{property_id}`, **resolving them inline**
before emitting the row.

Design decision — inline rather than a separate `page_properties` overflow
table:

- One row per page; no join is needed to read a complete relation.
- An overflow table would split a *partially* truncated page across two tables
  with no clear signal about which properties live where, making every
  consumer join defensively for correctness.
- The extra API calls only fire for properties that were actually truncated.

What this means for the schema:

- **`pages.properties` is `MAP<STRING, STRING>` where each value is a JSON
  string.** Notion property values are recursive (a relation is an array of
  objects, a rollup wraps an aggregate), so no flat map type can hold them.
  Parse with `from_json` / `get_json_object` downstream.
- **`pages.truncated_properties` is `ARRAY<STRING>`** naming the properties
  that were re-fetched for that row — an audit trail for which rows took the
  slow path. Empty for the common case.

Cost controls: set `resolve_truncated_properties` to `"false"` to skip the
extra calls entirely, or lower `max_property_items` (default 1000) to bound
how much of one pathological property is pulled back. When the cap bites, the
emitted property keeps `has_more: true` so the truncation stays visible rather
than looking complete.

### `file_uploads`

Unlike most attachment APIs, Notion exposes a real list endpoint
(`GET /v1/file_uploads`), so the connector enumerates uploads directly rather
than scraping ids out of blocks and hydrating them one at a time.

Limitation: this covers files uploaded **through the API**. Files attached by
external URL, and Notion-hosted files predating the File Upload API, appear in
block and property payloads but are not in this table. To find where a file is
used, join `file_uploads.id` against the `file_upload.id` inside the
`image` / `file` / `video` / `pdf` / `audio` columns of `blocks`.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

Add any of these to `externalOptionsAllowList` if you set them.

| Option | Applies to | Default | Description |
|---|---|---|---|
| `max_records_per_batch` | all CDC tables | `200` | Rows per microbatch. Lower it if you are close to Notion's rate limit. |
| `resolve_truncated_properties` | `pages` | `true` | Set `false` to skip the property-repair API calls and accept 25-item truncation. |
| `max_property_items` | `pages` | `1000` | Ceiling on items pulled back for one truncated property. |
| `max_block_depth` | `blocks` | `5` | How many levels deep the recursive block crawl descends. |
| `file_upload_status` | `file_uploads` | *(none)* | Restrict to one status: `pending`, `uploaded`, `expired`, `failed`. |

## Data Type Mapping

| Notion Type | Databricks/Spark Type |
|-------------|----------------------|
| id / URL / ISO 8601 timestamp | `StringType` |
| `boolean` | `BooleanType` |
| `content_length` (bytes) | `LongType` |
| `rich_text[]`, `title[]`, `data_sources[]` | `ArrayType(MapType(StringType, StringType))` |
| nested object (`parent`, `icon`, `cover`, `created_by`, block content) | `MapType(StringType, StringType)` |
| page / data-source `properties` | `MapType(StringType, StringType)`, values are JSON |
| view `filter` / `sorts` / `configuration` | `StringType`, JSON |

### Special Column Notes

- **Timestamps**: All timestamp fields (`created_time`, `last_edited_time`, etc.) are ISO 8601 strings.
- **Nested map values are JSON-encoded.** A field like `cover` arrives as
  `{"type": "external", "external": {"url": "…"}}` — two levels deep, which a
  string map cannot hold. Scalars pass through unquoted (`parent["type"]`
  reads as `database_id`, not `"database_id"`); objects and arrays are
  JSON-encoded. Use `get_json_object` to reach into them.
- **`in_trash`, not `archived`.** API version `2026-03-11` renamed the field.
  The connector reads both names from the source, and always exposes
  `in_trash`.
- **Block type union**: only the column named by the row's `type` is non-null.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure the tables you want to sync:

```json
{
  "pipeline_spec": {
    "connection_name": "your-notion-connection",
    "object": [
      {
        "table": {
          "source_table": "pages",
          "table_configuration": {
            "scd_type": "SCD_TYPE_1"
          }
        }
      },
      {
        "table": {
          "source_table": "databases"
        }
      },
      {
        "table": {
          "source_table": "data_sources"
        }
      },
      {
        "table": {
          "source_table": "views"
        }
      },
      {
        "table": {
          "source_table": "blocks",
          "table_configuration": {
            "max_block_depth": "5"
          }
        }
      },
      {
        "table": {
          "source_table": "file_uploads"
        }
      },
      {
        "table": {
          "source_table": "users"
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start small**: sync `pages` alone first to confirm sharing and auth, then add tables.
- **Watch the request count on `blocks`**: it is the most expensive table by far —
  roughly one request per page, plus one per nested container. `comments` is
  one request per page. Everything else is a handful of requests per sync.
- **Set appropriate schedules**: Notion allows roughly 3 requests/second.
  Every 15–60 minutes is usually right; the connector retries `429` with
  `Retry-After` backoff, but a tight schedule on a large workspace will spend
  most of its time throttled.
- **Share everything you want ingested**, including the parent pages of any
  blocks or comments — an unshared parent means its children never appear.
- **Keep `resolve_truncated_properties` on** unless rate limits force your hand.
  Turning it off is a correctness tradeoff, not just a performance one: pages
  with more than 25 relations will be missing data.

#### Troubleshooting

**Common Issues:**

| Issue | Possible Cause | Solution |
|-------|---------------|----------|
| "401 Unauthorized" | Invalid or expired API token | Regenerate your integration token and update the connection |
| "403 Forbidden" | Integration lacks access to resource | Share the page/database with your integration |
| "404 Not Found" | Resource no longer exists or integration not shared | Verify the resource exists and integration has access |
| "429 Too Many Requests" | Rate limit exceeded | Wait and retry; the connector retries with `Retry-After` backoff. Lower `max_records_per_batch`, or set `resolve_truncated_properties: "false"` |
| Empty results from `blocks` / `comments` | Integration lacks parent page access | Share parent pages with the integration. Notion makes unshared content *invisible*, not forbidden, so this looks like "no data" rather than an error |
| Empty `databases` table | No data sources shared | Databases are only reachable through the data sources they own — share at least one database's contents |
| `views` rows have null `name` / `filter` | `notion_version` pinned before `2026-03-11` | The Views API requires `2026-03-11` |
| No `meeting_notes` rows in `blocks` | `notion_version` pinned before `2026-03-11` | Check the `transcription` column instead, or upgrade the version |
| Relation property looks capped at 25 | `resolve_truncated_properties` disabled | Re-enable it; check `truncated_properties` to see which rows were repaired |

## References

- [Notion API Documentation](https://developers.notion.com/reference/intro)
- [Notion Developer Portal](https://www.notion.so/my-integrations)
- [Lakeflow Community Connectors Documentation](https://github.com/databrickslabs/lakeflow-community-connectors)