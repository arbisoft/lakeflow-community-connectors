# Notion Connector

The Notion connector enables incremental data ingestion from Notion into Databricks using the Lakeflow Connect framework.

## Overview

This connector supports the following Notion objects:

| Table | Description | Ingestion Type | Cursor Field |
|-------|-------------|----------------|--------------|
| `pages` | Page content and metadata | CDC | `last_edited_time` |
| `databases` | Database definitions and metadata | CDC | `last_edited_time` |
| `blocks` | Block content within pages | CDC | `last_edited_time` |
| `users` | Workspace users | Snapshot | N/A |
| `comments` | Page/block comments | CDC | `created_time` |

## Prerequisites

1. **Notion Account** with admin or member access
2. **Notion Integration** created at https://www.notion.so/my-integrations
3. **Internal Integration Token** (API token)
4. **Pages/Databases shared** with your integration

## Setup

### 1. Create a Notion Integration

1. Go to https://www.notion.so/my-integrations
2. Click **"+ New integration"**
3. Give it a name (e.g., "Lakeflow Connector")
4. Select your workspace
5. Click **Submit**
6. Copy the **Internal Integration Token** (starts with `secret_`)

### 2. Share Pages with Your Integration

1. Open the page or database you want to sync
2. Click **Share** in the top-right
3. Click **Invite**
4. Search for and select your integration
5. Click **Invite**

### 3. Create the Unity Catalog Connection

```sql
CREATE CONNECTION notion_conn
WITH
  type = 'lakeflow_connect',
  provider = 'notion',
  api_token = 'secret_your_token_here';
```

## Table Schemas

### pages

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Unique page identifier |
| `url` | STRING | Full URL to the page |
| `archived` | BOOLEAN | Whether the page is archived |
| `created_time` | STRING | ISO 8601 creation timestamp |
| `last_edited_time` | STRING | ISO 8601 last edit timestamp |
| `created_by` | MAP | User who created the page |
| `last_edited_by` | MAP | User who last edited the page |
| `parent` | MAP | Parent reference (workspace/page/database) |
| `properties` | MAP | Page properties (dynamic schema) |

### databases

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Unique database identifier |
| `url` | STRING | Full URL to the database |
| `title` | ARRAY<MAP> | Database title as rich text |
| `description` | ARRAY<MAP> | Database description |
| `is_inline` | BOOLEAN | Whether database is inline |
| `created_time` | STRING | ISO 8601 creation timestamp |
| `last_edited_time` | STRING | ISO 8601 last edit timestamp |
| `properties` | MAP | Database property definitions |

### users

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Unique user identifier |
| `name` | STRING | Display name |
| `avatar_url` | STRING | URL to avatar image |
| `type` | STRING | `person` or `bot` |
| `person` | MAP | Person details (email, etc.) |
| `bot` | MAP | Bot details (owner, workspace) |

### comments

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Unique comment identifier |
| `parent` | MAP | Parent page or block reference |
| `discussion_id` | STRING | Discussion thread ID |
| `created_by` | MAP | User who created the comment |
| `created_time` | STRING | ISO 8601 creation timestamp |
| `rich_text` | ARRAY<MAP> | Comment content |

## Table Options

| Option | Default | Description |
|--------|---------|-------------|
| `max_records_per_batch` | 100 | Maximum records per microbatch |
| `lookback_seconds` | 5 | Seconds to look back for concurrent updates |

## Incremental Sync

The connector supports incremental sync using Notion's `last_edited_time` and `created_time` fields:

1. **Cursor-based pagination**: Uses `start_cursor`/`next_cursor` for pagination
2. **Time-based filtering**: Filters records by `last_edited_time >= cursor`
3. **Lookback window**: Applies a 5-second lookback to catch concurrent updates
4. **Termination**: Stops when cursor reaches init time (no new data)

## Rate Limits

- **Limit**: ~3 requests per second per integration
- **Handling**: Automatic retry with exponential backoff
- **429 Response**: Waits for `retry-after` header value

## Known Limitations

1. **Dynamic Properties**: Page/database properties have dynamic schemas based on templates
2. **Block Hierarchy**: Blocks can have nested children (limited to 30 levels)
3. **Permissions**: Only pages/databases shared with the integration are accessible
4. **Users Stream**: Requires "Read user information" permission in integration settings

## References

- [Notion API Documentation](https://developers.notion.com/)
- [Airbyte Notion Connector](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-notion)