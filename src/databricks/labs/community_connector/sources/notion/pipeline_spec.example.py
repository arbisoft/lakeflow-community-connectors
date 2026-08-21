from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "notion"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of tables to ingest
#     └── table
#         ├── source_table (required): The table name in the source system
#         ├── destination_catalog (optional): Target catalog (defaults to pipeline's default)
#         ├── destination_schema (optional): Target schema (defaults to pipeline's default)
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (optional)
#             ├── scd_type: "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys: List of columns to override connector's default keys
#             └── (other options): See README.md's "Source-specific table_configuration options"
#
# AVAILABLE SOURCE TABLES
#   pages          - Notion pages (rows). properties truncated at 25 items by
#                    Notion are repaired inline unless resolve_truncated_properties=false.
#   databases      - Database containers (icon, cover, title, owned data_source ids).
#                    Only reachable through data_sources you have access to.
#   data_sources   - The schema/table living under a database (properties, is_inline).
#   views          - Saved filter/sort views on a data_source. Requires
#                    notion_version >= 2026-03-11 or name/filter come back null.
#   blocks         - Page content blocks, recursively crawled up to max_block_depth
#                    levels. Includes meeting_notes on notion_version >= 2026-03-11.
#   comments       - Comment threads on pages/blocks.
#   users          - Workspace members and bots. Snapshot (full refresh) table.
#   file_uploads   - Uploaded file metadata (filename, content_type, status, expiry).
#
# Remember: Notion only exposes what's been explicitly shared with your
# integration. Share the parent pages/databases of anything you want ingested,
# including for blocks/comments/views which are only reachable through a
# shared parent.
# =============================================================================

# Please update the spec below to configure your ingestion pipeline.

pipeline_spec = {
    "connection_name": "notion_connector",
    "objects": [
        # Core content: every page, SCD Type 1 (overwrite on change)
        {
            "table": {
                "source_table": "pages",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                },
            }
        },
        # Database containers
        {
            "table": {
                "source_table": "databases",
            }
        },
        # Data source schemas (the tables living under each database)
        {
            "table": {
                "source_table": "data_sources",
            }
        },
        # Saved views (filters/sorts) per data source
        {
            "table": {
                "source_table": "views",
            }
        },
        # Full config example: page content blocks, recursive crawl depth capped.
        # destination_catalog/schema are shown here for reference -- set them
        # explicitly only if this table needs to land somewhere different
        # from the pipeline's default target; omit them (like the other
        # tables above) to just use the pipeline default.
        {
            "table": {
                "source_table": "blocks",
                "destination_catalog": "<YOUR_CATALOG>",
                "destination_schema": "<YOUR_SCHEMA>",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "max_block_depth": "5",
                },
            }
        },
        # Comment threads
        {
            "table": {
                "source_table": "comments",
            }
        },
        # Workspace members/bots - snapshot table, no cursor_field
        {
            "table": {
                "source_table": "users",
            }
        },
        # Uploaded file metadata; restrict to files that finished uploading
        {
            "table": {
                "source_table": "file_uploads",
                "table_configuration": {
                    "file_upload_status": "uploaded",
                },
            }
        },
        # ... more tables to ingest...
    ],
}


# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
