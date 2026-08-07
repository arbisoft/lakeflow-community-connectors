import time
from datetime import datetime, timezone
from typing import Dict, Iterator, List

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

FULL_REFRESH_PAGE_SIZE = 100
BATCH_READ_PROPERTY_CHUNK_SIZE = 100
UPDATED_AT_EXCLUSIVE_OFFSET_KEY = "_updatedAt_exclusive"
PIPELINE_OBJECT_TYPES = ("deals", "tickets")
PIPELINES_TABLE_SUFFIX = "_pipelines"

# Stage-propagation history: one row per stage transition for a deal/ticket
# moving through its pipeline. HubSpot uses a different "current stage"
# property name per object type.
STAGE_HISTORY_TABLE_SUFFIX = "_stage_history"
STAGE_HISTORY_PROPERTY_BY_OBJECT_TYPE = {
    "deals": "dealstage",
    "tickets": "hs_pipeline_stage",
}
# HubSpot's batch/read endpoint caps `propertiesWithHistory` requests at 50
# inputs per call -- lower than the 100-item cap on plain property batch
# reads used elsewhere in this connector.
STAGE_HISTORY_BATCH_SIZE = 50

# Owners: HubSpot's user directory (deal/ticket/company owners). This is a
# dedicated API (crm/v3/owners), not a CRM object with properties, so it
# gets a single fixed-name snapshot table rather than one per object type.
OWNERS_TABLE_NAME = "owners"
OWNERS_PAGE_SIZE = 100


class HubspotLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict) -> None:
        self.access_token = options["access_token"]
        self.base_url = "https://api.hubapi.com"
        self.auth_header = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        # Cache for discovered schemas to avoid repeated API calls
        self._schema_cache = {}
        # Cache for table metadata
        self._metadata_cache = {}

        # Freeze the upper cursor bound at init time so read_table returns a
        # stable cursor across microbatches in a single Trigger.AvailableNow
        # trigger.  Without this, the connector would chase continuously
        # arriving updatedAt timestamps indefinitely.  HubSpot timestamps are
        # ISO 8601 with milliseconds (e.g. "2026-04-23T12:34:56.789Z").
        self._init_ts = (
            datetime.now(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z"
        )

        # Centralized object metadata configuration
        # supports_deletes: HubSpot only supports archived/deleted queries for core CRM objects
        self._object_config = {
            "contacts": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "lastmodifieddate",
                "associations": ["companies"],
                "supports_deletes": True,
            },
            "companies": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts"],
                "supports_deletes": True,
            },
            "deals": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "tickets"],
                "supports_deletes": True,
            },
            "tickets": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals"],
                "supports_deletes": True,
            },
            "calls": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": False,  # HubSpot doesn't support archived queries for calls
            },
            "emails": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "meetings": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": False,  # HubSpot doesn't support archived queries for meetings
            },
            "tasks": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "notes": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
        }

        # Default config for custom objects
        self._default_object_config = {
            "primary_keys": ["id"],
            "cursor_field": "updatedAt",
            "cursor_property_field": "hs_lastmodifieddate",
            "associations": [],
            "supports_deletes": False,
            # Custom objects don't support archived queries by default.
        }
        self._pipeline_object_types = self._discover_pipeline_object_types()
        self._stage_history_object_types = [
            object_type
            for object_type in self._pipeline_object_types
            if object_type in STAGE_HISTORY_PROPERTY_BY_OBJECT_TYPE
        ]

    def _discover_pipeline_object_types(self) -> List[str]:
        """Return CRM object types that expose the HubSpot pipelines API."""
        available_types = list(PIPELINE_OBJECT_TYPES)

        try:
            for custom_object in self._discover_custom_objects():
                try:
                    self._fetch_pipelines(custom_object)
                    available_types.append(custom_object)
                except Exception:
                    continue
        except Exception:
            pass

        # Preserve order while removing duplicates.
        return list(dict.fromkeys(available_types))

    @staticmethod
    def _is_pipelines_table(table_name: str) -> bool:
        return table_name.endswith(PIPELINES_TABLE_SUFFIX)

    @staticmethod
    def _pipeline_table_name(object_type: str) -> str:
        return f"{object_type}{PIPELINES_TABLE_SUFFIX}"

    def _get_pipeline_object_type(self, table_name: str) -> str:
        if not self._is_pipelines_table(table_name):
            raise ValueError(f"Table is not a pipelines table: {table_name}")
        return table_name[: -len(PIPELINES_TABLE_SUFFIX)]

    @staticmethod
    def _is_stage_history_table(table_name: str) -> bool:
        if not table_name.endswith(STAGE_HISTORY_TABLE_SUFFIX):
            return False
        object_type = table_name[: -len(STAGE_HISTORY_TABLE_SUFFIX)]
        return object_type in STAGE_HISTORY_PROPERTY_BY_OBJECT_TYPE

    @staticmethod
    def _stage_history_table_name(object_type: str) -> str:
        return f"{object_type}{STAGE_HISTORY_TABLE_SUFFIX}"

    def _get_stage_history_object_type(self, table_name: str) -> str:
        if not self._is_stage_history_table(table_name):
            raise ValueError(f"Table is not a stage history table: {table_name}")
        return table_name[: -len(STAGE_HISTORY_TABLE_SUFFIX)]

    @staticmethod
    def _get_updated_at_offset_state(
        start_offset: dict | None,
    ) -> tuple[str | None, bool]:
        """Return the checkpoint timestamp and whether it is exclusive."""
        if not start_offset:
            return None, False
        return (
            start_offset.get("updatedAt"),
            bool(start_offset.get(UPDATED_AT_EXCLUSIVE_OFFSET_KEY)),
        )

    @staticmethod
    def _build_updated_at_offset(
        updated_at: str | None, exclusive: bool = False
    ) -> dict:
        """Build the incremental checkpoint payload for HubSpot reads."""
        if not updated_at:
            return {}
        offset = {"updatedAt": updated_at}
        if exclusive:
            offset[UPDATED_AT_EXCLUSIVE_OFFSET_KEY] = True
        return offset

    @staticmethod
    def _chunk_list(values: List[str], chunk_size: int) -> Iterator[List[str]]:
        """Yield stable slices of ``values`` no larger than ``chunk_size``."""
        for start in range(0, len(values), chunk_size):
            yield values[start:start + chunk_size]

    def list_tables(self) -> list[str]:
        """
        List available tables including standard CRM objects and custom objects.
        """
        # Standard HubSpot CRM objects
        standard_tables = [
            "contacts",
            "companies",
            "deals",
            "tickets",
            "calls",
            "emails",
            "meetings",
            "tasks",
            "notes",
        ]
        standard_tables.extend(
            self._pipeline_table_name(object_type)
            for object_type in self._pipeline_object_types
        )
        standard_tables.extend(
            self._stage_history_table_name(object_type)
            for object_type in self._stage_history_object_types
        )
        standard_tables.append(OWNERS_TABLE_NAME)

        # Add dynamic discovery of custom objects
        try:
            custom_objects = self._discover_custom_objects()
            standard_tables.extend(custom_objects)
        except Exception as e:
            print(f"Warning: Could not discover custom objects: {e}")

        return standard_tables

    def _discover_custom_objects(self) -> List[str]:
        """
        Discover custom objects from HubSpot CRM schemas API
        """
        try:
            url = f"{self.base_url}/crm/v3/schemas"
            resp = requests.get(url, headers=self.auth_header, timeout=60)

            if resp.status_code != 200:
                return []

            data = resp.json()
            custom_objects = []

            # Extract custom object names
            for schema in data.get("results", []):
                object_type = schema.get("objectTypeId", "")
                name = schema.get("name", "")

                # Skip standard objects, only include custom ones
                if object_type not in ["0-1", "0-2", "0-3", "0-5"] and name:
                    custom_objects.append(name.lower())

            return custom_objects
        except Exception as e:
            print(f"Error discovering custom objects: {e}")
            return []

    def _get_object_config(self, table_name: str) -> Dict:
        """Get configuration for a specific object type"""
        return self._object_config.get(table_name, self._default_object_config)

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        Args:
            table_name: The name of the table to fetch the schema for.

        Returns:
            A StructType object representing the schema of the table.
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Discover schema via API
        schema = self._discover_table_schema(table_name)

        # Cache the result
        self._schema_cache[table_name] = schema

        return schema

    @staticmethod
    def _parse_max_records(table_options: Dict[str, str] | None) -> int | None:
        """Read max_records_per_batch from table options. None means no cap
        (opt-in admission control)."""
        if not table_options:
            return None
        raw = table_options.get("max_records_per_batch")
        if raw is None:
            return None
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        Args:
            table_name: The name of the table to fetch the metadata for.

        Returns:
            A dictionary containing table metadata such as primary keys,
            cursor field, and ingestion type.
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        # Check cache first
        if table_name in self._metadata_cache:
            return self._metadata_cache[table_name]

        # Get metadata from object configuration
        metadata = self._get_table_metadata(table_name)

        # Cache the result
        self._metadata_cache[table_name] = metadata

        return metadata

    def _discover_table_schema(self, table_name: str) -> StructType:
        """
        Discover table schema by calling HubSpot Properties API.

        Args:
            table_name: Name of the table/object to discover schema for

        Returns:
            StructType representing the table schema
        """
        if self._is_pipelines_table(table_name):
            return self._discover_pipeline_schema()

        if self._is_stage_history_table(table_name):
            return self._discover_stage_history_schema()

        if table_name == OWNERS_TABLE_NAME:
            return self._discover_owners_schema()

        # All CRM objects follow the same schema pattern
        return self._discover_crm_object_schema(table_name)

    def _get_table_metadata(self, table_name: str) -> dict:
        """
        Get metadata for a table based on object configuration.
        """
        if self._is_pipelines_table(table_name):
            object_type = self._get_pipeline_object_type(table_name)
            return {
                "primary_keys": ["pipelineId"],
                "object_type": object_type,
                "ingestion_type": "snapshot",
            }

        if self._is_stage_history_table(table_name):
            object_type = self._get_stage_history_object_type(table_name)
            base_config = self._get_object_config(object_type)
            return {
                "primary_keys": ["objectId", "stageId", "enteredAt"],
                "cursor_field": "enteredAt",
                "cursor_property_field": base_config["cursor_property_field"],
                "object_type": object_type,
                "ingestion_type": "cdc",
            }

        if table_name == OWNERS_TABLE_NAME:
            return {
                "primary_keys": ["ownerId"],
                "ingestion_type": "snapshot",
            }

        config = self._get_object_config(table_name)

        # Get property names and cursor property field for API calls
        properties = self._get_object_properties(table_name)
        property_names = [prop["name"] for prop in properties]

        # Use cdc_with_deletes only for tables that support archived queries
        supports_deletes = config.get("supports_deletes", False)
        ingestion_type = "cdc_with_deletes" if supports_deletes else "cdc"

        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config["cursor_field"],
            "cursor_property_field": config["cursor_property_field"],
            "property_names": property_names,
            "associations": config.get("associations", []),
            "ingestion_type": ingestion_type,
        }

    @staticmethod
    def _discover_pipeline_schema() -> StructType:
        stage_metadata_schema = StructType(
            [
                StructField("isClosed", StringType(), True),
                StructField("probability", StringType(), True),
                StructField("ticketState", StringType(), True),
            ]
        )
        stage_schema = StructType(
            [
                StructField("stageId", StringType(), True),
                StructField("label", StringType(), True),
                StructField("displayOrder", LongType(), True),
                StructField("active", BooleanType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
                StructField("metadata", stage_metadata_schema, True),
            ]
        )
        return StructType(
            [
                StructField("pipelineId", StringType(), True),
                StructField("objectType", StringType(), True),
                StructField("objectTypeId", StringType(), True),
                StructField("label", StringType(), True),
                StructField("displayOrder", LongType(), True),
                StructField("active", BooleanType(), True),
                StructField("default", BooleanType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
                StructField("stages", ArrayType(stage_schema), True),
            ]
        )

    @staticmethod
    def _discover_stage_history_schema() -> StructType:
        return StructType(
            [
                StructField("objectId", StringType(), True),
                StructField("objectType", StringType(), True),
                StructField("pipelineId", StringType(), True),
                StructField("stageId", StringType(), True),
                StructField("stageLabel", StringType(), True),
                StructField("enteredAt", StringType(), True),
                StructField("sourceType", StringType(), True),
                StructField("sourceId", StringType(), True),
                StructField("updatedByUserId", StringType(), True),
                StructField("isCurrentStage", BooleanType(), True),
            ]
        )

    @staticmethod
    def _discover_owners_schema() -> StructType:
        team_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("primary", BooleanType(), True),
            ]
        )
        return StructType(
            [
                StructField("ownerId", StringType(), True),
                StructField("email", StringType(), True),
                StructField("firstName", StringType(), True),
                StructField("lastName", StringType(), True),
                StructField("userId", StringType(), True),
                StructField("userIdIncludingInactive", StringType(), True),
                StructField("archived", BooleanType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
                StructField("teams", ArrayType(team_schema), True),
            ]
        )

    def _discover_crm_object_schema(self, table_name: str) -> StructType:
        """
        Discover CRM object schema using HubSpot Properties API.
        Works for contacts, companies, deals, tickets, and custom objects.
        """
        # Get object configuration and properties
        config = self._get_object_config(table_name)
        properties = self._get_object_properties(table_name)

        # Build base schema fields (these are always present for CRM objects)
        base_fields = [
            StructField("id", StringType(), True),
            StructField("createdAt", StringType(), True),
            StructField("updatedAt", StringType(), True),
            StructField("archived", BooleanType(), True),
        ]

        # Add association fields based on configuration
        for association in config["associations"]:
            base_fields.append(StructField(association, ArrayType(StringType()), True))

        # Build nested properties schema based on API response
        properties_fields = []

        if isinstance(properties, list):
            for prop in properties:
                prop_name = prop.get("name", "")
                prop_type = prop.get("type", "string")

                spark_type = self._map_hubspot_type_to_spark(prop_type)
                properties_fields.append(StructField(prop_name, spark_type, True))

        # Create nested properties StructType
        properties_struct = (
            StructType(properties_fields) if properties_fields else StructType([])
        )

        # Add properties as a nested field
        base_fields.append(StructField("properties", properties_struct, True))

        schema = StructType(base_fields)

        return schema

    def _get_associations_for_object(self, table_name: str) -> List[str]:
        """Get associations to include for the given object type"""
        config = self._get_object_config(table_name)
        return config["associations"]

    def _get_object_properties(self, object_type: str) -> List[Dict]:
        """
        Fetch object properties from HubSpot Properties API
        """
        url = f"{self.base_url}/properties/v2/{object_type}/properties"

        try:
            resp = requests.get(url, headers=self.auth_header, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"HubSpot Properties API error for {object_type}: "
                    f"{resp.status_code} {resp.text}"
                )

            return resp.json()
        except Exception as e:
            # Re-raise (don't return an error dict): callers iterate this as a
            # list of property dicts, so a dict here turns a real API error into
            # an unrelated TypeError far downstream.
            raise RuntimeError(
                f"Failed to get object properties for {object_type}: {e}"
            ) from e

    def _map_hubspot_type_to_spark(self, hubspot_type: str) -> DataType:
        """
        Map HubSpot property types to Spark DataTypes
        Following the requirement: strings -> StringType, integers -> LongType
        """
        type_mapping = {
            "string": StringType(),
            "enumeration": StringType(),
            "bool": BooleanType(),
            "number": LongType(),
            "date": StringType(),
            "date-time": StringType(),
            "datetime": StringType(),
            "json": StringType(),
            "phone_number": StringType(),
            "object_coordinates": StringType(),
        }

        return type_mapping.get(hubspot_type.lower(), StringType())

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read data from HubSpot API.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
            table_options: Additional options for reading

        Returns:
            Tuple of (records, new_offset)
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        # Determine if this is an incremental read
        if self._is_pipelines_table(table_name):
            return self._read_pipeline_table(table_name)

        if self._is_stage_history_table(table_name):
            return self._read_stage_history_table(
                table_name, start_offset, table_options
            )

        if table_name == OWNERS_TABLE_NAME:
            return self._read_owners_table()

        is_incremental = (
            start_offset is not None and start_offset.get("updatedAt") is not None
        )

        return self._read_data(
            table_name,
            start_offset,
            incremental=is_incremental,
            table_options=table_options,
        )

    def _read_pipeline_table(self, table_name: str) -> (Iterator[dict], dict):
        """Read snapshot pipeline metadata for a HubSpot object type."""
        object_type = self._get_pipeline_object_type(table_name)
        records = self._fetch_pipelines(object_type)
        return [self._transform_pipeline_record(record) for record in records], {}

    def _read_owners_table(self) -> (Iterator[dict], dict):
        """Read snapshot owner (user directory) metadata."""
        records = self._fetch_owners()
        return [self._transform_owner_record(record) for record in records], {}

    def _fetch_owners(self) -> List[Dict]:
        """Fetch all HubSpot owners, paginating through the owners API."""
        url = f"{self.base_url}/crm/v3/owners/"
        all_owners = []
        after = None

        while True:
            params = {"limit": str(OWNERS_PAGE_SIZE)}
            if after:
                params["after"] = after

            resp = requests.get(url, headers=self.auth_header, params=params, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"HubSpot owners API error: {resp.status_code} {resp.text}"
                )

            data = resp.json()
            all_owners.extend(data.get("results", []))
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break
            time.sleep(0.1)

        return all_owners

    def _transform_owner_record(self, record: Dict) -> Dict:
        """Normalize a HubSpot owner into a stable snapshot row."""
        teams = []
        for team in record.get("teams", []) or []:
            teams.append(
                {
                    "id": self._stringify_optional(team.get("id")),
                    "name": team.get("name"),
                    "primary": team.get("primary"),
                }
            )
        return {
            "ownerId": str(record.get("id", "")) or None,
            "email": record.get("email"),
            "firstName": record.get("firstName"),
            "lastName": record.get("lastName"),
            "userId": self._stringify_optional(record.get("userId")),
            "userIdIncludingInactive": self._stringify_optional(
                record.get("userIdIncludingInactive")
            ),
            "archived": record.get("archived"),
            "createdAt": self._normalize_pipeline_timestamp(record.get("createdAt")),
            "updatedAt": self._normalize_pipeline_timestamp(record.get("updatedAt")),
            "teams": teams,
        }

    def _read_stage_history_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Emit one row per stage transition for deals/tickets moving through a
        pipeline.

        Reuses the underlying object's own incremental cursor (updatedAt /
        hs_lastmodifieddate) to find which deals/tickets changed since the
        last checkpoint -- a stage change always bumps that cursor -- then
        hydrates each changed record's full stage history via the batch/read
        `propertiesWithHistory` API.

        Because a record's *entire* stage history is re-fetched whenever any
        of its properties change (not just the stage), the same transition
        can be re-emitted on a later run. That's fine: the primary key
        (objectId, stageId, enteredAt) makes re-emission idempotent under
        cdc upsert semantics.
        """
        object_type = self._get_stage_history_object_type(table_name)
        stage_property = STAGE_HISTORY_PROPERTY_BY_OBJECT_TYPE[object_type]

        is_incremental = (
            start_offset is not None and start_offset.get("updatedAt") is not None
        )

        # Reuse the object's own read path: gives us exactly the set of
        # deals/tickets that changed since the last checkpoint, plus a
        # correctly capped offset we can hand straight back to the framework.
        changed_records, offset = self._read_data(
            object_type,
            start_offset,
            incremental=is_incremental,
            table_options=table_options,
        )

        if not changed_records:
            return [], offset

        stage_lookup = self._build_stage_lookup(object_type)

        history_rows = []
        record_ids = [
            str(record["id"]) for record in changed_records if record.get("id")
        ]
        for id_chunk in self._chunk_list(record_ids, STAGE_HISTORY_BATCH_SIZE):
            history_by_id = self._fetch_stage_history_batch(
                object_type, id_chunk, stage_property
            )
            for object_id, transitions in history_by_id.items():
                history_rows.extend(
                    self._transform_stage_history_transitions(
                        object_id, object_type, transitions, stage_lookup
                    )
                )

        return history_rows, offset

    def _fetch_stage_history_batch(
        self, object_type: str, record_ids: List[str], stage_property: str
    ) -> Dict[str, List[Dict]]:
        """
        Batch-read the full historical values of `stage_property` for a set
        of records.

        Returns {objectId: [ {value, timestamp, sourceType, sourceId,
        updatedByUserId}, ... ]} -- HubSpot returns these newest-first.
        """
        if not record_ids:
            return {}

        url = f"{self.base_url}/crm/v3/objects/{object_type}/batch/read"
        payload = {
            "inputs": [{"id": record_id} for record_id in record_ids],
            "propertiesWithHistory": [stage_property],
        }
        resp = requests.post(url, headers=self.auth_header, json=payload, timeout=60)
        if resp.status_code != 200:
            raise Exception(
                "HubSpot stage-history batch read error for "
                f"{object_type}: {resp.status_code} {resp.text}"
            )

        history_by_id = {}
        for record in resp.json().get("results", []):
            object_id = str(record.get("id", ""))
            if not object_id:
                continue
            property_history = record.get("propertiesWithHistory", {}) or {}
            history_by_id[object_id] = property_history.get(stage_property, []) or []
        return history_by_id

    def _build_stage_lookup(self, object_type: str) -> Dict[str, tuple]:
        """Map stageId -> (pipelineId, stageLabel) for a given object type."""
        lookup = {}
        try:
            pipelines = self._fetch_pipelines(object_type)
        except Exception:
            return lookup
        for pipeline in pipelines:
            pipeline_id = str(pipeline.get("pipelineId", ""))
            for stage in pipeline.get("stages", []) or []:
                stage_id = str(stage.get("stageId", ""))
                if stage_id:
                    lookup[stage_id] = (pipeline_id, stage.get("label"))
        return lookup

    def _transform_stage_history_transitions(
        self,
        object_id: str,
        object_type: str,
        transitions: List[Dict],
        stage_lookup: Dict[str, tuple],
    ) -> List[Dict]:
        """
        HubSpot returns propertiesWithHistory entries newest-first. Emit one
        row per entry, marking the newest as the current stage.
        """
        rows = []
        for index, entry in enumerate(transitions):
            stage_id = str(entry.get("value", "")) or None
            pipeline_id, stage_label = stage_lookup.get(stage_id, (None, None))
            rows.append(
                {
                    "objectId": object_id,
                    "objectType": object_type,
                    "pipelineId": pipeline_id,
                    "stageId": stage_id,
                    "stageLabel": stage_label,
                    "enteredAt": self._normalize_pipeline_timestamp(
                        entry.get("timestamp")
                    ),
                    "sourceType": entry.get("sourceType"),
                    "sourceId": entry.get("sourceId"),
                    "updatedByUserId": self._stringify_optional(
                        entry.get("updatedByUserId")
                    ),
                    "isCurrentStage": index == 0,
                }
            )
        return rows

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read deleted (archived) records from HubSpot API.

        HubSpot uses "archived" status to represent deleted records. This method
        fetches all archived records and filters them client-side for incremental reads.

        Internally uses archivedAt for filtering, but copies it to updatedAt in the
        output records and offset for consistency with the normal read flow cursor.

        Args:
            table_name: Name of the table to read deleted records from
            start_offset: Dictionary containing cursor information for incremental reads
                         (uses 'updatedAt' key, which stores archivedAt values)
            table_options: Additional options for reading

        Returns:
            Tuple of (deleted_records, new_offset) where updatedAt contains archivedAt value
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        # Short-circuit once the cursor has caught up to the init-time cap,
        # so Trigger.AvailableNow can terminate.
        if (
            start_offset
            and start_offset.get("updatedAt", "") >= self._init_ts
        ):
            return [], start_offset

        # Get discovered properties (no associations needed for deletes)
        metadata = self.read_table_metadata(table_name, table_options)
        property_names = metadata.get("property_names", [])

        max_records = self._parse_max_records(table_options)

        all_records = []
        after = None
        # Use updatedAt from offset (which stores archivedAt values for delete flow)
        checkpoint = start_offset.get("updatedAt") if start_offset else None
        latest_archived = checkpoint

        while True:
            # Fetch archived records using the Objects API with archived=true
            records, after = self._fetch_full_refresh_batch(
                table_name, property_names, associations=[], after=after, archived=True
            )

            if not records:
                break

            # Filter client-side for incremental deletes based on archivedAt (from raw records)
            if checkpoint:
                records = [r for r in records if r.get("archivedAt", "") > checkpoint]

            # Transform records
            transformed_records = self._transform_records(records, table_name)

            # Copy archivedAt to updatedAt for consistency with normal flow cursor
            for i, transformed in enumerate(transformed_records):
                archived_at = records[i].get("archivedAt")
                if archived_at:
                    transformed["updatedAt"] = archived_at
                    if not latest_archived or archived_at > latest_archived:
                        latest_archived = archived_at

            all_records.extend(transformed_records)

            if not after:
                break

            # Stop if we've hit the per-microbatch record cap. The next
            # microbatch resumes from latest_archived via the cursor filter.
            if max_records is not None and len(all_records) >= max_records:
                break

            # Rate limiting
            time.sleep(0.1)

        # Cap the returned cursor at the init-time bound so the next call
        # eventually short-circuits.
        if latest_archived and latest_archived > self._init_ts:
            latest_archived = self._init_ts

        # Return offset with updatedAt key for consistency with normal flow
        offset = {"updatedAt": latest_archived} if latest_archived else {}
        return all_records, offset

    def _read_data(
        self, table_name: str, start_offset: dict = None, incremental: bool = False,
        table_options: Dict[str, str] = None
    ):
        """Read active (non-archived) data from HubSpot API"""

        # Short-circuit once the cursor has caught up to the init-time cap,
        # so Trigger.AvailableNow can terminate.
        if (
            incremental
            and start_offset
            and start_offset.get("updatedAt", "") >= self._init_ts
        ):
            return [], start_offset

        # Get discovered properties and object configuration
        metadata = self.read_table_metadata(table_name, table_options)
        property_names = metadata.get("property_names", [])
        cursor_property_field = metadata.get("cursor_property_field")
        associations = metadata.get("associations", [])

        # Only cap on the incremental path — full-refresh snapshots need
        # to drain everything in a single call to keep the snapshot atomic.
        max_records = self._parse_max_records(table_options) if incremental else None

        all_records = []
        after = None
        checkpoint, checkpoint_is_exclusive = self._get_updated_at_offset_state(
            start_offset
        )
        latest_updated = checkpoint
        stop_after_current_page = False
        drain_boundary_updated_at = None

        while True:
            if incremental:
                # Use search API for incremental reads
                records, after, updated_time = self._fetch_incremental_batch(
                    table_name,
                    property_names,
                    cursor_property_field,
                    checkpoint,
                    checkpoint_is_exclusive,
                    after,
                )
                if updated_time and (
                    not latest_updated or updated_time > latest_updated
                ):
                    latest_updated = updated_time
            else:
                # Use objects API for full refresh
                records, after = self._fetch_full_refresh_batch(
                    table_name, property_names, associations, after, archived=False
                )

            if not records:
                break

            if incremental:
                transformed_records = []
                for raw_record in records:
                    if (
                        max_records is not None
                        and drain_boundary_updated_at is None
                        and len(all_records) + len(transformed_records) >= max_records
                    ):
                        drain_boundary_updated_at = latest_updated

                    updated_at = raw_record.get("updatedAt")
                    if (
                        drain_boundary_updated_at is not None
                        and updated_at
                        and updated_at > drain_boundary_updated_at
                    ):
                        stop_after_current_page = True
                        break

                    transformed = self._transform_single_record(raw_record, table_name)
                    transformed_records.append(transformed)
                    if updated_at and (
                        not latest_updated or updated_at > latest_updated
                    ):
                        latest_updated = updated_at
            else:
                # Transform records
                transformed_records = self._transform_records(records, table_name)

                # Update latest timestamp
                for record in transformed_records:
                    updated_at = record.get("updatedAt")
                    if updated_at and (
                        not latest_updated or updated_at > latest_updated
                    ):
                        latest_updated = updated_at

            all_records.extend(transformed_records)

            if stop_after_current_page:
                break

            if not after:
                break

            # Stop if we've hit the per-microbatch record cap. The next
            # microbatch resumes from latest_updated via the cursor filter.
            if (
                max_records is not None
                and len(all_records) >= max_records
                and drain_boundary_updated_at is None
            ):
                break

            # Rate limiting
            time.sleep(0.1)

        # Cap the returned cursor at the init-time bound so the next call
        # eventually short-circuits.
        if incremental and latest_updated and latest_updated > self._init_ts:
            latest_updated = self._init_ts

        offset = (
            self._build_updated_at_offset(latest_updated, exclusive=incremental)
            if latest_updated
            else {}
        )
        return all_records, offset

    def _fetch_full_refresh_batch(
        self,
        table_name: str,
        property_names: List[str],
        associations: List[str],
        after: str = None,
        archived: bool = False,
    ):
        """Fetch a full-refresh page without large property query strings."""
        records, next_after = self._list_full_refresh_page(
            table_name, associations, after=after, archived=archived
        )
        if not records or archived or not property_names:
            return records, next_after

        hydrated_records = self._hydrate_record_properties(
            table_name, records, property_names
        )
        return hydrated_records, next_after

    def _list_full_refresh_page(
        self,
        table_name: str,
        associations: List[str],
        after: str = None,
        archived: bool = False,
    ):
        """List one page of objects with compact query parameters."""
        url = f"{self.base_url}/crm/v3/objects/{table_name}"
        params = {
            "limit": str(FULL_REFRESH_PAGE_SIZE),
            "archived": "true" if archived else "false",
        }
        if after:
            params["after"] = after
        if associations:
            params["associations"] = ",".join(associations)

        resp = requests.get(
            url, headers=self.auth_header, params=params, timeout=60
        )
        if resp.status_code != 200:
            raise Exception(
                f"HubSpot API error for {table_name}: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        records = data.get("results", [])
        next_after = data.get("paging", {}).get("next", {}).get("after")

        return records, next_after

    def _fetch_pipelines(self, object_type: str) -> List[Dict]:
        """Fetch pipeline metadata for a supported HubSpot object type."""
        url = f"{self.base_url}/crm-pipelines/v1/pipelines/{object_type}"
        params = {"includeInactive": "EXCLUDE_DELETED"}
        resp = requests.get(
            url, headers=self.auth_header, params=params, timeout=60
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"HubSpot pipelines API error for {object_type}: "
                f"{resp.status_code} {resp.text}"
            )
        return resp.json().get("results", [])

    def _hydrate_record_properties(
        self, table_name: str, records: List[Dict], property_names: List[str]
    ) -> List[Dict]:
        """Attach full property payloads via batch-read POST calls."""
        if not records or not property_names:
            return records

        record_ids = [str(record["id"]) for record in records if record.get("id")]
        if not record_ids:
            return records

        properties_by_id = {record_id: {} for record_id in record_ids}
        for property_chunk in self._chunk_list(
            property_names, BATCH_READ_PROPERTY_CHUNK_SIZE
        ):
            batch_results = self._batch_read_properties(
                table_name, record_ids, property_chunk
            )
            for batch_record in batch_results:
                record_id = str(batch_record.get("id", ""))
                if not record_id:
                    continue
                properties_by_id.setdefault(record_id, {}).update(
                    batch_record.get("properties", {}) or {}
                )

        hydrated_records = []
        for record in records:
            record_id = str(record.get("id", ""))
            merged = dict(record)
            merged["properties"] = properties_by_id.get(record_id, {})
            hydrated_records.append(merged)
        return hydrated_records

    def _batch_read_properties(
        self, table_name: str, record_ids: List[str], property_names: List[str]
    ) -> List[Dict]:
        """Read object properties via POST body so large schemas avoid URI limits."""
        if not record_ids or not property_names:
            return []

        url = f"{self.base_url}/crm/v3/objects/{table_name}/batch/read"
        payload = {
            "inputs": [{"id": record_id} for record_id in record_ids],
            "properties": property_names,
        }
        resp = requests.post(
            url, headers=self.auth_header, json=payload, timeout=60
        )
        if resp.status_code != 200:
            raise Exception(
                "HubSpot batch read API error for "
                f"{table_name}: {resp.status_code} {resp.text}"
            )
        return resp.json().get("results", [])

    def _fetch_incremental_batch(
        self,
        table_name: str,
        property_names: List[str],
        cursor_property_field: str,
        checkpoint: str | None,
        checkpoint_is_exclusive: bool,
        after: str = None,
    ):
        """Fetch a batch of records using incremental search API"""
        last_updated = checkpoint or "1970-01-01T00:00:00.000Z"

        # Convert to milliseconds for HubSpot
        try:
            last_updated_ms = int(
                datetime.fromisoformat(last_updated.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except Exception:
            last_updated_ms = 0

        search_body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": cursor_property_field,
                            "operator": (
                                "GT" if checkpoint_is_exclusive else "GTE"
                            ),
                            "value": str(last_updated_ms),
                        }
                    ]
                }
            ],
            "sorts": [
                {"propertyName": cursor_property_field, "direction": "ASCENDING"}
            ],
            "limit": 100,
            "properties": property_names or [],
        }

        if after:
            search_body["after"] = after

        url = f"{self.base_url}/crm/v3/objects/{table_name}/search"
        resp = requests.post(url, headers=self.auth_header, json=search_body, timeout=60)

        if resp.status_code != 200:
            raise Exception(
                f"HubSpot API error for {table_name}: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        records = data.get("results", [])
        next_after = data.get("paging", {}).get("next", {}).get("after")

        # Get latest update time from this batch
        latest_in_batch = last_updated
        for record in records:
            updated_at = record.get("updatedAt")
            if updated_at and updated_at > latest_in_batch:
                latest_in_batch = updated_at

        return records, next_after, latest_in_batch

    def _transform_records(self, records: List[Dict], table_name: str) -> List[Dict]:
        """Transform HubSpot records by flattening properties and associations"""
        return [self._transform_single_record(record, table_name) for record in records]

    def _transform_single_record(self, record: Dict, table_name: str) -> Dict:
        """Transform a single HubSpot record"""
        transformed_record = {}

        # Copy base fields
        for field in ["id", "createdAt", "updatedAt", "archived"]:
            if field in record:
                transformed_record[field] = record[field]

        # Copy properties with empty string to None conversion
        # HubSpot API returns "" for null values in many fields
        if "properties" in record:
            transformed_record["properties"] = self._sanitize_properties(record["properties"])

        # Handle associations
        transformed_record.update(self._extract_associations(record, table_name))

        return transformed_record

    def _transform_pipeline_record(self, record: Dict) -> Dict:
        """Normalize HubSpot pipeline metadata into a stable snapshot row."""
        transformed = {
            "pipelineId": str(record.get("pipelineId", "")) or None,
            "objectType": record.get("objectType"),
            "objectTypeId": record.get("objectTypeId"),
            "label": record.get("label"),
            "displayOrder": record.get("displayOrder"),
            "active": record.get("active"),
            "default": record.get("default"),
            "createdAt": self._normalize_pipeline_timestamp(record.get("createdAt")),
            "updatedAt": self._normalize_pipeline_timestamp(record.get("updatedAt")),
            "stages": [],
        }
        for stage in record.get("stages", []) or []:
            transformed["stages"].append(
                {
                    "stageId": str(stage.get("stageId", "")) or None,
                    "label": stage.get("label"),
                    "displayOrder": stage.get("displayOrder"),
                    "active": stage.get("active"),
                    "createdAt": self._normalize_pipeline_timestamp(
                        stage.get("createdAt")
                    ),
                    "updatedAt": self._normalize_pipeline_timestamp(
                        stage.get("updatedAt")
                    ),
                    "metadata": {
                        "isClosed": self._stringify_optional(
                            (stage.get("metadata") or {}).get("isClosed")
                        ),
                        "probability": self._stringify_optional(
                            (stage.get("metadata") or {}).get("probability")
                        ),
                        "ticketState": self._stringify_optional(
                            (stage.get("metadata") or {}).get("ticketState")
                        ),
                    },
                }
            )
        return transformed

    @staticmethod
    def _normalize_pipeline_timestamp(value) -> str | None:
        """Convert HubSpot millisecond or ISO timestamps into ISO-8601 UTC."""
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return (
                datetime.fromtimestamp(value / 1000, timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z"
            )
        value_str = str(value)
        if value_str.isdigit():
            return (
                datetime.fromtimestamp(int(value_str) / 1000, timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z"
            )
        return value_str

    @staticmethod
    def _stringify_optional(value) -> str | None:
        if value in (None, ""):
            return None
        return str(value)

    def _sanitize_properties(self, properties: Dict) -> Dict:
        """Convert empty strings to None in properties dict.

        HubSpot API returns empty strings "" instead of null for many fields,
        which causes issues when parsing to typed schemas (e.g., LongType).
        """
        if not properties:
            return properties
        return {k: (None if v == "" else v) for k, v in properties.items()}

    def _extract_associations(self, record: Dict, table_name: str) -> Dict:
        """Extract association IDs from record"""
        associations_data = record.get("associations", {})
        config = self._get_object_config(table_name)
        expected_associations = config["associations"]
        result = {}

        for association_type in expected_associations:
            association_list = []
            if association_type in associations_data:
                assoc_data = associations_data[association_type]
                if isinstance(assoc_data, dict) and "results" in assoc_data:
                    association_list = [
                        item.get("id", "") for item in assoc_data["results"]
                    ]
                elif isinstance(assoc_data, list):
                    association_list = [
                        str(item) if not isinstance(item, dict) else item.get("id", "")
                        for item in assoc_data
                    ]

            result[association_type] = association_list

        return result

    def test_connection(self) -> dict:
        """Test the connection to HubSpot API"""
        try:
            url = f"{self.base_url}/crm/v3/objects/contacts?limit=1"
            resp = requests.get(url, headers=self.auth_header, timeout=60)

            if resp.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {resp.status_code} {resp.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
