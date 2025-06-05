#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
import re

from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, SyncMode, Type
from airbyte_cdk.sources.streams import Stream

import dateutil.parser
import json


class AzureTableStream(Stream):
    primary_key = None
    cursor_field = None

    def __init__(self, stream_name: str, reader: object, **kwargs):
        super(Stream, self).__init__(**kwargs)
        self.stream_name = stream_name
        self.azure_table_reader = reader
        self._state: Optional[Mapping[str, Any]] = None
        self.query_filters = reader.query_filters.get(stream_name, None)

        print(f"Initializing AzureTableStream for {stream_name} with filters: {self.query_filters}")

    @property
    def name(self):
        return self.stream_name

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):

        if not self._state:
            self._state = latest_record.data.get(self.cursor_field[0])
        return {self.cursor_field[0]: self._state}

    def _update_state(self, latest_cursor):
        self._state = latest_cursor

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Create and retrieve the report.
        Decrypt and parse the report is its fully proceed, then yield the report document records.
        """
        table_client = self.azure_table_reader.get_table_client(self.stream_name)

        # Get the filter query for the current table
        filter_query = self.query_filters

        # Log the filter query being applied
        self.logger.info(f"Applying filter query for table '{self.stream_name}': {filter_query}")

        if sync_mode == SyncMode.full_refresh:
            # Log the sync mode
            self.logger.info(f"Sync mode: FULL_REFRESH for table '{self.stream_name}'")
            for row in self.azure_table_reader.read_table(table_client, filter_query=filter_query):
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(stream=self.stream_name, data=row, emitted_at=int(datetime.now().timestamp()) * 1000),
                )
        elif sync_mode == SyncMode.incremental:
            # Log the sync mode
            self.logger.info(f"Sync mode: INCREMENTAL for table '{self.stream_name}'")
            if not cursor_field or not isinstance(cursor_field, list) or not cursor_field:
                raise ValueError("cursor_field must be a non-empty list")
            
            cursor_field = cursor_field[0]
            cursor_value = stream_state.get(cursor_field)
            self.logger.info(f"Using cursor field '{cursor_field}' with value: {cursor_value}")

            # Only add cursor filter if cursor_value is set (not initial sync)
            if cursor_value:
                # Always treat as datetime for filter
                cursor_filter = f"{cursor_field} gt datetime'{cursor_value}'"
                filter_query = f"{filter_query} and {cursor_filter}" if filter_query else cursor_filter
                self.logger.info(f"Updated filter query for incremental sync: {filter_query}")
            else:
                self.logger.info(f"Initial incremental sync - no cursor filter applied, using base filter: {filter_query}")

            rows = self.azure_table_reader.read_table(table_client, filter_query=filter_query)

            first_cursor_value = None

            def to_dt(val):
                if isinstance(val, datetime):
                    return val
                if isinstance(val, str):
                    # Simple ISO8601 datetime check (you can improve this regex if needed)
                    if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", val):
                        try:
                            return dateutil.parser.parse(val)
                        except Exception as e:
                            # Log and skip invalid datetime strings
                            self.logger.warning(f"Failed to parse datetime from value '{val}': {e}")
                            return None
                    else:
                        # Not a datetime string
                        return None
                return None

            for row in rows:
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(stream=self.stream_name, data=row, emitted_at=int(datetime.now().timestamp()) * 1000),
                )
                row_cursor_value = row.get(cursor_field)

                if not first_cursor_value:
                    first_cursor_value = row_cursor_value

            self.logger.info(f"Saving state for stream '{self.stream_name}': {first_cursor_value}")
            self._update_state(first_cursor_value)
