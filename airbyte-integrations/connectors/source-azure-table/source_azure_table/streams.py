#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, SyncMode, Type
from airbyte_cdk.sources.streams import Stream

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
        return {self.cursor_field[0]: latest_record.data.get(self.cursor_field[0])}

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
            cursor_field = cursor_field[0]
            cursor_value = 0 if stream_state.get(cursor_field) is None else stream_state.get(cursor_field)
            if filter_query:
                filter_query = f"{filter_query} and {cursor_field} gt '{cursor_value}'"
            else:
                filter_query = f"{cursor_field} gt '{cursor_value}'"

            # Log the updated filter query for incremental sync
            self.logger.info(f"Updated filter query for incremental sync: {filter_query}")

            rows = self.azure_table_reader.read_table(table_client, filter_query=filter_query)

            for row in rows:
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(stream=self.stream_name, data=row, emitted_at=int(datetime.now().timestamp()) * 1000),
                )
                cursor_value = row[cursor_field]

            self._update_state(latest_cursor=cursor_value)
            self.logger.info(f"Updated state for stream '{self.stream_name}': {cursor_value}")
