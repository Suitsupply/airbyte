#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    SyncMode,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.utils.schema_helpers import split_config
from airbyte_cdk.utils.event_timing import create_timer

from .azure_table import AzureTableReader
from .streams import AzureTableStream


class SourceAzureTable(AbstractSource):
    """This source helps to sync data from one azure data table a time"""

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        pass

    def _as_airbyte_record(self, stream_name: str, data: Mapping[str, Any]):
        return data

    @property
    def get_typed_schema(self) -> object:
        """Static schema for tables"""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": {"PartitionKey": {"type": "string"}},
        }

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            reader = AzureTableReader(logger, config)
            client = reader.get_table_service_client()
            tables_iterator = client.list_tables(results_per_page=1)
            next(tables_iterator)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except StopIteration:
            logger.info("The credentials you provided are valid, but no tables were found in the Storage Account.")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        reader = AzureTableReader(logger, config)
        tables = reader.get_tables()

        streams = []
        for table in tables:
            stream_name = table.name
            # Use dynamic schema discovery instead of static schema
            schema = self.get_table_schema(table.name, logger, config)
            stream = AirbyteStream(
                name=stream_name,
                json_schema=schema,
                supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
                source_defined_cursor=False,
            )
            streams.append(stream)
        logger.info(f"Total {len(streams)} streams found.")
        return AirbyteCatalog(streams=streams)

    def streams(self, logger: logging.Logger, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: The user-provided configuration as specified by the source's spec.
        Any stream construction related operation should happen here.
        :return: A list of the streams in this source connector.
        """

        try:
            reader = AzureTableReader(logger, config)
            tables = reader.get_tables()

            streams = []
            for table in tables:
                stream_name = table.name
                stream = AzureTableStream(stream_name=stream_name, reader=reader)
                streams.append(stream)
            return streams
        except Exception as e:
            raise Exception(f"An exception occurred: {str(e)}")

    def read(
        self, logger: logging.Logger, config: Mapping[str, Any], catalog: ConfiguredAirbyteCatalog, state: MutableMapping[str, Any] = None
    ) -> Iterator[AirbyteMessage]:
        """
        This method is overridden to check whether the stream `quotes` exists in the source, if not skip reading that stream.
        """
        stream_instances = {s.name: s for s in self.streams(logger=logger, config=config)}
        state_manager = ConnectorStateManager(stream_instance_map=stream_instances, state=state)
        logger.info(f"Starting syncing {self.name}")
        config, internal_config = split_config(config)
        self._stream_to_instance_map = stream_instances
        with create_timer(self.name) as timer:
            for configured_stream in catalog.streams:
                stream_instance = stream_instances.get(configured_stream.stream.name)
                stream_instance.cursor_field = configured_stream.cursor_field
                if not stream_instance and configured_stream.stream.name == "quotes":
                    logger.warning("Stream `quotes` does not exist in the source. Skip reading `quotes` stream.")
                    continue
                if not stream_instance:
                    raise KeyError(
                        f"The requested stream {configured_stream.stream.name} was not found in the source. Available streams: {stream_instances.keys()}"
                    )

                try:
                    yield from self._read_stream(
                        logger=logger,
                        stream_instance=stream_instance,
                        configured_stream=configured_stream,
                        state_manager=state_manager,
                        internal_config=internal_config,
                    )
                except Exception as e:
                    logger.exception(f"Encountered an exception while reading stream {self.name}")
                    raise e
                finally:
                    logger.info(f"Finished syncing {self.name}")
                    logger.info(timer.report())

        logger.info(f"Finished syncing {self.name}")

    def get_table_schema(self, table_name: str, logger: logging.Logger, config: Mapping[str, Any]) -> object:
        """Dynamically discover schema for a specific table by sampling entities"""
        reader = AzureTableReader(logger, config)
        client = reader.get_table_service_client()
        table_client = client.get_table_client(table_name)

        # Use a filter if you want, or "" for all entities
        query_filter = ""  # or your filter string
        entities = []
        pages = table_client.query_entities(query_filter=query_filter, results_per_page=10).by_page()
        try:
            first_page = next(pages)
            for i, entity in enumerate(first_page):
                entities.append(entity)
                if i >= 9:
                    break
        except StopIteration:
            pass

        properties = {
            "PartitionKey": {"type": "string"},
            "RowKey": {"type": "string"},
        }

        # Add all properties found in the sampled entities
        for entity in entities:
            for key, value in entity.items():
                if key not in properties and key not in ("PartitionKey", "RowKey"):
                    # Determine property type based on Python type
                    if isinstance(value, str):
                        # Check if it's a datetime string
                        if key.lower().endswith(('date', 'time')) or 'timestamp' in key.lower():
                            properties[key] = {
                                "type": "string", 
                                "format": "date-time",
                                "airbyte_type": "timestamp_without_timezone"
                            }
                        else:
                            properties[key] = {"type": "string"}
                    elif isinstance(value, bool):
                        properties[key] = {"type": "boolean"}
                    elif isinstance(value, (int, float)):
                        properties[key] = {"type": ["number", "integer"]}
                    elif isinstance(value, dict):
                        properties[key] = {"type": "object"}
                    elif isinstance(value, list):
                        properties[key] = {"type": "array"}
                    else:
                        # Default to string for unknown types
                        properties[key] = {"type": "string"}

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }
