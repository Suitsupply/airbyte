#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys
import traceback
from datetime import datetime

from airbyte_cdk import AirbyteEntrypoint, AirbyteMessage, Type, launch
from airbyte_cdk.models import AirbyteErrorTraceMessage, AirbyteTraceMessage, TraceType
from airbyte_cdk.sources.file_based.stream.cursor import DefaultFileBasedCursor
from source_azure_blob_storage import SourceAzureBlobStorage, SourceAzureBlobStorageSpec, SourceAzureBlobStorageStreamReader
from source_azure_blob_storage.config_migrations import MigrateCredentials, MigrateLegacyConfig


def run():
    args = sys.argv[1:]
    catalog_path = AirbyteEntrypoint.extract_catalog(args)
    config_path = AirbyteEntrypoint.extract_config(args)
    state_path = AirbyteEntrypoint.extract_state(args)
    try:
        source = SourceAzureBlobStorage(
            SourceAzureBlobStorageStreamReader(),
            SourceAzureBlobStorageSpec,
            SourceAzureBlobStorage.read_catalog(catalog_path) if catalog_path else None,
            SourceAzureBlobStorage.read_config(config_path) if catalog_path else None,
            SourceAzureBlobStorage.read_state(state_path) if catalog_path else None,
            cursor_cls=DefaultFileBasedCursor,
        )
        MigrateLegacyConfig.migrate(sys.argv[1:], source)
        MigrateCredentials.migrate(sys.argv[1:], source)
    except Exception:
        # Pydantic v2 compatibility: use model_dump_json
        error_msg = AirbyteMessage(
            type=Type.TRACE,
            trace=AirbyteTraceMessage(
                type=TraceType.ERROR,
                emitted_at=int(datetime.now().timestamp() * 1000),
                error=AirbyteErrorTraceMessage(
                    message="Error starting the sync. This could be due to an invalid configuration or catalog. Please contact Support for assistance.",
                    stack_trace=traceback.format_exc(),
                ),
            ),
        )
        try:
            # Newer models (Pydantic v2-based) expose model_dump_json
            print(error_msg.model_dump_json())
        except Exception:
            # Fallback to legacy .json() if available
            try:
                print(error_msg.json())
            except Exception:
                # Final fallback
                print(str(error_msg))
    else:
        launch(source, args)
