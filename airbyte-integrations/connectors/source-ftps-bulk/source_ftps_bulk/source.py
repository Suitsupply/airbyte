#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from typing import Any, Mapping, Optional

from airbyte_cdk import ConfiguredAirbyteCatalog, TState
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import DefaultFileBasedCursor
from source_ftps_bulk.spec import SourceFTPSBulkSpec
from source_ftps_bulk.stream_reader import SourceFTPSBulkStreamReader

logger = logging.getLogger("airbyte")


class SourceFTPSBulk(FileBasedSource):
    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: Optional[TState]):
        super().__init__(
            stream_reader=SourceFTPSBulkStreamReader(),
            spec_class=SourceFTPSBulkSpec,
            catalog=catalog,
            config=config,
            state=state,
            cursor_cls=DefaultFileBasedCursor,
        )
