#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Mapping, Optional

from airbyte_cdk import AdvancedAuth, ConfiguredAirbyteCatalog, ConnectorSpecification, OAuthConfigSpecification, TState
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import DefaultFileBasedCursor
from source_microsoft_sharepoint_user.spec import SourceMicrosoftSharePointUserSpec
from source_microsoft_sharepoint_user.stream_reader import SourceMicrosoftSharePointUserStreamReader


class SourceMicrosoftSharePointUser(FileBasedSource):
    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: Optional[TState]):
        super().__init__(
            stream_reader=SourceMicrosoftSharePointUserStreamReader(),
            spec_class=SourceMicrosoftSharePointUserSpec,
            catalog=catalog,
            config=config,
            state=state,
            cursor_cls=DefaultFileBasedCursor,
        )