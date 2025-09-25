# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Literal, Optional, Union
from airbyte_cdk import OneOfOptionConfig
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec, DeliverRawFiles, DeliverRecords
from pydantic.v1 import BaseModel, Field
from enum import Enum

class EncryptionMethod(str, Enum):
    explicit = "Explicit"
    implicit = "Implicit"

class SourceFTPSBulkSpec(AbstractFileBasedSpec):
    class Config:
        title = "FTPS Bulk Source Spec"

    host: str = Field(title="Host Address", description="The server host address", examples=["www.host.com", "192.0.2.1"], order=2)
    username: str = Field(title="User Name", description="The server user", order=3)
    password: str = Field(title="Password", description="Password", airbyte_secret=True, order=4)
    fingerprint: str = Field(title="Fingerprint", description="A hash of the server's TLS certificate", airbyte_secret=True, order=5)

    encryption_method: EncryptionMethod = Field(
        title="Encryption Method",
        description="Select the FTPS encryption method: Explicit or Implicit",
        default=EncryptionMethod.explicit,
        display_type="radio",
        examples=["Explicit", "Implicit"],
        order=6,
    )
    
    port: int = Field(title="Host Port", description="The server port", default=22, examples=["22"], order=7)
    folder_path: Optional[str] = Field(
        title="Folder Path",
        description="The directory to search files for sync",
        examples=["/logs/2022"],
        order=8,
        default="/",
        pattern_descriptor="/folder_to_sync",
    )

    delivery_method: Union[DeliverRecords, DeliverRawFiles] = Field(
        title="Delivery Method",
        discriminator="delivery_type",
        type="object",
        order=9,
        display_type="radio",
        group="advanced",
        default="use_records_transfer",
    )

    @classmethod
    def documentation_url(cls) -> str:
        return "https://docs.airbyte.com/integrations/sources/ftps-bulk"
