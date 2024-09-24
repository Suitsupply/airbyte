#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Dict, Literal, Optional, Union

import dpath.util
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from pydantic.v1 import BaseModel, Field


class SourceMicrosoftSharePointUserSpec(AbstractFileBasedSpec, BaseModel):
    """
    SourceMicrosoftSharePointUserSpec class for Microsoft SharePoint Source Specification.
    This class combines the authentication details with additional configuration for the SharePoint API.
    """

    class Config:
        title = "Microsoft SharePoint Source Spec"

    user_login: str = Field(
        title="User Login",
        description="User name to access Microsoft Sharepoint",
        order=3,
    )
    user_password: str = Field(
        title="User Password",
        description="User password to access Microsoft Sharepoint",
        airbyte_secret=True,
        order=4,
    )

    company_url: str = Field(
        title="Company URL",
        description="Microsoft Sharepoint URL for the company, for example, 'https://my-company.sharepoint.com'",
        pattern="^https://[A-Za-z0-9_\-]+\.sharepoint\.com$",
        order=0,
    )

    sharepoint_site: str = Field(
        title="Sharepoint Site",
        description="Microsoft Sharepoint site in format 'My Sharepoint Site'",
        pattern="^[A-Za-z0-9_ /\-]+$",
        order=1,
    )

    folder_path: str = Field(
        title="Sharepoint Folder Path",
        description="Microsoft Sharepoint folder in the site to read the files from, for example, 'Import/My Daily Tasks'",
        order=2,
    )
    
    @classmethod
    def documentation_url(cls) -> str:
        """Provides the URL to the documentation for this specific source."""
        return "https://docs.airbyte.com/integrations/sources/microsoft-sharepoint"
