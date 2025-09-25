#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Dict, Literal, Optional, Union

import dpath.util
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from pydantic.v1 import BaseModel, Field


class OAuthCredentials(BaseModel):
    """
    OAuthCredentials class for Microsoft OAuth authentication with app registration.
    Uses delegated permissions to act on behalf of the authenticated user.
    """

    class Config:
        title = "Microsoft OAuth Authentication"

    tenant_id: str = Field(
        title="Tenant ID", 
        description="Tenant ID of your Azure Active Directory",
        airbyte_secret=True,
        order=0
    )
    client_id: str = Field(
        title="Client ID",
        description="Client ID of your Azure App Registration with delegated permissions Files.Read.All",
        airbyte_secret=True,
        order=1
    )
    client_secret: str = Field(
        title="Client Secret",
        description="Client Secret of your Azure App Registration",
        airbyte_secret=True,
        order=2
    )
    refresh_token: str = Field(
        title="Refresh Token",
        description="OAuth refresh token obtained through the consent flow. Use the helper script to generate this.",
        airbyte_secret=True,
        order=3
    )


class SourceMicrosoftSharePointUserSpec(AbstractFileBasedSpec, BaseModel):
    """
    SourceMicrosoftSharePointUserSpec class for Microsoft SharePoint Source Specification.
    This class combines the authentication details with additional configuration for the SharePoint API.
    """

    class Config:
        title = "Microsoft SharePoint Source Spec"

    # OAuth authentication for secure access to SharePoint
    credentials: OAuthCredentials = Field(
        title="Authentication",
        description="OAuth credentials for connecting to SharePoint with app registration",
        order=0,
    )

    company_url: str = Field(
        title="Company URL",
        description="Microsoft Sharepoint URL for the company, for example, 'https://my-company.sharepoint.com'",
        pattern="^https://[A-Za-z0-9_\-]+\.sharepoint\.com$",
        order=1,
    )

    sharepoint_site: str = Field(
        title="Sharepoint Site",
        description="Microsoft Sharepoint site in format 'My Sharepoint Site'",
        pattern="^[A-Za-z0-9_ /\-]+$",
        order=2,
    )

    folder_path: str = Field(
        title="Sharepoint Folder Path",
        description="Microsoft Sharepoint folder in the site to read the files from, for example, 'Import/My Daily Tasks'",
        order=3,
    )
    
    @classmethod
    def documentation_url(cls) -> str:
        """Provides the URL to the documentation for this specific source."""
        return "https://docs.airbyte.com/integrations/sources/microsoft-sharepoint"

    @classmethod
    def schema(cls, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Generates the schema mapping for configuration fields.
        It also cleans up the schema by removing legacy settings and discriminators.
        """
        schema = super().schema(*args, **kwargs)

        # Remove legacy settings related to streams
        dpath.util.delete(schema, "properties/streams/items/properties/legacy_prefix")
        dpath.util.delete(schema, "properties/streams/items/properties/format/oneOf/*/properties/inference_type")

        return schema
