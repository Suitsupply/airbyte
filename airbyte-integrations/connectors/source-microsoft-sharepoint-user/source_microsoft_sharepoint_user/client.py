#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from functools import lru_cache
from typing import Optional, Union

import backoff
from airbyte_cdk import AirbyteTracedException, FailureType
from msal import ConfidentialClientApplication
from office365.graph_client import GraphClient
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.token_response import TokenResponse

from source_microsoft_sharepoint_user.spec import SourceMicrosoftSharePointUserSpec, OAuthCredentials

# set default timeout to 300 seconds
REQUEST_TIMEOUT = 300

logger = logging.getLogger("airbyte")


def handle_backoff(details):
    logger.warning("Connection error. Waiting {wait} seconds and retrying...".format(**details))


class SourceMicrosoftSharePointUserClient:
    """
    Client to interact with Microsoft SharePoint using Microsoft Graph API.
    Uses OAuth authentication with delegated permissions to act on behalf of a user.
    """

    def __init__(self, config: SourceMicrosoftSharePointUserSpec):
        self.config = config
        self._client = None
        self._client_context = None
        self._msal_app = ConfidentialClientApplication(
            self.config.credentials.client_id,
            authority=f"https://login.microsoftonline.com/{self.config.credentials.tenant_id}",
            client_credential=self.config.credentials.client_secret,
        )

    @property
    def client(self):
        """Initializes and returns a GraphClient instance."""
        if not self.config:
            raise ValueError("Configuration is missing; cannot create the Office365 graph client.")
        if not self._client:
            self._client = GraphClient(self._get_access_token)
        return self._client

    def _get_access_token(self):
        """Retrieves an access token for Microsoft Graph using OAuth refresh token."""
        scope = ["https://graph.microsoft.com/.default"]
        
        # OAuth authentication with refresh token
        refresh_token = self.config.credentials.refresh_token
        if not refresh_token:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message="Refresh token is required for OAuth authentication",
                internal_message="No refresh token provided in OAuth credentials",
            )
        
        result = self._msal_app.acquire_token_by_refresh_token(refresh_token, scopes=scope)
        if "access_token" in result:
            return result["access_token"]
        else:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message=f"Failed to refresh access token: {result.get('error_description', 'Unknown error')}",
                internal_message=f"MSAL refresh token error: {result}",
            )

    def _get_sharepoint_access_token(self):
        """Retrieves an access token specifically for SharePoint access."""
        sharepoint_scope = [f"{self.config.company_url}/.default"]
        
        # OAuth authentication with refresh token
        refresh_token = self.config.credentials.refresh_token
        if not refresh_token:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message="Refresh token is required for OAuth authentication",
                internal_message="No refresh token provided in OAuth credentials",
            )
        
        result = self._msal_app.acquire_token_by_refresh_token(refresh_token, scopes=sharepoint_scope)
        if "access_token" in result:
            return result["access_token"]
        else:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message=f"Failed to refresh SharePoint access token: {result.get('error_description', 'Unknown error')}",
                internal_message=f"MSAL SharePoint refresh token error: {result}",
            )

    @property
    def client_context(self) -> ClientContext:
        """Returns a SharePoint ClientContext authenticated with MSAL token."""
        if self._client_context is None:
            site_url = f"{self.config.company_url}/sites/{self.config.sharepoint_site}"
            
            # Create a token provider function that returns the access token
            def acquire_token():
                token = self._get_sharepoint_access_token()
                # Create a token response object that the Office365 library expects
                return TokenResponse.from_json({
                    "access_token": token,
                    "token_type": "Bearer"
                })
            
            # Create ClientContext with token provider
            self._client_context = ClientContext(site_url).with_access_token(acquire_token)
        
        return self._client_context

    # If connection is snapped during connect flow, retry up to a
    # minute for connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(backoff.expo, EOFError, max_tries=6, on_backoff=handle_backoff, jitter=None, factor=2)
    def test_connection(self):
        """Test the connection to SharePoint."""
        try:
            # Test the connection by trying to get the site/web information
            web = self.client_context.web
            self.client_context.load(web)
            self.client_context.execute_query()
            logger.info(f"Successfully connected to SharePoint site: {web.title}")
            return True
        except Exception as e:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message=f"Unable to connect to SharePoint, error: {str(e)}",
                internal_message=f"SharePoint connection test failed: {str(e)}",
            )
