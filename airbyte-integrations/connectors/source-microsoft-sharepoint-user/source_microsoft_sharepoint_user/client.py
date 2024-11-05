#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import io
import logging
from typing import Optional

import backoff
from airbyte_cdk import AirbyteTracedException, FailureType

from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential

# set default timeout to 300 seconds
REQUEST_TIMEOUT = 300

logger = logging.getLogger("airbyte")


def handle_backoff(details):
    logger.warning("SSH Connection closed unexpectedly. Waiting {wait} seconds and retrying...".format(**details))


class SourceMicrosoftSharePointUserClient:

    _client_context: ClientContext = None

    def __init__(
        self,
        company_url: str,
        sharepoint_site: str,
        user_login: str,
        user_password: str
    ):
        self.company_url = company_url
        self.sharepoint_site = sharepoint_site
        self.user_login = user_login
        self.user_password = user_password

        self._connect()

    # If connection is snapped during connect flow, retry up to a
    # minute for SSH connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(backoff.expo, EOFError, max_tries=6, on_backoff=handle_backoff, jitter=None, factor=2)
    def _connect(self):
        
        if self._client_context is not None:
            return

        try:
            site_url = f"{self.company_url}/sites/{self.sharepoint_site}"
            self._client_context = ClientContext(site_url).with_credentials(UserCredential(self.user_login, password=self.user_password))
            web = self._client_context.web
            self._client_context.load(web)
            self._client_context.execute_query()

        except Exception as e:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message=f"Unable to connect, error: {str(e)}",
                internal_message=f"Authentication failed, error: {str(e)}",
            )

    @property
    def client_context(self) -> ClientContext:
        return self._client_context
