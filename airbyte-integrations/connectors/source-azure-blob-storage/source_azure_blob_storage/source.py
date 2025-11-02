#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any

from airbyte_cdk import AdvancedAuth, ConnectorSpecification, OAuthConfigSpecification
from airbyte_cdk.models import AuthFlowType, OauthConnectorInputSpecification
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource


class SourceAzureBlobStorage(FileBasedSource):
    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        """
        Returns the specification describing what fields can be configured by a user when setting up a file-based source.
        """

        oauth_connector_input_specification = OauthConnectorInputSpecification(
            consent_url="https://login.microsoftonline.com/{{tenant_id}}/oauth2/v2.0/authorize?{{client_id_param}}&{{redirect_uri_param}}&{{state_param}}&{{scope_param}}&response_type=code&response_mode=query",
            access_token_url="https://login.microsoftonline.com/{{tenant_id}}/oauth2/v2.0/token",
            access_token_headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            access_token_params={
                "code": "{{ auth_code_value }}",
                "client_id": "{{ client_id_value }}",
                "redirect_uri": "{{ redirect_uri_value }}",
                "client_secret": "{{ client_secret_value }}",
                "grant_type": "authorization_code",
            },
            scope="https://storage.azure.com/.default offline_access",
        )

        return ConnectorSpecification(
            documentationUrl=self.spec_class.documentation_url(),
            connectionSpecification=self.spec_class.schema(),
            advanced_auth=AdvancedAuth(
                auth_flow_type=AuthFlowType.oauth2_0,
                predicate_key=["credentials", "auth_type"],
                predicate_value="oauth2",
                oauth_config_specification=OAuthConfigSpecification(
                    oauth_connector_input_specification=oauth_connector_input_specification,
                    complete_oauth_output_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "refresh_token": {
                                "type": "string",
                                "path_in_connector_config": ["credentials", "refresh_token"],
                                "path_in_oauth_response": ["refresh_token"],
                            }
                        },
                    },
                    complete_oauth_server_input_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"client_id": {"type": "string"}, "client_secret": {"type": "string"}},
                    },
                    complete_oauth_server_output_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "client_id": {"type": "string", "path_in_connector_config": ["credentials", "client_id"]},
                            "client_secret": {"type": "string", "path_in_connector_config": ["credentials", "client_secret"]},
                        },
                    },
                    oauth_user_input_from_connector_config_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"tenant_id": {"type": "string", "path_in_connector_config": ["credentials", "tenant_id"]}},
                    },
                ),
            ),
        )
