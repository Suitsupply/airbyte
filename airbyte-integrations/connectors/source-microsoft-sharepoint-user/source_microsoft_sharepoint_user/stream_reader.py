#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from datetime import datetime
from io import IOBase, BytesIO, StringIO
from typing import Iterable, List, Optional
import gzip

from airbyte_cdk import AirbyteTracedException, FailureType
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from source_microsoft_sharepoint_user.spec import SourceMicrosoftSharePointUserSpec
from source_microsoft_sharepoint_user.client import SourceMicrosoftSharePointUserClient

from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder

class SourceMicrosoftSharePointUserStreamReader(AbstractFileBasedStreamReader):
    """
    A stream reader for Microsoft SharePoint. Handles file enumeration and reading from SharePoint.
    """

    ROOT_PATH = [".", "/"]


    def __init__(self):
        super().__init__()
        self._sharepoint_client = None


    @property
    def config(self) -> SourceMicrosoftSharePointUserSpec:
        return self._config


    @property
    def sharepoint_client(self) -> SourceMicrosoftSharePointUserClient:
        if self._sharepoint_client is None:
            self._sharepoint_client = SourceMicrosoftSharePointUserClient(
                company_url=self.config.company_url,
                sharepoint_site=self.config.sharepoint_site,
                user_login=self.config.user_login,
                user_password=self.config.user_password,
            )
        return self._sharepoint_client


    @config.setter
    def config(self, value: SourceMicrosoftSharePointUserSpec):
        """
        The FileBasedSource reads and parses configuration from a file, then sets this configuration in its StreamReader. While it only
        uses keys from its abstract configuration, concrete StreamReader implementations may need additional keys for third-party
        authentication. Therefore, subclasses of AbstractFileBasedStreamReader should verify that the value in their config setter
        matches the expected config type for their StreamReader.
        """
        assert isinstance(value, SourceMicrosoftSharePointUserSpec)
        self._config = value


    def get_matching_files(
        self,
        globs: List[str],
        prefix: Optional[str],
        logger: logging.Logger,
    ) -> Iterable[RemoteFile]:
        directories = [self._config.folder_path or "/"]

        default_globs = globs
        if not default_globs:
            default_globs = ['**/*']

        # Iterate through directories and subdirectories
        while directories:
            current_dir = directories.pop()
            try:
                folder_url = f"/sites/{self._config.sharepoint_site}/{current_dir}"
                folder = self.sharepoint_client.client_context.web.get_folder_by_server_relative_url(folder_url)
                self.sharepoint_client.client_context.load(folder)
                self.sharepoint_client.client_context.execute_query()
                
                subfolders = folder.folders
                files = folder.files

                self.sharepoint_client.client_context.load(subfolders)
                self.sharepoint_client.client_context.execute_query()

                self.sharepoint_client.client_context.load(files)
                self.sharepoint_client.client_context.execute_query()

            except Exception as e:
                logger.warning(f"Failed to list files in directory: {e}")
                continue

            for item in subfolders:
                directories.append(f"{current_dir}/{item.name}")
            
            for item in files:
                yield from self.filter_files_by_globs_and_start_date(
                    [RemoteFile(uri=f"{current_dir}/{item.name}", last_modified=item.time_last_modified)],
                    default_globs,
                )


    def open_file(self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        try:
            response = File.open_binary(self.sharepoint_client.client_context, f"/sites/{self._config.sharepoint_site}/{file.uri}")
            binary_data = BytesIO(response.content)

            file_extension = file.uri.split(".")[-1]
            if file_extension in ["gz"]:
            
                binary_data.seek(0)
                with gzip.GzipFile(filename=file.uri, fileobj=binary_data, mode='rb') as gz_file:
                    # Read the content into a BytesIO variable
                    binary_data = BytesIO(gz_file.read())

            if mode == FileReadMode.READ:
                text_data = binary_data.getvalue().decode(encoding or "utf-8")
                return StringIO(text_data)
            
            return binary_data

        except Exception as e:
            logger.exception(f"Error opening file {file.uri}: {e}")
            raise Exception(f"Error opening file {file.uri}: {e}")