# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import datetime
import logging
import stat
import gzip
from io import IOBase, TextIOWrapper
from typing import Iterable, List, Optional

from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from source_ftps_bulk.client import FTPSClient
from source_ftps_bulk.spec import SourceFTPSBulkSpec


class SourceFTPSBulkStreamReader(AbstractFileBasedStreamReader):
    def __init__(self):
        super().__init__()
        self._ftps_client = None

    @property
    def config(self) -> SourceFTPSBulkSpec:
        return self._config

    @config.setter
    def config(self, value: SourceFTPSBulkSpec):
        """
        FileBasedSource reads the config from disk and parses it, and once parsed, the source sets the config on its StreamReader.

        Note: FileBasedSource only requires the keys defined in the abstract config, whereas concrete implementations of StreamReader
        will require keys that (for example) allow it to authenticate with the 3rd party.

        Therefore, concrete implementations of AbstractFileBasedStreamReader's config setter should assert that `value` is of the correct
        config type for that type of StreamReader.
        """
        assert isinstance(value, SourceFTPSBulkSpec)
        self._config = value

    @property
    def ftps_client(self) -> FTPSClient:
        if self._ftps_client is None:
            authentication = (
                {"password": self.config.credentials.password}
                if self.config.credentials.auth_type == "password"
                else {"private_key": self.config.credentials.private_key}
            )
            self._ftps_client = FTPSClient(
                host=self.config.host,
                username=self.config.username,
                **authentication,
                port=self.config.port,
            )
        return self._ftps_client

    def get_matching_files(
        self,
        globs: List[str],
        prefix: Optional[str],
        logger: logging.Logger,
    ) -> Iterable[RemoteFile]:
        directories = [self._config.folder_path or "/"]

        # Iterate through directories and subdirectories
        while directories:
            current_dir = directories.pop()
            try:
                items = self.ftps_client.ftps_connection.listdir_attr(current_dir)
            except Exception as e:
                logger.warning(f"Failed to list files in directory: {e}")
                continue

            for item in items:
                if item.st_mode and stat.S_ISDIR(item.st_mode):
                    directories.append(f"{current_dir}/{item.filename}")
                else:
                    # Skip empty files
                    if item.st_size == 0:
                        logger.info(f"Skipping empty file: {current_dir}/{item.filename}")
                        continue

                    yield from self.filter_files_by_globs_and_start_date(
                        [RemoteFile(uri=f"{current_dir}/{item.filename}", last_modified=datetime.datetime.fromtimestamp(item.st_mtime))],
                        globs,
                    )

    def open_file(self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        try:
            # Determine file mode for gzip and standard files
            open_mode = 'rt' if mode == FileReadMode.READ else 'rb'
            open_encoding = encoding or 'utf-8'
            errors = "ignore"

            # Open gzipped files with gzip
            if file.uri.endswith('.gz'):
                remote_file = self.ftps_client.ftps_connection.open(file.uri, mode='rb')  # Open as binary for gzip handling
                remote_file = gzip.open(remote_file, mode=open_mode, encoding=open_encoding if mode == FileReadMode.READ else None, errors=errors)

            else:
                # Check if prefetching or buffer size adjustments are necessary
                if not self.use_file_transfer():
                    remote_file = self.ftps_client.ftps_connection.open(file.uri, mode=mode.value)
                else:
                    remote_file = self.ftps_client.ftps_connection.open(file.uri, mode=mode.value, bufsize=262144)
                    remote_file.prefetch(remote_file.stat().st_size)

                # Apply encoding and error handling if in text read mode
                if mode == FileReadMode.READ:
                    remote_file = TextIOWrapper(remote_file, encoding=open_encoding, errors=errors)

            return remote_file

        except Exception as e:
            logger.exception(f"Error opening file {file.uri}: {e}")
            raise Exception(f"Error opening file {file.uri}: {e}")
        
    def file_size(self, file: RemoteFile):
        file_size = self.ftps_client.ftps_connection.stat(file.uri).st_size
        return file_size
