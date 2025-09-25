# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import datetime
import logging
import stat
import gzip
import zipfile
from io import BytesIO, IOBase, TextIOWrapper
from typing import Dict, Iterable, List, Optional

# Update imports to remove unavailable classes
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from source_sftp_bulk.client import SFTPClient
from source_sftp_bulk.spec import SourceSFTPBulkSpec


# Define your own FileTooLargeError class since it doesn't exist in the CDK
class FileTooLargeError(Exception):
    """Exception raised when a file is too large to be processed."""
    def __init__(self, file_path: str, file_size: int, max_size: int):
        self.file_path = file_path
        self.file_size = file_size
        self.max_size = max_size
        message = f"File {file_path} is too large ({file_size} bytes) to be processed. Maximum allowed size is {max_size} bytes."
        super().__init__(message)


# Define your own FileWithCache class since it doesn't exist in your CDK version
class FileWithCache:
    """A wrapper around a file-like object that can be cached."""
    
    def __init__(self, file_handle: IOBase):
        self.file_handle = file_handle
        self._cache = None
    
    @classmethod
    def from_file_handle(cls, file_handle: IOBase) -> 'FileWithCache':
        """Create a FileWithCache instance from a file handle."""
        return cls(file_handle)
    
    def __enter__(self):
        return self.file_handle
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_handle.close()


class SourceSFTPBulkStreamReader(AbstractFileBasedStreamReader):
    def __init__(self):
        super().__init__()
        self._sftp_client = None

    @property
    def config(self) -> SourceSFTPBulkSpec:
        return self._config

    @config.setter
    def config(self, value: SourceSFTPBulkSpec):
        """
        FileBasedSource reads the config from disk and parses it, and once parsed, the source sets the config on its StreamReader.

        Note: FileBasedSource only requires the keys defined in the abstract config, whereas concrete implementations of StreamReader
        will require keys that (for example) allow it to authenticate with the 3rd party.

        Therefore, concrete implementations of AbstractFileBasedStreamReader's config setter should assert that `value` is of the correct
        config type for that type of StreamReader.
        """
        assert isinstance(value, SourceSFTPBulkSpec)
        self._config = value

    @property
    def sftp_client(self) -> SFTPClient:
        if self._sftp_client is None:
            authentication = (
                {"password": self.config.credentials.password}
                if self.config.credentials.auth_type == "password"
                else {"private_key": self.config.credentials.private_key}
            )
            self._sftp_client = SFTPClient(
                host=self.config.host,
                username=self.config.username,
                **authentication,
                port=self.config.port,
            )
        return self._sftp_client

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
                items = self.sftp_client.sftp_connection.listdir_attr(current_dir)
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
                    
    def get_file(self, file: RemoteFile, config: Dict, logger: logging.Logger) -> FileWithCache:
        """
        Download a file from SFTP server and return it as a FileWithCache.
        This is required by the AbstractFileBasedStreamReader interface.
        """
        try:
            file_size = self.file_size(file)
            max_file_size = config.get('max_file_size', float('inf'))
            
            if 0 < max_file_size < file_size:
                raise FileTooLargeError(file.uri, file_size, max_file_size)
            
            # In this implementation, we simply open the file without downloading it
            # entirely first, letting our open_file method handle the appropriate reading
            file_handle = self.open_file(file, FileReadMode.READ, config.get('encoding'), logger)
            
            # Create and return a FileWithCache
            return FileWithCache.from_file_handle(file_handle)
        except Exception as e:
            logger.exception(f"Error retrieving file {file.uri}: {e}")
            raise

    def open_file(self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        try:
            # Determine file mode for gzip and standard files
            open_mode = 'rt' if mode == FileReadMode.READ else 'rb'
            open_encoding = encoding or 'utf-8'
            errors = "ignore"

            # Open gzipped files with gzip
            if file.uri.endswith('.gz'):
                remote_file = self.sftp_client.sftp_connection.open(file.uri, mode='rb')  # Open as binary for gzip handling
                remote_file = gzip.open(remote_file, mode=open_mode, encoding=open_encoding if mode == FileReadMode.READ else None, errors=errors)
            
            # Handle zip files
            elif file.uri.endswith('.zip'):
                logger.info(f"Processing ZIP file: {file.uri}")
                # First read the file as binary
                remote_file_binary = self.sftp_client.sftp_connection.open(file.uri, mode='rb')
                file_content = remote_file_binary.read()
                remote_file_binary.close()
                
                # Process the zip file
                buffer = BytesIO(file_content)
                zip_file = zipfile.ZipFile(buffer)
                
                # Get the first file in the archive
                if zip_file.namelist():
                    first_file_name = zip_file.namelist()[0]
                    logger.info(f"Extracting file {first_file_name} from ZIP archive {file.uri}")
                    extracted_file = zip_file.open(first_file_name)
                    
                    if mode == FileReadMode.READ:
                        return TextIOWrapper(extracted_file, encoding=open_encoding, errors=errors)
                    return extracted_file
                else:
                    raise Exception(f"ZIP file {file.uri} is empty")

            else:
                # Check if prefetching or buffer size adjustments are necessary
                if not self.use_file_transfer():
                    remote_file = self.sftp_client.sftp_connection.open(file.uri, mode=mode.value)
                else:
                    remote_file = self.sftp_client.sftp_connection.open(file.uri, mode=mode.value, bufsize=262144)
                    remote_file.prefetch(remote_file.stat().st_size)

                # Apply encoding and error handling if in text read mode
                if mode == FileReadMode.READ:
                    remote_file = TextIOWrapper(remote_file, encoding=open_encoding, errors=errors)

            return remote_file

        except Exception as e:
            logger.exception(f"Error opening file {file.uri}: {e}")
            raise Exception(f"Error opening file {file.uri}: {e}")
        
    def file_size(self, file: RemoteFile):
        file_size = self.sftp_client.sftp_connection.stat(file.uri).st_size
        return file_size
