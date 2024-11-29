import datetime
import logging
import gzip
from io import BytesIO, TextIOWrapper, IOBase
from typing import Iterable, List, Optional
from ftplib import FTP_TLS, error_perm

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
    def ftps_client(self) -> FTP_TLS:
        if self._ftps_client is None:
            self._ftps_client = FTPSClient(
                host=self.config.host,
                username=self.config.username,
                password=self.config.password,
                port=self.config.port,
                encryption_method=self.config.encryption_method,
                fingerprint=self.config.fingerprint,
            ).ftps_connection
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
                self.ftps_client.cwd(current_dir)
                items = self.ftps_client.nlst()
            except error_perm as e:
                logger.warning(f"Failed to list files in directory {current_dir}: {e}")
                continue

            for item in items:
                path = f"{current_dir}/{item}"
                try:
                    self.ftps_client.voidcmd(f"SIZE {path}")
                    is_file = True
                except error_perm:
                    is_file = False

                if not is_file:
                    directories.append(path)
                else:
                    yield from self.filter_files_by_globs_and_start_date(
                        [RemoteFile(uri=path, last_modified=self._get_last_modified(path))],
                        globs,
                    )

    def open_file(self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        try:
            # Determine file mode for gzip and standard files
            open_mode = "rt" if mode == FileReadMode.READ else "rb"
            open_encoding = encoding or "utf-8"
            errors = "ignore"

            # Open gzipped files with gzip
            with BytesIO() as buffer:
                self.ftps_client.retrbinary(f"RETR {file.uri}", buffer.write)
                buffer.seek(0)

                if file.uri.endswith(".gz"):
                    remote_file = gzip.open(buffer, mode=open_mode, encoding=open_encoding if mode == FileReadMode.READ else None, errors=errors)
                else:
                    remote_file = buffer

                if mode == FileReadMode.READ and not file.uri.endswith(".gz"):
                    remote_file = TextIOWrapper(remote_file, encoding=open_encoding, errors=errors)

                return remote_file

        except Exception as e:
            logger.exception(f"Error opening file {file.uri}: {e}")
            raise Exception(f"Error opening file {file.uri}: {e}")
        
    def file_size(self, file: RemoteFile):
        response = self.ftps_client.sendcmd(f"SIZE {file.uri}")
        return int(response.split()[1])

    def _get_last_modified(self, path: str) -> datetime.datetime:
        response = self.ftps_client.sendcmd(f"MDTM {path}")
        return datetime.datetime.strptime(response.split()[1], "%Y%m%d%H%M%S")
