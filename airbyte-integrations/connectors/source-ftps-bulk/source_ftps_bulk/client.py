import logging
import ftplib
from typing import Optional
from airbyte_cdk import AirbyteTracedException, FailureType
from enum import Enum

# set default timeout to 300 seconds
REQUEST_TIMEOUT = 300

logger = logging.getLogger("airbyte")


class EncryptionMethod(str, Enum):
    explicit = "Explicit"
    implicit = "Implicit"


class FTPSClient:
    _connection: Optional[ftplib.FTP_TLS] = None

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: Optional[int] = 21,
        encryption_method: EncryptionMethod = EncryptionMethod.explicit,
        timeout: Optional[int] = REQUEST_TIMEOUT,
        fingerprint: Optional[str] = None,
    ):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port)
        self.encryption_method = encryption_method
        self.timeout = float(timeout)
        self.fingerprint = fingerprint

        self._connect()

    def _connect(self):
        try:
            self._connection = ftplib.FTP_TLS(timeout=self.timeout)

            # Implicit FTPS starts the TLS negotiation immediately after connecting
            if self.encryption_method == EncryptionMethod.implicit:
                self._connection.connect(self.host, self.port)
                self._connection.auth()  # Implicit encryption

            # Explicit FTPS starts with plain FTP and upgrades to TLS after AUTH command
            else:
                self._connection.connect(self.host, self.port)
                self._connection.login(self.username, self.password)
                self._connection.prot_p()  # Protect command channel with TLS

            # Verify server fingerprint if provided
            if self.fingerprint:
                self._validate_fingerprint()

            self._connection.login(self.username, self.password)
            logger.info("FTPS connection established.")

        except ftplib.error_perm as e:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message=f"FTP Authentication failed: {str(e)}",
                internal_message="Check your username, password, or encryption method.",
            )
        except Exception as e:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message="Failed to connect to FTPS server.",
                internal_message=str(e),
            )

    def _validate_fingerprint(self):
        # Fetch the server certificate for fingerprint comparison
        sock = self._connection.sock
        server_cert = sock.getpeercert(binary_form=True)
        from hashlib import sha256

        cert_fingerprint = ":".join(
            ["{:02x}".format(byte) for byte in sha256(server_cert).digest()]
        )
        if cert_fingerprint.lower() != self.fingerprint.lower():
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                message="Server fingerprint mismatch.",
                internal_message=f"Expected {self.fingerprint}, got {cert_fingerprint}.",
            )

    def __del__(self):
        if self._connection:
            try:
                self._connection.quit()
                logger.info("FTPS connection closed.")
            except Exception as e:
                logger.warning(f"Error while closing connection: {e}")

    @property
    def ftps_connection(self) -> ftplib.FTP_TLS:
        return self._connection
