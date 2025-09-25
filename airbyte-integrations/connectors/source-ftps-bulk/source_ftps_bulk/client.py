import logging
import ftplib
import hashlib
import ssl

from typing import Optional
from airbyte_cdk import AirbyteTracedException, FailureType
from enum import Enum

# set default timeout to 300 seconds
REQUEST_TIMEOUT = 300

logger = logging.getLogger("airbyte")

class ReusedSslSocket(ssl.SSLSocket):
    def unwrap(self):
        pass

class SharedFTP_TLS(ftplib.FTP_TLS):
    """Explicit FTPS, with shared TLS session"""
    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)  # reuses TLS session            
            conn.__class__ = ReusedSslSocket  # we should not close reused ssl socket when file transfers finish
        return conn, size
    

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


    def _verify_fingerprint(self):
        server_cert = self._connection.sock.getpeercert(binary_form=True)
        actual_fingerprint = hashlib.sha256(server_cert).hexdigest().upper()
        formatted_fingerprint = ":".join(actual_fingerprint[i:i+2] for i in range(0, len(actual_fingerprint), 2))
        
        if formatted_fingerprint == self.fingerprint.upper():
            logger.info("Fingerprint verification successful.")
        else:
            logger.error(f"Fingerprint mismatch!\nExpected: {self.fingerprint}\nActual: {formatted_fingerprint}")
            self._connection.sock.close()
            raise ValueError("Fingerprint verification failed.")
            

    def _connect(self):
        self._connection = None
        try:
            ctx = ssl._create_stdlib_context(ssl.PROTOCOL_TLSv1_2)

            if self.encryption_method == "Explicit":
                self._connection = SharedFTP_TLS(context=ctx)
                self._connection.connect(self.host, self.port, timeout=REQUEST_TIMEOUT)
                self._connection.auth()  # Explicitly upgrade the connection to secure
            elif self.encryption_method == "Implicit":
                self._connection = SharedFTP_TLS(context=ctx)
                self._connection.connect(self.host, self.port, timeout=REQUEST_TIMEOUT)  # Implicit FTPS connects securely by default
            else:
                raise ValueError(f"Unsupported encryption type: {self.encryption_method}. Use 'Explicit' or 'Implicit'.")

            self._verify_fingerprint()
            self._connection.login(self.username, self.password)
            self._connection.prot_p()
            self._connection.set_pasv(True)

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

    def _close_connection(self):
        if self._connection:
            try:
                self._connection.quit()  # Gracefully terminate the FTP session
                logger.info("Connection closed successfully.")
            except ftplib.error_perm as e:
                logger.error(f"Permission error during quit: {e}")
            except ftplib.all_errors as e:
                logger.error(f"Error while closing connection: {e}")
            finally:
                try:
                    self._connection.close()  # Close the socket if not closed
                    logger.info("Socket closed.")
                except Exception as e:
                    logger.error(f"Error closing socket: {e}")   
            
            logger.info("Erase ftps_connection.")
            self._connection = None


    def __del__(self):
        self._close_connection()


    @property
    def ftps_connection(self) -> SharedFTP_TLS:
        return self._connection
