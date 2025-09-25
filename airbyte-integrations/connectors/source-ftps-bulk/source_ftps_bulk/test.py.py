import ftplib
import ssl

def test_ftps_connection(host, port, username, password, use_tls=True):
    try:
        # Create an FTPS connection with optional TLS/SSL
        if use_tls:
            context = ssl.create_default_context()
            ftps = ftplib.FTP_TLS(context=context)
        else:
            ftps = ftplib.FTP()
        
        print(f"Connecting to {host}:{port}...")
        ftps.connect(host, port, timeout=30)
        
        if use_tls:
            ftps.auth()  # Explicit TLS handshake
            ftps.prot_p()  # Secure data connection
        
        print("Logging in...")
        ftps.login(username, password)
        
        print("Login successful. Listing files...")
        files = ftps.nlst()  # List files and directories
        for file in files:
            print(file)
        
        print("Closing connection...")
        ftps.quit()

    except ftplib.all_errors as e:
        print(f"Error: {e}")


# Replace with your FTPS server details
host = "ftps.modexpress.com"
port = 990  # Use 21 for Explicit FTPS or 990 for Implicit FTPS
username = "SUITSUPPLY"
password = "GyvrMP/~yUrsQrOF{shJSevr"
use_tls = True  # Set to False for plain FTP, True for FTPS

test_ftps_connection(host, port, username, password, use_tls)
