import paramiko
import io
from Time import timeit
import polars as pl

class SFTPClient:
    def __init__(self):
        self.client = None
        self.sftp = None
        self.is_connected = False

    @timeit
    def connect(self, host, username, password):
        if self.is_connected:
            print("Already connected.")
            return False
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(host, username=username, password=password)
            self.sftp = self.client.open_sftp()
            self.is_connected = True
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    @timeit
    def read_file(self, file_path):
        if not self.is_connected:
            print("Not connected to any server.")
            return None

        try:
            # Get file extension
            file_extension = file_path.lower().split('.')[-1]
            
            # Read remote file content
            with self.sftp.open(file_path, 'rb') as remote_file:
                file_content = remote_file.read()

            # Create file-like object from content
            file_stream = io.BytesIO(file_content)
            
            # Read file based on extension
            if file_extension in ['xlsx', 'xls']:
                df = pl.read_excel(file_stream)
            elif file_extension == 'csv':
                # Decode bytes to string for CSV
                text_content = file_content.decode('utf-8')
                text_stream = io.StringIO(text_content)
                df = pl.read_csv(text_stream)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
                
            return df
        except Exception as e:
            print(f"Error: Unable to read file {file_path}. {e}")
            return None

    @timeit
    def disconnect(self):
        if self.is_connected:
            self.sftp.close()
            self.client.close()
            self.is_connected = False

    def __del__(self):
        self.disconnect()


    

