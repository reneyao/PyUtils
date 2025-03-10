import smbclient
import socket
import os
from Time import timeit
import pandas as pd
import io

class SMBClient:
    def __init__(self, server, username, password, share_name):
       # 初始化
        self.server = server
        self.username = username
        self.password = password
        self.share_name = share_name

    @timeit
    def server_connection(self, port=445):
        """
        测试是否能连接到 SMB 服务器
        :param port: 默认 SMB 端口 445
        :return: True 表示可以连接，否则返回 False
        """
        try:
            socket.create_connection((self.server, port), timeout=3)
            return True
        except socket.error:
            return False

    @timeit
    def read_file_via_smbclient(self, file_path):
        """
        通过 smbclient 连接 SMB 服务器并读取文件内容
        支持 Excel 和 CSV 文件格式
        """
        try:
            if not self.server_connection():
                raise ConnectionError(f"无法连接到服务器 {self.server}")

            smbclient.register_session(self.server, username=self.username, password=self.password)
            file_url = f"\\\\{self.server}\\{self.share_name}\\{file_path}"

            # 根据文件扩展名决定读取方式
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext in ['.xlsx', '.xls']:
                # 读取 Excel 文件
                with smbclient.open_file(file_url, mode='rb') as file:
                    content = file.read()
                    return pd.read_excel(io.BytesIO(content))
            elif file_ext == '.csv':
                # 读取 CSV 文件
                with smbclient.open_file(file_url, mode='r') as file:
                    return pd.read_csv(io.StringIO(file.read()))
            else:
                # 其他文件格式按文本读取
                with smbclient.open_file(file_url, mode='r') as file:
                    return file.read()

        except ConnectionError as e:
            print(f"连接错误: {e}")
        except Exception as e:
            print(f"发生错误: {type(e).__name__} - {e}")
        return None

    @timeit
    def read_file_from_network_share(self, file_path):
        """
        通过 net use 命令挂载网络共享，然后读取文件内容
        支持 Excel 和 CSV 文件格式
        """
        share = f"\\\\{self.server}\\{self.share_name}"
        try:
            os.system(f'net use {share} /user:{self.username} {self.password} >nul 2>&1')
            
            full_path = f"{share}\\{file_path}"
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext in ['.xlsx', '.xls']:
                return pd.read_excel(full_path)
            elif file_ext == '.csv':
                return pd.read_csv(full_path)
            else:
                with open(full_path, 'r') as file:
                    return file.readlines()
                    
        except Exception as e:
            print(f"读取网络共享文件错误: {e}")
            return None
        finally:
            os.system(f'net use {share} /delete >nul 2>&1')




   
