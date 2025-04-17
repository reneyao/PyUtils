import os
import yaml
import importlib.resources
from threading import Lock
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from urllib.parse import quote_plus
import pandas as pd
from clickhouse_driver import Client


class DatabaseConnection:
    _instance = {}
    _lock = Lock()

    def __new__(cls, server_name, *args, **kwargs):
        with cls._lock:
            if server_name not in cls._instance:
                instance = super().__new__(cls)
                cls._instance[server_name] = instance
                instance.engine = None
                instance.server_name = server_name
        return cls._instance[server_name]

    def _load_config(self):
        """加载配置文件或从环境变量获取数据库信息"""
        try:
            with importlib.resources.open_text("PyUtils", "config.yaml") as file:
                config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError("Database config not found.")

        if "databases" not in config or self.server_name not in config["databases"]:
            raise ValueError(
                f"Database configuration for '{self.server_name}' not found."
            )

        return config["databases"][self.server_name]

    def connect(self):
        """使用连接池创建数据库连接"""
        if self.engine is None:
            db_config = self._load_config()
            connection_string = f"mysql+pymysql://{db_config['user']}:{quote_plus(db_config['password'])}@{db_config['host']}/{db_config['name']}"
            self.engine = create_engine(
                connection_string,
                pool_size=db_config.get("pool_size", 20),
                max_overflow=db_config.get("max_overflow", 20),
                pool_pre_ping=True,  # 检查连接有效性
            )
        return self.engine

    def get_session(self):
        """获取数据库会话对象"""
        engine = self.connect()
        Session = sessionmaker(bind=engine)
        return Session()

    @contextmanager
    def session_scope(self):
        """上下文管理器，自动处理事务和连接关闭"""
        session = self.get_session()
        try:
            yield session
            session.commit()  # 提交事务
        except Exception as e:
            session.rollback()  # 回滚事务
            raise e
        finally:
            session.close()  # 关闭连接


def get_db_session(server_name: str):
    """获取数据库连接"""
    db = DatabaseConnection(server_name)
    return db.session_scope()


def query(server_name, sql):
    """查询函数，查询数据库中的数据"""
    with get_db_session(server_name) as session:
        session.query()
        # result = session.execute(text(sql)).fetchall()
        return session.execute(text(sql))


def query_pd(server_name, sql):
    """查询函数，查询数据库中的数据并返回DataFrame"""
    with get_db_session(server_name) as session:
        # 执行SQL查询
        result = session.execute(text(sql))
        # 获取列名
        column_names = result.keys()
        # 获取所有行的数据
        data = result.fetchall()
        # 创建DataFrame\
        df = pd.DataFrame(data, columns=column_names)
        return df


def choose_database(database: str) -> Client:
    """
    选取clickhouse中所需要读取的数据库

    参数:
    database: 数据库名称  分为datayes和dataapi

    返回:
    Client: clickhouse读取的交互的客户端接口
    """
    click_client = Client(
        host="192.168.1.91",
        port="9000",
        user="uer",
        password="your_password",
        database=database,
    )
    return click_client, database


def get_df(database: str, table: str, start: str, end: str) -> pd.DataFrame:
    """
    获取选取数据表中给定日期区间的所有数据

    参数:
    database: 数据库名称  分为datayes和dataapi
    table: 表格名称, 格式为字符串
    start: 日期数据，格式为 YYYYMMDD
    end:   日期数据，格式为 YYYYMMDD

    返回:
    DataFrame: 包含所选表格对应日期区间的所有数据
    """
    click_client, database = choose_database(database=database)
    describe_result = click_client.execute(f"DESCRIBE TABLE {table}")
    # 提取列名
    column_names = [row[0] for row in describe_result]
    sql = f"SELECT * FROM {database}.{table} WHERE tradingday BETWEEN {start} AND {end}"
    all_data = click_client.execute(sql)
    df = pd.DataFrame(all_data, columns=column_names)
    return df
