"""Oracle 数据库访问层：连接、查询执行。"""
import oracledb

from . import config


def connect():
    """创建一条新的 Oracle 连接。"""
    dsn = oracledb.makedsn(config.DB_HOST, config.DB_PORT, service_name=config.DB_SERVICE)
    return oracledb.connect(user=config.DB_USER, password=config.DB_PASSWORD, dsn=dsn)


def fetch_all(query, params=None):
    """执行查询并返回全部结果行（每行为 position 元组）。调用方负责关闭连接。"""
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params or {})
            return cursor.fetchall()
