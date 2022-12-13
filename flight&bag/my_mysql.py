#!/usr/bin/python
# coding=utf-8

import pymysql
import sys
from threading import Thread, Lock
from queue import Queue
import logging


class Database:
    """Database connection class."""

    def __init__(self, host, username, password, port, dbname, pool_size=10):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        # self.conn = None
        # 创建队列用于存储连接
        self.queue = Queue(self.pool_size)
        # 初始化连接池
        self.init_pool()

    def init_pool(self):
        # 创建指定数量的连接并加入队列
        for i in range(self.pool_size):
            self.queue.put(self.open_connection())

    def open_connection(self):
        """Connect to MySQL Database."""
        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.username,
                passwd=self.password,
                db=self.dbname,
                connect_timeout=5,
                cursorclass=pymysql.cursors.DictCursor
            )
        except pymysql.MySQLError as e:
            logging.error(e)
            sys.exit()
        finally:
            # logging.info('Connection opened successfully.')
            pass

    def get_connection(self):
        # 从队列中获取一个连接
        return self.queue.get()

    def return_connection(self, connection):
        # 将连接返回队列中
        self.queue.put(connection)

    def run_query(self, query):
        """Execute SQL query."""
        connection = self.get_connection()
        try:
            # self.open_connection()
            with connection.cursor() as cur:
                if "select" in query or "SELECT" in query:
                    records = []
                    cur.execute(query)
                    result = cur.fetchall()
                    for row in result:
                        records.append(row)
                    finally_result = records
                    # cur.close()
                    # return records
                result = cur.execute(query)
                self.conn.commit()
                affected = f"{cur.rowcount} rows affected."
                # cur.close()
                # return affected
                finally_result = affected
        except pymysql.MySQLError as e:
            logging.error(e)
            sys.exit()
            # pass
        finally:
            cur.close()
            return finally_result

    def close(self):
        # 循环队列，关闭所有连接
        while not self.queue.empty():
            connection = self.queue.get()
            connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
