#!/usr/bin/python
# coding=utf-8

import pymysql
import sys
import logging
from threading import Thread, Lock
from queue import Queue


class NewDatabase:
    """Database connection class."""

    def __init__(self, pool_size=10, **kwargs):
        self.kwargs = kwargs
        self.pool_size = pool_size

        # 创建队列用于存储连接
        self.queue = Queue(self.pool_size)
        # 创建锁
        self.lock = Lock()
        # 初始化连接池
        self.init_pool()

    def init_pool(self):
        # 创建指定数量的连接并加入队列
        for i in range(self.pool_size):
            self.queue.put(self.open_connection())

    def open_connection(self):
        """Connect to MySQL Database."""
        try:
            return pymysql.connect(
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=5,
                **self.kwargs
            )
        except pymysql.MySQLError as e:
            logging.error(e)
            sys.exit()

    def get_connection(self):
        # 从队列中获取一个连接
        with self.lock:
            return self.queue.get()

    def return_connection(self, connection):
        # 将连接返回队列中
        with self.lock:
            self.queue.put(connection)

    def run_query(self, query):
        # 从队列中获取一个连接
        connection = self.get_connection()
        """Execute SQL query."""
        with connection.cursor() as cur:
            if 'select' in query or 'SELECT' in query:
                cur.execute(query)
                result = cur.fetchall()
                cur.close()
                return result
            result = cur.execute(query)
            connection.commit()
            affected = f"{cur.rowcount} rows affected."
            cur.close()
            return affected

    def close(self):
        # 循环队列，关闭所有连接
        while not self.queue.empty():
            connection = self.queue.get()
            connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
