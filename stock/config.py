#!/usr/bin/python3
# coding=utf-8
"""
配置抽离文件 - 数据库、日志、业务参数统一管理
"""
import os
from datetime import datetime

# ====================== 数据库配置（SQLAlchemy）======================
# 登录信息（从你原来的 DB_CONFIG 拆出来）
# DB_HOST = "192.168.87.128"
# DB_PORT = 3306
# DB_USER = "wangxp01"
# DB_PWD = "111111"
# DB_NAME = "stock"
DB_HOST = "172.22.22.11"
DB_PORT = 3306
DB_USER = "wangxp01"
DB_PWD = "111111"
DB_NAME = "stock"
DB_CHARSET = "utf8mb4"

# 拼接成 SQLAlchemy 要求的 URL（关键！）
DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}"

# 纯连接池配置（不再包含登录信息）
DB_CONFIG = {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_recycle": 3600,
    "pool_pre_ping": True
}


# # ====================== 数据库配置（SQLAlchemy）======================
# DB_CONFIG = {
#     "host": "10.31.9.24",
#     "port": 3306,
#     "user": "it",
#     "password": "1111111",
#     "database": "ics",
#     # host='192.168.87.128',
#     # port=3306,
#     # user='root',
#     # password='example',
#     # database='stock',
#     "charset": "utf8mb4",
#     "pool_size": 10,        # 连接池常驻连接数
#     "max_overflow": 20,     # 连接池最大溢出连接数
#     "pool_recycle": 3600,   # 连接自动回收时间（秒），防止超时
#     "pool_pre_ping": True   # 连接前检测，剔除失效连接
# }

# # 拼接SQLAlchemy连接串
# DB_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"

# ====================== 日志配置 ======================
LOG_CONFIG = {
    "level": "DEBUG",        # 日志级别：DEBUG/INFO/WARNING/ERROR/CRITICAL（生产建议INFO/ERROR）
    "file_path": "c://work//log//test.log",  # 日志文件路径
    "file_mode": "a",       # 写入模式：a=追加，w=覆盖
    "datefmt": "%Y-%m-%d %H:%M:%S",
    "format": "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    "console_format": "%(asctime)s - %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s"
}

# ====================== 业务配置 ======================
BUSINESS_CONFIG = {
    "target_date": "2026-03-11",                       # 目标日期，默认当前日期
    "report_time_start": "2011-01-01",                 # 财务数据起始时间
    "sleep_time_min": 0.1,                               # 股票处理间隔最小秒数
    "sleep_time_max": 0.3,                               # 股票处理间隔最大秒数
    "batch_insert_size": 20,                         # 批量插入单次条数（防大数据量溢出）
    "unprocessed_flag": "N" ,                   # 未处理股票标志位（stock_list表）
    "unprocessed_secucode": "300206.sz" ,       # 未处理股票代码，测试条件
    "delay_days": 100,                           # 财报发布日期与财报记录日期的差值
    "db_retry_times": 2
}