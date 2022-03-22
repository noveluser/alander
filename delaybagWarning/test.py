#!/usr/bin/python
# coding=utf-8

# 获取当日行李状态
# 还有一个需要获取最新ID，读取接下来的行李，现在有可能有遗留
# v0.2

import cx_Oracle
import pymysql
import logging
import sched
import time
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//writeonlinebag.log',
                    filemode='a')


# 打开数据库连接
# db = pymysql.connect(host='10.110.191.24',
db = pymysql.connect(host='10.31.9.24',
                     user='it',
                     password='1111111',
                     database='test',
                     charset='utf8mb4')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def writeMysql(sql):
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        # db.commit()
        # print("success")
    except Exception as e:
        # Rollback in case there is any error
        db.rollback()
        logging.error(e)
    # 关闭数据库连接
    # cursor.close()
    # db.close()


def collectbaginfo(id):
    print(id)


def main():
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    maxIDquery = "select currentIDNumber from IDnumber"
    maxID = cursor.run_query(maxIDquery)[0][0]
    while True:
        s = sched.scheduler(time.time, time.sleep)
        s.enter(1, 1, collectbaginfo, (maxID,))
        s.run()


if __name__ == '__main__':
    main()
