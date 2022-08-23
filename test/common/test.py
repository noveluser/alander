#!/usr/bin/python
# coding=utf-8

# 获取当日行李状态
# 还有一个需要获取最新ID，读取接下来的行李，现在有可能有遗留
# v0.2

import cx_Oracle
import logging
import sched
import time
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//temp.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def collectbaginfo():
    sqlquery = "SELECT EVENTTS FROM WC_TRACKINGREPORT where rownum = 1"
    logging.warning("{}".format(sqlquery))
    data = accessOracle(sqlquery)
    for row in data:
        print(type(row[0]))
        localTime = row[0] + datetime.timedelta(hours=8)
        create_time = localTime.strftime("%Y-%m-%d %H:%M:%S")
        print(create_time)


def main():
    collectbaginfo()


if __name__ == '__main__':
    main()
