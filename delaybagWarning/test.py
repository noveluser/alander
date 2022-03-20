#!/usr/bin/python
# coding=utf-8

# 检查当前行李状态
#
# v0.2

import cx_Oracle
import logging
import time
import random
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//onlinebag.log',
                    filemode='a')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def firstCheck():
    cursor = Database(dbname='test', username='szit', password='11111111', host='10.227.64.10', port='3306')
    for i in range(200):
        sqlquery = "insert  into test.monitorbags (created_time, lpc, currentstation, destination, DEPAIRLINE, DEPFLIGHT, STD, status)  VALUES ('2022-03-18 10:40:25' ,ceiling( RAND()* 500000000), 110, 79, 'CH', '1234', '2022-03-18 16:00:00', {})".format(random.randint(0, 4))
        cursor.run_query(sqlquery)
        time.sleep(30)


def main():
    firstCheck()


if __name__ == '__main__':
    main()
