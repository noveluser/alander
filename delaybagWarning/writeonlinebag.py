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


def collectbaginfo(startID, endID):
    sqlquery = "WITH cr AS ( SELECT EVENTTS, lpc, bid, pid, CURRENTSTATIONID, L_DESTINATIONSTATIONID, DEPAIRLINE, DEPFLIGHT,idevent FROM WC_PACKAGEINFO  WHERE IDEVENT > {} and IDEVENT <= {}  AND EXECUTEDTASK = 'Registration'  AND DEPAIRLINE IS NOT NULL  ORDER BY idevent  ) SELECT cr.*, ffs.STD  FROM cr, FACT_FLIGHT_SUMMARIES_V ffs  WHERE concat( cr.DEPAIRLINE, cr.DEPFLIGHT ) = ffs.FLIGHTNR  AND ffs.STD > TRUNC( SYSDATE ) and idevent = ( SELECT MIN( idevent) FROM cr )".format(startID, endID)
    data = accessOracle(sqlquery)
    for row in data:
        localTime = row[0] + datetime.timedelta(hours=8)
        create_time = localTime.strftime("%Y-%m-%d %H:%M:%S")
        sqlquery = "insert into ics.onlinebag (created_time, lpc, bid, pid, currentstation,destination, DEPAIRLINE, DEPFLIGHT, STD)  values ('{}',{},{},{},'{}','{}','{}',{}, '{}')".format(create_time, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[9])
        cursor.run_query(sqlquery)
    updateIDnumber = "update ics.IDnumber set currentIDnumber= {}".format(endID)
    cursor.run_query(updateIDnumber)
    logging.info("write down online bag data ")


def main():
    startIDquery = "select currentIDNumber from IDnumber"
    startID = cursor.run_query(startIDquery)[0][0]
    while True:
        # schedule.run_pending()
        # time.sleep(10)
        endIDquery = "select max(IDEVENT) from WC_PACKAGEINFO"
        endID = accessOracle(endIDquery)[0][0]
        s = sched.scheduler(time.time, time.sleep)
        s.enter(60, 1, collectbaginfo, (startID, endID))
        s.run()
        startID = endID


if __name__ == '__main__':
    main()
