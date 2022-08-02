#!/usr/bin/python
# coding=utf-8

# wangle
# 获取设备错误日志
# v0.2

import cx_Oracle
import logging
import sched
import time
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/equipment.log',
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
    sqlquery = "SELECT DT_CREATED,SEVERITY,ISC_ID,DESCRIPTION FROM FACT_EQUIPMENTEVENT_SUMMARIES  WHERE id > {} and id < {} order by id ".format(startID, endID)
    data = accessOracle(sqlquery)
    for row in data:
        match row[1]:
            case "INFO":
                logging.info("{}--{}--{}".format(row[0], row[2], row[3]))
            case "LOW":
                logging.warning("{}--{}--{}".format(row[0], row[2], row[3]))
            case "MEDIUM":
                logging.error("{}--{}--{}".format(row[0], row[2], row[3]))
            case "HIGH":
                logging.critical("{}--{}--{}".format(row[0], row[2], row[3]))


def main():
    startIDquery = "select currentID from equipmentlogID"
    startID = cursor.run_query(startIDquery)[0][0]
    while True:
        endIDquery = "select max(ID) from FACT_EQUIPMENTEVENT_SUMMARIES"
        endID = accessOracle(endIDquery)[0][0]
        s = sched.scheduler(time.time, time.sleep)
        s.enter(60, 1, collectbaginfo, (startID, endID))
        s.run()
        startID = endID


if __name__ == '__main__':
    main()
