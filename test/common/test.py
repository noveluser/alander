#!/usr/bin/python
# coding=utf-8

# 获取当日到港延误行李
# author wangle
# v0.1


import time
import sched
import logging
from asyncio import exceptions

import cx_Oracle
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/data/package/crontab/log/temp_delayarrive.log',
                    filename='c://work//log//colectheartbeatdata.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
# cursor = Database(dbname='ics', username='it', password='1111111', host='10.110.191.24', port='3306')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    try:
        c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
        result = c.fetchall()
    except exceptions as e:
        logging.error(e)
        result = []
    finally:
        conn.close()
    return result


def collectheartbeatdata(startID, endID):  
    sqlquery = "SELECT count(*)  FROM WC_ELLIFESIGNREQUEST  WHERE IDEVENT > {}  AND IDEVENT < {}  AND L_AREAID = 'AreaID=12'".format(startID, endID)
    data = accessOracle(sqlquery)
    logging.info("12fsc have {} records".format(data[0][0]))
    updateIDnumber = "update ics.commonidrecord set IDnumber= {} where checktablename = 'FACT_BAG_SUMMARIES_V'".format(endID)
    cursor.run_query(updateIDnumber)


def main():
    startIDquery = "select idnumber from commonidrecord where checktablename = 'WC_ELLIFESIGNREQUEST'"
    startID = cursor.run_query(startIDquery)[0][0]
    while True:
        # schedule.run_pending()
        # time.sleep(10)
        endIDquery = "select max(IDevent) from WC_ELLIFESIGNREQUEST"   # 读取FACT_BAG_SUMMARIES_V非常慢，超过10s,所以改用FACT_BAG_SUMMARIES，发现两个表的ID是保持一致的
        endID = accessOracle(endIDquery)[0][0]
        s = sched.scheduler(time.time, time.sleep)
        print("start")
        s.enter(60, 1, collectheartbeatdata, (startID, endID))
        s.run()
        print("end")
        startID = endID


if __name__ == "__main__":
    main()
