#!/usr/bin/python
# coding=utf-8

# 检查弃包行李
#
# v0.2

from asyncio import exceptions
import cx_Oracle
import logging
import time
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/dumpbag.log',
                    filemode='a')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    try:
        c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
        result = c.fetchall()
    except Exception as e:
        logging.error(e)
        result = []
    finally:
        conn.close()
    return result


def dumpcheck():
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    searchbag = "SELECT DEREGISTER_DT, lpc, xbid, xpid, REGISTER_LOCATION, DEREGISTER_LOCATION, flightnr FROM FACT_BAG_SUMMARIES_V  WHERE DEREGISTER_DT >= to_date( to_char( SYSDATE - 1/ ( 24 * 60 ), 'yyyy-mm-dd hh24:mi:ss' ), 'yyyy-mm-dd hh24:mi:ss' )  AND DEREGISTER_LOCATION IN ( 'M41', 'M81', 'SAT-M10a', 'SAT-M10b' ) order by  DEREGISTER_DT DESC"
    destinationResult = accessOracle(searchbag)
    for row in destinationResult:
        searchbag = "select bid  from ics.dumpbag where bid = '{}'".format(row[2])
        bid = cursor.run_query(searchbag)
        if not bid:     # 新行李
            if row[1]:
                lpc = row[1]
            else:
                lpc = 'NULL'
            if row[6]:
                flight = row[6]
            else:
                flight = ""
            adddumpBag = "insert into ics.dumpbag (created_time, lpc, bid, pid, REGISTER_LOCATION, DEREGISTER_LOCATION, flight) values ('{}', {}, '{}', {}, '{}', '{}', '{}'); ".format(row[0], lpc, row[2], row[3], row[4], row[5], flight)
            cursor.run_query(adddumpBag)
            logging.info("the bag {} have already arrived destination:{}".format(row[1], row[5]))


def main():
    while True:
        dumpcheck()
        time.sleep(60)


if __name__ == '__main__':
    main()
