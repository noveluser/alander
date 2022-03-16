#!/usr/bin/python
# coding=utf-8

# 第二次检查疑似延误行李
#
# v0.3

import cx_Oracle
import pymysql
import logging
import time
import datetime
from my_mysql import Database
import sched


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//delaybag.log',
                    filemode='a')


def accessOracle(query):
    # dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def secondcheck():
    print("start")
    today = datetime.datetime.now().strftime("%d-%m-%Y")
    searchbag = "select lpc from ics.delaybag where created_time > '{} and lpc = 3479853085' ".format(today)
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.110.191.24',port='3306')
    queryResult = cursor.run_query(searchbag)
    sqlquery = "WITH cr AS ( SELECT * FROM WC_PACKAGEINFO WHERE lpc = {} AND TARGETPROCESSID LIKE 'BSIS%' AND EXECUTEDTASK = 'Deregistration' ORDER BY EVENTTS DESC ) SELECT CURRENTSTATIONID, L_DESTINATIONSTATIONID , DEPAIRLINE, DEPFLIGHT FROM  WC_PACKAGEINFO  WHERE  IDEVENT = ( SELECT max( IDEVENT ) FROM cr )".format(3479853085)
    destinationResult = accessOracle(sqlquery)
    print("3")
    for row in destinationResult:
        searchBagPosition = "with dr as ( SELECT  CURRENTSTATIONID,  IDEVENT,  EVENTTS,  lpc,  bid,  pid,  DEPAIRLINE,  DEPFLIGHT,  EXECUTEDTASK,  L_DESTINATIONSTATIONID,  TARGETPROCESSID  FROM  WC_PACKAGEINFO  WHERE  lpc = {}  ) select * from dr where IDEVENT = ( SELECT max( IDEVENT ) FROM dr )".format(3479853085)
        position = accessOracle(searchBagPosition)
        if int(position[0][0]) not in [100, 110, 200, 210]:
            logging.info("the bag:{} from flight:{}{} is location in store {}".format(3479853085, row[2], row[3], row[0]))
            searchbag = "select lpc from ics.storebag where lpc = {}".format(3479853085)
            lpc = cursor.run_query(searchbag)
            print("true")
        else:
            print("false")
            searchbag = "select lpc from ics.delaybag where lpc = {}".format(3479853085)
            lpc = cursor.run_query(searchbag)
            logging.info("the bag:{} didn't arrive, the lastest position is {}".format(3479853085, position[0][0]))
    print("end")


def print_time(a='default'):
    print("From print_time", time.time(), a)


def main():
    while True:
        # secondcheck()
        # time.sleep(600)
        list1 = []
        for i in list1:
            if i[0] == i[1]:
                print("1")
            else:
                print("23")
        time.sleep(1)
        print("ok")

if __name__ == '__main__':
    main()
