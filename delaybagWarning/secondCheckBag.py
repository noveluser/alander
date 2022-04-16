#!/usr/bin/python
# coding=utf-8

# 第二次检查疑似延误行李
#
# v0.2

from asyncio import exceptions
import cx_Oracle
import logging
import time
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/delaybag.log',
                    filemode='a')


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


def secondcheck():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    searchbag = "select lpc, created_time, DEPAIRLINE, DEPFLIGHT, STD from ics.delaybag where created_time > '{}' ".format(today)
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(searchbag)
    for lpc_list in queryResult:
        sqlquery = "WITH cr AS ( SELECT * FROM WC_PACKAGEINFO WHERE lpc = {} AND TARGETPROCESSID LIKE 'ODB%' AND EXECUTEDTASK = 'Deregistration' ORDER BY EVENTTS DESC ) SELECT CURRENTSTATIONID, L_DESTINATIONSTATIONID , DEPAIRLINE, DEPFLIGHT FROM  WC_PACKAGEINFO  WHERE  IDEVENT = ( SELECT max( IDEVENT ) FROM cr )".format(lpc_list[0])
        destinationResult = accessOracle(sqlquery)
        # print(destinationResult)
        for row in destinationResult:
            if row[0] == row[1]:     # 当前位置就是目的地
                updatebagstatus = "update onlinebag set status = 'arrived', currentstation='{}',destination = '{}' where lpc = {}".format(row[0], row[1], lpc_list[0])
                cursor.run_query(updatebagstatus)
                deleteDelayBag = "DELETE FROM ics.delaybag WHERE lpc = {};".format(lpc_list[0])
                cursor.run_query(deleteDelayBag)
                logging.info("the bag {} have already arrived destination:{} and removed from delaybag list".format(lpc_list[0], row[0]))
            elif int(row[0]) in [41, 42, 81, 82, 220, 221]:   # 已到达弃包处
                updatebagstatus = "update onlinebag set status = 'dump', currentstation='{}',destination = '{}' where lpc = {}".format(row[0], row[1], lpc_list[0])
                cursor.run_query(updatebagstatus)
                deleteDelayBag = "DELETE FROM ics.delaybag WHERE lpc = {};".format(lpc_list[0])
                cursor.run_query(deleteDelayBag)
                logging.info("the bag:{} from flight:{}{} has already dumped to {} and removed from the delaybag list".format(lpc_list[0], lpc_list[2], lpc_list[3], row[0]))
            else:
                searchBagPosition = "with dr as ( SELECT  CURRENTSTATIONID,  IDEVENT,  EVENTTS,  lpc,  bid,  pid,  DEPAIRLINE,  DEPFLIGHT,  EXECUTEDTASK,  L_DESTINATIONSTATIONID,  TARGETPROCESSID  FROM  WC_PACKAGEINFO  WHERE  lpc = {}  ) select * from dr where IDEVENT = ( SELECT max( IDEVENT ) FROM dr )".format(lpc_list[0])
                # searchBagPosition = "select count(*) from  WC_PACKAGEINFO"
                position = accessOracle(searchBagPosition)
                updatebaglocation = "update onlinebag set currentstation='{}',destination = '{}' where lpc = {}".format(position[0][0], position[0][9], lpc_list[0])
                logging.info(updatebaglocation)
                queryResult = cursor.run_query(updatebaglocation)
                if int(position[0][0]) in [100, 110, 200, 210]:    # 这部分好像不需要
                    logging.info("the bag:{} from flight:{}{} is location in store {}".format(lpc_list[0], lpc_list[2], lpc_list[3], row[0]))
                    updatebagstatus = "update onlinebag set status = 'store' where lpc = {}".format(lpc_list[0])
                    queryResult = cursor.run_query(updatebagstatus)
                    searchbag = "select lpc from ics.storebag where lpc = {}".format(lpc_list[0])
                    lpc = cursor.run_query(searchbag)
                    if not lpc:
                        addStoreBag = "insert into ics.storebag (created_time, lpc, DEPAIRLINE,  DEPFLIGHT, STD) values ('{}', {}, '{}', {}, '{}'); ".format(lpc_list[1], lpc_list[0], lpc_list[2], lpc_list[3], lpc_list[4])
                        queryResult = cursor.run_query(addStoreBag)
                else:
                    logging.info("the bag:{} didn't arrive, the lastest position is {}".format(lpc_list[0], position[0][0]))


def main():
    while True:
        secondcheck()
        time.sleep(60)


if __name__ == '__main__':
    main()
