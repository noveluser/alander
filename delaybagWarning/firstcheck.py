#!/usr/bin/python
# coding=utf-8

# 检查当前行李状态
#
# v0.2

import cx_Oracle
import logging
import time
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/onlinebag.log',
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


def firstCheck():    # 需要补充一个STD时间距离现在不到1小时的紧急行李
    before30mins = (datetime.datetime.now() - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    searchbag = "select lpc, created_time, DEPAIRLINE, DEPFLIGHT, STD from ics.onlinebag where created_time > '{}' and status is NULL and (created_time < '{}' or STD < NOW()+INTERVAL 1 HOUR) ".format(today, before30mins)
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(searchbag)
    for lpc_list in queryResult:
        sqlquery = "WITH cr AS ( SELECT * FROM WC_PACKAGEINFO WHERE lpc = {} AND TARGETPROCESSID LIKE 'ODB%' AND EXECUTEDTASK = 'Deregistration' ORDER BY EVENTTS DESC ) SELECT CURRENTSTATIONID, L_DESTINATIONSTATIONID  FROM  WC_PACKAGEINFO  WHERE  IDEVENT = ( SELECT max( IDEVENT ) FROM cr )".format(lpc_list[0])
        destinationResult = accessOracle(sqlquery)
        for row in destinationResult:
            if row[0] == row[1]:     # 当前位置就是目的地
                updatebagstatus = "update onlinebag set status = 'arrived', currentstation='{}',destination = '{}' where lpc = {}".format(row[0], row[1], lpc_list[0])
                queryResult = cursor.run_query(updatebagstatus)
                # logging.info(queryResult)
            elif int(row[0]) in [41, 42, 81, 82, 220, 221]:   # 已到达弃包处
                updatebagstatus = "update onlinebag set status = 'dump', currentstation='{}',destination = '{}' where lpc = {}".format(row[0], row[1], lpc_list[0])
                queryResult = cursor.run_query(updatebagstatus)
                logging.info("the bag:{} from flight:{}{} was dump,it is location in {}".format(lpc_list[0], lpc_list[2], lpc_list[3], row[0]))
            else:
                searchBagPosition = "with dr as ( SELECT  CURRENTSTATIONID,  IDEVENT,  EVENTTS,  lpc,  bid,  pid,  DEPAIRLINE,  DEPFLIGHT,  EXECUTEDTASK,  L_DESTINATIONSTATIONID,  TARGETPROCESSID  FROM  WC_PACKAGEINFO  WHERE  lpc = {}  ) select * from dr where IDEVENT = ( SELECT max( IDEVENT ) FROM dr )".format(lpc_list[0])
                # searchBagPosition = "select count(*) from  WC_PACKAGEINFO"
                position = accessOracle(searchBagPosition)
                if int(position[0][0]) in [100, 110, 200, 210]:
                    logging.info("the bag:{} from flight:{}{} is location in store {}".format(lpc_list[0], lpc_list[2], lpc_list[3], position[0][0]))
                    updatebagstatus = "update onlinebag set status = 'store', currentstation='{}',destination = '{}' where lpc = {}".format(position[0][0], row[1], lpc_list[0])
                    queryResult = cursor.run_query(updatebagstatus)
                    searchbag = "select lpc from ics.storebag where lpc = {}".format(lpc_list[0])
                    lpc = cursor.run_query(searchbag)
                    if not lpc:
                        addStoreBag = "insert into ics.storebag (created_time, lpc, DEPAIRLINE,  DEPFLIGHT, STD) values ('{}', {}, '{}', '{}', '{}'); ".format(lpc_list[1], lpc_list[0], lpc_list[2], lpc_list[3], lpc_list[4])
                        queryResult = cursor.run_query(addStoreBag)
                else:
                    updatebaglocation = "update onlinebag set currentstation='{}',destination = '{}' where lpc = {}".format(position[0][0], position[0][9], lpc_list[0])
                    queryResult = cursor.run_query(updatebaglocation)
                    searchbag = "select lpc from ics.delaybag where lpc = {}".format(lpc_list[0])
                    lpc = cursor.run_query(searchbag)
                    if not lpc:
                        addDelayBag = "insert into ics.delaybag (created_time, lpc, DEPAIRLINE, DEPFLIGHT, STD) values ('{}', {}, '{}', '{}', '{}'); ".format(lpc_list[1], lpc_list[0], lpc_list[2], lpc_list[3], lpc_list[4])
                        queryResult = cursor.run_query(addDelayBag)
                    logging.info("the bag:{} didn't arrive, the lastest position is {}".format(lpc_list[0], position[0][0]))


def main():
    while True:
        firstCheck()
        time.sleep(60)


if __name__ == '__main__':
    main()
