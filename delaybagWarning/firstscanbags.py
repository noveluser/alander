#!/usr/bin/python
# coding=utf-8

# 获取当日所有行李状态
# 包括空框，UFO等等
# v0.2

from asyncio import exceptions
import cx_Oracle
import logging
import schedule
import time
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/firstscanbags.log',
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
    startIDquery = "select idnumber from commonidrecord where checktablename = 'WC_PACKAGEINFO' and user = 'firstscanbags' "
    startID = cursor.run_query(startIDquery)[0][0]
    endIDquery = "select max(IDEVENT) from WC_PACKAGEINFO"
    endID = accessOracle(endIDquery)[0][0]
    if endID - startID > 10000:
        startID = endID - 10000
    '''先尽快完成实体，先跳过SQL写法,先用多次SQL查询，效率上会有影响，数据查询不会有问题'''
    sqlquery = "SELECT DISTINCT pid FROM WC_PACKAGEINFO WHERE IDEVENT > {} AND IDEVENT <= {} and EXECUTEDTASK = 'AutoScan' ".format(startID, endID)
    # 注意，我这里筛选了EXECUTEDTASK = 'AutoScan'，暂不知是否存在没有autoscan，但是行李正常的情况，也许存在中转行李这种情况，后面再查
    data = accessOracle(sqlquery)
    for row in data:
        searchlpcquery = "WITH ar AS ( SELECT * FROM WC_PACKAGEINFO WHERE pid = {} AND EXECUTEDTASK IS NOT NULL ) , br as ( SELECT EVENTTS, lpc, pid, ( DEPAIRLINE || DEPFLIGHT ) flightnr, CURRENTSTATIONID, L_DESTINATIONSTATIONID  FROM WC_PACKAGEINFO  WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM ar ) ) select br.*,std FROM br left join FACT_FLIGHT_SUMMARIES_V ffs on br.FLIGHTNR = ffs.FLIGHTNR  AND ffs.STD > TRUNC( SYSDATE +8/24 )".format(row[0])
        # baginfo格式EVENTTS, lpc, pid, flightnr, CURRENTSTATIONID, L_DESTINATIONSTATIONID，STD
        baginfo = accessOracle(searchlpcquery)
        for item in baginfo:
            if item[1] and item[1] != 0:       # 存在LPC可能拥有多个PID的情况，此时以LPC为准。但MCS会输入特殊LPC000000，所以必须不为0
                searchbag = "select lpc from ics.temp_bags where lpc = {} and TO_DAYS(bsm_time) > TO_DAYS(NOW())-2 ".format(item[1])
            else:
                searchbag = "select pid from ics.temp_bags where pid = {} and TO_DAYS(bsm_time) > TO_DAYS(NOW())-2 ".format(item[2])
            identification = cursor.run_query(searchbag)
            if not identification:
                if not item[6]:     # 为解决字符串为空时无法写入sql的问题的临时解决办法
                    STD = 'Null'
                else:
                    STD = "'{}'".format(item[6])
                if not item[3]:
                    flightnr = 'Null'
                else:
                    flightnr = "'{}'".format(item[3])
                localBMSTime = item[0] + datetime.timedelta(hours=8)
                register_time = localBMSTime.strftime("%Y-%m-%d %H:%M:%S")
                orignal_sqlquery = "insert into ics.temp_bags (bsm_time, lpc, pid, flightnr, current_location, orginal_destination, STD)  values ('{}',{},{},{},'{}','{}',{})".format(register_time, item[1], item[2], flightnr, item[4], item[5], STD)
                optimizal_sqlquery = orignal_sqlquery.replace("None", "Null")
                logging.info(optimizal_sqlquery)
                cursor.run_query(optimizal_sqlquery)
                logging.info(item[1])
                if not item[4]:
                    logging.error("异常 {}".format(item))
    updateIDnumber = "update ics.commonidrecord set IDnumber= {} where checktablename = 'WC_PACKAGEINFO' and user = 'firstscanbags' ".format(endID)
    cursor.run_query(updateIDnumber)


def main():
    schedule.every(60).seconds.do(collectbaginfo)
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
