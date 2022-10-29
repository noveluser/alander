#!/usr/bin/python
# coding=utf-8

# 更新当日所有新增行李状态
#
# v0.2


from asyncio import exceptions
import cx_Oracle
import logging
import schedule
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/secondscanbags.log',
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


def collectbaginfo(scanqueuenumber):
    findbagquery = "select lpc, pid,flightnr from temp_bags where status {} and TO_DAYS(bsm_time) > TO_DAYS(NOW()) - 2 ".format(scanqueuenumber)
    data = cursor.run_query(findbagquery)
    for row in data:
        if row[0]:
            ID = "lpc = {}".format(row[0])
        else:
            ID = "pid = {}".format(row[1])
        searchlpcquery = "WITH baginfo AS ( SELECT CURRENTSTATIONID, IDEVENT, pid, lpc FROM WC_PACKAGEINFO WHERE {} ), maxbaginfo AS ( SELECT * FROM baginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM baginfo ) ), trackinginfo AS (  SELECT   IDEVENT,   lpc,   pid,   EVENTTS,   AREAID,   ZONEID,   EQUIPMENTID,   L_DESTINATION,   L_CARRIER,   PROCESSPLANIDNAME   FROM   WC_TRACKINGREPORT track   WHERE   {}   ),  maxtrakinginfo AS ( SELECT * FROM trackinginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM trackinginfo ) ) SELECT  CURRENTSTATIONID,  maxbaginfo.lpc,  maxbaginfo.pid,  EVENTTS,  ( AREAID || '.' || ZONEID || '.' || EQUIPMENTID ) location,  L_DESTINATION,  L_CARRIER,  PROCESSPLANIDNAME  FROM  maxbaginfo  LEFT JOIN maxtrakinginfo ON maxbaginfo.pid = maxtrakinginfo.pid".format(ID, ID)
        baginfo = accessOracle(searchlpcquery)
        for item in baginfo:
            # item格式为CURRENTSTATIONID, lpc, pid, EVENTTS, location, L_DESTINATION, L_CARRIER,flightnr
            # 当packageinfo未找到lpc时，要从trackingreport中补充
            #     lpc = item[1]v
            # if len(row) < 2:    # 如果flightnr为空
            if item[1]:
                lpc = item[1]
                try:
                    flightnr = "'{}{}'".format(item[7].split("_")[0], item[7].split("_")[1])
                except Exception as e:
                    flightnr = None
                    logging.error(e)
            else:
                flightnr = None
                lpc = None
            if item[0] == item[5]:
                status = "arrived"
            elif int(item[0]) in [41, 42, 81, 82, 220, 221]:   # 已到达弃包处
                status = "dump"
            elif int(item[0]) in [100, 110, 200, 210]:
                status = "store"
            else:
                status = "unkonwn"
            if item[6]:    # 如果有托盘号，取数字，否则直接取None
                tubid = int(item[6].split(",")[0][3:])
            else:
                tubid = 'Null'
            if item[4] == '..':   # 当位置信息为空时
                location = ""
            else:
                location = item[4]
            if item[3]:
                localtime = item[3] + datetime.timedelta(hours=8)
                latest_time = localtime.strftime("%Y-%m-%d %H:%M:%S")
            else:
                latest_time = None
            if latest_time:
                if row[0]:
                    updatebagstatus = "update temp_bags set latest_time = '{}' ,current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, item[4], item[5], tubid, status, ID)
                else:
                    updatebagstatus = "update temp_bags set latest_time = '{}' ,lpc = {}, flightnr = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, lpc, flightnr, location, item[5], tubid, status, ID)             # 更新temp_bags当前位置
            else:
                updatebagstatus = "update temp_bags set status = '{}' where {}".format("notsorted", ID)             # 更新temp_bags当前位置
            optimizal_sqlquery = updatebagstatus.replace("None", "Null")
            result = cursor.run_query(optimizal_sqlquery)
            logging.info(optimizal_sqlquery)
            logging.info("{} uptade result:{}".format(ID, result))


def main():
    schedule.every(600).seconds.do(collectbaginfo, scanqueuenumber="is null")
    schedule.every(60).seconds.do(collectbaginfo, scanqueuenumber="not in ('arrived', 'dump')")
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
