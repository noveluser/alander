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
    findbagquery = "select lpc, pid from temp_bags where status {} and TO_DAYS(bsm_time) > TO_DAYS(NOW()) - 2 ".format(scanqueuenumber)
    data = cursor.run_query(findbagquery)
    for row in data:
        if row[0]:
            ID = "lpc = {}".format(row[0])
        else:
            ID = "pid = {}".format(row[1])
        searchlpcquery = "WITH ar AS ( SELECT IDEVENT, EVENTTS, AREAID, ZONEID, EQUIPMENTID FROM WC_TRACKINGREPORT track WHERE {} ), br AS ( SELECT lpc, pid, EVENTTS, ( AREAID || '.' || ZONEID || '.' || EQUIPMENTID ) location, L_DESTINATION, L_CARRIER  FROM WC_TRACKINGREPORT  WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM ar )  ) , dr AS  (  SELECT CURRENTSTATIONID, IDEVENT, lpc FROM WC_PACKAGEINFO WHERE {} )  SELECT CURRENTSTATIONID, br.* FROM dr, br  WHERE dr.IDEVENT = ( SELECT max( IDEVENT ) FROM dr )".format(ID, ID)
        baginfo = accessOracle(searchlpcquery)
        for item in baginfo:
            # item格式为CURRENTSTATIONID, lpc, pid, EVENTTS, location, L_DESTINATION, L_CARRIER
            if item[0] == item[5]:
                status = "arrived"
            elif int(item[0]) in [41, 42, 81, 82, 220, 221]:   # 已到达弃包处
                status = "dumped"
            elif int(item[0]) in [100, 110, 200, 210]:
                status = "stored"
            else:
                status = "unkonwed"
            if item[6]:    # 如果有托盘号，取数字，否则直接取None
                tubid = int(item[6].split(",")[0][3:])
            else:
                tubid = 'Null'
            latest_time = (item[3] + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            updatebagstatus = "update temp_bags set latest_time = '{}' ,current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, item[4], item[5], tubid, status, ID)             # 更新temp_bags当前位置
            result = cursor.run_query(updatebagstatus)
            logging.info("{} uptade result:{}".format(ID, result))


def main():
    schedule.every(600).seconds.do(collectbaginfo, scanqueuenumber="is null")
    schedule.every(60).seconds.do(collectbaginfo, scanqueuenumber="not in ('arrived', 'dump')")
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
