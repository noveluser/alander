#!/usr/bin/python
# coding=utf-8

# 更新当日所有新增行李状态
#
# v0.2


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


def updatebaglocation(scanqueuenumber):
    # findbagquery = "select lpc, pid, latest_time, status, flightnr from temp_bags where status {} and TO_DAYS(bsm_time) > TO_DAYS(NOW()) - 2 ".format(scanqueuenumber)
    findbagquery = "select lpc, pid, latest_time, status, flightnr from temp_bags where status {} and bsm_time >= DATE_ADD(NOW(),INTERVAL - 3 HOUR) ".format(scanqueuenumber)
    data = cursor.run_query(findbagquery)
    for row in data:
        if row[0]:
            ID = "lpc = {}".format(row[0])
            judging_condition = "lpc"
        else:
            ID = "pid = {}".format(row[1])
            judging_condition = "pid"
        searchlpcquery = "WITH baginfo AS ( SELECT CURRENTSTATIONID, IDEVENT, pid, lpc FROM WC_PACKAGEINFO WHERE {} ), maxbaginfo AS ( SELECT * FROM baginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM baginfo ) ), trackinginfo AS (  SELECT   IDEVENT,   lpc,   pid,   EVENTTS,   AREAID,   ZONEID,   EQUIPMENTID,   L_DESTINATION,   L_CARRIER,   PROCESSPLANIDNAME   FROM   WC_TRACKINGREPORT track   WHERE   {}   ),  maxtrakinginfo AS ( SELECT * FROM trackinginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM trackinginfo ) ) SELECT  CURRENTSTATIONID,  maxbaginfo.lpc,  maxbaginfo.pid,  EVENTTS,  ( AREAID || '.' || ZONEID || '.' || EQUIPMENTID ) location,  L_DESTINATION,  L_CARRIER,  PROCESSPLANIDNAME  FROM  maxbaginfo  LEFT JOIN maxtrakinginfo ON maxbaginfo.{} = maxtrakinginfo.{}".format(ID, ID, judging_condition, judging_condition)
        baginfo = accessOracle(searchlpcquery)
        for item in baginfo:
            if item[5] is None:
                logging.error("destination为空。{}".format(item))
            # item格式为CURRENTSTATIONID, lpc, pid, EVENTTS, location, L_DESTINATION, L_CARRIER,flightnr
            # 当packageinfo未找到lpc及flightnr时，要从trackingreport中补充
            if int(item[0]) in [41, 42, 81, 82, 220, 221]:    # 已到达弃包处
                status = "dump"
            elif item[0] == item[5]:
                status = "arrived"
            elif int(item[0]) in [100, 110, 200, 210]:
                status = "store"
            else:
                status = "unkonwn"
            if item[4] in ["12.43.1", "13.43.1", "28.43.1", "29.43.1"]:   # 当最新位置是43.1时，就是去MCS
                status = "mcs"
            if item[3] or status == 'arrived':     # 如果有更新记录或行李状态发生变化时，则update
                logging.info("pid={},locate={}".format(item[2], item[3]))
                localtime = item[3] + datetime.timedelta(hours=8)
                latest_time = localtime.strftime("%Y-%m-%d %H:%M:%S.%f")
                # 当行李安检未通过时，有LPC，但行李不会进入系统，可能去开包间，这时WC_TRACKINGREPORT不会有记录，存在有LPC，但无track记录的情况，这时如果status已更新，不需要更新记录
                if row[2] != localtime:       # 存在更新的记录,注意row[2]是timestamp，而latest_time是字符串，不能直接比较

                    if item[6]:    # 如果有托盘号，取数字，否则直接取None
                        tubid = int(item[6].split(",")[0][3:])
                    else:
                        tubid = 'Null'
                    updatebagstatus = "update temp_bags set latest_time = '{}', current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time,  item[4], item[5], tubid, status, ID)
                    optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                    logging.info(optimizal_sqlquery)
                    result = cursor.run_query(optimizal_sqlquery)
                    logging.info("{} 位置已更新至{},uptade result:{}".format(ID, item[4], result))
                elif status != row[3]:  # 如果不存在更新记录，但行李状态发生变化，只更新状态.这种情况发生在packageinfo更新记录速度比tracking慢的时候
                    updatebagstatus = "update temp_bags set status = '{}' where {}".format(status, ID)
                    optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                    logging.info(optimizal_sqlquery)
                    result = cursor.run_query(optimizal_sqlquery)
                    logging.info("{} 状态已更新至{},uptade result:{}".format(ID, status, result))
                else:
                    logging.info("lpc={},pid={}位置未变动".format(row[0], row[1]))
            else:
                logging.info("lpc={},pid={}不存在位置更新数据".format(row[0], row[1]))


def updatebagstatus(scanqueuenumber):
    findbagquery = "select lpc, pid, latest_time, status, flightnr from temp_bags where status {} and bsm_time >= DATE_ADD(NOW(),INTERVAL - 6 HOUR) ".format(scanqueuenumber)
    data = cursor.run_query(findbagquery)
    for row in data:
        if row[0]:
            ID = "lpc = {}".format(row[0])
            judging_condition = "lpc"
        else:
            ID = "pid = {}".format(row[1])
            judging_condition = "pid"
        searchlpcquery = "WITH baginfo AS ( SELECT CURRENTSTATIONID, IDEVENT, pid, lpc FROM WC_PACKAGEINFO WHERE {} ), maxbaginfo AS ( SELECT * FROM baginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM baginfo ) ), trackinginfo AS (  SELECT   IDEVENT,   lpc,   pid,   EVENTTS,   AREAID,   ZONEID,   EQUIPMENTID,   L_DESTINATION,   L_CARRIER,   PROCESSPLANIDNAME   FROM   WC_TRACKINGREPORT track   WHERE   {}   ),  maxtrakinginfo AS ( SELECT * FROM trackinginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM trackinginfo ) ) SELECT  CURRENTSTATIONID,  maxbaginfo.lpc,  maxbaginfo.pid,  EVENTTS,  ( AREAID || '.' || ZONEID || '.' || EQUIPMENTID ) location,  L_DESTINATION,  L_CARRIER,  PROCESSPLANIDNAME  FROM  maxbaginfo  LEFT JOIN maxtrakinginfo ON maxbaginfo.{} = maxtrakinginfo.{}".format(ID, ID, judging_condition, judging_condition)
        baginfo = accessOracle(searchlpcquery)
        for item in baginfo:
            # item格式为CURRENTSTATIONID, lpc, pid, EVENTTS, location, L_DESTINATION, L_CARRIER,flightnr
            # 当packageinfo未找到lpc及flightnr时，要从trackingreport中补充，这两个值只在第一次扫描中输入，二次，三次扫描不更新Lpc和flightnr,先观察2天
            if item[3]:     # 如果有更新记录，则updata，否则无操作
                localtime = item[3] + datetime.timedelta(hours=8)
                latest_time = localtime.strftime("%Y-%m-%d %H:%M:%S.%f")
                # 当行李安检未通过时，有LPC，但行李不会进入系统，可能去开包间，这时WC_TRACKINGREPORT不会有记录，存在有LPC，但无track记录的情况，这时如果status已更新，不需要更新记录
                # if row[2] != localtime:       # 存在更新的记录,注意row[2]是timestamp，而latest_time是字符串，不能直接比较
                # 需增加一个到达MCS的目的地
                if int(item[0]) in [41, 42, 81, 82, 220, 221]:    # 已到达弃包处
                    status = "dump"
                elif item[0] == item[5]:
                    status = "arrived"
                elif int(item[0]) in [100, 110, 200, 210]:
                    status = "store"
                else:
                    status = "unkonwn"
                    logging.info("pid={},CURRENTSTATIONID={},DESTINATION={}".format(item[2], item[0], item[5]))
                if item[4] in ["12.43.1", "13.43.1", "28.43.1", "29.43.1"]:   # 当最新位置是43.1时，就是去MCS
                    status = "mcs"
                if item[6]:    # 如果有托盘号，取数字，否则直接取None
                    tubid = int(item[6].split(",")[0][3:])
                else:
                    tubid = 'Null'
                # 更正row[4],查看row[4]不存在时是否报错
                if item[1]:     # 如果lpc不存在baginfo中，,试图获取traking记录
                    lpc = item[1]
                else:
                    lpc = None
                if item[7]:     # 如果航班不存在baginfo表记录
                    flightnr = "'{}{}'".format(item[7].split("_")[0], item[7].split("_")[1])
                else:
                    flightnr = 'Null'
                if row[0] and row[4]:      # LPC及flightnr都存在
                    updatebagstatus = "update temp_bags set latest_time = '{}' , current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, item[4], item[5], tubid, status, ID)
                elif row[0] and not row[4]:   # LPC存在，flightnr不存在，只更新flightnr
                    updatebagstatus = "update temp_bags set latest_time = '{}', flightnr = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, flightnr, item[4], item[5], tubid, status, ID)
                elif not row[0] and row[4]:   # LPC不存在，flightnr存在,只更新lpc
                    updatebagstatus = "update temp_bags set latest_time = '{}' ,lpc = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, lpc, item[4], item[5], tubid, status, ID)
                else:     # LPC及flightnr都不存在
                    updatebagstatus = "update temp_bags set latest_time = '{}' ,lpc = {}, flightnr = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, lpc, flightnr, item[4], item[5], tubid, status, ID)
                optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                logging.info(optimizal_sqlquery)
                result = cursor.run_query(optimizal_sqlquery)
            else:
                updatebagstatus = "update temp_bags set status = '{}' where {}".format("notsorted", ID)             # 更新temp_bags当前位置
                optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                logging.info(optimizal_sqlquery)
                result = cursor.run_query(optimizal_sqlquery)
            logging.info("{} uptade result:{}".format(ID, result))


def main():
    schedule.every(600).seconds.do(updatebagstatus, scanqueuenumber="is null")
    schedule.every(110).seconds.do(updatebaglocation, scanqueuenumber="not in ('arrived', 'dump')")
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
