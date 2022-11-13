#!/usr/bin/python
# coding=utf-8

# 获取当日行李状态
# 还有一个需要获取最新ID，读取接下来的行李，现在有可能有遗留
# v0.2


import cx_Oracle
import logging
import sched
import time
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/writeonlinebag.log',
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
    sqlquery = "WITH cr AS ( SELECT * FROM WC_PACKAGEINFO  WHERE  idevent > {} and idevent <= {} AND EXECUTEDTASK = 'Registration'  AND DEPAIRLINE IS NOT NULL  and TARGETPROCESSID like 'BSIS%' ORDER BY idevent  ) SELECT DISTINCT(lpc), cr.EVENTTS, bid, pid, CURRENTSTATIONID, L_DESTINATIONSTATIONID, DEPAIRLINE, DEPFLIGHT, ffs.STD ,ffs.INTERNATIONALORDOMESTIC FROM cr, FACT_FLIGHT_SUMMARIES_V ffs  WHERE concat( cr.DEPAIRLINE, cr.DEPFLIGHT ) = ffs.FLIGHTNR  AND ffs.STD > TRUNC( SYSDATE +8/24 )  AND arrivalordeparture = 'D' ".format(startID, endID)
    logging.warning("{}".format(sqlquery))
    data = accessOracle(sqlquery)
    for row in data:
        searchlpcquery = "select lpc from onlinebag where created_time > curdate() and lpc = {}".format(row[0])
        lpc = cursor.run_query(searchlpcquery)
        if not lpc:
            localTime = row[1] + datetime.timedelta(hours=8)
            create_time = localTime.strftime("%Y-%m-%d %H:%M:%S")
            sqlquery = "insert into ics.onlinebag (created_time, lpc, bid, pid, currentstation,destination, DEPAIRLINE, DEPFLIGHT, STD, flighttype)  values ('{}',{},{},{},'{}','{}','{}','{}', '{}', '{}')".format(create_time, row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
            cursor.run_query(sqlquery)
            # 修改创建时间为bsm时间 开始
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            find_bsm = "WITH ar AS ( SELECT IDEVENT FROM WC_PACKAGEDATA WHERE lpc = {} ) SELECT EVENTTS  FROM WC_PACKAGEDATA  WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM ar)".format(row[0])
            try:
                # 存在无BSM的行李，需改用try
                bsm_created_time = accessOracle(find_bsm)[0][0]
                localBMSTime = bsm_created_time + datetime.timedelta(hours=8)
                strlocalBMSTime = localBMSTime.strftime("%Y-%m-%d %H:%M:%S")
                updateBSMtime = "update onlinebag set created_time = '{}' where lpc = {} and created_time > '{}'".format(strlocalBMSTime, row[0], today)
                cursor.run_query(updateBSMtime)
                # logging.info("{} {}".format(strlocalBMSTime, row[0]))
                # 修改创建时间为bsm时间 结束
            except Exception as e:
                logging.error(updateBSMtime)
                logging.error(e)
            logging.info("write down online bag data for lpc:{} ".format(row[0]))
    updateIDnumber = "update ics.commonidrecord set IDnumber= {} where checktablename = 'WC_PACKAGEINFO' and user = 'writeonlinebag' ".format(endID)
    cursor.run_query(updateIDnumber)


def main():
    startIDquery = "select idnumber from commonidrecord where user = 'writeonlinebag' and checktablename = 'WC_PACKAGEINFO'"    
    startID = cursor.run_query(startIDquery)[0][0]
    while True:
        # schedule.run_pending()
        # time.sleep(10)
        endIDquery = "select max(IDEVENT) from WC_PACKAGEINFO"
        endID = accessOracle(endIDquery)[0][0]
        if endID - startID > 10000:
            startID = endID - 10000
        s = sched.scheduler(time.time, time.sleep)
        s.enter(60, 1, collectbaginfo, (startID, endID))
        s.run()
        startID = endID


if __name__ == '__main__':
    main()
