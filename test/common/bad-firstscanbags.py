#!/usr/bin/python
# coding=utf-8

# 获取当日所有行李状态
# 包括空框，UFO等等
# v0.2


import cx_Oracle
import logging
import datetime
import time
from my_mysql import NewDatabase
from apscheduler.schedulers.background import BackgroundScheduler


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/data/package/crontab/log/firstscanbags.log',
                    filename='c://work//log//1.log',
                    filemode='a')


# envioments
# cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
# cursor = Database(pool_size=10, host='10.31.9.24', user='it', password='1111111', database='ics', port=3306)


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def executemysql(query):
    with NewDatabase(pool_size=10, host='10.31.9.24', user='it', password='1111111', database='ics', port=3306) as pool:
        return pool.run_query(query)


def collectbaginfo():
    startIDquery = "select idnumber from commonidrecord where checktablename = 'WC_PACKAGEINFO' and user = 'firstscanbags' "
    try:
        startID = executemysql(startIDquery)[0].get("idnumber")
    except Exception as e:
        logging.error(e)
    endIDquery = "select max(IDEVENT) from WC_PACKAGEINFO"
    endID = accessOracle(endIDquery)[0][0]
    if endID - startID > 10000:
        endID = startID + 10000
    '''先尽快完成实体，先跳过SQL写法,先用多次SQL查询，效率上会有影响，数据查询不会有问题'''
    sqlquery = "SELECT DISTINCT pid FROM WC_PACKAGEINFO WHERE IDEVENT > {} AND IDEVENT <= {} and EXECUTEDTASK = 'AutoScan' order by pid ".format(startID, endID)
    # 注意，我这里筛选了EXECUTEDTASK = 'AutoScan'，暂不知是否存在没有autoscan，但是行李正常的情况，也许存在中转行李这种情况，后面再查
    data = accessOracle(sqlquery)
    for row in data:
        print(len(pid_list))
        if row[0] not in pid_list:
            searchlpcquery = "WITH ar AS ( SELECT * FROM WC_PACKAGEINFO WHERE pid = {} AND EXECUTEDTASK IS NOT NULL ) , br as ( SELECT EVENTTS, lpc, pid, ( DEPAIRLINE || DEPFLIGHT ) flightnr, CURRENTSTATIONID, L_DESTINATIONSTATIONID  FROM WC_PACKAGEINFO  WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM ar ) ) select br.*,std FROM br left join FACT_FLIGHT_SUMMARIES_V ffs on br.FLIGHTNR = ffs.FLIGHTNR  AND ffs.STD > TRUNC( SYSDATE +8/24 )".format(row[0])
            # baginfo格式EVENTTS, lpc, pid, flightnr, CURRENTSTATIONID, L_DESTINATIONSTATIONID，STD
            baginfo = accessOracle(searchlpcquery)
            for item in baginfo:
                if item[1] and item[1] != 0:       # 存在LPC可能拥有多个PID的情况，此时以LPC为准。但MCS会输入特殊LPC000000，所以必须不为0
                    searchbag = "select lpc from ics.temp_bags where lpc = {} and TO_DAYS(bsm_time) > TO_DAYS(NOW())-2 ".format(item[1])
                else:
                    searchbag = "select pid from ics.temp_bags where pid = {} and TO_DAYS(bsm_time) > TO_DAYS(NOW())-2 ".format(item[2])
                identification = executemysql(searchbag)
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
                    register_time = localBMSTime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    orignal_sqlquery = "insert into ics.temp_bags (bsm_time, lpc, pid, flightnr, current_location, orginal_destination, STD)  values ('{}',{},{},{},'{}','{}',{})".format(register_time, item[1], item[2], flightnr, item[4], item[5], STD)
                    optimizal_sqlquery = orignal_sqlquery.replace("None", "Null")
                    logging.info(optimizal_sqlquery)
                    executemysql(optimizal_sqlquery)
                    # with NewDatabase(pool_size=10, host='10.31.9.24', user='it', password='1111111', database='ics', port=3306) as pool:
                    #     pool.run_query(optimizal_sqlquery)
                    if not item[4]:
                        logging.error("异常 {}".format(item))
            pid_list.append(row[0])
        else:
            logging.info("pass {}".format(row[0]))
    updateIDnumber = "update ics.commonidrecord set IDnumber= {} where checktablename = 'WC_PACKAGEINFO' and user = 'firstscanbags' ".format(endID)
    executemysql(updateIDnumber)
    logging.info("step 6")


def highFrequencyWord(query):
    recentpid_list = []
    pidResult = executemysql(query)
    if pidResult:
        for item in pidResult:
            recentpid_list.append(item.get("pid"))
        return pidResult
    else:
        return None


if __name__ == "__main__":
    highquery = "SELECT pid FROM  temp_bags  WHERE  (status not in ('arrived', 'dump') or status is null)  AND bsm_time >= DATE_ADD(NOW(),INTERVAL - 1 HOUR)  order by id"
    pid_list = executemysql(highquery)
    scheduler = BackgroundScheduler()
    # 添加任务
    # scheduler.add_job(executemysql, 'interval', [query, ], seconds=10)
    scheduler.add_job(collectbaginfo, 'interval', max_instances=10, seconds=60)
    scheduler.add_job(highFrequencyWord, 'interval', [highquery, ], hours=1)
    scheduler.start()
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(10)
    except Exception as e:
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        logging.error(e)
        scheduler.shutdown()

