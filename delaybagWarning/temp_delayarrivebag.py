#!/usr/bin/python
# coding=utf-8

# 获取当日到港延误行李
# author wangle
# v0.1


import time
import sched
import logging
from asyncio import exceptions
from influxdb import InfluxDBClient
import cx_Oracle
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/temp_delayarrive.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')


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


def collectbaginfo(startID, endID):  # 先处理有LPC的
    sqlquery = "SELECT lpc, xpid,FLIGHTNR,REGISTER_DT,REGISTER_LOCATION, DEREGISTER_DT,DEREGISTER_LOCATION FROM FACT_BAG_SUMMARIES_V  WHERE ID > {} AND ID <= {} and REGISTER_LOCATION like 'SAT%' ORDER BY ID ".format(startID, endID)
    data = accessOracle(sqlquery)
    for row in data:
        during_time = (row[5] - row[3]).seconds
        # if row[0]:
        #     lpc = row[0]
        # else:
        #     lpc = "NULL"
        if during_time > 1000:    # 大于1000秒
            searchlpcquery = "select pid from temp_delayarrivebag where register_time > curdate() and pid = {}".format(row[1])
            pid = cursor.run_query(searchlpcquery)
            if not pid:
                register_time = row[3].strftime("%Y-%m-%d %H:%M:%S")
                deregister_time = row[5].strftime("%Y-%m-%d %H:%M:%S")
                orignal_sqlquery = "insert into ics.temp_delayarrivebag (register_time, lpc, pid, flightnr, REGISTER_LOCATION, deregister_time,DEREGISTER_LOCATION,duringtime)  values ('{}',{},{},'{}','{}','{}','{}', {})".format(register_time, row[0], row[1], row[2], row[4], deregister_time, row[6], during_time)
                optimizal_sqlquery = orignal_sqlquery.replace("None", "Null")
                # logging.info(optimizal_sqlquery)
                cursor.run_query(optimizal_sqlquery)
    updateIDnumber = "update ics.commonidrecord set IDnumber= {} where checktablename = 'FACT_BAG_SUMMARIES_V'".format(endID)
    cursor.run_query(updateIDnumber)
    # 写入influxdb
    searchbag = "select * from temp_delayarrivebag where register_time > curdate() "
    queryResult = cursor.run_query(searchbag)
    data = influxdata(queryResult)
    accessinfluxdb(data)


def accessinfluxdb(data):
    # influxClient = InfluxDBClient("10.227.64.10", "8086", "root", "11111111", "test")
    influxClient = InfluxDBClient("10.31.1.170", "8086", "szit", "11111111", "vanderlande")
    try:
        influxClient.write_points(data)
    except Exception as e:
        logging.error(e)
    finally:
        influxClient.close()
    # time.sleep(1)


def influxdata(delaybaglist):
    ''' delaybaglist 格式实例 (195, 3479036952, 8692780, 'ZH9878', datetime.datetime(2022, 9, 23, 17, 27, 16), datetime.datetime(2022, 9, 23, 17, 37, 17), 'SAT-DC07', '13', 601)
    '''
    bagdata = []
    for row in delaybaglist:
        # logging.info(row)
        # logging.info("lpc:{}".format(row[1]))
        if not row[1]:
            lpc = None    # 0意味着未获取
        else:
            lpc = int(row[1])
        bag_dist = {
            'measurement': 'temp_delayarrivebag',
            'tags': {
                'createdTime': row[4],
                "flight": row[3],
                "REGISTER_LOCATION": row[6],
                "DEREGISTER_DT": row[5],
                "DEREGISTER_LOCATION": row[7]
                        },
            'fields': {
                    "lpc": lpc,
                    "pid": int(row[2]),
                    "DURINGTIME": row[8]
                    }
                    }
        bagdata.append(bag_dist)
    return bagdata


def main():
    startIDquery = "select idnumber from commonidrecord where checktablename = 'FACT_BAG_SUMMARIES_V'"
    startID = cursor.run_query(startIDquery)[0][0]
    while True:
        # schedule.run_pending()
        # time.sleep(10)
        endIDquery = "select max(ID) from FACT_BAG_SUMMARIES"   # 读取FACT_BAG_SUMMARIES_V非常慢，超过10s,所以改用FACT_BAG_SUMMARIES，发现两个表的ID是保持一致的
        endID = accessOracle(endIDquery)[0][0]
        s = sched.scheduler(time.time, time.sleep)
        s.enter(60, 1, collectbaginfo, (startID, endID))
        s.run()
        startID = endID


if __name__ == "__main__":
    main()
