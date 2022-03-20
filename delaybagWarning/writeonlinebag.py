#!/usr/bin/python
# coding=utf-8

# 获取当日行李状态
# 还有一个需要获取最新ID，读取接下来的行李，现在有可能有遗留
# v0.2

import cx_Oracle
import pymysql
import logging
import sched
import time
import datetime


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//writeonlinebag.log',
                    filemode='a')


# 打开数据库连接
# db = pymysql.connect(host='10.110.191.24',
db = pymysql.connect(host='10.31.9.24',
                     user='it',
                     password='1111111',
                     database='test',
                     charset='utf8mb4')


def writeMysql(sql):
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        # db.commit()
        # print("success")
    except Exception as e:
        # Rollback in case there is any error
        db.rollback()
        logging.error(e)
    # 关闭数据库连接
    # cursor.close()
    # db.close()


def collectbaginfo():
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    sqlquery = "WITH cr AS ( SELECT EVENTTS, lpc, bid, pid, CURRENTSTATIONID, L_DESTINATIONSTATIONID, DEPAIRLINE, DEPFLIGHT FROM WC_PACKAGEINFO  WHERE EVENTTS >= to_date( to_char( SYSDATE - 1/ ( 24 * 60 ), 'yyyy-mm-dd hh24:mi:ss' ), 'yyyy-mm-dd hh24:mi:ss' )  AND TARGETPROCESSID LIKE 'BSIS%'  AND EXECUTEDTASK = 'Registration'  AND DEPAIRLINE IS NOT NULL  ORDER BY EVENTTS  ) SELECT cr.*, ffs.STD  FROM cr ,FACT_FLIGHT_SUMMARIES_V ffs where  concat(cr.DEPAIRLINE,cr.DEPFLIGHT) = ffs.FLIGHTNR and ffs.STD > TRUNC(SYSDATE) "
    c.execute(sqlquery)  # use triple quotes if you want to spread your query across multiple lines
    i = 0
    for row in c:
        localTime = row[0] + datetime.timedelta(hours=8)
        create_time = localTime.strftime("%Y-%m-%d %H:%M:%S")
        sqlquery = "insert into ics.onlinebag (created_time, lpc, bid, pid, currentstation,destination, DEPAIRLINE, DEPFLIGHT, STD)  values ('{}',{},{},{},'{}','{}','{}',{}, '{}')".format(create_time, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
        writeMysql(sqlquery)      # 写入mysql
        # logging.info("{} {}".format(row[1], i))
        """完成ICSbag表的批量写入"""
        i += 1
        if i > 100:
            try:
                db.commit()
                # print("finish 100")
            except pymysql.MySQLError as e:
                logging.error(e)
            i = 0
    conn.close()
    """写入最后少于100部分进入commit"""
    try:
        db.commit()
    except pymysql.MySQLError as e:
        logging.error(e)
    logging.info("write down online bag data ")


def main():
    # schedule.every(1).minutes.do(collectbaginfo)
    while True:
        # schedule.run_pending()
        # time.sleep(10)
        s = sched.scheduler(time.time, time.sleep)
        s.enter(60, 1, collectbaginfo)
        s.run()


if __name__ == '__main__':
    main()
