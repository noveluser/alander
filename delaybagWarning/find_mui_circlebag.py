#!/usr/bin/python
# coding=utf-8

# 临时查找多次循环行李
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
                    filename='/data/package/crontab/log/overcirclebag.log',
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


def secondcheck():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    searchbag = "select lpc, created_time, DEPAIRLINE, DEPFLIGHT, STD from ics.delaybag where created_time > '{}' ".format(today)    # 暂时
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(searchbag)
    for lpc_list in queryResult:
        sqlquery = "select lpc, sum( CASE WHEN EXECUTEDTASK = 'AutoScan' THEN 1 ELSE 0 END ) autoscan  FROM WC_PACKAGEINFO  WHERE lpc ={} AND TARGETPROCESSID LIKE 'BSIS%' and CURRENTSTATIONID in (581,582,583,584,585,586,587,588) and EVENTTS > TO_TIMESTAMP( '{}', 'YYYY-MM-DD' )GROUP BY lpc".format(lpc_list[0], today)
        Result = accessOracle(sqlquery)
        if Result[0][1] > 4:
            add_mulcirclebag = "insert into ics.overcirclebag (created_time, lpc, autoscantimes) values ('{}', '{}', {})".format(today, Result[0][0], Result[0][1])
            queryResult = cursor.run_query(add_mulcirclebag)
            logging.info("the bag:{} cricle time is {}".format(lpc_list[0], Result[0][1]))


def main():
    check_interval = 60  # 可配置参数
    while True:
        try:
            start_time = time.time()
            secondcheck()
            elapsed = time.time() - start_time
            # 动态调整休眠时间保证间隔准确
            sleep_time = max(0, check_interval - elapsed)
            logging.info(f"检查完成，耗时{elapsed:.2f}秒，下次检查将在{sleep_time}秒后")
            time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"检查过程中发生异常: {str(e)}", exc_info=True)
            # 异常后保持运行，可根据需要调整重试策略
            time.sleep(10)  # 短暂休眠后重试


if __name__ == '__main__':
    main()
