#!/usr/bin/python
# coding=utf-8

# 收集航班信息并写入mysql
# author wangle
# v0.1


import sched
import time
import logging
import cx_Oracle
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/collectFlight.log',
                    # filename='c://work//log//test.log',
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
    except Exception as e:
        logging.error(e)
        result = []
    finally:
        conn.close()
    return result


def collectinfo():  # 收集航班信息
    sqlquery = "SELECT flightdate,FLIGHTNR, std,ARRIVALORDEPARTURE,INTERNATIONALORDOMESTIC, HANDLER,HANDLING_TERMINAL, INTIME_ALLOCATED_SORT,UPDATETS FROM FACT_FLIGHT_SUMMARIES_V  WHERE  trunc(EVENTTS)=trunc( SYSDATE + 8/24,'dd')  ORDER BY std"
    flightdata = accessOracle(sqlquery)
    for row in flightdata:
        find_sameflight_sqlquery = "select create_time, flightnr, std, ARRIVALORDEPARTURE, INTERNATIONALORDOMESTIC, HANDLER, HANDLING_TERMINAL, original_destination from flight where create_time = '{}' and flightnr = '{}'".format(row[0], row[1])
        search_result = cursor.run_query(find_sameflight_sqlquery)
        if not search_result:
            orignal_sqlquery = "insert into ics.flight (create_time, flightnr, std, ARRIVALORDEPARTURE, INTERNATIONALORDOMESTIC, HANDLER, HANDLING_TERMINAL, original_destination, update_time)  values ('{}','{}','{}','{}','{}','{}','{}','{}', '{}')".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
        else:
            orignal_sqlquery = "update ics.flight set create_time = '{}', flightnr = '{}', std = '{}', ARRIVALORDEPARTURE = '{}', INTERNATIONALORDOMESTIC = '{}', HANDLER = '{}', HANDLING_TERMINAL = '{}', original_destination = '{}', update_time = '{}' where create_time = '{}' and flightnr = '{}' ".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[0], row[1])
        optimizal_sqlquery = orignal_sqlquery.replace("'None'", "Null")
        logging.info(optimizal_sqlquery)
        result = cursor.run_query(optimizal_sqlquery)
        logging.info("{} uptade result:{}".format(row[1], result))


def main():
    while True:
        s = sched.scheduler(time.time, time.sleep)
        s.enter(3600, 1, collectinfo)
        s.run()
    # collectinfo()


if __name__ == "__main__":
    main()
