#!/usr/bin/python
# coding=utf-8

# 获取当日到港行李及运输时长
# author wangle
# v0.1


import schedule
import logging
from asyncio import exceptions
from influxdb import InfluxDBClient
import cx_Oracle
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/flightstatic.log',
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
    sqlquery = "SELECT FLIGHTNR, ARRIVALORDEPARTURE,INTERNATIONALORDOMESTIC ,std,HANDLER,HANDLING_TERMINAL,TOTAL_NUMBER_OF_BAGS,TOTAL_NUMBER_ONTIME FROM FACT_FLIGHT_SUMMARIES_V  WHERE  trunc(EVENTTS)=trunc(sysdate) ORDER BY ID"
    flightdata = accessOracle(sqlquery)
    data = influxdata(flightdata)
    influxClient = InfluxDBClient("10.31.1.170", "8086", "szit", "11111111", "vanderlande")
    try:
        influxClient.write_points(data)
    except Exception as e:
        logging.error(e)
    finally:
        influxClient.close()


def influxdata(datalist):
    ''' datalist 格式实例 (HU7668, A, D, 'ZH9878', datetime.datetime(2022, 10, 25, 21, 10), 'ST3', 'T3', None, None)
    '''
    flightdata = []
    for row in datalist:
        if not row[6]:
            totalbags = None    # 0意味着未获取
        else:
            totalbags = int(row[6])
        if not row[7]:
            totalontimebags = None    # 0意味着未获取
        else:
            totalontimebags = int(row[7])
        bag_dist = {
            'measurement': 'temp_currentflight',
            'tags': {
                "type": row[1],
                "zone": row[2],
                "STD": row[3],
                "parking": row[4],
                "handing": row[5]
                        },
            'fields': {
                    "flight": row[0],
                    "totalbags": totalbags,
                    "totalontimebags": totalontimebags
                    }
                    }
        flightdata.append(bag_dist)
    return flightdata


def main():
    schedule.every(3600).seconds.do(collectinfo)
    while True:
        schedule.run_pending()


if __name__ == "__main__":
    main()
