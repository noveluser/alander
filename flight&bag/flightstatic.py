#!/usr/bin/python
# coding=utf-8

# 获取当日航班信息并写入influxdb
# author wangle
# v0.1


import schedule
import logging
from influxdb import InfluxDBClient
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/flightstatic.log',
                    # filename='c://work//log//test.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')


def collectinfo(sqlquery):  # 收集航班信息
    flightdata = cursor.run_query(sqlquery)
    data = influxFlightData(flightdata)
    influxClient = InfluxDBClient("10.31.1.170", "8086", "szit", "11111111", "vanderlande")
    try:
        influxClient.write_points(data)
    except Exception as e:
        logging.error(e)
    finally:
        influxClient.close()


def influxFlightData(datalist):
    ''' datalist 格式实例 (HU7668, datetime.datetime(2022, 10, 25, 21, 10), D, ST3, M68, M68, 6, notsorted,1,NULL,0)
    '''
    flightdata = []
    for row in datalist:
        bag_dist = {
            'measurement': 'flight',
            'tags': {
                "flight": row[0],
                "STD": row[1],
                "type": row[2],
                "parking": row[3],
                "original_destination": row[4],
                "first_destination": row[5],
                "second_destination": row[7],
                "third_destination": row[9]
                        },
            'fields': {
                    "first_sort_bags": row[6] if row[6] else 0,
                    "second_sort_bags": row[8] if row[8] else 0,
                    "third_sort_bags": row[10] if row[10] else 0
                    }
                    }
        flightdata.append(bag_dist)
        logging.info(bag_dist)
    return flightdata


def main():
    flightquery = "select flightnr,std,ARRIVALORDEPARTURE,`HANDLER`,original_destination,first_destination,first_sort_bags,second_destination,second_SORT__BAGS,third_destination,third_SORT_BAGS from flight where to_days(create_time) = to_days(now()) order by std"
    schedule.every(600).seconds.do(collectinfo, flightquery)
    while True:
        schedule.run_pending()
    # collectinfo()


if __name__ == "__main__":
    main()
