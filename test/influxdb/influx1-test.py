#!/usr/bin/python
# coding=utf-8

# influxdb 1.0版

from datetime import datetime
from influxdb import InfluxDBClient
import random
import time
from my_mysql import Database

# You can generate an API token from the "API Tokens Tab" in the UI


def accessinfluxdb(data):
    influxClient = InfluxDBClient("10.227.64.10", "8086", "root", "11111111", "test")
    try:
        influxClient.write_points(data)
    except Exception as e:
        print(e)
    finally:
        influxClient.close()
    time.sleep(1)


def bagdata():
    bagdata = []
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    searchbag = "select created_time,lpc,currentstation,destination, DEPAIRLINE, DEPFLIGHT, STD, status from ics.onlinebag where created_time > '{}' and status is NULL".format(today)
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.110.191.24', port='3306')
    queryResult = cursor.run_query(searchbag)
    for row in queryResult:
        bag_dist = {
            'measurement': 'bags',
            'tags': {
                'createdTime': row[0],
                "flight": "{}".format(row[4], row[5]),
                "STD": row[6]
                        },
            'fields': {
                    "lpc": float(row[1]),
                    "currentstation":row[2],
                    "destination": row[3],
                    "status": random.randint(0, 4)
                    }
                    }
        bagdata.append(bag_dist)
    return bagdata


def main():
    while True:
        data = bagdata()
        accessinfluxdb(data)
    # for i in range(100):
    #     accessinfluxdb()
    #     time.sleep(30)


if __name__ == '__main__':
    main()
