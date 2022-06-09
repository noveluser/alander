#!/usr/bin/python
# coding=utf-8

# influxdb 1.0版

from asyncio import exceptions
import datetime
from influxdb import InfluxDBClient
import time
import logging
from my_mysql import Database

logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/writeinfluxdb.log',
                    filemode='a')


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


def bagdata():
    bagdata = []
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    # before30mins = (datetime.datetime.now() - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    searchbag = "select created_time,lpc,currentstation,destination, DEPAIRLINE, DEPFLIGHT, STD, status from ics.onlinebag where created_time > '{}' and (STATUS != 'arrived' OR STATUS IS NULL) ".format(today)
    queryResult = cursor.run_query(searchbag)
    for row in queryResult:
        try:
            destination = int(row[3])
        except:   # 遗留问题，如何优雅的输出exceptions
            if row[3] == "100,110,200,210,220,221,42,82":
                destination = 1000    # 弃包和中转总称
            elif row[3] == "220,221,41,42,81,82":
                destination = 1001  # 弃包地
            elif row[3] == "None":
                logging.error("destination write error.{}".format(row[3]))
                destination = None
            else:
                destination = 1002   # 其他异常
                logging.error("1002 error-{}-{}".format(row[1], row[3]))
        match row[7]:
            case "arrived":
                status = 1
            case "store":
                status = 2
            case "dump":
                status = 3
            case _:
                status = 4
        if len(row[5]) < 4:
            flightnr = str(row[5]).zfill(4)
        else:
            flightnr = str(row[5])
        flight = "{}{}".format(row[4], flightnr)
        bag_dist = {
            'measurement': 'bags',
            'tags': {
                'createdTime': row[0],
                "flight": flight,
                "STD": row[6]
                        },
            'fields': {
                    "lpc": int(row[1]),
                    "currentstation": int(row[2]),
                    "destination": destination,
                    "status": status
                    }
                    }
        bagdata.append(bag_dist)
    searchdumpbag = "select created_time,lpc,bid,pid,REGISTER_LOCATION, DEREGISTER_LOCATION, flight from ics.dumpbag where created_time > '{}' ".format(today)
    queryResult = cursor.run_query(searchdumpbag)
    for row in queryResult:
        if not row[1]:
            lpc = ''
        else:
            lpc = row[1]
        if not row[6]:
            flight = ''
        else:
            flight = row[6]
        dumpbag_dist = {
            'measurement': 'dumpbags',
            'tags': {
                'createdTime': row[0],
                "lpc": lpc,
                "bid": row[2],
                "REGISTER_LOCATION": row[4],
                "DEREGISTER_LOCATION": row[5],
                "FLIGHT": flight
                        },
            'fields': {
                    "pid": int(row[3])
                    }
                    }
        bagdata.append(dumpbag_dist)
    searchdelaybag = "with cr as ( select lpc  from delaybag where created_time > curdate() ) select  created_time, onlinebag.lpc, currentstation, destination, DEPAIRLINE, DEPFLIGHT, STD, `status` from onlinebag ,cr where onlinebag.lpc = cr.lpc  and created_time > CURDATE() "
    queryResult = cursor.run_query(searchdelaybag)
    for row in queryResult:
        try:
            destination = int(row[3])
        except:   # 遗留问题，如何优雅的输出exceptions
            if row[3] == "100,110,200,210,220,221,42,82":
                destination = 1000    # 弃包和中转总称
            elif row[3] == "220,221,41,42,81,82":
                destination = 1001  # 弃包地
            elif row[3] == "None":
                logging.error("destination write error.{}".format(row[3]))
                destination = None
            else:
                destination = 1002   # 其他异常
                logging.error("1002 error-{}-{}".format(row[1], row[3]))
        match row[7]:
            case "arrived":
                status = 1
            case "store":
                status = 2
            case "dump":
                status = 3
            case _:
                status = 4
        delaybag_dist = {
            'measurement': 'delaybags',
            'tags': {
                'createdTime': row[0],
                "flight": "{}{}".format(row[4], row[5]),
                "STD": row[6]
                        },
            'fields': {
                    "lpc": int(row[1]),
                    "currentstation": int(row[2]),
                    "destination": destination,
                    "status": status
                    }
                    }
        bagdata.append(delaybag_dist)
    return bagdata


def main():
    while True:
        data = bagdata()
        accessinfluxdb(data)
        time.sleep(60)
    # for i in range(100):
    #     accessinfluxdb()
    #     time.sleep(30)


if __name__ == '__main__':
    main()
