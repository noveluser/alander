#!/usr/bin/python
# coding=utf-8

# influxdb 2.0版

from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import random
import time

# You can generate an API token from the "API Tokens Tab" in the UI


def accessinfluxdb():
    token = "mytoken"
    org = "my-org"
    bucket = "my-bucket"
    with InfluxDBClient(url="http://10.227.64.10:8087", token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        point = Point("test") \
            .tag("createdTime", "2022-03-18 10:10:25") \
            .field("lpc", random.randint(1, 100000000)) \
            .field("currentstation", random.randint(1, 99)) \
            .field("destination", random.randint(1, 99)) \
            .tag("flight", "CR2312") \
            .tag("STD", "2022-03-18 16:00:00") \
            .tag("status", random.randint(0, 4)) \
            .time(datetime.utcnow(), WritePrecision.NS)
        write_api.write(bucket, org, point)


def main():
    for i in range(100):
        accessinfluxdb()
        time.sleep(30)


if __name__ == '__main__':
    main()
