#!/usr/bin/python
# coding=utf-8


# 更新航班行李实时数据
# 10分钟一次
# v0.2


import logging
import cx_Oracle
import time
from my_mysql import Database

logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/data/package/crontab/log/updateflight&bagNumber.log',
                    filename='c://work//log//1.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def confirmbaginfo(item):
    if not item[4] is None:
        if not item[3] is None:
            destination = item[3]
        else:
            destination = item[4]
        if item[4] == "unkonwn":
            destination = item[4]
    else:
        destination = "transiting"
    bagNumber = item[2]
    return destination, bagNumber


def confirmpid(IDcondition):
    sqlquery = "with bags as ( select lpc, pid, flightnr from temp_bags where {} ) select flight.ARRIVALORDEPARTURE from bags left join flight on bags.flightnr = flight.flightnr limit 1".format(IDcondition)
    data = accessOracle(sqlquery)
    

def collect(pid):
    b = ["None"]
    a = b[1][0]
    if not a is None:
        print("ok")
    else:
        print("error")


def main():
    # while True:
    #     s = sched.scheduler(time.time, time.sleep)
    #     s.enter(600, 1, collect)
    #     s.run()
    collect(10201451)


if __name__ == '__main__':
    main()
