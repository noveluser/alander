#!/usr/bin/python
# coding=utf-8

# test
#
# v0.2

from email.utils import encode_rfc2231
import pandas as pd
# import pymysql
import time
import datetime
import os
import logging
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='d://1//bagnumber.log',
                    filename='c://work//log//bagnumber.log',
                    filemode='a')


def main():
    df = pd.DataFrame()
    endDay = startDate
    while endDay < startDate + datetime.timedelta(days=7):
        currentDay = endDay.strftime("%d-%m-%Y")
        line = [currentDay]
        for i in range(6):
            if i+1 > 5:
                decision = "> 5"
            else:
                decision = "= {}".format(i+1)
            query = "select count(*) from tubstatus where date = '{}' and usetimes {}".format(currentDay, decision)
            queryResult = cursor.run_query(query)
            line.append(queryResult[0][0])
        df2 = pd.DataFrame([line])
        df = pd.concat([df, df2])
        # df =df.append(line)
        line.clear()
        endDay = endDay + datetime.timedelta(days=1)
    with pd.ExcelWriter("{}tub_w.xlsx".format(file_path)) as writer:
        df.to_excel(writer)



if __name__ == '__main__':
    file_path = "D://workcenter//整理后文档//各类报告//202209W//"
    cursor = Database(dbname='test', username='it', password='1111111', host='10.31.9.24',port='3306')
    startDay = '28-02-2022 00:00:00'
    startDate = datetime.datetime.strptime(startDay, "%d-%m-%Y %H:%M:%S")
    main()
