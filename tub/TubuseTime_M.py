#!/usr/bin/python
# coding=utf-8

# test
#
# v0.2


import pandas as pd
import datetime
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
    while endDay < startDate + datetime.timedelta(days=31):
        currentDay = endDay.strftime("%d-%m-%Y")
        line = [currentDay]
        type = ["< 25000", ">= 25000"]
        for tub_type in type:
            query = "select count(*) from tubstatus where date = '{}' and usetimes > 0 and id {}".format(currentDay, tub_type)
            queryResult = cursor.run_query(query)
            line.append(queryResult[0][0])
        df2 = pd.DataFrame([line])
        df = pd.concat([df, df2])
        # df =df.append(line)
        line.clear()
        endDay = endDay + datetime.timedelta(days=1)
    with pd.ExcelWriter("{}tub_m.xlsx".format(file_path)) as writer:
        df.to_excel(writer)


if __name__ == '__main__':
    file_path = "c://1//"
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    startDay = '01-05-2024 00:00:00'
    startDate = datetime.datetime.strptime(startDay, "%d-%m-%Y %H:%M:%S")
    main()
