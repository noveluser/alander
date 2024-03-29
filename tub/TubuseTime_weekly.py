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
    while endDay < startDate + datetime.timedelta(days=7):
        currentDay = endDay.strftime("%d-%m-%Y")
        line = [currentDay]
        for i in range(6):
            mintimes = 2*i
            if i == 5:
                maxtimes = 100
            else:
                maxtimes = 2*(i+1) + 1
            query = "select count(*) from tubstatus where date = '{}' and usetimes > {} and usetimes < {}".format(currentDay, mintimes, maxtimes)
            queryResult = cursor.run_query(query)
            # print("queryResult[0]")
            line.append(queryResult[0][0])
        df2 = pd.DataFrame([line])
        df = pd.concat([df, df2])
        # df =df.append(line)
        line.clear()
        endDay = endDay + datetime.timedelta(days=1)
    with pd.ExcelWriter(filename) as writer:
        df.to_excel(writer)


if __name__ == '__main__':
    currentDay = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime("%Y%m%d")
    filename = "c://work//Datacollector//weeklyreport//tubtimes-{}.xlsx".format(currentDay)  # 要追加或者修改表格的文件名。
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    startDate = (datetime.datetime.now()-datetime.timedelta(days=7))
    # startDate = datetime.datetime.strptime(startDay, "%d-%m-%Y %H:%M:%S")
    main()
