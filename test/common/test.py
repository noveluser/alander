#!/usr/bin/python
# coding=utf-8

from my_mysql import Database
import datetime
import pandas as pd
import logging
import json


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//dump.log',
                    filemode='a')


def main():
    # 假设data是你的JSON数据
    # startday = datetime.datetime.strptime(config.get(reportname, 'startDay'), "%Y%m%d")
    today = datetime.datetime.now().date()
    file_list = []
    for i in range(8, 1, -1):
        nexttime = today - datetime.timedelta(days=i)
        nextday = nexttime.strftime("%Y-%m-%d")
        outfeedFile = "all-{}.xlsx".format(nextday)
        file_list.append(outfeedFile)
    print(file_list)



if __name__ == '__main__':
    main()

