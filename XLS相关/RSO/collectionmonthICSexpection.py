#!/usr/bin/python
# coding=utf-8

# 将导出的icsexpection数据按照特定格式提取到一个xls文件里
# Alex.Wang
# v0.1

import logging
import pandas as pd
import configparser
import datetime


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='/root/1.log',
                    filename='d://data//rso//1.log',
                    filemode='a')


def main():
    # 实例化configParser对象
    config = configparser.ConfigParser()
    # -read读取ini文件
    config.read('c://work//conf//icsreport_monthly.ini')
    # -sections得到所有的section，并以列表的形式返回
    reportnames = config.sections()
    for reportname in reportnames:
        file_path = config.get(reportname, 'file_path')
        startday = datetime.datetime.strptime(config.get(reportname, 'startDay'), "%Y%m%d")
        period = config.get(reportname, 'period')
        file_list = []
        for i in range(int(period)):
            nexttime = startday + datetime.timedelta(days=i)
            nextday = nexttime.strftime("%Y-%m-%d")
            outfeedFile = "{}{}.xlsx".format(file_path, nextday)
            file_list.append(outfeedFile)
    df = pd.DataFrame()
    for element in file_list:
        print(element)
        originDf = pd.read_excel(element)
        df1 = originDf.fillna(0)   # 将NAN转换成0
        df = pd.concat([df, df1])
    with pd.ExcelWriter("{}icsexpection.xlsx".format(file_path)) as writer:
        df.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()