#!/usr/bin/python
# coding=utf-8

# 将安检日志从数据库导出到xls
# Alex.Wang
# v0.3

import logging
import pandas as pd
# import xlwings as xw
# from openpyxl import load_workbook
# import time
import datetime


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='/root/1.log',
                    filename='d://data//rso//1.log',
                    filemode='a')


def main():
    sheetName = 'Sheet1'
    df = pd.read_excel("d://data//rso//202110/raw_test.xlsx", sheet_name=sheetName)
    # df1 = pd.DataFrame(df, columns=["EVENTTIME", "AREAID", "CLASS", "EQUIPMENTID", "ICSEVENT", "ICSEVENTDETAIL", "ID", "ZONEID"])
    # columns = df.columns.values.tolist()  # 获取excel 表头 ，第一行
    for idx, row in df.iterrows():    # 迭代数据 以键值对的形式 获取 每行的数据
        # device_code = "{}.{}.{}".format(row["ICSEVENT"], row["ID"], row["zone"])
        event = row["ICSEVENT"]
        # eventTime = datetime.datetime.strptime(row["EVENTTS"], "%Y/%m/%d %H:%M")
        eventTime = row["EVENTTS"]
    print(idx)



if __name__ == '__main__':
    main()
