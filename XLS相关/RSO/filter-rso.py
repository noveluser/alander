#!/usr/bin/python
# coding=utf-8

# 将icsexpection日志里Push与pull类型匹配
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


# def data_write(xlsfile, datas, heads):
#     wb = Workbook()
#     sheet = wb.active
#     # 将数据写入第 i 行，第 j 列
#     i = 0
#     for number in range(len(heads)):   # 写文件头
#         sheet.cell(row=1, column=number+1, value=heads[number])
#     for data in datas:
#         for j in range(len(data)):  # 写正文
#             sheet.cell(row=i+2, column=j+1, value=data[j])
#         i = i + 1
#     wb.save(xlsfile)


def main():
    sheetName = 'Sheet1'
    df = pd.read_excel("d://data//rso//202302/raw.xlsx", sheet_name=sheetName)
    # df1 = pd.DataFrame(columns=["EVENTTS", "date", "hour", "time", "ICSEVENT", "ID", "LIC", "STATUS", "zone"])
    matchPushDf = notmatchPushDf = matchPullDf = notmatchPullDf = pd.DataFrame(columns=["EVENTTS", "date", "hour", "time", "ICSEVENT", "ID", "LIC", "STATUS", "zone"])
    # matchPushDf = pd.DataFrame(columns=["EVENTTS", "date", "hour", "time", "ICSEVENT", "ID", "LIC", "STATUS", "zone"])
    # matchPullDf = pd.DataFrame(columns=["EVENTTS", "date", "hour", "time", "ICSEVENT", "ID", "LIC", "STATUS", "zone"])
    pullIDgroup = []
    for idx, row in df.iterrows():    # 迭代数据 以键值对的形式 获取 每行的数据
        # device_code = "{}.{}.{}".format(row["ICSEVENT"], row["ID"], row["zone"])
        event = row["ICSEVENT"]
        flag = 0
        # eventTime = datetime.datetime.strptime(row["EVENTTS"], "%Y/%m/%d %H:%M")
        eventTime = row["EVENTTS"]
        if event == "ICS-PUSHED-LOSTANDFOUND":
            searchRowNumber = 0   # 默认搜索接下来0行
            maxRowNumber = 100
            if idx + 1 + maxRowNumber > len(df):
                maxRowNumber = len(df) - idx - 1
            i = 0
            # for i in range(0, maxRowNumber):   # 为何for循环i> maxRowNumber时还继续循环？
            while i < maxRowNumber:
                nextEventTime = df.iloc[[idx+i], [0]].values[0][0]
                if eventTime + datetime.timedelta(minutes=30) > nextEventTime:
                    searchRowNumber = i
                else:  # 如果日志超出半小时，跳出循环，搜索页只计算到＜30mins
                    i = 200
                i += 1
            j = 1
            while j < searchRowNumber+1:
                id = row["ID"]
                if df.iloc[[idx+j], [4]].values[0][0] == "ICS-PULLED-LOSTANDFOUND" and df.iloc[[idx+j], [5]].values[0][0] == id:
                    # 将匹配的Push放入matchPushDf
                    matchPushDfTemp = df.loc[[idx]]
                    matchPushDf = pd.concat([matchPushDf, matchPushDfTemp])
                    # 将匹配的Pull放入matchPushDf
                    matchPullDfTemp = df.loc[[idx+j]]
                    matchPullDf = pd.concat([matchPullDf, matchPullDfTemp])
                    flag = 1   # 存在push and pull
                    pullIDgroup.append(idx+j)
                    j = 200
                j += 1
            if flag == 0:
                # 将不匹配的Push放入notmatchPushDf
                notmatchPushDfTemp = df.loc[[idx]]
                notmatchPushDf = pd.concat([notmatchPushDf, notmatchPushDfTemp])
        else:
            # 将不匹配的Pull放入nomatchPullDf
            if idx not in pullIDgroup:
                notmatchPullDfTemp = df.loc[[idx]]
                notmatchPullDf = pd.concat([notmatchPullDf, notmatchPullDfTemp])
            # else:
            #     # 已经记录到matchpull dataframe里
            #     continue
    # print(pullIDgroup)
    matchPushDf.to_excel("d://data//rso//202302/matchPush_test.xlsx")
    matchPullDf.to_excel("d://data//rso//202302/matchPull_test.xlsx")
    notmatchPushDf.to_excel("d://data//rso//202302/notmatchPush_test.xlsx")
    notmatchPullDf.to_excel("d://data//rso//202302/notmatchPull_test.xlsx")


if __name__ == '__main__':
    main()
