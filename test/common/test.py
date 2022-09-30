#!/usr/bin/python
# coding=utf-8

# 将导出的infeed数据按照特定格式提取到一个xls文件里
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


def get_bagcount(infeedFile, olddf):
    originDf = pd.read_csv(infeedFile)
    df = originDf.fillna(0)   # 将NAN转换成0
    bag_dictionary = {"SAT-DC01": 0, "SAT-DC02": 0, "SAT-DC03": 0, "SAT-DC04": 0, "SAT-DC05": 0, "SAT-DC06": 0, "SAT-DC07": 0, "SAT-DC08": 0, "SAT-DC09": 0}
    for idx, row in df.iterrows():    # 迭代数据 以键值对的形式 获取 每行的数据
        totalbagnumber = row["LocalBags"] + row["TransferBags"] + row["UnknownBags"]
        # choice(row["REGISTER_LOCATION"], totalbagnumber)
        if row["REGISTER_LOCATION"] == "SAT-DC01":
            bag_dictionary["SAT-DC01"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC02":
            bag_dictionary["SAT-DC02"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC03":
            bag_dictionary["SAT-DC03"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC04":
            bag_dictionary["SAT-DC04"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC05":
            bag_dictionary["SAT-DC05"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC06":
            bag_dictionary["SAT-DC06"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC07":
            bag_dictionary["SAT-DC07"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC08":
            bag_dictionary["SAT-DC08"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-DC09":
            bag_dictionary["SAT-DC09"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "SAT-OOG":
            bag_dictionary["SAT-OOG"] = row["LocalBags"]
        else:
            # print(row)
            pass
    values = bag_dictionary.values()
    bagcount_list = list(values)
    df2 = pd.DataFrame([bagcount_list])
    df1 = pd.concat([olddf, df2])
    return df1


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
