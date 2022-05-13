#!/usr/bin/python
# coding=utf-8

# 将导出的Outfeed数据按照特定格式提取到一个xls文件里
# Alex.Wang
# v0.1

import logging
import pandas as pd


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='/root/1.log',
                    filename='d://data//rso//1.log',
                    filemode='a')

# init dictionary
bag_dictionary = {"MCS01": 0, "MCS02": 0, "MCS03": 0, "MCS04": 0, "SAT-MCS01": 0, "T3-MCS05": 0, "T3-MCS06": 0}
keys = bag_dictionary.keys()   # 获取关键词列表
chute_list = list(keys)
file_path = "D://workcenter//整理后文档//各类报告//202217W//"
bagDay = ["0501"]
file_list = []
for element in bagDay:
    mcsFile = "{}mcs_{}.csv".format(file_path, element)
    file_list.append(mcsFile)


def get_bagcount(mcsFile, olddf):
    orgin_df = pd.read_csv(mcsFile)
    df = orgin_df.fillna(0)   # 将NAN转换成0
    for idx, row in df.iterrows():    # 迭代数据 以键值对的形式 获取 每行的数据
        if row["MANUAL_SCAN_LOCATION"] == "MCS01":
            bag_dictionary["MCS01"] = row["NULL"]
        elif row["MANUAL_SCAN_LOCATION"] == "MCS02":
            bag_dictionary["MCS02"] = row["NULL"]
        elif row["MANUAL_SCAN_LOCATION"] == "MCS03":
            bag_dictionary["MCS03"] = row["NULL"]
        elif row["MANUAL_SCAN_LOCATION"] == "MCS04":
            bag_dictionary["MCS04"] = row["NULL"]
        elif row["MANUAL_SCAN_LOCATION"] == "T3-MCS05":
            bag_dictionary["T3-MCS05"] = row["NULL"]
        elif row["MANUAL_SCAN_LOCATION"] == "T3-MCS06":
            bag_dictionary["T3-MCS06"] = row["NULL"]
        elif row["MANUAL_SCAN_LOCATION"] == "SAT-MCS01":
            bag_dictionary["SAT-MCS01"] = row["NULL"]
        else:
            # print(row)
            pass
    values = bag_dictionary.values()
    bagcount_list = list(values)
    print(bagcount_list)
    df2 = pd.DataFrame([bagcount_list])
    df1 = pd.concat([olddf, df2])
    for key, value in bag_dictionary.items():   # 重置字典所有元素
        bag_dictionary[key] = 0
    return df1


def main():
    df_contact = pd.DataFrame([chute_list])
    for element in file_list:
        df_contact = get_bagcount(element, df_contact)
    with pd.ExcelWriter("{}mcs_w.xlsx".format(file_path)) as writer:
        # s.to_Excel(writer, sheet_name='another sheet', index=False)
        df_contact.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()
