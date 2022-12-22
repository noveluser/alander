#!/usr/bin/python
# coding=utf-8

# 将导出的infeed数据按照特定格式提取到一个xls文件里
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


def get_bagcount(infeedFile, olddf):
    originDf = pd.read_csv(infeedFile)
    df = originDf.fillna(0)   # 将NAN转换成0
    bag_dictionary = {"20-6.5.1": 0, "16-6.22.1": 0, "12-7.4.1": 0, "9-8.3.1": 0, "5-4.50.1": 0, "1-5.27.1": 0, "453-22.5.1": 0, "449-22.22.1": 0, "445-23.4.1": 0, "442-24.3.1": 0, "438-20.27.1": 0, "434-21.27.1": 0, "T3E-OOG": 0, "T3W-OOG": 0, "SAT-OOG": 0}
    for idx, row in df.iterrows():    # 迭代数据 以键值对的形式 获取 每行的数据
        if row["REGISTER_LOCATION"] == "20-6.5.1":
            bag_dictionary["20-6.5.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "16-6.22.1":
            bag_dictionary["16-6.22.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "12-7.4.1":
            bag_dictionary["12-7.4.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "9-8.3.1":
            bag_dictionary["9-8.3.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "5-4.50.1":
            bag_dictionary["5-4.50.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "1-5.27.1":
            bag_dictionary["1-5.27.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "453-22.5.1":
            bag_dictionary["453-22.5.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "449-22.22.1":
            bag_dictionary["449-22.22.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "445-23.4.1":
            bag_dictionary["445-23.4.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "442-24.3.1":
            bag_dictionary["442-24.3.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "438-20.27.1":
            bag_dictionary["438-20.27.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "434-21.27.1":
            bag_dictionary["434-21.27.1"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "T3E-OOG":
            bag_dictionary["T3E-OOG"] = row["LocalBags"]
        elif row["REGISTER_LOCATION"] == "T3W-OOG":
            bag_dictionary["T3W-OOG"] = row["LocalBags"]
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
    # 数据初始化----
    bag_dictionary = {"20-6.5.1": 0, "16-6.22.1": 0, "12-7.4.1": 0, "9-8.3.1": 0, "5-4.50.1": 0, "1-5.27.1": 0, "453-22.5.1": 0, "449-22.22.1": 0, "445-23.4.1": 0, "442-24.3.1": 0, "438-20.27.1": 0, "434-21.27.1": 0, "T3E-OOG": 0, "T3W-OOG": 0, "SAT-OOG": 0}
    keys = bag_dictionary.keys()
    atr_list = list(keys)
    file_path = "D://workcenter//整理后文档//各类报告//202250W//"
    bagDay = ["1212", "1213", "1214", "1215", "1216", "1217", "1218"]
    file_list = []
    # ---数据初始化
    for element in bagDay:
        infeedFile = "{}infeed_{}.csv".format(file_path, element)
        file_list.append(infeedFile)
    df_contact = pd.DataFrame([atr_list])
    for element in file_list:
        df_contact = get_bagcount(element, df_contact)
    with pd.ExcelWriter("{}infeed_w.xlsx".format(file_path)) as writer:
        df_contact.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()
