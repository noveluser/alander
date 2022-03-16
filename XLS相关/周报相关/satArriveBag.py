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


# def choice(value, totalbagnumber):
#     match value:
#         case "M10":
#             bag_dictionary["M10"] = totalbagnumber
#         case "M11":
#             bag_dictionary["M11"] = totalbagnumber

            

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
    # 数据初始化----
    bag_dictionary = {"SAT-DC01": 0, "SAT-DC02": 0, "SAT-DC03": 0, "SAT-DC04": 0, "SAT-DC05": 0, "SAT-DC06": 0, "SAT-DC07": 0, "SAT-DC08": 0, "SAT-DC09": 0, "SAT-OOG": 0}
    keys = bag_dictionary.keys()
    atr_list = list(keys)
    file_path = "D://workcenter//整理后文档//各类报告//202209W//"
    bagDay = ["0228", "0301", "0302", "0303", "0304", "0305", "0306"]
    file_list = []
    # ---数据初始化
    for element in bagDay:
        infeedFile = "{}infeed_{}.csv".format(file_path, element)
        file_list.append(infeedFile)
    df_contact = pd.DataFrame([atr_list])
    for element in file_list:
        df_contact = get_bagcount(element, df_contact)
    with pd.ExcelWriter("{}arrive_w.xlsx".format(file_path)) as writer:
        df_contact.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()
