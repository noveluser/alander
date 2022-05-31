#!/usr/bin/python
# coding=utf-8

# 将导出的infeed数据按照特定格式提取到一个xls文件里
# Alex.Wang
# v0.1

from hashlib import blake2b
import logging
import pandas as pd


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='/root/1.log',
                    filename='d://data//rso//1.log',
                    filemode='a')

# init 数据
bag_dictionary = {"M10": 0, "M11": 0, "M12": 0, "M13": 0, "M14": 0, "M15": 0, "M16": 0, "M17": 0, "M18": 0, "M19": 0, "M20": 0, "M21": 0, "M22": 0, "M23": 0, "M24": 0, "M25": 0, "M26": 0, "M27": 0, "M28": 0, "M29": 0, "M30": 0, "M31": 0, "M32": 0, "M33": 0, "M34": 0, "M35": 0, "M36": 0, "M37": 0, "M38": 0, "M39": 0, "M40": 0, "M41": 0, "M42": 0, "M50": 0, "M51": 0, "M52": 0, "M53": 0, "M54": 0, "M55": 0, "M56": 0, "M57": 0, "M58": 0, "M59": 0, "M60": 0, "M61": 0, "M62": 0, "M63": 0, "M64": 0, "M65": 0, "M66": 0, "M67": 0, "M68": 0, "M69": 0, "M70": 0, "M71": 0, "M72": 0, "M73": 0, "M74": 0, "M75": 0, "M76": 0, "M77": 0, "M78": 0, "M79": 0, "M80": 0, "M81": 0, "M82": 0, "SAT-M01": 0, "SAT-M02": 0, "SAT-M03": 0, "SAT-M04": 0, "SAT-M05": 0, "SAT-M06": 0, "SAT-M07": 0, "SAT-M08": 0, "SAT-M09": 0, "SAT-M10a": 0, "SAT-M10b": 0, "SAT-M11": 0, "SAT-M12": 0, "SAT-M13": 0, "SAT-M14": 0, "SAT-M15": 0, "SAT-M16": 0, "SAT-M17": 0, "SAT-M18": 0, "SAT-M19": 0, "SAT-M20": 0, "SAT-M21": 0,  "T3-Total": 0, "SAT-Total": 0, "SAT-OGO01": 0}
keys = bag_dictionary.keys()   # 获取关键词列表
chute_list = list(keys)
file_path = "D://workcenter//整理后文档//各类报告//202221W//"
bagDay = ["0523", "0524", "0525", "0526", "0527", "0528", "0529"]
file_list = []
for element in bagDay:
    outfeedFile = "{}outfeed_{}.csv".format(file_path, element)
    file_list.append(outfeedFile)


def choice(value, totalbagnumber):
    match value:
        case "M10":
            bag_dictionary["M10"] = totalbagnumber
        case "M11":
            bag_dictionary["M11"] = totalbagnumber
        case "M12":
            bag_dictionary["M12"] = totalbagnumber
        case "M13":
            bag_dictionary["M13"] = totalbagnumber
        case "M14":
            bag_dictionary["M14"] = totalbagnumber
        case "M15":
            bag_dictionary["M15"] = totalbagnumber
        case "M16":
            bag_dictionary["M16"] = totalbagnumber
        case "M17":
            bag_dictionary["M17"] = totalbagnumber
        case "M18":
            bag_dictionary["M18"] = totalbagnumber
        case "M19":
            bag_dictionary["M19"] = totalbagnumber
        case "M20":
            bag_dictionary["M20"] = totalbagnumber
        case "M21":
            bag_dictionary["M21"] = totalbagnumber
        case "M22":
            bag_dictionary["M22"] = totalbagnumber
        case "M23":
            bag_dictionary["M23"] = totalbagnumber
        case "M24":
            bag_dictionary["M24"] = totalbagnumber
        case "M25":
            bag_dictionary["M25"] = totalbagnumber
        case "M26":
            bag_dictionary["M26"] = totalbagnumber
        case "M27":
            bag_dictionary["M27"] = totalbagnumber
        case "M28":
            bag_dictionary["M28"] = totalbagnumber
        case "M29":
            bag_dictionary["M29"] = totalbagnumber
        case "M30":
            bag_dictionary["M30"] = totalbagnumber
        case "M31":
            bag_dictionary["M31"] = totalbagnumber
        case "M32":
            bag_dictionary["M32"] = totalbagnumber
        case "M33":
            bag_dictionary["M33"] = totalbagnumber
        case "M34":
            bag_dictionary["M34"] = totalbagnumber
        case "M35":
            bag_dictionary["M35"] = totalbagnumber
        case "M36":
            bag_dictionary["M36"] = totalbagnumber
        case "M37":
            bag_dictionary["M37"] = totalbagnumber
        case "M38":
            bag_dictionary["M38"] = totalbagnumber
        case "M39":
            bag_dictionary["M39"] = totalbagnumber
        case "M40":
            bag_dictionary["M40"] = totalbagnumber
        case "M41":
            bag_dictionary["M41"] = totalbagnumber
        case "M42":
            bag_dictionary["M42"] = totalbagnumber
        case "M50":
            bag_dictionary["M50"] = totalbagnumber
        case "M51":
            bag_dictionary["M51"] = totalbagnumber
        case "M52":
            bag_dictionary["M52"] = totalbagnumber
        case "M53":
            bag_dictionary["M53"] = totalbagnumber
        case "M54":
            bag_dictionary["M54"] = totalbagnumber
        case "M55":
            bag_dictionary["M55"] = totalbagnumber
        case "M56":
            bag_dictionary["M56"] = totalbagnumber
        case "M57":
            bag_dictionary["M57"] = totalbagnumber
        case "M58":
            bag_dictionary["M58"] = totalbagnumber
        case "M59":
            bag_dictionary["M59"] = totalbagnumber
        case "M60":
            bag_dictionary["M60"] = totalbagnumber
        case "M61":
            bag_dictionary["M61"] = totalbagnumber
        case "M62":
            bag_dictionary["M62"] = totalbagnumber
        case "M63":
            bag_dictionary["M63"] = totalbagnumber
        case "M64":
            bag_dictionary["M64"] = totalbagnumber
        case "M65":
            bag_dictionary["M65"] = totalbagnumber
        case "M66":
            bag_dictionary["M66"] = totalbagnumber
        case "M67":
            bag_dictionary["M67"] = totalbagnumber
        case "M68":
            bag_dictionary["M68"] = totalbagnumber
        case "M69":
            bag_dictionary["M69"] = totalbagnumber
        case "M70":
            bag_dictionary["M70"] = totalbagnumber
        case "M71":
            bag_dictionary["M71"] = totalbagnumber
        case "M72":
            bag_dictionary["M72"] = totalbagnumber
        case "M73":
            bag_dictionary["M73"] = totalbagnumber
        case "M74":
            bag_dictionary["M74"] = totalbagnumber
        case "M75":
            bag_dictionary["M75"] = totalbagnumber
        case "M76":
            bag_dictionary["M76"] = totalbagnumber
        case "M77":
            bag_dictionary["M77"] = totalbagnumber
        case "M78":
            bag_dictionary["M78"] = totalbagnumber
        case "M79":
            bag_dictionary["M79"] = totalbagnumber
        case "M80":
            bag_dictionary["M80"] = totalbagnumber
        case "M81":
            bag_dictionary["M81"] = totalbagnumber
        case "M82":
            bag_dictionary["M82"] = totalbagnumber
        case "SAT-M01":
            bag_dictionary["SAT-M01"] = totalbagnumber
        case "SAT-M02":
            bag_dictionary["SAT-M02"] = totalbagnumber
        case "SAT-M03":
            bag_dictionary["SAT-M03"] = totalbagnumber
        case "SAT-M04":
            bag_dictionary["SAT-M04"] = totalbagnumber
        case "SAT-M05":
            bag_dictionary["SAT-M05"] = totalbagnumber
        case "SAT-M06":
            bag_dictionary["SAT-M06"] = totalbagnumber
        case "SAT-M07":
            bag_dictionary["SAT-M07"] = totalbagnumber
        case "SAT-M08":
            bag_dictionary["SAT-M08"] = totalbagnumber
        case "SAT-M09":
            bag_dictionary["SAT-M09"] = totalbagnumber
        case "SAT-M10a":
            bag_dictionary["SAT-M10a"] = totalbagnumber
        case "SAT-M10b":
            bag_dictionary["SAT-M10b"] = totalbagnumber
        case "SAT-M11":
            bag_dictionary["SAT-M11"] = totalbagnumber
        case "SAT-M12":
            bag_dictionary["SAT-M12"] = totalbagnumber
        case "SAT-M13":
            bag_dictionary["SAT-M13"] = totalbagnumber
        case "SAT-M14":
            bag_dictionary["SAT-M14"] = totalbagnumber
        case "SAT-M15":
            bag_dictionary["SAT-M15"] = totalbagnumber
        case "SAT-M16":
            bag_dictionary["SAT-M16"] = totalbagnumber
        case "SAT-M17":
            bag_dictionary["SAT-M17"] = totalbagnumber
        case "SAT-M18":
            bag_dictionary["SAT-M18"] = totalbagnumber
        case "SAT-M19":
            bag_dictionary["SAT-M19"] = totalbagnumber
        case "SAT-M20":
            bag_dictionary["SAT-M20"] = totalbagnumber
        case "SAT-M21":
            bag_dictionary["SAT-M21"] = totalbagnumber
        case "SAT-OGO01":
            bag_dictionary["SAT-OGO01"] = totalbagnumber
        case _:
            # print("没有匹配到任何status")
            pass


def get_bagcount(outfeedFile, olddf):
    T3_total = 0
    SAT_total = 0
    orgin_df = pd.read_csv(outfeedFile)
    df = orgin_df.fillna(0)   # 将NAN转换成0
    for idx, row in df.iterrows():    # 迭代数据 以键值对的形式 获取 每行的数据
        totalbagnumber = row["LocalBags"] + row["TransferBags"] + row["UnknownBags"]
        choice(row["DEREGISTER_LOCATION"], totalbagnumber)
    values = bag_dictionary.values()
    bagcount_list = list(values)
    for i in range(65):   # 0--64属于t3
        T3_total += bagcount_list[i]
    T3_total = T3_total - bagcount_list[32]    #剔除M42滑槽
    for j in range(66, 88):
        SAT_total += bagcount_list[j]
    bag_dictionary["T3-Total"] = T3_total
    bag_dictionary["SAT-Total"] = SAT_total
    values = bag_dictionary.values()    # 重新获取增加total的清单
    bagcount_list = list(values)
    df2 = pd.DataFrame([bagcount_list])
    df1 = pd.concat([olddf, df2])
    for key, value in bag_dictionary.items():   #重置字典所有元素
        bag_dictionary[key] = 0
    return df1


def main():
    df_contact = pd.DataFrame([chute_list])
    for element in file_list:
        df_contact = get_bagcount(element, df_contact)
    with pd.ExcelWriter("{}outfeed_w.xlsx".format(file_path)) as writer:
        # s.to_Excel(writer, sheet_name='another sheet', index=False)
        df_contact.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()
