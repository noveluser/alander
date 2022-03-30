#!/usr/bin/python
# coding=utf-8

# test
#
# v0.2

import pandas as pd
from my_mysql import Database
import datetime
import os
import logging
import time


# init enviroments
update_volumns = ["TIME", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "12_INF02", "13_INF02", "D01-D12", "J01-J08", "12_INF01", "13_INF01", "D13-D24", "13_INF05", "12_INF06", "12_INF05", "13_INF06", "12_INF04", "13_INF04", "13_INF03", "12_INF03", "12_INF07", "13_INF07", "12_INF14", "12_INF08", "28_INF09", "29_INF09", "13_INF14", "13_INF08", "28_INF10", "29_INF10", "12_INF12", "13_INF12", "12_INF13", "13_INF13", "ETR.A01-A12", "ETR.A13-A24", "ETR.B01-B12", "ETR.B13-B24", "ETR.C01-C12", "ETR.C13-C24", "ETR.D01-D12", "ETR.D13-D24", "ETR.J01-J08", "VCC16", "H01-H12", "H13-H24", "G01-G12", "G13-G24", "F01-F12", "F13-F24", "K01-K08", "28_INF02", "29_INF02", "E13-E24", "28_INF01", "29_INF01", "E01-E12", "28_INF05", "28_INF06", "29_INF05", "29_INF06", "28_INF04", "29_INF04", "28_INF03", "29_INF03", "28_INF07", "29_INF07", "12_INF09", "13_INF09", "28_INF14", "28_INF08", "12_INF10", "13_INF10", "29_INF14", "29_INF08", "28_INF12", "29_INF12", "28_INF13", "29_INF13", "DC06", "DC07", "DC08", "DC09", "SAT-TRANS", "DC01", "DC02", "DC03", "DC04", "DC05", "DVT202", "DVT102", "TF03", "DVT201", "DVT101", "DVT402", "DVT302", "DVT401", "DVT301", "TF04", "ETR.E01-E12", "ETR.E13-E24", "ETR.F01-F12", "ETR.F13-F24", "ETR.G01-G12", "ETR.G13-G24", "ETR.H01-H12", "ETR.H13-H24", "ETR.K01-K08", "VCC32", "LDZ_SAT", "UNZ_SAT", "LDZ_T3E", "UNZ_T3E", "LDZ_T3W", "UNZ_T3W", "12_INF11", "13_INF11", "P01-P06", "TF01", "TA04", "TA05", "28_INF11", "29_INF11", "TF02", "TA02", "TA03", "SAT_TT01", "SAT_TT02", "SAT_TT03", "SAT_TT04", "SAT_TT05", "SAT_TT06", "SAT_TT07a", "SAT_TT07b", "T3_TT01", "T3_TT02", "T3_TT03", "T3_TT04", "T3_TT05", "T3_TT06", "T3_TT07a", "T3_TT07b", "SAT_TT08a", "SAT_TT08b", "SAT_TT09", "SAT_TT10", "SAT_TT11", "SAT_TT12", "SAT_TT13", "SAT_TT14", "T3_TT08a", "T3_TT08b", "T3_TT09", "T3_TT10", "T3_TT11", "T3_TT12", "T3_TT13", "T3_TT14", "T3_TT15", "T3_TT16", "T3_TT17", "T3_TT18"]
# update_volumns = ["time", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "12_2_Infeed", "13_2_Infeed", "D01-D12", "J01-J08", "12_1_Infeed", "13_1_Infeed", "D13-D24", "12_5_Infeed", "12_6_Infeed", "13_5_Infeed", "13_6_Infeed", "12_4_Infeed", "13_4_Infeed", "12_3_Infeed", "13_3_Infeed", "12_7_Infeed", "13_7_Infeed", "12_14_Infeed", "12_8_Infeed", "28_9_Infeed", "29_9_Infeed", "13_14_Infeed", "13_8_Infeed", "28_10_Infeed", "29_10_Infeed", "12_12_Infeed", "13_12_Infeed", "12_13_Infeed", "13_13_Infeed", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "D01-D12", "D13-D24", "J01-J08", "VCC_Outfeed", "H01-H12", "H13-H24", "G01-G12", "G13-G24", "F01-F12", "F13-F24", "K01-K08", "28_2_Infeed", "29_2_Infeed", "E13-E24", "28_1_Infeed", "29_1_Infeed", "E01-E12", "28_5_Infeed", "28_6_Infeed", "29_5_Infeed", "29_6_Infeed", "28_4_Infeed", "29_4_Infeed", "28_3_Infeed", "29_3_Infeed", "28_7_Infeed", "29_7_Infeed", "12_9_Infeed", "13_9_Infeed", "28_14_Infeed", "28_8_Infeed", "12_10_Infeed", "13_10_Infeed", "29_14_Infeed", "29_8_Infeed", "28_12_Infeed", "29_12_Infeed", "28_13_Infeed", "29_13_Infeed", "DC06", "DC07", "DC08", "DC09", "SAT_SAT_Transfer", "DC01", "DC02", "DC03", "DC04", "DC05", "SAT_Lower_Sorter", "SAT_Upper_Sorter", "SAT_East_Transfer", "SAT_Lower_Sorter", "SAT_Upper_Sorter", "SAT_Lower_Sorter", "SAT_Upper_Sorter", "SAT_Lower_Sorter", "SAT_Upper_Sorter", "SAT_West_Transfer", "E01-E12", "E13-E24", "F10-F12", "F13-F24", "G01-G12", "G13-G24", "H01-H12", "H13-H24", "K01-K08", "VCC_Outfeed", "OOG_LDZ", "OOG_UNZ", "OOG_LDZ", "OOG_UNZ", "OOG_LDZ", "OOG_UNZ", "12_11_Infeed", "13_11_Infeed", "P01-P06", "T3_East_Transfer", "TA04", "TA05", "28_11_Infeed", "29_11_Infeed", "T3_West_Transfer", "TA02", "TA03", "SAT_arrive_01", "SAT_arrive_02", "SAT_arrive_03", "SAT_arrive_04", "SAT_arrive_05", "SAT_arrive_06", "SAT_arrive_07a", "SAT_arrive_07b", "T3_Arrive_01", "T3_Arrive_02", "T3_Arrive_03", "T3_Arrive_04", "T3_Arrive_05", "T3_Arrive_06", "T3_Arrive_07a", "T3_Arrive_07b", "SAT_arrive_08a", "SAT_arrive_08b", "SAT_arrive_09", "SAT_arrive_10", "SAT_arrive_11", "SAT_arrive_12", "SAT_arrive_13", "SAT_arrive_14", "T3_Arrive_08a", "T3_Arrive_08b", "T3_Arrive_09", "T3_Arrive_10", "T3_Arrive_11", "T3_Arrive_12", "T3_Arrive_13", "T3_Arrive_14", "T3_Arrive_15", "T3_Arrive_16", "T3_Arrive_17", "T3_Arrive_18"]   # 输出首行


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='d://1//bagnumber.log',
                    filename='c://work//log//bagnumber.log',
                    filemode='a')


def getBagNumber(time):
    list1 = [time]
    query = "WITH cr AS ( select _name, max( _timestamp ) time FROM baggage_collection GROUP BY _name ) select BAG._NAME, bag._VALUE FROM baggage_collection bag, cr  WHERE bag._name = cr._name  AND bag._timestamp = cr.time AND bag._timestamp > '2022-03-25 13:00:00' order by _name"
    cursor = Database(dbname='test', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(query)
    for row in queryResult:
        list1.append(row[1])
    return list1


def plcBagNumber():
    currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    currentDay = currentTime.split( )[0]
    filename = "c://work//bagnumber//all-{}.xlsx".format(currentDay)  # 要追加或者修改表格的文件名。
    if os.path.isfile(filename):
        df1 = pd.read_excel(filename)
    else:
        df1 = pd.DataFrame()
    simulateList = getBagNumber(currentTime)
    df2 = pd.DataFrame([simulateList], columns=update_volumns)
    df1 = pd.concat([df1, df2], ignore_index=True)
    # dc = df1[["TIME", 'DC01', "DC02", "DC03", "DC04", "DC05", "DC06", "DC07", "DC08", "DC09"]]
    try:
        with pd.ExcelWriter(filename) as writer:
            df1.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)
    # 分别写入不同的xls
    dcfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "DC", currentDay)
    dc_index = ["TIME", 'DC01', "DC02", "DC03", "DC04", "DC05", "DC06", "DC07", "DC08", "DC09"]
    outputsinglexls(df1, dcfile, dc_index)
    # 到港数据
    ttfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "arrive", currentDay)
    tt_index = ["TIME", "SAT_TT01", "SAT_TT02", "SAT_TT03", "SAT_TT04", "SAT_TT05", "SAT_TT06", "SAT_TT07a", "SAT_TT07b", "SAT_TT08a", "SAT_TT08b", "SAT_TT09", "SAT_TT10", "SAT_TT11", "SAT_TT12", "SAT_TT13", "SAT_TT14", "T3_TT01", "T3_TT02", "T3_TT03", "T3_TT04", "T3_TT05", "T3_TT06", "T3_TT07a", "T3_TT07b", "T3_TT08a", "T3_TT08b", "T3_TT09", "T3_TT10", "T3_TT11", "T3_TT12", "T3_TT13", "T3_TT14", "T3_TT15", "T3_TT16", "T3_TT17", "T3_TT18"]
    outputsinglexls(df1, ttfile, tt_index)
    # 值机岛数据
    checkinfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "checkin", currentDay)
    checkin_index = ["TIME", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "D01-D12", "D13-D24", "E01-E12", "E13-E24", "F01-F12", "F13-F24", "G01-G12", "G13-G24", "H01-H12", "H13-H24", "J01-J08", "K01-K08", "P01-P06"]
    outputsinglexls(df1, checkinfile, checkin_index)
    # 空框数据
    ETRfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "ETR", currentDay)
    ETR_index = ["TIME", "VCC16", "VCC32", "ETR.A01-A12", "ETR.A13-A24", "ETR.B01-B12", "ETR.B13-B24", "ETR.C01-C12", "ETR.C13-C24", "ETR.D01-D12", "ETR.D13-D24", "ETR.E01-E12", "ETR.E13-E24", "ETR.F01-F12", "ETR.F13-F24", "ETR.G01-G12", "ETR.G13-G24", "ETR.H01-H12", "ETR.H13-H24", "ETR.J01-J08", "ETR.K01-K08"]
    outputsinglexls(df1, ETRfile, ETR_index)
    # 大件及中转数据
    OOGfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "OOG", currentDay)
    OOG_index = ["TIME", "UNZ_SAT", "UNZ_T3E", "UNZ_T3W", "LDZ_SAT", "LDZ_T3E", "LDZ_T3W", "TA02", "TA03", "TA04", "TA05", "TF01", "TF02", "TF03", "TF04", "SAT-TRANS"]
    outputsinglexls(df1, OOGfile, OOG_index)
    # 东分拣机数据
    ESHEfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "ESHE", currentDay)
    ESHE_index = ["TIME", "12_INF01", "12_INF02", "12_INF03", "12_INF04", "12_INF05", "12_INF06", "12_INF07", "12_INF08", "12_INF09", "12_INF10", "12_INF11", "12_INF12", "12_INF13", "12_INF14", "13_INF01", "13_INF02", "13_INF03", "13_INF04", "13_INF05", "13_INF06", "13_INF07", "13_INF08", "13_INF09", "13_INF10", "13_INF11", "13_INF12", "13_INF13", "13_INF14", "DVT101", "DVT102", "DVT201", "DVT202"]
    outputsinglexls(df1, ESHEfile, ESHE_index)
    # 西分拣机数据
    WSHEfile = "{}{}-{}.xlsx".format("c://work//bagnumber//", "WSHE", currentDay)
    WSHE_index = ["TIME", "28_INF01", "28_INF02", "28_INF03", "28_INF04", "28_INF05", "28_INF06", "28_INF07", "28_INF08", "28_INF09", "28_INF10", "28_INF11", "28_INF12", "28_INF13", "28_INF14", "29_INF01", "29_INF02", "29_INF03", "29_INF04", "29_INF05", "29_INF06", "29_INF07", "29_INF08", "29_INF09", "29_INF10", "29_INF11", "29_INF12", "29_INF13", "29_INF14", "DVT301", "DVT302", "DVT401", "DVT402"]
    outputsinglexls(df1, WSHEfile, WSHE_index)

    print(currentTime)


def outputsinglexls(df, filename, columns):
    dc = df[columns]
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)


def main():
    while True:
        plcBagNumber()
        time.sleep(5)


if __name__ == '__main__':
    # lastday = datetime.datetime.now().strftime('%Y-%m-%d')
    main()
