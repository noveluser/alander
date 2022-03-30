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
update_volumns = ["TIME", "Data.PLC01.A01-A12", "Data.PLC01.A13-A24", "Data.PLC02.B01-B12", "Data.PLC02.B13-B24", "Data.PLC03.C01-C12", "Data.PLC03.C13-C24", "Data.PLC04.12_2_Infeed", "Data.PLC04.13_2_Infeed", "Data.PLC04.D01-D12", "Data.PLC04.J01-J08", "Data.PLC05.12_1_Infeed", "Data.PLC05.13_1_Infeed", "Data.PLC05.D13-D24", "Data.PLC06.12_5_Infeed", "Data.PLC06.12_6_Infeed", "Data.PLC06.13_5_Infeed", "Data.PLC06.13_6_Infeed", "Data.PLC07.12_4_Infeed", "Data.PLC07.13_4_Infeed", "Data.PLC08.12_3_Infeed", "Data.PLC08.13_3_Infeed", "Data.PLC09.12_7_Infeed", "Data.PLC09.13_7_Infeed", "Data.PLC10.12_14_Infeed", "Data.PLC10.12_8_Infeed", "Data.PLC10.28_9_Infeed", "Data.PLC10.29_9_Infeed", "Data.PLC11.13_14_Infeed", "Data.PLC11.13_8_Infeed", "Data.PLC11.28_10_Infeed", "Data.PLC11.29_10_Infeed", "Data.PLC14.12_12_Infeed", "Data.PLC14.13_12_Infeed", "Data.PLC15.12_13_Infeed", "Data.PLC15.13_13_Infeed", "Data.PLC16.A01-A12", "Data.PLC16.A13-A24", "Data.PLC16.B01-B12", "Data.PLC16.B13-B24", "Data.PLC16.C01-C12", "Data.PLC16.C13-C24", "Data.PLC16.D01-D12", "Data.PLC16.D13-D24", "Data.PLC16.J01-J08", "Data.PLC16.VCC_Outfeed", "Data.PLC17.H01-H12", "Data.PLC17.H13-H24", "Data.PLC18.G01-G12", "Data.PLC18.G13-G24", "Data.PLC19.F01-F12", "Data.PLC19.F13-F24", "Data.PLC19.K01-K08", "Data.PLC20.28_2_Infeed", "Data.PLC20.29_2_Infeed", "Data.PLC20.E13-E24", "Data.PLC21.28_1_Infeed", "Data.PLC21.29_1_Infeed", "Data.PLC21.E01-E12", "Data.PLC22.28_5_Infeed", "Data.PLC22.28_6_Infeed", "Data.PLC22.29_5_Infeed", "Data.PLC22.29_6_Infeed", "Data.PLC23.28_4_Infeed", "Data.PLC23.29_4_Infeed", "Data.PLC24.28_3_Infeed", "Data.PLC24.29_3_Infeed", "Data.PLC25.28_7_Infeed", "Data.PLC25.29_7_Infeed", "Data.PLC26.12_9_Infeed", "Data.PLC26.13_9_Infeed", "Data.PLC26.28_14_Infeed", "Data.PLC26.28_8_Infeed", "Data.PLC27.12_10_Infeed", "Data.PLC27.13_10_Infeed", "Data.PLC27.29_14_Infeed", "Data.PLC27.29_8_Infeed", "Data.PLC30.28_12_Infeed", "Data.PLC30.29_12_Infeed", "Data.PLC31.28_13_Infeed", "Data.PLC31.29_13_Infeed", "DC06", "DC07", "DC08", "DC09", "Data.PLC3101.SAT_SAT_Transfer", "DC01", "DC02", "DC03", "DC04", "DC05", "Data.PLC3113.SAT_Lower_Sorter", "Data.PLC3113.SAT_Upper_Sorter", "Data.PLC3114.SAT_East_Transfer", "Data.PLC3114.SAT_Lower_Sorter", "Data.PLC3114.SAT_Upper_Sorter", "Data.PLC3143.SAT_Lower_Sorter", "Data.PLC3143.SAT_Upper_Sorter", "Data.PLC3144.SAT_Lower_Sorter", "Data.PLC3144.SAT_Upper_Sorter", "Data.PLC3144.SAT_West_Transfer", "Data.PLC32.E01-E12", "Data.PLC32.E13-E24", "Data.PLC32.F10-F12", "Data.PLC32.F13-F24", "Data.PLC32.G01-G12", "Data.PLC32.G13-G24", "Data.PLC32.H01-H12", "Data.PLC32.H13-H24", "Data.PLC32.K01-K08", "Data.PLC32.VCC_Outfeed", "Data.PLC3201.OOG_LDZ", "Data.PLC3201.OOG_UNZ", "Data.PLC3205.OOG_LDZ", "Data.PLC3205.OOG_UNZ", "Data.PLC3245.OOG_LDZ", "Data.PLC3245.OOG_UNZ", "Data.PLC33.12_11_Infeed", "Data.PLC33.13_11_Infeed", "Data.PLC33.P01-P06", "Data.PLC33.T3_East_Transfer", "Data.PLC33.TA04", "Data.PLC33.TA05", "Data.PLC34.28_11_Infeed", "Data.PLC34.29_11_Infeed", "Data.PLC34.T3_West_Transfer", "Data.PLC34.TA02", "Data.PLC34.TA03", "Data.PLC35.SAT_arrive_01", "Data.PLC35.SAT_arrive_02", "Data.PLC35.SAT_arrive_03", "Data.PLC35.SAT_arrive_04", "Data.PLC35.SAT_arrive_05", "Data.PLC35.SAT_arrive_06", "Data.PLC35.SAT_arrive_07a", "Data.PLC35.SAT_arrive_07b", "Data.PLC35.T3_Arrive_01", "Data.PLC35.T3_Arrive_02", "Data.PLC35.T3_Arrive_03", "Data.PLC35.T3_Arrive_04", "Data.PLC35.T3_Arrive_05", "Data.PLC35.T3_Arrive_06", "Data.PLC35.T3_Arrive_07a", "Data.PLC35.T3_Arrive_07b", "Data.PLC36.SAT_arrive_08a", "Data.PLC36.SAT_arrive_08b", "Data.PLC36.SAT_arrive_09", "Data.PLC36.SAT_arrive_10", "Data.PLC36.SAT_arrive_11", "Data.PLC36.SAT_arrive_12", "Data.PLC36.SAT_arrive_13", "Data.PLC36.SAT_arrive_14", "Data.PLC36.T3_Arrive_08a", "Data.PLC36.T3_Arrive_08b", "Data.PLC36.T3_Arrive_09", "Data.PLC36.T3_Arrive_10", "Data.PLC36.T3_Arrive_11", "Data.PLC36.T3_Arrive_12", "Data.PLC36.T3_Arrive_13", "Data.PLC36.T3_Arrive_14", "Data.PLC37.T3_Arrive_15", "Data.PLC37.T3_Arrive_16", "Data.PLC37.T3_Arrive_17", "Data.PLC37.T3_Arrive_18"]
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
    # df1 = df1.append(df2, ignore_index=True)
    # df1.style.set_properties(subset=["time"], **{'width': '300px'})
    dc = df1[["TIME", 'DC01', "DC02", "DC03", "DC04", "DC05", "DC06", "DC07", "DC08", "DC09"]]
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)
    logging.info(currentTime)


def outputsinglexls(df, head):

    dc = df1[["TIME", 'DC01', "DC02", "DC03", "DC04", "DC05", "DC06", "DC07", "DC08", "DC09"]]
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)
    logging.info(currentTime)


def main():
    currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    currentDay = currentTime.split( )[0]
    filename = "c://work//bagnumber//all-{}.xlsx".format(currentDay)  # 要追加或者修改表格的文件名。
    while True:
        plcBagNumber()
        outputsinglexls(filename)
        time.sleep(5)


if __name__ == '__main__':
    # lastday = datetime.datetime.now().strftime('%Y-%m-%d')
    main()
