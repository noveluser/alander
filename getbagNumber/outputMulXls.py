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


# init enviroments
update_volumns = ["TIME", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "12_INF02", "13_INF02", "D01-D12", "J01-J08", "12_INF01", "13_INF01", "D13-D24", "12_INF05", "12_INF06", "13_INF05", "13_INF06", "12_INF04", "13_INF04", "12_INF03", "13_INF03", "12_INF07", "13_INF07", "12_INF14", "12_INF08", "28_INF09", "29_INF09", "13_INF14", "13_INF08", "28_INF10", "29_INF10", "12_INF12", "13_INF12", "12_INF13", "13_INF13", "E.A01-A12", "E.A13-A24", "E.B01-B12", "E.B13-B24", "E.C01-C12", "E.C13-C24", "E.D01-D12", "E.D13-D24", "E.J01-J08", "VCC16", "H01-H12", "H13-H24", "G01-G12", "G13-G24", "F01-F12", "F13-F24", "K01-K08", "28_INF02", "29_INF02", "E13-E24", "28_INF01", "29_INF01", "E01-E12", "28_INF05", "28_INF06", "29_INF05", "29_INF06", "28_INF04", "29_INF04", "28_INF03", "29_INF03", "28_INF07", "29_INF07", "12_INF09", "13_INF09", "28_INF14", "28_INF08", "12_INF10", "13_INF10", "29_INF14", "29_INF08", "28_INF12", "29_INF12", "28_INF13", "29_INF13", "DC06", "DC07", "DC08", "DC09", "SAT-TRANS", "DC01", "DC02", "DC03", "DC04", "DC05", "DVT202", "DVT102", "TF03", "DVT201", "DVT101", "DVT402", "DVT302", "DVT401", "DVT301", "TF04", "E.E01-E12", "E.E13-E24", "E.F01-F12", "E.F13-F24", "E.G01-G12", "E.G13-G24", "E.H13-H24", "E.H01-H12", "E.K01-K08", "VCC32", "LDZ_SAT", "UNZ_SAT", "LDZ_T3E", "UNZ_T3E", "LDZ_T3W", "UNZ_T3W", "12_INF11", "13_INF11", "P01-P06", "TF01", "TA04", "TA05", "28_INF11", "29_INF11", "TF02", "TA02", "TA03", "SAT_TT01", "SAT_TT02", "SAT_TT03", "SAT_TT04", "SAT_TT05", "SAT_TT06", "SAT_TT07a", "SAT_TT07b", "T3_TT01", "T3_TT02", "T3_TT03", "T3_TT04", "T3_TT05", "T3_TT06", "T3_TT07a", "T3_TT07b", "SAT_TT08a", "SAT_TT08b", "SAT_TT09", "SAT_TT10", "SAT_TT11", "SAT_TT12", "SAT_TT13", "SAT_TT14", "T3_TT08a", "T3_TT08b", "T3_TT09", "T3_TT10", "T3_TT11", "T3_TT12", "T3_TT13", "T3_TT14", "T3_TT15", "T3_TT16", "T3_TT17", "T3_TT18", "VCC04", "VCC05", "VCC06", "VCC07", "VCC08", "VCC20", "VCC21", "VCC22", "VCC23", "VCC24", "VCC38", "VCC39", "A_EI", "A_EO", "A_WI", "A_WO", "D01_O", "D02_O", "D01_I", "D02_I", "D01_Tun", "D02_Tun", "A01_Tun", "A02_Tun"]
# "Data.PLC32.H01-H12","Data.PLC32.H13-H24"这两个数据采集点在数据库里就已经标注错了，所以对调下
# update_volumns = ["Data.PLC01.A01-A12", "Data.PLC01.A13-A24", "Data.PLC02.B01-B12", "Data.PLC02.B13-B24", "Data.PLC03.C01-C12", "Data.PLC03.C13-C24", "Data.PLC04.12_2_Infeed", "Data.PLC04.13_2_Infeed", "Data.PLC04.D01-D12", "Data.PLC04.J01-J08", "Data.PLC05.12_1_Infeed", "Data.PLC05.13_1_Infeed", "Data.PLC05.D13-D24", "Data.PLC06.12_5_Infeed", "Data.PLC06.12_6_Infeed", "Data.PLC06.13_5_Infeed", "Data.PLC06.13_6_Infeed", "Data.PLC07.12_4_Infeed", "Data.PLC07.13_4_Infeed", "Data.PLC08.12_3_Infeed", "Data.PLC08.13_3_Infeed", "Data.PLC09.12_7_Infeed", "Data.PLC09.13_7_Infeed", "Data.PLC10.12_14_Infeed", "Data.PLC10.12_8_Infeed", "Data.PLC10.28_9_Infeed", "Data.PLC10.29_9_Infeed", "Data.PLC11.13_14_Infeed", "Data.PLC11.13_8_Infeed", "Data.PLC11.28_10_Infeed", "Data.PLC11.29_10_Infeed", "Data.PLC14.12_12_Infeed", "Data.PLC14.13_12_Infeed", "Data.PLC15.12_13_Infeed", "Data.PLC15.13_13_Infeed", "Data.PLC16.A01-A12", "Data.PLC16.A13-A24", "Data.PLC16.B01-B12", "Data.PLC16.B13-B24", "Data.PLC16.C01-C12", "Data.PLC16.C13-C24", "Data.PLC16.D01-D12", "Data.PLC16.D13-D24", "Data.PLC16.J01-J08", "Data.PLC16.VCC_Outfeed", "Data.PLC17.H01-H12", "Data.PLC17.H13-H24", "Data.PLC18.G01-G12", "Data.PLC18.G13-G24", "Data.PLC19.F01-F12", "Data.PLC19.F13-F24", "Data.PLC19.K01-K08", "Data.PLC20.28_2_Infeed", "Data.PLC20.29_2_Infeed", "Data.PLC20.E13-E24", "Data.PLC21.28_1_Infeed", "Data.PLC21.29_1_Infeed", "Data.PLC21.E01-E12", "Data.PLC22.28_5_Infeed", "Data.PLC22.28_6_Infeed", "Data.PLC22.29_5_Infeed", "Data.PLC22.29_6_Infeed", "Data.PLC23.28_4_Infeed", "Data.PLC23.29_4_Infeed", "Data.PLC24.28_3_Infeed", "Data.PLC24.29_3_Infeed", "Data.PLC25.28_7_Infeed", "Data.PLC25.29_7_Infeed", "Data.PLC26.12_9_Infeed", "Data.PLC26.13_9_Infeed", "Data.PLC26.28_14_Infeed", "Data.PLC26.28_8_Infeed", "Data.PLC27.12_10_Infeed", "Data.PLC27.13_10_Infeed", "Data.PLC27.29_14_Infeed", "Data.PLC27.29_8_Infeed", "Data.PLC30.28_12_Infeed", "Data.PLC30.29_12_Infeed", "Data.PLC31.28_13_Infeed", "Data.PLC31.29_13_Infeed", "Data.PLC3101.DC06", "Data.PLC3101.DC07", "Data.PLC3101.DC08", "Data.PLC3101.DC09", "Data.PLC3101.SAT_SAT_Transfer", "Data.PLC3104.DC01", "Data.PLC3104.DC02", "Data.PLC3104.DC03", "Data.PLC3104.DC04", "Data.PLC3104.DC05", "Data.PLC3113.SAT_Lower_Sorter", "Data.PLC3113.SAT_Upper_Sorter", "Data.PLC3114.SAT_East_Transfer", "Data.PLC3114.SAT_Lower_Sorter", "Data.PLC3114.SAT_Upper_Sorter", "Data.PLC3143.SAT_Lower_Sorter", "Data.PLC3143.SAT_Upper_Sorter", "Data.PLC3144.SAT_Lower_Sorter", "Data.PLC3144.SAT_Upper_Sorter", "Data.PLC3144.SAT_West_Transfer", "Data.PLC32.E01-E12", "Data.PLC32.E13-E24", "Data.PLC32.F10-F12", "Data.PLC32.F13-F24", "Data.PLC32.G01-G12", "Data.PLC32.G13-G24", "Data.PLC32.H01-H12", "Data.PLC32.H13-H24", "Data.PLC32.K01-K08", "Data.PLC32.VCC_Outfeed", "Data.PLC3201.OOG_LDZ", "Data.PLC3201.OOG_UNZ", "Data.PLC3205.OOG_LDZ", "Data.PLC3205.OOG_UNZ", "Data.PLC3245.OOG_LDZ", "Data.PLC3245.OOG_UNZ", "Data.PLC33.12_11_Infeed", "Data.PLC33.13_11_Infeed", "Data.PLC33.P01-P06", "Data.PLC33.T3_East_Transfer", "Data.PLC33.TA04", "Data.PLC33.TA05", "Data.PLC34.28_11_Infeed", "Data.PLC34.29_11_Infeed", "Data.PLC34.T3_West_Transfer", "Data.PLC34.TA02", "Data.PLC34.TA03", "Data.PLC35.SAT_arrive_01", "Data.PLC35.SAT_arrive_02", "Data.PLC35.SAT_arrive_03", "Data.PLC35.SAT_arrive_04", "Data.PLC35.SAT_arrive_05", "Data.PLC35.SAT_arrive_06", "Data.PLC35.SAT_arrive_07a", "Data.PLC35.SAT_arrive_07b", "Data.PLC35.T3_Arrive_01", "Data.PLC35.T3_Arrive_02", "Data.PLC35.T3_Arrive_03", "Data.PLC35.T3_Arrive_04", "Data.PLC35.T3_Arrive_05", "Data.PLC35.T3_Arrive_06", "Data.PLC35.T3_Arrive_07a", "Data.PLC35.T3_Arrive_07b", "Data.PLC36.SAT_arrive_08a", "Data.PLC36.SAT_arrive_08b", "Data.PLC36.SAT_arrive_09", "Data.PLC36.SAT_arrive_10", "Data.PLC36.SAT_arrive_11", "Data.PLC36.SAT_arrive_12", "Data.PLC36.SAT_arrive_13", "Data.PLC36.SAT_arrive_14", "Data.PLC36.T3_Arrive_08a", "Data.PLC36.T3_Arrive_08b", "Data.PLC36.T3_Arrive_09", "Data.PLC36.T3_Arrive_10", "Data.PLC36.T3_Arrive_11", "Data.PLC36.T3_Arrive_12", "Data.PLC36.T3_Arrive_13", "Data.PLC36.T3_Arrive_14", "Data.PLC37.T3_Arrive_15", "Data.PLC37.T3_Arrive_16", "Data.PLC37.T3_Arrive_17", "Data.PLC37.T3_Arrive_18", "Data.PLC04.VCC_output", "Data.PLC05.VCC_output", "Data.PLC06.VCC_output", "Data.PLC07.VCC_output", "Data.PLC08.VCC_output", "Data.PLC20.VCC_output", "Data.PLC21.VCC_output", "Data.PLC22.VCC_output", "Data.PLC23.VCC_output", "Data.PLC24.VCC_output", "Data.PLC38.VCC_output", "Data.PLC39.VCC_output", "Data.PLC3116.A_EI", "Data.PLC3115.A_EO", "Data.PLC3145.A_WI", "Data.PLC3146.A_WO", "Data.PLC3241.D01_O", "Data.PLC3242.D02_O", "Data.PLC3241.D01_I", "Data.PLC3242.D02_I", "Data.PLC3243.D01", "Data.PLC3244.D02", "Data.PLC3205.A01", "Data.PLC3245.A02"]   # 输出首行


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='d://1//bagnumber.log',
                    filename='c://work//log//getnumber.log',
                    filemode='a')


def getBagNumber(localtime):
    list1 = [localtime]
    query = "WITH ar AS (  SELECT _name, max( _timestamp ) time FROM baggage_collection GROUP BY _name  ), br AS ( SELECT bag.*  FROM ar, baggage_collection bag  WHERE bag._name = ar._name  AND bag._timestamp = ar.time  AND bag._timestamp > '2022-05-25 13:00:00'  ) SELECT br._name,br._value  FROM br, plcname_contrast  WHERE plcname_contrast.plcname = br._name  ORDER BY plcname_contrast.id"
    cursor = Database(dbname='test', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(query)
    for row in queryResult:
        list1.append(int(row[1]))
    return list1


def main():
    currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    currentDay = currentTime.split( )[0]
    filename = "c://work//Datacollector//all-{}.xlsx".format(currentDay)  # 要追加或者修改表格的文件名。
    if os.path.isfile(filename):
        df1 = pd.read_excel(filename)
    else:
        df1 = pd.DataFrame()
    simulateList = getBagNumber(currentTime)
    df2 = pd.DataFrame([simulateList], columns=update_volumns)
    df1 = pd.concat([df1, df2], ignore_index=True)
    try:
        with pd.ExcelWriter(filename) as writer:
            df1.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)
    # 到港数据
    ttfile = "{}{}-{}.xlsx".format("c://work//Datacollector//Arrive//", "ARRIVE", currentDay)
    tt_index = ["TIME", 'DC01', "DC02", "DC03", "DC04", "DC05", "DC06", "DC07", "DC08", "DC09", "SAT_TT01", "SAT_TT02", "SAT_TT03", "SAT_TT04", "SAT_TT05", "SAT_TT06", "SAT_TT07a", "SAT_TT07b", "SAT_TT08a", "SAT_TT08b", "SAT_TT09", "SAT_TT10", "SAT_TT11", "SAT_TT12", "SAT_TT13", "SAT_TT14", "T3_TT01", "T3_TT02", "T3_TT03", "T3_TT04", "T3_TT05", "T3_TT06", "T3_TT07a", "T3_TT07b", "T3_TT08a", "T3_TT08b", "T3_TT09", "T3_TT10", "T3_TT11", "T3_TT12", "T3_TT13", "T3_TT14", "T3_TT15", "T3_TT16", "T3_TT17", "T3_TT18"]
    outputsinglexls(df1, ttfile, tt_index)
    # 值机岛数据
    checkinfile = "{}{}-{}.xlsx".format("c://work//Datacollector//Checkin//", "CHECKIN", currentDay)
    checkin_index = ["TIME", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "D01-D12", "D13-D24", "E01-E12", "E13-E24", "F01-F12", "F13-F24", "G01-G12", "G13-G24", "H01-H12", "H13-H24", "J01-J08", "K01-K08", "P01-P06"]
    outputsinglexls(df1, checkinfile, checkin_index)
    # 空框数据
    ETRfile = "{}{}-{}.xlsx".format("c://work//Datacollector//ETR//", "ETR", currentDay)
    ETR_index = ["TIME", "VCC16", "VCC32", "E.A01-A12", "E.A13-A24", "E.B01-B12", "E.B13-B24", "E.C01-C12", "E.C13-C24", "E.D01-D12", "E.D13-D24", "E.E01-E12", "E.E13-E24", "E.F01-F12", "E.F13-F24", "E.G01-G12", "E.G13-G24", "E.H01-H12", "E.H13-H24", "E.J01-J08", "E.K01-K08"]
    outputsinglexls(df1, ETRfile, ETR_index)
    # 大件及中转数据
    OOGfile = "{}{}-{}.xlsx".format("c://work//Datacollector//OOG&trans//", "OOG", currentDay)
    OOG_index = ["TIME", "UNZ_SAT", "UNZ_T3E", "UNZ_T3W", "LDZ_SAT", "LDZ_T3E", "LDZ_T3W", "TA02", "TA03", "TA04", "TA05", "TF01", "TF02", "TF03", "TF04", "SAT-TRANS"]
    outputsinglexls(df1, OOGfile, OOG_index)
    # 东分拣机数据
    ESHEfile = "{}{}-{}.xlsx".format("c://work//Datacollector//ESHE//", "ESHE", currentDay)
    ESHE_index = ["TIME", "12_INF01", "12_INF02", "12_INF03", "12_INF04", "12_INF05", "12_INF06", "12_INF07", "12_INF08", "12_INF09", "12_INF10", "12_INF11", "12_INF12", "12_INF13", "12_INF14", "13_INF01", "13_INF02", "13_INF03", "13_INF04", "13_INF05", "13_INF06", "13_INF07", "13_INF08", "13_INF09", "13_INF10", "13_INF11", "13_INF12", "13_INF13", "13_INF14", "DVT101", "DVT102", "DVT201", "DVT202"]
    outputsinglexls(df1, ESHEfile, ESHE_index)
    # 西分拣机数据
    WSHEfile = "{}{}-{}.xlsx".format("c://work//Datacollector//WSHE//", "WSHE", currentDay)
    WSHE_index = ["TIME", "28_INF01", "28_INF02", "28_INF03", "28_INF04", "28_INF05", "28_INF06", "28_INF07", "28_INF08", "28_INF09", "28_INF10", "28_INF11", "28_INF12", "28_INF13", "28_INF14", "29_INF01", "29_INF02", "29_INF03", "29_INF04", "29_INF05", "29_INF06", "29_INF07", "29_INF08", "29_INF09", "29_INF10", "29_INF11", "29_INF12", "29_INF13", "29_INF14", "DVT301", "DVT302", "DVT401", "DVT402"]
    outputsinglexls(df1, WSHEfile, WSHE_index)
    # VCC数据
    VCCfile = "{}{}-{}.xlsx".format("c://work//Datacollector//VCC//", "VCC", currentDay)
    VCC_index = ["TIME", "VCC04", "VCC05", "VCC06", "VCC07", "VCC08", "VCC20", "VCC21", "VCC22", "VCC23", "VCC24", "VCC38", "VCC39"]
    outputsinglexls(df1, VCCfile, VCC_index)
    # Loop&Tunnel数据
    LTfile = "{}{}-{}.xlsx".format("c://work//Datacollector//Loop&Tunnel//", "L&T", currentDay)
    LT_index = ["TIME", "A_EI", "A_EO", "A_WI", "A_WO", "D01_O", "D02_O", "D01_I", "D02_I", "D01_Tun", "D02_Tun", "A01_Tun", "A02_Tun"]
    outputsinglexls(df1, LTfile, LT_index)
    logging.info(currentTime)
    # print(currentTime)


def outputsinglexls(df, filename, columns):
    dc = df[columns]
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    # lastday = datetime.datetime.now().strftime('%Y-%m-%d')
    main()
