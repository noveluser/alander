#!/usr/bin/python
# coding=utf-8

# 输出月统计数据
#
# v0.2


import pandas as pd
import datetime
import logging
from calendar import monthrange


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='d://1//bagnumber.log',
                    filename='c://work//log//bagnumber_M.log',
                    filemode='a')


# init enviroments
update_volumns = ["TIME", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "12_INF02", "13_INF02", "D01-D12", "J01-J08", "12_INF01", "13_INF01", "D13-D24", "12_INF05", "12_INF06", "13_INF05", "13_INF06", "12_INF04", "13_INF04", "12_INF03", "13_INF03", "12_INF07", "13_INF07", "12_INF14", "12_INF08", "28_INF09", "29_INF09", "13_INF14", "13_INF08", "28_INF10", "29_INF10", "12_INF12", "13_INF12", "12_INF13", "13_INF13", "E.A01-A12", "E.A13-A24", "E.B01-B12", "E.B13-B24", "E.C01-C12", "E.C13-C24", "E.D01-D12", "E.D13-D24", "E.J01-J08", "VCC16", "H01-H12", "H13-H24", "G01-G12", "G13-G24", "F01-F12", "F13-F24", "K01-K08", "28_INF02", "29_INF02", "E13-E24", "28_INF01", "29_INF01", "E01-E12", "28_INF05", "28_INF06", "29_INF05", "29_INF06", "28_INF04", "29_INF04", "28_INF03", "29_INF03", "28_INF07", "29_INF07", "12_INF09", "13_INF09", "28_INF14", "28_INF08", "12_INF10", "13_INF10", "29_INF14", "29_INF08", "28_INF12", "29_INF12", "28_INF13", "29_INF13", "DC06", "DC07", "DC08", "DC09", "SAT-TRANS", "DC01", "DC02", "DC03", "DC04", "DC05", "DVT202", "DVT102", "TF03", "DVT201", "DVT101", "DVT402", "DVT302", "DVT401", "DVT301", "TF04", "E.E01-E12", "E.E13-E24", "E.F01-F12", "E.F13-F24", "E.G01-G12", "E.G13-G24", "E.H13-H24", "E.H01-H12", "E.K01-K08", "VCC32", "LDZ_SAT", "UNZ_SAT", "LDZ_T3E", "UNZ_T3E", "LDZ_T3W", "UNZ_T3W", "12_INF11", "13_INF11", "P01-P06", "TF01", "TA04", "TA05", "28_INF11", "29_INF11", "TF02", "TA02", "TA03", "SAT_TT01", "SAT_TT02", "SAT_TT03", "SAT_TT04", "SAT_TT05", "SAT_TT06", "SAT_TT07a", "SAT_TT07b", "T3_TT01", "T3_TT02", "T3_TT03", "T3_TT04", "T3_TT05", "T3_TT06", "T3_TT07a", "T3_TT07b", "SAT_TT08a", "SAT_TT08b", "SAT_TT09", "SAT_TT10", "SAT_TT11", "SAT_TT12", "SAT_TT13", "SAT_TT14", "T3_TT08a", "T3_TT08b", "T3_TT09", "T3_TT10", "T3_TT11", "T3_TT12", "T3_TT13", "T3_TT14", "T3_TT15", "T3_TT16", "T3_TT17", "T3_TT18"]
# "Data.PLC32.H01-H12","Data.PLC32.H13-H24"这两个数据采集点在数据库里就已经标注错了，所以对调下


def allDays(y, m):
    return ['{:04d}-{:02d}-{:02d}'.format(y, m, d) for d in range(1, monthrange(y, m)[1] + 1)]


def checkExistFile(filename):
    try:
        f = open(filename)
        f.close()
        return True
    except IOError:
        logging.error("{} is not accessible".format(filename))
        return False


def outputsinglexls(df, filename, columns):
    dc = df[columns]
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)


# 写入不同功能的表
def outputMulxls(all_element, month):
    # 到港数据
    ttfile = "{}{}-{}.xlsx".format("c://work//Datacollector//Arrive//", "ARRIVE", month)
    tt_index = ["TIME", 'DC01', "DC02", "DC03", "DC04", "DC05", "DC06", "DC07", "DC08", "DC09", "SAT_TT01", "SAT_TT02", "SAT_TT03", "SAT_TT04", "SAT_TT05", "SAT_TT06", "SAT_TT07a", "SAT_TT07b", "SAT_TT08a", "SAT_TT08b", "SAT_TT09", "SAT_TT10", "SAT_TT11", "SAT_TT12", "SAT_TT13", "SAT_TT14", "T3_TT01", "T3_TT02", "T3_TT03", "T3_TT04", "T3_TT05", "T3_TT06", "T3_TT07a", "T3_TT07b", "T3_TT08a", "T3_TT08b", "T3_TT09", "T3_TT10", "T3_TT11", "T3_TT12", "T3_TT13", "T3_TT14", "T3_TT15", "T3_TT16", "T3_TT17", "T3_TT18"]
    outputsinglexls(all_element, ttfile, tt_index)
    # 值机岛数据
    checkinfile = "{}{}-{}.xlsx".format("c://work//Datacollector//Checkin//", "CHECKIN", month)
    checkin_index = ["TIME", "A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "D01-D12", "D13-D24", "E01-E12", "E13-E24", "F01-F12", "F13-F24", "G01-G12", "G13-G24", "H01-H12", "H13-H24", "J01-J08", "K01-K08", "P01-P06"]
    point = ["A01-A12", "A13-A24", "B01-B12", "B13-B24", "C01-C12", "C13-C24", "D01-D12", "D13-D24", "E01-E12", "E13-E24", "F01-F12", "F13-F24", "G01-G12", "G13-G24", "H01-H12", "H13-H24", "J01-J08", "K01-K08", "P01-P06"]
    df_checkin = all_element[checkin_index]
    df_checkin['total'] = df_checkin[point].sum(axis=1)
    try:
        with pd.ExcelWriter(checkinfile) as writer:
            df_checkin.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)
    # 空框数据
    ETRfile = "{}{}-{}.xlsx".format("c://work//Datacollector//ETR//", "ETR", month)
    ETR_index = ["TIME", "VCC16", "VCC32", "E.A01-A12", "E.A13-A24", "E.B01-B12", "E.B13-B24", "E.C01-C12", "E.C13-C24", "E.D01-D12", "E.D13-D24", "E.E01-E12", "E.E13-E24", "E.F01-F12", "E.F13-F24", "E.G01-G12", "E.G13-G24", "E.H01-H12", "E.H13-H24", "E.J01-J08", "E.K01-K08"]
    outputsinglexls(all_element, ETRfile, ETR_index)
    # 大件及中转数据
    OOGfile = "{}{}-{}.xlsx".format("c://work//Datacollector//OOG&trans//", "OOG", month)
    OOG_index = ["TIME", "UNZ_SAT", "UNZ_T3E", "UNZ_T3W", "LDZ_SAT", "LDZ_T3E", "LDZ_T3W", "TA02", "TA03", "TA04", "TA05", "TF01", "TF02", "TF03", "TF04", "SAT-TRANS"]
    outputsinglexls(all_element, OOGfile, OOG_index)
    # 东分拣机数据
    ESHEfile = "{}{}-{}.xlsx".format("c://work//Datacollector//ESHE//", "ESHE", month)
    ESHE_index = ["TIME", "12_INF01", "12_INF02", "12_INF03", "12_INF04", "12_INF05", "12_INF06", "12_INF07", "12_INF08", "12_INF09", "12_INF10", "12_INF11", "12_INF12", "12_INF13", "12_INF14", "13_INF01", "13_INF02", "13_INF03", "13_INF04", "13_INF05", "13_INF06", "13_INF07", "13_INF08", "13_INF09", "13_INF10", "13_INF11", "13_INF12", "13_INF13", "13_INF14", "DVT101", "DVT102", "DVT201", "DVT202"]
    outputsinglexls(all_element, ESHEfile, ESHE_index)
    # 西分拣机数据
    WSHEfile = "{}{}-{}.xlsx".format("c://work//Datacollector//WSHE//", "WSHE", month)
    WSHE_index = ["TIME", "28_INF01", "28_INF02", "28_INF03", "28_INF04", "28_INF05", "28_INF06", "28_INF07", "28_INF08", "28_INF09", "28_INF10", "28_INF11", "28_INF12", "28_INF13", "28_INF14", "29_INF01", "29_INF02", "29_INF03", "29_INF04", "29_INF05", "29_INF06", "29_INF07", "29_INF08", "29_INF09", "29_INF10", "29_INF11", "29_INF12", "29_INF13", "29_INF14", "DVT301", "DVT302", "DVT401", "DVT402"]
    outputsinglexls(all_element, WSHEfile, WSHE_index)
    # VCC数据
    VCCfile = "{}{}-{}.xlsx".format("c://work//Datacollector//VCC//", "VCC", month)
    VCC_index = ["TIME", "VCC04", "VCC05", "VCC06", "VCC07", "VCC08", "VCC20", "VCC21", "VCC22", "VCC23", "VCC24", "VCC38", "VCC39"]
    outputsinglexls(all_element, VCCfile, VCC_index)
    # Loop&Tunnel数据
    LTfile = "{}{}-{}.xlsx".format("c://work//Datacollector//Loop&Tunnel//", "L&T", month)
    LT_index = ["TIME", "A_EI", "A_EO", "A_WI", "A_WO", "D01_O", "D02_O", "D01_I", "D02_I", "D01_Tun", "D02_Tun", "A01_Tun", "A02_Tun"]
    outputsinglexls(all_element, LTfile, LT_index)


def main():
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    year_month = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m')
    summarry_month_file = "{}{}-{}.xlsx".format("c://work//Datacollector//", "all-monthly", year_month)
    if checkExistFile(summarry_month_file):
        df = pd.read_excel(summarry_month_file)
    else:
        df = pd.DataFrame()
    summarry_filename = "c://work//Datacollector//all-{}.xlsx".format(yesterday)
    flag = checkExistFile(summarry_filename)
    if flag:
        df_temp = pd.read_excel(summarry_filename)
        # 空框数据
        ETR_index = ["VCC16", "VCC32", "E.A01-A12", "E.A13-A24", "E.B01-B12", "E.B13-B24", "E.C01-C12", "E.C13-C24", "E.D01-D12", "E.D13-D24", "E.E01-E12", "E.E13-E24", "E.F01-F12", "E.F13-F24", "E.G01-G12", "E.G13-G24", "E.H01-H12", "E.H13-H24", "E.J01-J08", "E.K01-K08"]
        for element in ETR_index:
            df_temp.at[df_temp.index[-1], element] = df_temp.at[df_temp.index[-2], element]
        df1 = df_temp.tail(1)
        df = df.append(df1)
    try:
        with pd.ExcelWriter(summarry_month_file) as writer:
            df.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)
    # 写入不同的功能表
    outputMulxls(df, year_month)


if __name__ == '__main__':
    # lastday = datetime.datetime.now().strftime('%Y-%m-%d')
    main()
