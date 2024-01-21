#!/usr/bin/python
# coding=utf-8

# 蔡刚需要特定格式的数据计算提取到一个xls文件里
# Alex.Wang
# v0.1

import pandas as pd
import datetime
import logging


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//1.log',
                    filemode='a')


def oneweekxls(df, filename, columns):
    dc = df[columns]
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
    except Exception as e:
        logging.error(e)


def main():
    path = "c://work//Datacollector//"
    outputpath = "c://work//Datacollector//weeklyreport//caigang//"
    # path = "d://1//3//"
    today = datetime.datetime.now().date()
    df = pd.DataFrame()
    for i in range(7, 0, -1):
        nexttime = today - datetime.timedelta(days=i)
        nextday = nexttime.strftime("%Y-%m-%d")
        outfeedFile = "{}all-{}.xlsx".format(path, nextday)
        # file_list.append(outfeedFile)
        ori_df = onedaydf(outfeedFile)
        df = pd.concat([df, ori_df])
    LTfile = "{}{}.xlsx".format(outputpath, today)
    LT_index = ['TIME', 'CI值机行李量', "diff_CI值机行李量", '分拣机导入口（东）', "diff_分拣机导入口（东）", '分拣机导入口（西）', "diff_分拣机导入口（西）", "DC处理量", "diff_DC处理量","分拣机卫星厅导出口（东）", "diff_分拣机卫星厅导出口（东）", "分拣机卫星厅导出口（西）", "diff_分拣机卫星厅导出口（西）", "OOG卸载", "diff_OOG卸载", "OOG&Trans Data中转", "diff_OOG&Trans Data中转","SAT到港行李量", "diff_SAT到港行李量", "T3到港行李量", "diff_T3到港行李量", "T3到港分拣环导入口", "diff_T3到港分拣环导入口", "SAT离港分拣环导入口", "diff_SAT离港分拣环导入口", "内侧隧道离港","diff_内侧隧道离港", "外侧隧道离港", "diff_外侧隧道离港", "内侧隧道到港", "diff_内侧隧道到港", "外侧隧道到港", "diff_外侧隧道到港"]
    oneweekxls(df, LTfile, LT_index)


def onedaydf(xlsfile):
    originDf = pd.read_excel(xlsfile)
    df = originDf.fillna(0)   # 将NAN转换成0
    # 计算第一列和第二列的数值相加
    df['CI值机行李量'] = df["A01-A12"] + df["A13-A24"] + df["B01-B12"] + df["B13-B24"] + df["C01-C12"] + df["C13-C24"] + df["D01-D12"] + df["D13-D24"] + df["E01-E12"] + df["E13-E24"] + df["F01-F12"] + df["F13-F24"] + df["G01-G12"] + df["G13-G24"] + df["H01-H12"] + df["H13-H24"] + df["J01-J08"] + df["K01-K08"] + df["P01-P06"]
    df['分拣机导入口（东）'] = df["12_INF01"] + df["12_INF02"] + df["12_INF03"] + df["12_INF04"] + df["12_INF05"] + df["12_INF06"] + df["12_INF07"] + df["12_INF08"] + df["12_INF09"] + df["12_INF10"] + df["12_INF11"] + df["12_INF12"] + df["12_INF13"] + df["12_INF14"] + df["13_INF01"] + df["13_INF02"] + df["13_INF03"] + df["13_INF04"] + df["13_INF05"] + df["13_INF06"] + df["13_INF07"] + df["13_INF08"] + df["13_INF09"] + df["13_INF10"] + df["13_INF11"] + df["13_INF12"] + df["13_INF13"] + df["13_INF14"]
    df['分拣机导入口（西）'] = df["28_INF01"] + df["28_INF02"] + df["28_INF03"] + df["28_INF04"] + df["28_INF05"] + df["28_INF06"] + df["28_INF07"] + df["28_INF08"] + df["28_INF09"] + df["28_INF10"] + df["28_INF11"] + df["28_INF12"] + df["28_INF13"] + df["28_INF14"] + df["29_INF01"] + df["29_INF02"] + df["29_INF03"] + df["29_INF04"] + df["29_INF05"] + df["29_INF06"] + df["29_INF07"] + df["29_INF08"] + df["29_INF09"] + df["29_INF10"] + df["29_INF11"] + df["29_INF12"] + df["29_INF13"] + df["29_INF14"]
    df['DC处理量'] = df["DC01"] + df["DC02"] + df["DC03"] + df["DC04"] + df["DC05"] + df["DC06"] + df["DC07"] + df["DC08"] + df["DC09"] 
    df['分拣机卫星厅导出口（东）'] = df["DVT101"] + df["DVT102"] + df["DVT201"] + df["DVT202"]
    df['分拣机卫星厅导出口（西）'] = df["DVT301"] + df["DVT302"] + df["DVT401"] + df["DVT402"]
    df['OOG卸载'] = df["UNZ_SAT"] + df["UNZ_T3E"] + df["UNZ_T3W"] + df["LDZ_SAT"] + df["LDZ_T3E"] + df["LDZ_T3W"]
    df['OOG&Trans Data中转'] = df["TA02"] + df["TA03"] + df["TA04"] + df["TA05"] + df["TF01"] + df["TF01"] + df["TF03"] + df["TF04"]
    df['SAT到港行李量'] = df["SAT_TT01"] + df["SAT_TT02"] + df["SAT_TT03"] + df["SAT_TT04"] + df["SAT_TT05"] + df["SAT_TT06"] + df["SAT_TT07a"] + df["SAT_TT07b"] + df["SAT_TT08a"] + df["SAT_TT08b"] + df["SAT_TT09"] + df["SAT_TT10"] + df["SAT_TT11"] + df["SAT_TT12"] + df["SAT_TT13"] + df["SAT_TT14"]
    df['T3到港行李量'] = df["T3_TT01"] + df["T3_TT02"] + df["T3_TT03"] + df["T3_TT04"] + df["T3_TT05"] + df["T3_TT06"] + df["T3_TT07a"] + df["T3_TT07b"] + df["T3_TT08a"] + df["T3_TT08b"] + df["T3_TT09"] + df["T3_TT10"] + df["T3_TT11"] + df["T3_TT12"] + df["T3_TT13"] + df["T3_TT14"] + df["T3_TT15"] + df["T3_TT16"] + df["T3_TT17"] + df["T3_TT18"]
    df['T3到港分拣环导入口'] = df["A_EI"] + df["A_EO"] + df["A_WI"] + df["A_WO"]
    df['SAT离港分拣环导入口'] = df["D01_O"] + df["D02_O"] + df["D01_I"] + df["D02_I"]
    df["内侧隧道离港"] = df["D01_Tun"]
    df["外侧隧道离港"] = df["D02_Tun"]
    df["内侧隧道到港"] = df["A01_Tun"]
    df["外侧隧道到港"] = df["A02_Tun"]
    origin_index = ["CI值机行李量", "分拣机导入口（东）", "分拣机导入口（西）", "DC处理量", "分拣机卫星厅导出口（东）", "分拣机卫星厅导出口（西）", "OOG卸载", "OOG&Trans Data中转", "SAT到港行李量", "T3到港行李量", "T3到港分拣环导入口", "SAT离港分拣环导入口","内侧隧道离港", "外侧隧道离港", "内侧隧道到港", "外侧隧道到港"]
    for row in origin_index:
        next_row = df[row].shift(-1)
        # 计算下一行减去上一行的结果
        result = next_row - df[row]
        new_column = "diff_{}".format(row)
        # 将结果输出到另外一列
        df[new_column] = result
        # 将结果存储到下一行
        df[new_column] = df[new_column].shift(1)
    return df


if __name__ == '__main__':
    main()
