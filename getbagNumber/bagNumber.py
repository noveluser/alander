#!/usr/bin/python
# coding=utf-8

# test
#
# v0.2

import pandas as pd
import pymysql
import time
import datetime
import os
import schedule
import logging


#init enviroments
volumns = ["Data.PLC01.A01-A12" , "Data.PLC01.A13-A24" , "Data.PLC02.B01-B12" , "Data.PLC02.B13-B24" , "Data.PLC03.C01-C12" , "Data.PLC03.C13-C24" , "Data.PLC04.D01-D12" , "Data.PLC04.J01-J08" , "Data.PLC05.D13-D24" , "Data.PLC17.H01-H12" , "Data.PLC17.H13-H24" , "Data.PLC18.G01-G12" , "Data.PLC18.G13-G24" , "Data.PLC19.F01-F12" , "Data.PLC19.F13-F24" , "Data.PLC19.K01-K08" , "Data.PLC20.E13-E24" , "Data.PLC21.E01-E12", "Data.PLC33.P01-P06"]
update_volumns = ["TIME", "A01-A12" , "A13-A24" , "B01-B12" , "B13-B24" , "C01-C12" , "C13-C24" , "D01-D12" , "J01-J08" , "D13-D24" , "H01-H12" , "H13-H24" , "G01-G12" , "G13-G24" , "F01-F12" , "F13-F24" , "K01-K08" , "E13-E24" , "E01-E12", "P01-P06", "TOTAL"]    # 输出首行
# plcName_list = ['Data.PLC01.A13-A24']
for i in range(0, len(volumns)):
    frontSqlQuery = "SELECT _VALUE from baggage_collection WHERE  ID = (SELECT MAX(ID) FROM baggage_collection WHERE _name = "
    if i == 0:
        sqlquery = "SELECT _VALUE from baggage_collection WHERE  ID = (SELECT MAX(ID) FROM baggage_collection WHERE _name = '{}')".format(volumns[i])
    else:
        nextSqlquery = "SELECT _VALUE from baggage_collection WHERE  ID = (SELECT MAX(ID) FROM baggage_collection WHERE _name = '{}')".format( volumns[i])
        sqlquery = "{} ; {}".format(sqlquery, nextSqlquery)
    sqllist = sqlquery.split(';')
# 打开数据库连接
# db = pymysql.connect(host='10.110.191.24',
db = pymysql.connect(host='10.31.9.24',
                     user='it',
                     password='1111111',
                     database='test',
                     charset='utf8mb4')


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='d://1//bagnumber.log',
                    filename='c://work//log//bagnumber.log',
                    filemode='a')


def getBagNumber(localtime):
    list1 = [localtime]
    total = 0
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # print(sqllist)
    for singleQuery in sqllist:
        # 使用 execute()  方法执行 SQL 查询 
        try:
            cursor.execute(singleQuery)
            # 使用 fetchone() 方法获取单条数据.
            data = cursor.fetchone()
            bagNumber = int(data[0])
        except Exception as e:
            logging.error(e)
            bagNumber = 0
        finally:
            total += bagNumber
            list1.append(bagNumber)
    list1.append(total)
    # 关闭数据库连接
    cursor.close()
    db.close()
    return list1


def plcBagNumber():
    # global lastday
    # pd.set_option('max_colwidth', 500)
    currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    currentDay = currentTime.split( )[0]
    filename = "c://work//bagnumber//{}.xlsx".format(currentDay)  # 要追加或者修改表格的文件名。
    # filename = "D://cyytempsvn//test//mysql//1.xls"
    if os.path.isfile(filename):
        df1 = pd.read_excel(filename)
        # eventtime = datetime.datetime.now().strftime('%H:%M')
    else:
        # simulateList = [currentTime, 100, 200]
        # df1 = pd.DataFrame([simulateList], columns=volumn)
        df1 = pd.DataFrame()
    # simulateList = [currentTime, 300, 400]
    simulateList = getBagNumber(currentTime)
    df2 = pd.DataFrame([simulateList], columns=update_volumns)
    df1 = pd.concat([df1, df2], ignore_index=True)
    try:
        with pd.ExcelWriter(filename) as writer:
            # s.to_Excel(writer, sheet_name='another sheet', index=False)
            df1.to_excel(writer, sheet_name='sheet1', index=False)
            worksheet = writer.sheets["sheet1"]
            worksheet.set_column("A:A", 20)  # set column width
            worksheet.set_column("B:T", 8)  # set column width
    except Exception as e:
        logging.error(e)
    logging.info(currentTime)


def main():
    plcBagNumber()
    # # writeFile(lastday)
    # schedule.every(30).minutes.do(plcBagNumber)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)

    # volumn = ["Data.PLC01.A01-A12", "Data.PLC01.A13-A24"]
    # simulateList = [100, 200]
    # df1 = pd.DataFrame([simulateList], index=["0:00"], columns=volumn)
    # simulateList2 = [ 300, 500]
    # df2 = pd.DataFrame([simulateList2], index=pd.date_range('20130102', periods=1), columns=volumn)
    # df1 = pd.concat([df1, df2])
    # #第三项
    # simulateList3 = [ 123, 456]
    # df2 = pd.DataFrame(simulateList3).T
    # df2.columns = df1.columns
    # df2.index=pd.date_range('20130103', periods=1)
    # df1 = pd.concat([df1, df2],ignore_index=False)
    # print(df1)


if __name__ == '__main__':
    # lastday = datetime.datetime.now().strftime('%Y-%m-%d')
    main()
