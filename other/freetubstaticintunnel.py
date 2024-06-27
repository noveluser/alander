#!/usr/bin/python
# coding=utf-8

# 导出选定日期内的所有ido统计数据
# wangle
# v0.2


import pandas as pd
import configparser
import logging
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//bagnumber.log',
                    filemode='a')


def write_to_tubnumberstatic(writer):
    query = "select id , sum(usetimes) as sum  from tubstatus where date like '%{}' group by id order by id".format(custom_month)
    queryResult = cursor.run_query(query) 
    df = pd.DataFrame(queryResult)  
    df.to_excel(writer, sheet_name='Sheet2',index=True, header=True)


def write_to_freetubstatic(writer):
    df1 = pd.DataFrame()
    for ido in ido_list:
        areaid = int(ido[:4])
        zoneid = int(ido[5:7])
        # query = "select  date(EVENTTS),  sum( `VALUE` )  total FROM  ido_statis  WHERE  1 = 1  AND STATISTICALID IN ( 5,6,7,8 )   and EVENTTS > '{}'   AND EVENTTS < '{}'   and AREAID = {}  and ZONEID = {} GROUP BY date(EVENTTS),  AREAID,  ZONEID ORDER BY date(EVENTTS),  AREAID,  ZONEID".format(startday, endday, areaid, zoneid)
        query = "select date(EVENTTS), SUM(CASE WHEN STATISTICALID IN (1, 2, 3, 4) THEN value ELSE 0 END) AS sum_1_to_4, SUM(CASE WHEN STATISTICALID IN (5, 6, 7, 8) THEN value ELSE 0 END) AS sum_5_to_8 FROM  ido_statis  WHERE  1 = 1  and EVENTTS > '{}'   AND EVENTTS < '{}'   and AREAID = {}  and ZONEID = {} GROUP BY date(EVENTTS) ORDER BY date(EVENTTS)".format(startday, endday, areaid, zoneid)
        queryResult = cursor.run_query(query)
        # 从数据库中检索数据并存储在DataFrame中
        df2 = pd.DataFrame(queryResult)
        # df1 = pd.concat([df1, df2[['1', '2']]], axis=1)
        df1 = pd.concat([df1, df2], axis=1)
    df = df1
    # custom_index = ["date", "daparture_e_sbt",  "daparture_e_obt", "daparture_w_sbt",  "daparture_w_obt", "arrival_e_sbt", "arrival_e_sbt", "arrival_w_sbt", "arrival_w_sbt"]
    df.to_excel(writer, sheet_name='Sheet1',index=True, header=True)


def main():
    # 创建ExcelWriter对象
    writer = pd.ExcelWriter("{}freetubstatic.xlsx".format(file_path), engine='xlsxwriter')    
    write_to_freetubstatic(writer)
    write_to_tubnumberstatic(writer)
    writer.save()


if __name__ == '__main__':
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    # 实例化configParser对象
    config = configparser.ConfigParser()
    # -read读取ini文件
    config.read('c://work//conf//freetubstatic.ini')
    # -sections得到所有的section，并以列表的形式返回
    reportnames = config.sections()
    for reportname in reportnames:
        file_path = config.get(reportname, 'file_path')
        startday = config.get(reportname, 'startday')
        endday = config.get(reportname, 'endday')
        ido1 = config.get(reportname, 'departure_e')
        ido2 = config.get(reportname, 'departure_w')
        ido3 = config.get(reportname, 'arrival_e')
        ido4 = config.get(reportname, 'arrival_e')
        ido_list = [ido1, ido2, ido3, ido4]
        custom_month = config.get(reportname, 'monthfortub')
        main()
