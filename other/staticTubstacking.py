#!/usr/bin/python
# coding=utf-8

# 获取当日指定点位的IDO统计数据
# 用于各个堆垛，拆垛托盘数量
# v0.2


import logging
import pymysql
# from my_mysql import Database
import pandas as pd
import datetime
import json



# logging.basicConfig(
#                     level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     filename='/data/package/crontab/log/stackingTub.log',
#                     filemode='a')


# envioments
# cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
cursor = pymysql.connect(host='10.31.9.24', user='it', password='1111111', database='ics')

IDO = {"3117.11.98":
       {
           "areaid": 3117,
           "zoneid": 11,
           "equipmentid": 98
       },
       "3118.03.98":
       {
                "areaid": 3118,
                "zoneid": 3,
                "equipmentid": 98
        },
        "3117.53.97": 
        {
                "areaid": 3117,
                "zoneid": 53,
                "equipmentid": 97
        },
        "3118.43.97":
        {
                "areaid": 3118,
                "zoneid": 43,
                "equipmentid": 97
        },
        "3148.11.98":
        {
                "areaid": 3148,
                "zoneid": 11,
                "equipmentid": 98
        },
        "3147.03.98": {
                "areaid": 3147,
                "zoneid": 3,
                "equipmentid": 98
                },
        "3148.51.97": {
                "areaid": 3148,
                "zoneid": 51,
                "equipmentid": 97
                },
        "3147.47.97": {
                "areaid": 3147,
                "zoneid": 47,
                "equipmentid": 97
                },
        "3107.21.97": {
                "areaid": 3107,
                "zoneid": 21,
                "equipmentid": 97
                },
        "3107.15.97": {
                "areaid": 3107,
                "zoneid": 15,
                "equipmentid": 97
                },           
        "3110.15.97": {
                "areaid": 3110,
                "zoneid": 15,
                "equipmentid": 97
                },  
        "3110.21.97": {
                "areaid": 3110,
                "zoneid": 21,
                "equipmentid": 97
                },  
        "3105.11.98": {
                "areaid": 3105,
                "zoneid": 11,
                "equipmentid": 98
                },
        "3105.19.98": {
                "areaid": 3105,
                "zoneid": 19,
                "equipmentid": 98
                },
        "3108.09.98": {
                "areaid": 3108,
                "zoneid": 9,
                "equipmentid": 98
                },
        "3108.17.98": {
                "areaid": 3108,
                "zoneid": 17,
                "equipmentid": 98
                }
        }


def main():
    file_path = 'c://work//Datacollector//weeklyreport//'
    """每月初第一天凌晨4点执行"""
    endtime = datetime.datetime.now() - datetime.timedelta(days=1)
    starttime = endtime.replace(day=1)
    startday = starttime.strftime("%Y-%m-%d")
    endday = endtime.strftime("%Y-%m-%d")
    df = pd.DataFrame()
    #    IDO_list = ["3117.11.98", "3118.03.98", "3117.53.97", "3118.43.97", "3148.11.98", "3147.03.98", "3148.51.97", "3147.47.97", "3107.21.97", "3107.15.97", "3110.15.97", "3110.21.97", "3105.11.98", "3147.03.98", "3108.09.98", "3108.17.98"]
    for ido_location, ido in IDO.items():
        if type(ido) is dict:
            sqlquery = "SELECT AREAID, ZONEID,EQUIPMENTID,date(EVENTTS) as date, sum( `VALUE` )  total FROM ido_statis  WHERE 1 = 1  AND STATISTICALID IN ( 1,2,3,4,5,6,7,8 )  and EVENTTS > '{}'  AND EVENTTS <= '{}' + INTERVAL '1' DAY  and AREAID = {} and ZONEID = {} and equipmentid = {} GROUP BY date(EVENTTS), AREAID, ZONEID ORDER BY date(EVENTTS), AREAID, ZONEID".format(startday, endday, ido['areaid'], ido["zoneid"], ido["equipmentid"])
            # 使用pandas的read_sql_query函数执行SQL查询并将结果存储在DataFrame中
            df1 = pd.read_sql_query(sqlquery, cursor)
            df = pd.concat([df, df1])
            # 将DataFrame中的数据输出到xls文件
        with pd.ExcelWriter("{}tubstacking_{}.xlsx".format(file_path, endday)) as writer:
            df.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()
