#!/usr/bin/python
# coding=utf-8

# 导出IDO报告所有的托盘状态及隧道托盘统计数据
# wangle
# v0.2


import pandas as pd
from datetime import datetime, timedelta
import logging
from my_mysql import Database

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//bagnumber.log',
    filemode='a'
)
def get_startday_endday_monthtub():
    today = datetime.today() - timedelta(days=1)
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    startday = first_day_of_previous_month.strftime('%Y-%m-%d')
    month_tub = first_day_of_previous_month.strftime('%m-%Y')
    endday = first_day_of_current_month.strftime('%Y-%m-%d')
    return startday, endday, month_tub


def write_to_freetubstatic(writer, cursor, ido_list, startday, endday):
    df1 = pd.DataFrame()
    for ido in ido_list:
        areaid = int(ido[:4])
        zoneid = int(ido[5:7])
        query = " select  EVENTTS, `VALUE` FROM   ido_statis WHERE   1 = 1  AND STATISTICALID IN (1,2,3,4,5,6,7,8)  AND EVENTTS >= '{} 00:00:00'  AND EVENTTS < '{} 00:00:00'  AND AREAID = {}  AND ZONEID = {}  AND (DATE(EVENTTS), VALUE) IN    (SELECT DATE(EVENTTS) AS daily_date, MAX(VALUE) AS max_value     FROM ido_statis     WHERE STATISTICALID IN (1,2,3,4,5,6,7,8)     AND EVENTTS >= '{} 00:00:00'     AND EVENTTS < '{} 00:00:00'     AND AREAID = {}     AND ZONEID = {}     GROUP BY DATE(EVENTTS));".format(startday, endday, areaid, zoneid, startday, endday, areaid, zoneid)
        queryResult = cursor.run_query(query)
        df2 = pd.DataFrame(queryResult)
        df1 = pd.concat([df1, df2], axis=1)
        print(ido)
    df = df1
    df.to_excel(writer, sheet_name='freetubstatic', index=True, header=True)

   

def main(file_path):
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    with pd.ExcelWriter("{}to_du.xlsx".format(file_path), engine='xlsxwriter') as writer:
        startday, endday, month_tub=  get_startday_endday_monthtub()
        ido_list = [ "3113.19.99",  "3114.29.99",  "3143.19.99", "3144.29.99", "3243.11.99", "3244.07.99", "3203.29.99", "3204.29.99", "3111.49.99",  "3112.51.99", "3119.21.99",  "3120.17.99","3149.21.99",  "3150.17.99"]
        write_to_freetubstatic(writer, cursor, ido_list, startday, endday)

if __name__ == '__main__':
    main('c://work//rso//')
