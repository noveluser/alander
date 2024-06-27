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
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    startday = first_day_of_previous_month.strftime('%Y-%m-%d')
    month_tub = first_day_of_previous_month.strftime('%m-%Y')
    endday = first_day_of_current_month.strftime('%Y-%m-%d')
    return startday, endday, month_tub

def write_to_tubnumberstatic(writer, cursor, custom_month):
    query = "select id, sum(usetimes) as sum from tubstatus where date like '%{}' group by id order by id".format(custom_month)
    queryResult = cursor.run_query(query) 
    df = pd.DataFrame(queryResult)  
    df.to_excel(writer, sheet_name='tubnumberstatic', index=True, header=True)

def write_to_freetubstatic(writer, cursor, ido_list, startday, endday):
    df1 = pd.DataFrame()
    for ido in ido_list:
        areaid = int(ido[:4])
        zoneid = int(ido[5:7])
        query = "select date(EVENTTS), SUM(CASE WHEN STATISTICALID IN (1, 2, 3, 4) THEN value ELSE 0 END) AS sum_1_to_4, SUM(CASE WHEN STATISTICALID IN (5, 6, 7, 8) THEN value ELSE 0 END) AS sum_5_to_8 FROM ido_statis WHERE 1 = 1 and EVENTTS > '{}' AND EVENTTS < '{}' and AREAID = {} and ZONEID = {} GROUP BY date(EVENTTS) ORDER BY date(EVENTTS)".format(startday, endday, areaid, zoneid)
        queryResult = cursor.run_query(query)
        df2 = pd.DataFrame(queryResult)
        df1 = pd.concat([df1, df2], axis=1)
    df = df1
    df.to_excel(writer, sheet_name='freetubstatic', index=True, header=True)

def write_to_idostatic(writer, cursor, startday, endday):
    query = "select  AREAID,  ZONEID,  EQUIPMENTID,  CASE   WHEN STATISTICALID = 1 THEN 'good read'   WHEN STATISTICALID = 2 THEN 'no read'   WHEN STATISTICALID = 3 THEN 'invalid data'   WHEN STATISTICALID = 4 THEN 'unexpected data'  END AS type,  SUM(`VALUE`) AS total FROM  ido_statis WHERE  1 = 1  AND STATISTICALID IN (1,2,3,4)  AND EVENTTS >= '{}'  AND EVENTTS < '{}' GROUP BY  AREAID,  ZONEID,  STATISTICALID ORDER BY  AREAID,  ZONEID, STATISTICALID".format(startday, endday)
    queryResult = cursor.run_query(query)
    # 从数据库中检索数据并存储在DataFrame中
    df = pd.DataFrame(queryResult)
    df.to_excel(writer, sheet_name='idostatic', index=True, header=True)


def write_to_tub_monthly(writer, cursor):
    df = pd.DataFrame()
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    currentDay = first_day_of_previous_month
    while currentDay < first_day_of_current_month:
        currentDay_str = currentDay.strftime("%d-%m-%Y")
        line = [currentDay_str]
        type = ["< 25000", ">= 25000"]
        for tub_type in type:
            query = "select count(*) from tubstatus where date = '{}' and usetimes > 0 and id {}".format(currentDay_str, tub_type)
            queryResult = cursor.run_query(query)
            line.append(queryResult[0][0])
        df2 = pd.DataFrame([line])
        df = pd.concat([df, df2])
        line.clear()
        currentDay = currentDay + timedelta(days=1)
    df.to_excel(writer, sheet_name='tub_m', index=True, header=True)
   

def main(file_path):
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    with pd.ExcelWriter("{}to_du.xlsx".format(file_path), engine='xlsxwriter') as writer:
        startday, endday, month_tub=  get_startday_endday_monthtub()
        ido_list = ["3243.11.99", "3244.07.99", "3203.29.99", "3204.29.99"]
        write_to_freetubstatic(writer, cursor, ido_list, startday, endday)
        write_to_tubnumberstatic(writer, cursor, month_tub)
        write_to_idostatic(writer, cursor, startday, endday)
        write_to_tub_monthly(writer, cursor)

if __name__ == '__main__':
    main('c://work//rso//')
