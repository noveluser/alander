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


def main():
    query = "select  AREAID,  ZONEID,  EQUIPMENTID,  CASE   WHEN STATISTICALID = 1 THEN 'good read'   WHEN STATISTICALID = 2 THEN 'no read'   WHEN STATISTICALID = 3 THEN 'invalid data'   WHEN STATISTICALID = 4 THEN 'unexpected data'  END AS type,  SUM(`VALUE`) AS total FROM  ido_statis WHERE  1 = 1  AND STATISTICALID IN (1,2,3,4)  AND EVENTTS >= '{}'  AND EVENTTS < '{}' GROUP BY  AREAID,  ZONEID,  STATISTICALID ORDER BY  AREAID,  ZONEID, STATISTICALID".format(startday, endday)
    queryResult = cursor.run_query(query)
    # 从数据库中检索数据并存储在DataFrame中
    df = pd.DataFrame(queryResult)
    with pd.ExcelWriter("{}idostatic.xlsx".format(file_path)) as writer:
        df.to_excel(writer, index=False, header=False)


if __name__ == '__main__':
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    # 实例化configParser对象
    config = configparser.ConfigParser()
    # -read读取ini文件
    config.read('c://work//conf//idostatic.ini')
    # -sections得到所有的section，并以列表的形式返回
    reportnames = config.sections()
    for reportname in reportnames:
        file_path = config.get(reportname, 'file_path')
        startday = config.get(reportname, 'startday')
        endday = config.get(reportname, 'endday')
        main()
