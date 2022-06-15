#!/usr/bin/python
# coding=utf-8

# test
#
# v0.2

import pandas as pd
from my_mysql import Database
import datetime
import logging


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='d://1//bagnumber.log',
                    filename='c://work//log//getnumber.log',
                    filemode='a')


def getBagNumber(localtime):
    list1 = [localtime]
    query = "with br as (WITH cr AS ( select _name, max( _timestamp ) time FROM baggage_collection GROUP BY _name ) select BAG._NAME, bag._VALUE FROM baggage_collection bag, cr  WHERE bag._name = cr._name  AND bag._timestamp = cr.time AND bag._timestamp > '2022-05-25 13:00:00' ) select br._name ,br._value from br, plcname_contrast where br._name = plcname order by plcname_contrast.id"
    cursor = Database(dbname='test', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(query)
    for row in queryResult:
        list1.append(int(row[1]))
    return list1


def main():
    currentWeek = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime("%Y%m%d")
    filename = "c://work//Datacollector//weeklyreport//rsocheckdata-{}.xlsx".format(currentWeek)  # 要追加或者修改表格的文件名。
    query1 = "select count(*) from ics.noread"
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    queryResult = cursor.run_query(query1)
    rownumber = queryResult[0][0]
    query2 = "select * from ics.noread limit {},{}".format(int(rownumber)-7, int(rownumber))
    queryResult = cursor.run_query(query2)
    columns = ["date", "departure", "arrive"]
    df = pd.DataFrame(queryResult, columns=columns)
    dc = df
    try:
        with pd.ExcelWriter(filename) as writer:
            dc.to_excel(writer, sheet_name='sheet1', index=False)
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    # lastday = datetime.datetime.now().strftime('%Y-%m-%d')
    main()
