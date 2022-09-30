#!/usr/bin/python
# coding=utf-8

# 写入IDO读取托盘数据至ics.idodata表
# author wangle
# v0.1

import time
import datetime
import sched
import logging
from asyncio import exceptions
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/writeidodata.log',
                    filemode='a')


def main():
    '''需要传输不同数据库的数据,所以用了两个cursor'''
    ido_list = ["0199", "0999", "1599", "1999", "2399", "2999", "3399", "3999", "4599", "4999", "5599", "6199", "6599", "7199", "7799", " 8199", "8799", "9799"]
    # ido_list = ["0199"]
    for item in ido_list:
        area = 3111
        tablename = "tubid_{}{}".format(area, item)
        cursor = Database(dbname='test', username='it', password='1111111', host='10.31.9.24', port='3306')
        query = "select _TIMESTAMP, _VALUE from {} where _TIMESTAMP > curdate() - 1 and _TIMESTAMP < curdate() ".format(tablename)
        data = cursor.run_query(query)
        for row in data:
            writeidodataquery = "insert into ics.idodata (time, area, ido, tubid)  values ('{}',{},'{}',{})".format(row[0], area, item, row[1])
            optimizal_sqlquery = writeidodataquery.replace("None", "Null")
            anothercursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
            anothercursor.run_query(optimizal_sqlquery)
        logging.info("ido:{} tubid{}".format(item, row))


if __name__ == "__main__":
    main()
