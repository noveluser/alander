#!/usr/bin/python
# coding=utf-8


# 更新航班行李实时数据
# 10分钟一次
# v0.2


import logging
import sched
import time
from my_mysql import Database

logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/updateflight&bagNumber.log',
                    # filename='c://work//log//1.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')


def collect():
    """设定每循环一次，把相关记录记录到变量中，如果下一次循环的航班号更变，则把上一次的数据写入上一个航班里，同时重置所有变量"""
    sqlquery = "WITH BAGS AS ( select flightnr, final_destination, count(pid) as TOTAL ,status,current_location FROM temp_bags where  to_days(bsm_time) = to_days(now()) and flightnr is not null group by flightnr, status order by flightnr,total desc ) SELECT flightnr, final_destination, TOTAL,locationname, status,current_location FROM BAGS LEFT JOIN location_conversion on final_destination = locationid "
    data = cursor.run_query(sqlquery)
    last_flight = ""
    second_destination_flag = 0
    original_destination = None
    original_SORT_BAGS = 0
    second_destination = None
    second_SORT__BAGS = 0
    third_destination = ""
    third_SORT_BAGS = 0
    write_flag = 0
    for k in range(len(data)):
        '''row格式为flightnr, final_destination, count(pid) as TOTAL ,locationname, status, current_location'''
        if k == 0:
            pass
        elif last_flight == data[k][0]:
            second_destination_flag += 1
        else:
            write_flag = 1
        if write_flag == 1:
            orignal_sqlquery = "update ics.flight set original_destination = '{}', original_SORT_BAGS = {}, second_destination = '{}',second_SORT__BAGS = {},third_destination = '{}', third_SORT_BAGS = {} where flightnr = '{}' and to_days(create_time) = to_days(now()) ".format(original_destination, original_SORT_BAGS, second_destination, second_SORT__BAGS, third_destination, third_SORT_BAGS, last_flight)
            optimizal_sqlquery = orignal_sqlquery.replace("'None'", "Null").replace("''", "Null")
            logging.info(optimizal_sqlquery)
            cursor.run_query(optimizal_sqlquery)
            write_flag = 0
            second_destination_flag = 0
            original_destination = None
            original_SORT_BAGS = 0
            second_destination = None
            second_SORT__BAGS = 0
            third_destination = ""
            third_SORT_BAGS = 0
        if second_destination_flag == 0:
            if not data[k][3] is None:
                original_destination = data[k][3]
            original_SORT_BAGS = data[k][2]
        elif second_destination_flag == 1:
            if data[k][4] == "unkonwn":
                second_destination = data[k][5]
            elif data[k][4] is None:
                second_destination = "transit"
            elif not data[k][3] is None:
                second_destination = data[k][3]
            else:
                second_destination = "nosorted"
            second_SORT__BAGS = data[k][2]
        else:
            if data[k][4] == "unkonwn":
                third_destination = "{}{},".format(third_destination, data[k][5])
            elif data[k][4] is None:
                third_destination = "{}{},".format(third_destination, "transit")
            elif not data[k][3] is None:
                third_destination = "{}{},".format(third_destination, data[k][3])
            else:
                third_destination = "{}{},".format(third_destination, "nosorted")
            third_SORT_BAGS += data[k][2]
        last_flight = data[k][0]


def main():
    while True:
        s = sched.scheduler(time.time, time.sleep)
        s.enter(60, 1, collect)
        s.run()


if __name__ == '__main__':
    main()
