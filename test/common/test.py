#!/usr/bin/python
# coding=utf-8

from my_mysql import Database
# import pymysql
import pandas as pd
import logging
import json


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//dump.log',
                    filemode='a')


def main():
    # 假设data是你的JSON数据
    data = {"3111.17.98":
            {
                "areaid": 3108,
                "zoneid": 9,
                "equipmentid": 98
            },
            "3118.03.98":
            {
                "areaid": 3108,
                "zoneid": 17,
                "equipmentid": 98
            }
            }
    ido_list = ["3111.17.98", "3118.03.98"]

    # 读取age字段的值
    for k, v in data.items():
        # age = data[item]["areaid"]
        if type(v) is dict:
            print(v["areaid"])
            print(k)
            # for nk,nv in v.items():
            #     print(nk, "---", nv)



if __name__ == '__main__':
    main()

