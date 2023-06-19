#!/usr/bin/python
# coding=utf-8

from my_mysql import Database
# import pymysql
import pandas as pd
import logging
import os


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//dump.log',
                    filemode='a')


def main():
    path = "d://1//4//"
    # 遍历目录下的所有文件
    for root, dirs, files in os.walk(path):
        for filename in files:
            # 打印文件名
            file_path = os.path.join(root, filename)



if __name__ == '__main__':
    main()

