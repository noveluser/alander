#!/usr/bin/python
# coding=utf-8

# 将导出的icsexpection数据按照特定格式提取到一个xls文件里
# Alex.Wang
# v0.1

import logging
import pandas as pd
import configparser
import os


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    # filename='/root/1.log',
                    filename='d://data//rso//1.log',
                    filemode='a')


def main():
    file_list = []
    # 实例化configParser对象
    config = configparser.ConfigParser()
    # -read读取ini文件
    config.read('c://work//conf//tunnelbagstatic.ini')
    # -sections得到所有的section，并以列表的形式返回
    path_list = config.sections()
    for element in path_list:
        path = config.get(element, 'file_path')
        # 遍历目录下的所有文件
        for root, dirs, files in os.walk(path):
            for filename in files:
                # 打印文件名
                # filename = os.path.join(root, filename)
                filename = "{}{}".format(path, filename)
                file_list.append(filename)
    # print(file_list)
    df = pd.DataFrame()
    for element in file_list:
        originDf = pd.read_excel(element)
        df1 = originDf.fillna(0)   # 将NAN转换成0
        df = pd.concat([df, df1])
        # print(df)
    with pd.ExcelWriter("{}collect.xlsx".format(path)) as writer:
        df.to_excel(writer, sheet_name='sheet1', index=False)


if __name__ == '__main__':
    main()