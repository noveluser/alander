#!/usr/bin/python
# coding=utf-8

# 获取当日行李状态
# 还有一个需要获取最新ID，读取接下来的行李，现在有可能有遗留
# v0.2


import logging
import sched
import time
import datetime


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//writeonlinebag.log',
                    filemode='a')






def main():
    a = "100"
    print(len(a))
    # if a < 1000:
    #     flightnr = str(a).zfill(4)
    # print(flightnr)


if __name__ == '__main__':
    main()
