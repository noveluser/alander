#!/usr/bin/python
# coding=utf-8

# 获取当日行李状态
# 还有一个需要获取最新ID，读取接下来的行李，现在有可能有遗留
# v0.2



import logging



logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//writeonlinebag.log',
                    filemode='a')





def main():
    data = ['1', '2']
    log_flag = "wang"
    logging.info("本次操作{}条记录需要被输入{}".format(len(data), log_flag))


if __name__ == '__main__':
    main()
