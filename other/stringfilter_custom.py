#!/usr/bin/python
# coding=utf-8

# ATR log transfer
# author wangle
# v0.1

import configparser
import logging

# 配置日志输出
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//test.log',
    filemode='a'
)

def CheckContainM(line,keyword1,keyword2,keyword3):
    if keyword1 in line:
        if keyword2 in line or keyword3 in line:
            return line
    return ""

def main():
    config = configparser.ConfigParser()
    config.read('c://work//conf//stringfliter.ini')
    reportnames = config.sections()
    for reportname in reportnames:
        file_path = config.get(reportname, 'file_path')
        readfilename = config.get(reportname, 'readfilename')
        suffix = config.get(reportname, 'suffix')
        readfileDetail = f"{file_path}//{readfilename}.{suffix}"
        writefilename = f"{readfilename}_tra"
        writefileDetail = f"{file_path}//{writefilename}.{suffix}"

        with open(writefileDetail, 'a', encoding='utf-8') as f_write:
            try:
                # 打开读取文件
                with open(readfileDetail, "r", encoding='utf-8') as f_read:
                    lines = f_read.readlines()
                    for line in lines:
                        result_line = CheckContainM(line,"2026-01-13","M", "U  G")
                        if result_line:  
                            f_write.write(result_line)
                            f_write.flush()  # 立即刷新缓冲区
                logging.info(f"处理完成：{readfileDetail}")
            except FileNotFoundError:
                logging.error(f"文件不存在：{readfileDetail}")
            except Exception as e:
                logging.error(f"处理文件出错 {readfileDetail}：{str(e)}")

if __name__ == "__main__":
    main()