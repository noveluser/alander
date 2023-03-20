#!/usr/bin/python
# coding=utf-8

# ATR log transfer
# author wangle
# v0.1


import configparser
import logging


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/data/package/crontab/log/temp_arrive.log',
                    filename='c://work//log//test.log',
                    filemode='a')


def GetKeyword(content, keyword):
    split_str = content.split(keyword)
    result = []
    if len(split_str) > 2:
        endstr = "\n"
    else:
        endstr = ""
    date = split_str[0]
    for x in split_str[1:]:
        if "#" in x:
            continue
        result.append(date + keyword + x + endstr)
    return result


def main():
    config = configparser.ConfigParser()
    config.read('D://atrlog//config.ini')
    reportnames = config.sections()
    for reportname in reportnames:
        file_path = config.get(reportname, 'file_path')
        readfilename = config.get(reportname, 'readfilename')
        suffix = config.get(reportname, 'suffix')
        writefilename = "{}_tra".format(readfilename)
        readfileDetail = ("{}//{}.{}".format(file_path, readfilename, suffix))
        writefileDetail = ("{}//{}.{}".format(file_path, writefilename, suffix))
        f = open(writefileDetail, 'a')
        with open(readfileDetail, "r") as file:
            lines = file.readlines()
            for line in lines:
                result = GetKeyword(line, "#")
                f.writelines(result)
                f.flush()
            f.close()


if __name__ == "__main__":
    main()
