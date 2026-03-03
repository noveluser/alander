#!/usr/bin/python
# coding=utf-8

import sys

def my_check():
    return True

if __name__ == "__main__":
    try:
        if my_check():
            print "CRITICAL: The condition is true, alerting!"  # Python 2使用print语句而非函数
            sys.exit(2)  # 返回CRITICAL
        else:
            print "OK: The condition is false, everything is fine."  # Python 2使用print语句而非函数
            sys.exit(0)  # 返回OK
    except Exception, e:  # Python 2的异常处理语法
        print "UNKNOWN: Error occurred - %s" % str(e)  # Python 2的字符串格式化
        sys.exit(3)  # 返回UNKNOWN

