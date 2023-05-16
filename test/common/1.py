#!/usr/bin/python
# coding=utf-8


import datetime

now = datetime.datetime(2020, 11, 1)
before = datetime.datetime.now() - datetime.timedelta(days= 365)
print(now)
print(before)