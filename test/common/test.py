#!/usr/bin/python
# coding=utf-8


from my_mysql import Database
import datetime
from asyncio import exceptions


cursor = Database(dbname='ics', username='it', password='1111111', host='10.110.191.24', port='3306')

a = 'a'
try:
    destination = int(a)
    # except:   # 遗留问题，如何优雅的输出exceptions
except Exception as e:
    if "220" in a:
        destination = 1000    # 弃包和早到总称
    elif a == "None":
        print("destination write error.{}".format(a))
        destination = None
    else:
        destination = 1002   # 其他异常
        print("1002 error-{}".format( a))
    print(e)
