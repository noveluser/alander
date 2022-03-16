#!/usr/bin/python
# coding=utf-8

# test
#
# v0.2

import cx_Oracle
import logging
import pandas as pd
import datetime


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//rso.log',
                    filemode='a')

def rso():
    currentTime = datetime.datetime.now()
    yesterdayTime =  currentTime - datetime.timedelta(days=1)
    yesterday = yesterdayTime.strftime("%d-%m-%Y")
    beforeyerstetime = currentTime - datetime.timedelta(days=2)
    beforeyesterday = beforeyerstetime.strftime("%d-%m-%Y")
    filename = "c://work//rso//{}.xlsx".format(yesterdayTime.strftime("%Y-%m-%d"))
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    sql = "select * from OWNER_31_BPI_3_0.wc_icsexceptiontrace where EVENTTS >= TO_TIMESTAMP('{} 16:00:00', 'DD-MM-YYYY HH24:MI:SS') and EVENTTS < TO_TIMESTAMP('{} 16:00:00', 'DD-MM-YYYY HH24:MI:SS') order by eventts".format(beforeyesterday, yesterday)
    try:
        data = pd.read_sql(sql,conn)
    except Exception as e:
        logging.error(e)
    finally:
        conn.close()
    with pd.ExcelWriter(filename) as writer:
        data.to_excel(writer, sheet_name='sheet1', index=False)


def main():
    rso()


if __name__ == '__main__':
    main()
