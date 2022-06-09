#!/usr/bin/python
# coding=utf-8

# 获取一周弃包行李状态
# 
# v0.2

import cx_Oracle
import logging
import pandas as pd


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='c://work//log//dump.log',
                    filemode='a')


# init Environment
file_path = "D://workcenter//整理后文档//各类报告//202222W//"
filename = "{}dump_w.xlsx".format(file_path)


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def collector(starttime, endtime):
    # df = pd.DataFrame()
    sqlquery = "SELECT     DEREGISTER_LOCATION,     FINAL_ACTIVE_PROCESS , count(FINAL_ACTIVE_PROCESS) FROM     FACT_BAG_SUMMARIES_V WHERE     REGISTER_DT > TO_TIMESTAMP( '{} 00:00:00', 'DD-MM-YYYY HH24:MI:SS' )     AND REGISTER_DT < TO_TIMESTAMP( '{} 00:00:00', 'DD-MM-YYYY HH24:MI:SS' )     AND DEREGISTER_LOCATION IN ( 'M41', 'M81', 'SAT-M10a', 'SAT-M10b' ) group by DEREGISTER_LOCATION, FINAL_ACTIVE_PROCESS".format(starttime, endtime)
    data = accessOracle(sqlquery)
    # for row in data:
    #     df = df.append([row])
    df = pd.DataFrame(data)
    try:
        with pd.ExcelWriter(filename) as writer:
            df.to_excel(writer, sheet_name='sheet1', index=False)
    except Exception as e:
        logging.error(e)


def main():
    starttime = "01-06-2022"
    endtime = "06-06-2022"

    collector(starttime, endtime)


if __name__ == '__main__':
    main()
