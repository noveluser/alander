#!/usr/bin/python3
# coding=utf-8


# 查找紧急行李
# wangle
# v0.3


import tkinter as tk
from tkinter import messagebox, ttk
import oracledb
import logging
import time
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//urgentbag.log',
    filemode='a'
)

# 定义文件名
filename = 'lpc.txt'


def accessOracle(query, params=None):
    dsn_tns = '10.31.8.21:1521/ORABPI'
    try:
        with oracledb.connect(user='owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                return c.fetchall()
    except oracledb.DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Error occurred while accessing Oracle: {e}")
        return None


def packageinfo(airline, flightnt):
    bagQuery = """
        with  bsmtime as
        (
        SELECT DISTINCT
            lpc,
        ( DEPAIRLINE || DEPFLIGHT ) AS flightnr,
                eventts
        FROM
            WC_PACKAGEDATA 
        WHERE
            EVENTTS >= TRUNC( SYSDATE ) - 1 
            and EVENTTS < TRUNC( SYSDATE ) 
            and DEPAIRLINE = '{}'
            AND DEPFLIGHT = '{}' 
            AND BAG_MESSAGE_ID = 'BSM'
            AND TARGETPROCESSID LIKE 'ODB%' 
        ORDER BY
            EVENTTS 
            )
        SELECT
            fs.lpn,
            fs.FLIGHTNR,
            TO_CHAR(FROM_TZ(CAST(bt.eventts AS TIMESTAMP), 'UTC') AT TIME ZONE 'Asia/Shanghai', 'hh24:mi:ss') AS starttime,
            TO_CHAR(fs.EXITTIMESTAMP, 'hh24:mi:ss') AS endtime ,
            fs.EXITUSERID	
        FROM
            FACT_BAG_SUMMARIES_GUI_V  fs
        JOIN BSMTIME bt ON fs.lpn = bt.lpc
        where
            ENTRYTIMESTAMP >= TRUNC( SYSDATE ) - 1
            and 	ENTRYTIMESTAMP < TRUNC( SYSDATE ) 
        """.format(airline, flightnt)
    # print(bagQuery)
    return accessOracle(bagQuery)


def main(file_path):
    flight_input = "MU5340"
    try:
        bagresult = packageinfo(flight_input[:2], flight_input[2:])
        if bagresult:
            # print(bagresult)
            columns = ['条码', '航班', '创建时间', '到达时间', '目的地']
            df = pd.DataFrame(bagresult, columns=columns)
            with pd.ExcelWriter("{}flighbag.xlsx".format(file_path)) as writer:
                df.to_excel(writer, index=False, header=True)
            logging.info("success")
        else:
            logging.info("未查询到结果")
    except Exception as e:
        logging.error(f"发生错误: {e}")



if __name__ == '__main__':
    main('c://work//log//')