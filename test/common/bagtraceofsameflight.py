#!/usr/bin/python3
# coding=utf-8
# 查找紧急行李
# wangle
# v0.2

import oracledb
import logging
import pandas as pd
import os
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//urgent.log',
    filemode='a'
)

# 定义文件名
filename = 'c://work//log//flight.txt'

def access_oracle(query, params=None):
    dsn_tns = '10.31.8.21:1521/ORABPI'
    try:
        with oracledb.connect(user='owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    except oracledb.DatabaseError as e:
        logging.error(f"数据库访问错误: {e}")
        return None

    
def trace_bag(lpc):
    urgency_package_query = """
        SELECT lpc, areaid 
        FROM WC_TRACKINGREPORT 
        WHERE LPC = :lpc AND AREAID IN (3241, 3242) 
        ORDER BY IDEVENT DESC 
        FETCH FIRST 1 ROWS ONLY
    """
    return access_oracle(urgency_package_query, {'lpc': lpc})

def package_info(depairline: str, depflight: str, date: str):
    bag_query = """
        WITH bsmtime AS (
                           
                    
            SELECT DISTINCT lpc, (DEPAIRLINE || DEPFLIGHT) AS flightnr, MAX(eventts) AS EVENTTS 
                                        
                
            FROM WC_PACKAGEDATA 
                 
            WHERE EVENTTS >= TO_DATE(:date_start, 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '8' HOUR 
            AND EVENTTS < TO_DATE(:date_end, 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '8' HOUR 
            AND DEPAIRLINE = :depairline AND DEPFLIGHT = :depflight 
                                           
                                           
            AND BAG_MESSAGE_ID = 'BSM' AND TARGETPROCESSID LIKE 'ODB%' 
                    
                    
            GROUP BY lpc, (DEPAIRLINE || DEPFLIGHT) 
            ORDER BY EVENTTS 
                        
        )
              
                   
        SELECT fs.lpn, fs.FLIGHTNR 
            
        FROM FACT_BAG_SUMMARIES_GUI_V fs
        JOIN bsmtime bt ON fs.lpn = bt.lpc 
             
        WHERE ENTRYTIMESTAMP >= TO_DATE(:date_start, 'YYYY-MM-DD HH24:MI:SS') 
        AND ENTRYTIMESTAMP < TO_DATE(:date_end, 'YYYY-MM-DD HH24:MI:SS')
    """
    
                        
    params = {
        'date_start': f"{date} 00:00:00",
        'date_end': f"{date} 23:59:59",
        'depairline': depairline,
        'depflight': depflight
    }
    
    logging.info(bag_query)

    return access_oracle(bag_query, params)



def run_query(date):
    package_list = []
    flights = read_flights() 
    for flight_input in flights:
        airline = flight_input[:2]
        flightnt = flight_input[2:]
        try:
            bagresult = package_info(airline, flightnt, date)
            if bagresult:
                                     
                package_list.extend(bag[0] for bag in bagresult)
            else:
                logging.warning(f"未查询到航班 {flight_input} 的信息.")
        except Exception as e:
            logging.error(f"发生错误: {e}")
    
    return package_list



def read_flights():
    """从文本文件读取航班号并返回大写列表."""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
                                                           
            return [line.strip().upper() for line in file if line.strip()]
    except FileNotFoundError:
        logging.error("未找到文件：flight.txt")
        return []
    except Exception as e:
        logging.error(f"读取文件时发生错误：{e}")
        return []

    


def save_to_excel(data, file_path):
    """将查询结果保存到 Excel 文件."""
    if data:
        df = pd.DataFrame(data, columns=['条码', '经过点'])
                                                
                                                 
        df.to_excel(file_path, index=False, header=True)
        logging.info("数据成功保存到 Excel 文件.")
                   
    else:
        logging.info("未查询到结果.")
                    


def main():
    bag_trace_list = []
    package_list = run_query("2024-10-25")
    
    for package in package_list:             
        bagresult = trace_bag(package)
        if bagresult:
            bag_trace_list.append(bagresult[0])
            print(bagresult)
    
    save_to_excel(bag_trace_list, 'c://work//log//flighbag.xlsx')


if __name__ == '__main__':
    main()