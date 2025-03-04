#!/usr/bin/python
# coding=utf-8

# 检查当前行李状态
#
# v0.3 (优化版本)

import cx_Oracle
import logging
import time
import datetime
from my_mysql import Database  # 假设 my_mysql 模块已存在并包含连接池

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='/data/package/crontab/log/onlinebag.log',
    filemode='a')

# Oracle 数据库连接配置
ORACLE_DSN = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')
ORACLE_USER = r'owner_31_bpi_3_0'
ORACLE_PASSWORD = 'owner31bpi'

# 常量定义
DUMP_STATIONS = [41, 42, 81, 82, 220, 221]
STORE_STATIONS = [100, 110, 200, 210]

def get_oracle_connection():
    """获取Oracle连接（使用连接池最佳）"""
    try:
        conn = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
        return conn
    except cx_Oracle.Error as error:
        logging.error(f"Oracle connection error: {error}")
        return None

def execute_oracle_query(conn, query):
    """执行Oracle查询"""
    if conn is None:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except cx_Oracle.Error as error:
        logging.error(f"Oracle query error: {error}, Query: {query}")
        return []
    finally:
        cursor.close()

def update_bag_status(cursor, lpc, status, current_station, destination):
    """更新行李状态"""
    update_sql = "UPDATE onlinebag SET status = %s, currentstation = %s, destination = %s WHERE lpc = %s"
    try:
        cursor.execute(update_sql, (status, current_station, destination, lpc))
        return True
    except Exception as e:
        logging.error(f"Error updating bag status: {e}")
        return False

def check_bag_status():
    """检查行李状态的核心函数"""
    before30mins = (datetime.datetime.now() - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    search_bag_sql = f"SELECT lpc, created_time, DEPAIRLINE, DEPFLIGHT, STD FROM ics.onlinebag WHERE created_time > '{today}' AND status IS NULL AND (created_time < '{before30mins}' OR STD < NOW() + INTERVAL 1 HOUR) ORDER BY created_time"

    mysql_cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306') #使用连接池
    online_bags = mysql_cursor.run_query(search_bag_sql)

    oracle_conn = get_oracle_connection()
    if oracle_conn is None:
        return

    try:
        for lpc, created_time, airline, flight, std in online_bags:
            oracle_query = f"""
                WITH cr AS (
                    SELECT IDEVENT FROM WC_PACKAGEINFO WHERE lpc = {lpc} AND L_DESTINATIONSTATIONID IS NOT NULL ORDER BY EVENTTS DESC
                )
                SELECT CURRENTSTATIONID, L_DESTINATIONSTATIONID FROM WC_PACKAGEINFO WHERE IDEVENT = (SELECT MAX(IDEVENT) FROM cr)
            """
            destination_result = execute_oracle_query(oracle_conn, oracle_query)

            if destination_result:
                current_station, destination = destination_result[0]
                current_station = int(current_station) # 确保类型一致

                if current_station == int(destination):
                    update_bag_status(mysql_cursor, lpc, 'arrived', current_station, destination)
                elif current_station in DUMP_STATIONS:
                    update_bag_status(mysql_cursor, lpc, 'dump', current_station, destination)
                    logging.info(f"The bag:{lpc} from flight:{airline}{flight} was dumped, it is located in {current_station}")
                elif current_station in STORE_STATIONS:
                    update_bag_status(mysql_cursor, lpc, 'store', current_station, destination)
                    logging.info(f"The bag:{lpc} from flight:{airline}{flight} is located in store {current_station}")
                    if not mysql_cursor.run_query(f"SELECT lpc FROM ics.storebag WHERE lpc = {lpc}"):
                        insert_store_sql = "INSERT INTO ics.storebag (created_time, lpc, DEPAIRLINE, DEPFLIGHT, STD) VALUES (%s, %s, %s, %s, %s)"
                        mysql_cursor.run_query(insert_store_sql,(created_time, lpc, airline, flight, std))
                else:
                    if not mysql_cursor.run_query(f"SELECT lpc FROM ics.delaybag WHERE lpc = {lpc}"):
                        insert_delay_sql = "INSERT INTO ics.delaybag (created_time, lpc, DEPAIRLINE, DEPFLIGHT, STD, currentstation, destination) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        mysql_cursor.run_query(insert_delay_sql,(created_time, lpc, airline, flight, std, current_station, destination))
                    logging.info(f"The bag:{lpc} didn't arrive, the latest position is {current_station}")
    finally:
        oracle_conn.close()
        mysql_cursor.close()

def main():
    while True:
        try:
            check_bag_status()
        except Exception as e:  # 捕获main函数中的异常，防止程序崩溃
            logging.error(f"Main loop error: {e}")
        time.sleep(60)

if __name__ == '__main__':
    main()