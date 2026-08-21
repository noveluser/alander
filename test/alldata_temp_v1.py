#!/usr/bin/python
# coding=utf-8

"""
弃包行李统计（基于 packageinfo）
核心点：真实数据，所有下弃包的离港行李数据，不同与dumpbaganalysis_toairport这个版本
"""

import sys
import os
import oracledb
import logging
from datetime import datetime, timedelta
import pandas as pd

# ==================== 配置 ====================
DB_HOST = '10.31.8.21'
DB_PORT = '1521'
DB_SERVICE = 'ORABPI'
DB_USER = r'owner_31_bpi_3_0'
DB_PASSWORD = 'owner31bpi'

# ---------- 日志配置 ----------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='dumpbag.log',          # 与Excel同目录
                    filemode='a')

# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
# logging.getLogger('').addHandler(console)

# ==================== 数据库操作 ====================
def get_oracle_connection():
    dsn_tns = oracledb.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn_tns)

def execute_query(query, params=None):
    conn = get_oracle_connection()
    c = conn.cursor()
    if params:
        c.execute(query, params)
    else:
        c.execute(query)
    result = c.fetchall()
    conn.close()
    return result

# ==================== 核心统计 ====================
def dump_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
        WITH lpcinfo AS (
            SELECT
                CURRENTSTATIONID,
                TRUNC(EVENTTS + INTERVAL '8' HOUR) AS STAT_DATE
            FROM
                WC_PACKAGEINFO INFO 
            WHERE
                1 = 1 
                AND EVENTTS > :start_ts
                AND EVENTTS < :end_ts
                AND EXECUTEDTASK = 'Deregistration' 
                AND CURRENTSTATIONID IN ( 42, 82, 41, 81, 220, 221, 119, 129 ) 
                AND TARGETPROCESSID = 'BSIS_03997185' 
        ) 
        SELECT
            STAT_DATE,
            CASE
                WHEN CURRENTSTATIONID = 42 THEN 'M42' 
                WHEN CURRENTSTATIONID = 82 THEN 'M82' 
                WHEN CURRENTSTATIONID = 41 THEN 'M41' 
                WHEN CURRENTSTATIONID = 81 THEN 'M81' 
                WHEN CURRENTSTATIONID IN ( 220, 221 ) THEN 'SAT-M10' 
                WHEN CURRENTSTATIONID = 119 THEN 'DP01' 
                WHEN CURRENTSTATIONID = 129 THEN 'DP02' 
            END AS STATION_CODE,
            count( * ) AS CNT  
        FROM
            lpcinfo 
        GROUP BY
            STAT_DATE,
            CASE
                WHEN CURRENTSTATIONID = 42 THEN 'M42' 
                WHEN CURRENTSTATIONID = 82 THEN 'M82' 
                WHEN CURRENTSTATIONID = 41 THEN 'M41' 
                WHEN CURRENTSTATIONID = 81 THEN 'M81' 
                WHEN CURRENTSTATIONID IN ( 220, 221 ) THEN 'SAT-M10' 
                WHEN CURRENTSTATIONID = 119 THEN 'DP01' 
                WHEN CURRENTSTATIONID = 129 THEN 'DP02' 
            END 
        ORDER BY
            STAT_DATE,
            MIN( CURRENTSTATIONID )
    """
    # # 打印可执行SQL（用于调试）
    # log_sql = query.replace(':start_ts', f"TO_TIMESTAMP('{start_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # log_sql = log_sql.replace(':end_ts', f"TO_TIMESTAMP('{end_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # logging.info("Executable SQL (with literals):\n" + log_sql)

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=['DATE', 'STATION', 'CNT'])
    # 将日期列转换为仅日期（去除时间部分）
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    logging.info(f"共获取 {len(df)} 条记录")
    return df

def mcs_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
        WITH lpcinfo AS (
            SELECT
                CURRENTSTATIONID,
                TRUNC(EVENTTS + INTERVAL '8' HOUR) AS STAT_DATE
            FROM
                WC_PACKAGEINFO INFO 
            WHERE
                1 = 1 
                AND EVENTTS > :start_ts
                AND EVENTTS < :end_ts
                AND EXECUTEDTASK = 'ManualScan'
                AND CURRENTSTATIONID IN (91,92,93,94,191,192 ) 
                AND TARGETPROCESSID = 'BSIS_03997185' 
        ) 
        SELECT
            STAT_DATE,
            CASE
                WHEN CURRENTSTATIONID = 91 THEN 'MCS01' 
                WHEN CURRENTSTATIONID = 92 THEN 'MCS02' 
                WHEN CURRENTSTATIONID = 93 THEN 'MCS03' 
                WHEN CURRENTSTATIONID = 94 THEN 'MCS04' 
                WHEN CURRENTSTATIONID = 191 THEN 'SAT-MCS01' 
                WHEN CURRENTSTATIONID = 192 THEN 'SAT-MCS02' 
            END AS STATION_CODE,
            count( * ) AS CNT  
        FROM
            lpcinfo 
        GROUP BY
            STAT_DATE,
            CASE
                WHEN CURRENTSTATIONID = 91 THEN 'MCS01' 
                WHEN CURRENTSTATIONID = 92 THEN 'MCS02' 
                WHEN CURRENTSTATIONID = 93 THEN 'MCS03' 
                WHEN CURRENTSTATIONID = 94 THEN 'MCS04' 
                WHEN CURRENTSTATIONID = 191 THEN 'SAT-MCS01' 
                WHEN CURRENTSTATIONID = 192 THEN 'SAT-MCS02' 
            END 
        ORDER BY
            STAT_DATE,
            MIN( CURRENTSTATIONID )
    """

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=['DATE', 'STATION', 'CNT'])
    # 将日期列转换为仅日期（去除时间部分）
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    logging.info(f"共获取 {len(df)} 条记录")
    return df

def satarrive_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
        WITH lpcinfo AS (
            SELECT
                CURRENTSTATIONID,
                TRUNC(EVENTTS + INTERVAL '8' HOUR) AS STAT_DATE
            FROM
                WC_PACKAGEINFO INFO 
            WHERE
                1 = 1 
                AND EVENTTS > :start_ts
                AND EVENTTS < :end_ts
                AND EXECUTEDTASK = 'Deregistration'
                AND CURRENTSTATIONID IN (111,112,113,114,115,116,117,121,122,123,124,125,126,127 ) 
                AND TARGETPROCESSID = 'BSIS_03997185' 
        ) 
        SELECT
            STAT_DATE,
            count( * ) AS CNT  
        FROM
            lpcinfo 
        GROUP BY
            STAT_DATE
        ORDER BY
            STAT_DATE
    """

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=['DATE', 'CNT'])
    # 将日期列转换为仅日期（去除时间部分）
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    logging.info(f"共获取 {len(df)} 条记录")
    return df


# ==================== 获取用户输入的日期 ====================
def get_start_date():
    """
    获取起始日期：
    1. 若命令行提供了参数（如 python script.py 20260710），则使用该日期。
    2. 若无参数，则提示用户输入，格式为 YYYYMMDD（如 20260710）。
    3. 若用户直接回车，则默认使用昨天。
    """
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        try:
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            print(f"命令行日期格式错误，请使用 YYYYMMDD，例如 20260710")
            sys.exit(1)
    else:
        print("请输入上周一日期（格式 YYYYMMDD，例如 20260710），直接回车则使用前一周当日：")
        date_str = input().strip()
        if not date_str:
            return datetime.now() - timedelta(days=7)
        try:
            return datetime.strptime(date_str, '%Y%m%d')        
        except ValueError:
            print("输入日期格式错误，将使用昨天")
            return datetime.now() - timedelta(days=7)

# ==================== 主程序 ====================
def main():
    # 获取起始日期
    input_date = get_start_date()

    # 日期范围限制（最早14天前）
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    earliest_allowed = today - timedelta(days=14)
    if input_date < earliest_allowed:
        error_msg = f"输入日期 {input_date.strftime('%Y%m%d')} 超出数据库记录范围"
        print(error_msg)
        logging.error(error_msg)
        sys.exit(1)

    # 计算统计范围
    # start_ts = input_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_ts = input_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=8)
    end_candidate = start_ts + timedelta(days=7)          # 起始+7天
    yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=8)
    end_ts = min(end_candidate, yesterday + timedelta(days=1))

    if start_ts >= end_ts:
        logging.warning("起始日期晚于统计截止日期（昨日），无数据可统计")
        
        print("起始日期晚于昨日，无数据可统计")
        return

    logging.info(f"统计范围: {start_ts.strftime('%Y-%m-%d')} 至 {end_ts.strftime('%Y-%m-%d')} (不含结束日期)")
    print(f"统计范围: {start_ts.strftime('%Y-%m-%d')} 至 {end_ts.strftime('%Y-%m-%d')}")

    # 获取数据（一次性查询整个范围）
    df_dump = dump_data(start_ts, end_ts)
    df_mcs = mcs_data(start_ts, end_ts)
    df_satarrive = satarrive_data(start_ts, end_ts)
    if df_dump.empty or df_mcs.empty or df_satarrive.empty:
        logging.info("无数据，不生成Excel")
        print("无数据，不生成Excel")
        return

    # 有数据，生成Excel
    date_label = f"{start_ts.strftime('%Y-%m-%d')}_至_{end_ts.strftime('%Y-%m-%d')}"
    filename = f"各类数据统计_{date_label}.xlsx"
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # 可写入一个sheet，名称可用日期范围
        sheet1_name = f"dump"
        df_dump.to_excel(writer, sheet_name=sheet1_name, index=False)
        sheet2_name = f"mcs"
        df_mcs.to_excel(writer, sheet_name=sheet2_name, index=False)
        sheet3_name = f"satarrive"
        df_satarrive.to_excel(writer, sheet_name=sheet3_name, index=False)

    logging.info(f"Excel 报表已生成：{filename}")
    print(f"Excel 报表已生成：{filename}")

if __name__ == '__main__':
    main()
