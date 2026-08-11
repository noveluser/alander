#!/usr/bin/python
# coding=utf-8

"""
弃包行李统计（基于 FACT_BAG_SUMMARIES）
功能：
1. 按天统计指定日期范围（起始日期至起始+7天或昨日，取较短者）。
2. 每天输出：
   - 日志：ISCID_EXIT × FINAL_ACTIVE_PROCESS 交叉表（Sheet1）
   - Excel：ISCID_EXIT × 固定9种 DEREGISTER_REASON 交叉表（Sheet2，独立Sheet页）
3. 支持命令行参数或交互输入日期。
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
def fetch_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
        WITH lpcinfo AS (
            SELECT
                CURRENTSTATIONID,
                TRUNC( EVENTTS ) AS STAT_DATE 
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
            STAT_DATE,  -- 修复：不要写TRUNC(EVENTTS)
            CASE
                WHEN CURRENTSTATIONID = 42 THEN 'M42' 
                WHEN CURRENTSTATIONID = 82 THEN 'M82' 
                WHEN CURRENTSTATIONID = 41 THEN 'M41' 
                WHEN CURRENTSTATIONID = 81 THEN 'M81' 
                WHEN CURRENTSTATIONID IN ( 220, 221 ) THEN 'SAT-M10' 
                WHEN CURRENTSTATIONID = 119 THEN 'DP01' 
                WHEN CURRENTSTATIONID = 129 THEN 'DP02' 
            END AS STATION_CODE,
            count( * ) AS CNT  -- 修改别名避开关键字
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
            MIN( CURRENTSTATIONID );
    """
    # # 打印可执行SQL（用于调试）
    # log_sql = query.replace(':start_ts', f"TO_TIMESTAMP('{start_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # log_sql = log_sql.replace(':end_ts', f"TO_TIMESTAMP('{end_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # logging.info("Executable SQL (with literals):\n" + log_sql)

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()


    df = pd.DataFrame(data)
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

    # 生成Excel文件名
    date_label = f"{start_ts.strftime('%Y-%m-%d')}_至_{end_ts.strftime('%Y-%m-%d')}"
    filename = f"弃包统计_{date_label}.xlsx"

    # 逐日统计并写入Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # any_data = False
        day = start_ts
        df = fetch_data(day, end_ts)
        sheet_name = day.strftime('%Y-%m-%d')
        df.to_excel(writer, sheet_name=sheet_name)
        # while day < end_ts:
        #     day_end = day + timedelta(days=1)
        #     logging.info(f"统计日期: {day.strftime('%Y-%m-%d')}")
        #     print(f"正在统计 {day.strftime('%Y-%m-%d')} ...")
        #     df = fetch_data(day, day_end)

        #     if not df.empty:
        #         any_data = True
        #         # Excel 写入 Sheet2
        #         sheet_name = day.strftime('%Y-%m-%d')
        #         df.to_excel(writer, sheet_name=sheet_name)
        #     else:
        #         logging.info(f"{day.strftime('%Y-%m-%d')} 无数据")
        #         print(f"{day.strftime('%Y-%m-%d')} 无数据")

        #     day = day_end

        # if any_data:
        #     logging.info(f"Excel 报表已生成：{filename}")
        #     print(f"Excel 报表已生成：{filename}")
        # else:
        #     # 所有日期无数据，删除空文件（可能已创建但无Sheet）
        #     if os.path.exists(filename):
        #         os.remove(filename)
        #     logging.info("所有日期均无数据，未生成 Excel")
        #     print("所有日期均无数据，未生成 Excel")

if __name__ == '__main__':
    main()
