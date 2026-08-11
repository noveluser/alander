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


def mcs_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
        WITH first_lpc AS (
            SELECT
                LPC,
                MIN(EVENTTS) AS first_eventts 
            FROM
                WC_PACKAGEINFO
            WHERE
                EVENTTS > :start_ts
                AND EVENTTS < :end_ts
                and ACTIVEPROCESS is not null 
            GROUP BY
                LPC
        )
        SELECT
            TO_CHAR(first_eventts + INTERVAL '8' HOUR, 'YYYY-MM-DD HH24') AS start_hour,
            COUNT(*) AS lpc_count
        FROM
            first_lpc
        GROUP BY
            TO_CHAR(first_eventts + INTERVAL '8' HOUR, 'YYYY-MM-DD HH24')
        ORDER BY
            start_hour
    """

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=['DATE', 'CNT'])
    # mask = df['DATE'].astype(str).str[-2:] == '03'

    # # 2. 对满足条件的行，将 CNT 除以 10 并取整（使用 // 向下取整）
    # df.loc[mask, 'CNT'] = (df.loc[mask, 'CNT'] // 100).astype(int)
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
    df_peak = mcs_data(start_ts, end_ts)
    if df_peak.empty:
        logging.info("无数据，不生成Excel")
        print("无数据，不生成Excel")
        return

    # 有数据，生成Excel
    date_label = f"{start_ts.strftime('%Y-%m-%d')}_至_{end_ts.strftime('%Y-%m-%d')}"
    filename = f"行李峰谷统计_{date_label}.xlsx"
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # 可写入一个sheet，名称可用日期范围
        sheet1_name = f"peak"
        df_peak.to_excel(writer, sheet_name=sheet1_name, index=False)

    logging.info(f"Excel 报表已生成：{filename}")
    print(f"Excel 报表已生成：{filename}")

if __name__ == '__main__':
    main()
