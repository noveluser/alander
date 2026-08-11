#!/usr/bin/python
# coding=utf-8

"""
弃包行李统计工具（单表整合版）
对每个站组分别统计总数量和指定 ACTIVEPROCESS 类型的数量，
合并为一张 Excel 表格。
所有统计均使用精确匹配 TARGETPROCESSID = 'BSIS_03997185'。
"""

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

# 站组映射：显示名称 -> SQL 条件
STATION_MAP = {
    '41': "CURRENTSTATIONID IN (41)",
    '81': "CURRENTSTATIONID IN (81)",
    'SAT': "CURRENTSTATIONID IN (220, 221)"
}

# 需要统计的 ACTIVEPROCESS 类型（按此顺序输出）
ACTIVEPROCESS_TYPES = [
    'No Read', 'Deleted BSM', 'Dump Identification',
    'In Time Build', 'Dump Flight Build', 'Multi Read', 'Unplanned flight'
]

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='dumpbag.log',          # 与Excel同目录
                    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger('').addHandler(console)

# ==================== 数据库操作 ====================
def get_oracle_connection():
    dsn_tns = oracledb.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn_tns)

def execute_query(query, params=None):
    conn = get_oracle_connection()
    c = conn.cursor()
    c.execute(query, params) if params else c.execute(query)
    result = c.fetchall()
    conn.close()
    return result

# ==================== 核心统计 ====================
def fetch_statistics(date_str):
    """
    返回一个字典：
    {
        '41': {'总数量': 150, 'No Read': 5, ...},
        '81': {...},
        'SAT': {...}
    }
    """
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    end_ts = dt.replace(hour=16, minute=0, second=0, microsecond=0)
    start_ts = end_ts - timedelta(days=1)

    # 公共时间条件
    time_condition = "EVENTTS > :start_ts AND EVENTTS < :end_ts"

    stats = {}

    # ---------- 第一步：统计总数量 ----------
    logging.info("开始统计各站组总数量...")
    for station_name, condition in STATION_MAP.items():
        query = f"""
            SELECT COUNT(*)
            FROM WC_PACKAGEINFO
            WHERE {time_condition}
              AND EXECUTEDTASK = 'Deregistration'
              AND {condition}
              AND TARGETPROCESSID = 'BSIS_03997185'
        """
        # logging.info(f"查询 {query} 语句...")
        result = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
        total = result[0][0] if result else 0
        stats[station_name] = {'总数量': total}
        logging.info(f"站组 {station_name} 总数量: {total}")

    # ---------- 第二步：统计各类 ACTIVEPROCESS 数量 ----------
    logging.info("开始统计各站组分类数量...")
    for station_name, condition in STATION_MAP.items():
        # 初始化该站组各类别为 0
        type_counts = {act: 0 for act in ACTIVEPROCESS_TYPES}

        # 查询该站组所有 ACTIVEPROCESS 的计数
        query = f"""
            SELECT ACTIVEPROCESS, COUNT(*)
            FROM WC_PACKAGEINFO
            WHERE {time_condition}
              AND EXECUTEDTASK = 'Deregistration'
              AND {condition}
              AND TARGETPROCESSID = 'BSIS_03997185'
            GROUP BY ACTIVEPROCESS
        """
        # logging.info(f"查询activeprocess {query} 语句...")
        data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
        for act, cnt in data:
            if act in type_counts:
                type_counts[act] = cnt
        # 合并到 stats
        stats[station_name].update(type_counts)
        logging.info(f"站组 {station_name} 分类统计完成")

    return stats

# ==================== 生成 Excel（单表） ====================
def write_excel(stats, date_str):
    # 构建 DataFrame：行索引为站组，列为总数量 + 7种类型
    df = pd.DataFrame(stats).T   # 转置后行为站组，列为指标
    # 确保列顺序：总数量在前，然后是 ACTIVEPROCESS_TYPES
    columns = ['总数量'] + ACTIVEPROCESS_TYPES
    df = df[columns]   # 重新排序列

    # # 添加“分类小计”列（7种类型之和）
    # df['分类小计'] = df[ACTIVEPROCESS_TYPES].sum(axis=1)

    # # 添加总计行
    # total_row = df.sum(axis=0)
    # total_row.name = '总计'
    # df = pd.concat([df, total_row.to_frame().T])

    # 写入 Excel
    filename = f"弃包统计_{date_str}.xlsx"
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='统计表')

    logging.info(f"Excel 报表已生成：{filename}")

# ==================== 主程序 ====================
def main():
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    logging.info(f"开始统计日期: {date_str}")
    try:
        stats = fetch_statistics(date_str)
        write_excel(stats, date_str)
        logging.info("统计完成！")
    except Exception as e:
        logging.error(f"执行错误: {e}", exc_info=True)
        print(f"错误: {e}")

if __name__ == '__main__':
    main()