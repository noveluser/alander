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

def circle_check(start_ts, end_ts, lpc):
    """
    检查指定 LPC 在指定时间范围内是否经过特定站点（580-590）超过一定次数。
    若次数 >= 6，则判定为循环（Recirculations），否则返回 Dump Flight Build。
    """
    query = """
        SELECT COUNT(*)
        FROM WC_PACKAGEINFO INFO 
        WHERE lpc = :lpc
            AND EVENTTS > :start_ts
            AND EVENTTS < :end_ts
            AND TARGETPROCESSID = 'BSIS_03997185' 
            AND CURRENTSTATIONID BETWEEN 580 AND 590
            AND EXECUTEDTASK = 'AutoScan'
    """
    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts, 'lpc': lpc})
    circle_time = data[0][0] if data else 0
    logging.info(f"{lpc} 扫描次数: {circle_time}")
    if circle_time >= 6:
        return "Recirculations"
    else:
        return "Dump Flight Build"



# ==================== 核心统计 ====================
def manuallscan_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
            SELECT
                EVENTTS,
                lpc,
                pid,
                ACTIVEPROCESS,
                ASSIGNEDTASK,
                CURRENTSTATIONID,
                FLIGHTBUILDTIMELINESS,
                IDENTIFICATIONSTATE,
                MANUALIDTASK,
                PROCESSPLANIDNAME,
                PROCESSDEFINITIONNAME,
                RECOGNITIONSTATE 
            FROM
                WC_PACKAGEINFO INFO 
            WHERE
                1 = 1 
                AND EVENTTS > :start_ts
                AND EVENTTS < :end_ts
                AND EXECUTEDTASK in ( 'ManualScan','SpecialDestination')
                AND CURRENTSTATIONID IN (91,92,93,94)
                AND TARGETPROCESSID = 'BSIS_03997185' 
            ORDER BY
                EVENTTS
    """
    # # 打印可执行SQL（用于调试）
    # log_sql = query.replace(':start_ts:start_ts', f"TO_TIMESTAMP('{start_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # log_sql = log_sql.replace(':end_ts', f"TO_TIMESTAMP('{end_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # logging.info("Executable SQL (with literals):\n" + log_sql)

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()

    columns = [
        'EVENTTS','LPC', 'PID', 'ACTIVEPROCESS', 'ASSIGNEDTASK',
        'CURRENTSTATIONID', 'FLIGHTBUILDTIMELINESS', 'IDENTIFICATIONSTATE', 'MANUALIDTASK',
        'PROCESSPLANIDNAME', 'PROCESSDEFINITIONNAME', 'RECOGNITIONSTATE'
    ]
    df = pd.DataFrame(data, columns=columns)
    # # 截取 ISCID_EXIT 前4位（如 0012.81.99 → 0012）
    # df['ISCID_EXIT'] = df['ISCID_EXIT'].str[:4]

    # ------ 派生 DEREGISTER_REASON（完整 Splunk 逻辑）------
    """    派生 DEREGISTER_REASON逻辑。
    1.如果activeprocess包含Garbage的，且没有LPC，基本判定为空框
    2. row['ACTIVEPROCESS'] = ['Trace and Eject'],中控操作主动弹出行李
    3. row['IDENTIFICATIONSTATE'] == ['DELETED_BAGDATA']，判定为DEL BSM
    4.当RECOGNITIONSTATE为NO_READ','MULTI_READ'，判定未读或多读
    5.FLIGHTBUILDTIMELINESS'] == 'EARLY'，判定早到
    6.['ACTIVEPROCESS'] == 'Dump Flight Build'，检查分拣机循环圈数，确实绝大部分都是分拣机主动弃包，少量个例各有原因
    7.'ACTIVEPROCESS'] == 'Unplanned flight'，判定太晚
    8.其余默认判定为ACTIVEPROCESS
    """
    def derive_reason(row):
        # 计算中间值 DEREGISTER_REASON_MCS
        # if pd.notna(row['REASON_MCS']) and row['REASON_MCS'] != 'LOST':
        #     deregister_reason_mcs = row['REASON_MCS']
        # elif row['ATR_RECOGNITION'] in ['MULTI_READ', 'NO_READ']:
        #     deregister_reason_mcs = row['ATR_RECOGNITION']
        # elif row['ATR_IDENTIFICATION'] in ['DELETED_BAGDATA', 'NO_BAGDATA']:
        #     deregister_reason_mcs = row['ATR_IDENTIFICATION']
        # else:
        #     deregister_reason_mcs = 'NO_READ'

        # 最终 DEREGISTER_REASON
        if row['ACTIVEPROCESS'] in ['Lateral_41', 'Lateral_81','Garbage SAT', 'Garbage T3 East', 'Garbage T3 West'] and pd.isna(row['LPC']):
            return 'EMPTY'
        else:
            return row['ACTIVEPROCESS']

    df['DEREGISTER_REASON'] = df.apply(derive_reason, axis=1)
    logging.info(f"共获取 {len(df)} 条记录，涉及 {df['CURRENTSTATIONID'].nunique()} 个不同位置")
    return df



def fetch_data(start_ts, end_ts):
    """
    查询指定日期范围（不含结束时刻）的数据，返回 DataFrame。
    start_ts, end_ts: datetime 对象，精确到秒。
    """
    query = """
            SELECT
                EVENTTS,
                lpc,
                pid,
                ACTIVEPROCESS,
                ASSIGNEDTASK,
                CURRENTSTATIONID,
                FLIGHTBUILDTIMELINESS,
                IDENTIFICATIONSTATE,
                MANUALIDTASK,
                PROCESSPLANIDNAME,
                PROCESSDEFINITIONNAME,
                RECOGNITIONSTATE 
            FROM
                WC_PACKAGEINFO INFO 
            WHERE
                1 = 1 
                AND EVENTTS > :start_ts
                AND EVENTTS < :end_ts
                AND EXECUTEDTASK = 'Deregistration' 
                AND CURRENTSTATIONID IN ( 96,97,98,99 )
                AND TARGETPROCESSID = 'BSIS_03997185' 
            ORDER BY
                EVENTTS
    """
    # # 打印可执行SQL（用于调试）
    # log_sql = query.replace(':start_ts:start_ts', f"TO_TIMESTAMP('{start_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # log_sql = log_sql.replace(':end_ts', f"TO_TIMESTAMP('{end_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
    # logging.info("Executable SQL (with literals):\n" + log_sql)

    data = execute_query(query, {'start_ts': start_ts, 'end_ts': end_ts})
    if not data:
        logging.warning("未查询到符合条件的数据")
        return pd.DataFrame()

    columns = [
        'EVENTTS','LPC', 'PID', 'ACTIVEPROCESS', 'ASSIGNEDTASK',
        'CURRENTSTATIONID', 'FLIGHTBUILDTIMELINESS', 'IDENTIFICATIONSTATE', 'MANUALIDTASK',
        'PROCESSPLANIDNAME', 'PROCESSDEFINITIONNAME', 'RECOGNITIONSTATE'
    ]
    df = pd.DataFrame(data, columns=columns)
    # # 截取 ISCID_EXIT 前4位（如 0012.81.99 → 0012）
    # df['ISCID_EXIT'] = df['ISCID_EXIT'].str[:4]

    # ------ 派生 DEREGISTER_REASON（完整 Splunk 逻辑）------
    """    派生 DEREGISTER_REASON逻辑。
    1.如果activeprocess包含Garbage的，且没有LPC，基本判定为空框
    2. row['ACTIVEPROCESS'] = ['Trace and Eject'],中控操作主动弹出行李
    3. row['IDENTIFICATIONSTATE'] == ['DELETED_BAGDATA']，判定为DEL BSM
    4.当RECOGNITIONSTATE为NO_READ','MULTI_READ'，判定未读或多读
    5.FLIGHTBUILDTIMELINESS'] == 'EARLY'，判定早到
    6.['ACTIVEPROCESS'] == 'Dump Flight Build'，检查分拣机循环圈数，确实绝大部分都是分拣机主动弃包，少量个例各有原因
    7.'ACTIVEPROCESS'] == 'Unplanned flight'，判定太晚
    8.其余默认判定为ACTIVEPROCESS
    """
    def derive_reason(row):
        # 计算中间值 DEREGISTER_REASON_MCS
        # if pd.notna(row['REASON_MCS']) and row['REASON_MCS'] != 'LOST':
        #     deregister_reason_mcs = row['REASON_MCS']
        # elif row['ATR_RECOGNITION'] in ['MULTI_READ', 'NO_READ']:
        #     deregister_reason_mcs = row['ATR_RECOGNITION']
        # elif row['ATR_IDENTIFICATION'] in ['DELETED_BAGDATA', 'NO_BAGDATA']:
        #     deregister_reason_mcs = row['ATR_IDENTIFICATION']
        # else:
        #     deregister_reason_mcs = 'NO_READ'

        # 最终 DEREGISTER_REASON
        if row['ACTIVEPROCESS'] in ['Lateral_41', 'Lateral_81','Garbage SAT', 'Garbage T3 East', 'Garbage T3 West'] and pd.isna(row['LPC']):
            return 'EMPTY'
        else:
            return row['ACTIVEPROCESS']

    df['DEREGISTER_REASON'] = df.apply(derive_reason, axis=1)
    logging.info(f"共获取 {len(df)} 条记录，涉及 {df['CURRENTSTATIONID'].nunique()} 个不同位置")
    return df

def log_details(df, date_str):
    """
    输出该天每条记录的 DEREGISTER_REASON 和标识符到日志。
    标识符优先使用 xlpc，若为空则使用 pids。
    """
    # 构造标识符列
    df['标识符'] = df['LPC'].fillna(df['PID'])
    # 逐行输出
    for _, row in df.iterrows():
        logging.info(f"明细 - {date_str} - 位置:{row['CURRENTSTATIONID']} - 原因:{row['DEREGISTER_REASON']} - 标识符:{row['标识符']}")


# ==================== 辅助函数 ====================
def log_sheet1(df, date_str):
    """打印该天的 Sheet1 到日志"""
    pivot_final = pd.crosstab(df['CURRENTSTATIONID'], df['ACTIVEPROCESS'], margins=False)
    pivot_final = pivot_final.reindex(sorted(pivot_final.columns), axis=1)
    logging.info(f"===== Sheet1 ({date_str}) =====")
    logging.info("\n" + pivot_final.to_string())

def build_sheet2(df):
    """返回该天的 Sheet2 透视表（DataFrame），输出全部 DEREGISTER_REASON 类型，并添加总计列"""
    # reason_cols = [
    #     'Dump Flight Build', 'Dump Identification', 'EARLY',
    #     'EMPTY', 'DELETED_BAGDATA', 'NO_READ', 'Recirculations','MULTI_READ'
    #      'Unplanned flight'
    # ]
    pivot_reason = pd.crosstab(df['CURRENTSTATIONID'], df['DEREGISTER_REASON'], margins=False)
    # for col in reason_cols:
    #     if col not in pivot_reason.columns:
    #         pivot_reason[col] = 0
    # pivot_reason = pivot_reason[reason_cols]
    pivot_reason['总计'] = pivot_reason.sum(axis=1)
    pivot_reason.index.name = '位置'
    return pivot_reason


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
        print("请输入起始日期（格式 YYYYMMDD，例如 20260710），直接回车则使用昨天：")
        date_str = input().strip()
        if not date_str:
            return datetime.now() - timedelta(days=1)
        try:
            return datetime.strptime(date_str, '%Y%m%d')        
        except ValueError:
            print("输入日期格式错误，将使用昨天")
            return datetime.now() - timedelta(days=1)

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
    start_ts = input_date.replace(hour=16, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end_candidate = start_ts + timedelta(days=7)          # 起始+7天
    yesterday = (datetime.now() - timedelta(days=2)).replace(hour=16, minute=0, second=0, microsecond=0)
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
        any_data = False
        day = start_ts
        while day < end_ts:
            day_end = day + timedelta(days=1)
            logging.info(f"统计日期: {day.strftime('%Y-%m-%d')}")
            print(f"正在统计 {day.strftime('%Y-%m-%d')} ...")
            df = fetch_data(day, day_end)
            df2 = manuallscan_data(day, day_end)

            if not df.empty:
                any_data = True
                sheet_name = day.strftime('%Y-%m-%d')

                df.to_excel(writer, sheet_name=f"明细_{sheet_name}", index=False)
                pivot_reason = build_sheet2(df)
                pivot_reason.to_excel(writer, sheet_name=f"统计_{sheet_name}")
                
                # 再输出日志（即使日志出错，Excel 已经写入）
                try:
                    log_sheet1(df, day.strftime('%Y-%m-%d'))
                    log_details(df, day.strftime('%Y-%m-%d'))
                except Exception as e:
                    logging.error(f"日志输出出错: {e}")
            else:
                logging.info(f"{day.strftime('%Y-%m-%d')} 无数据")
                print(f"{day.strftime('%Y-%m-%d')} 无数据")

            if not df2.empty:
                any_data = True
                sheet_name = day.strftime('%Y-%m-%d')

                df2.to_excel(writer, sheet_name=f"manualscan_detail_{sheet_name}", index=False)
                pivot_reason2 = build_sheet2(df2)
                pivot_reason2.to_excel(writer, sheet_name=f"manualscan统计_{sheet_name}")
                
                # 再输出日志（即使日志出错，Excel 已经写入）
                try:
                    log_sheet1(df, day.strftime('%Y-%m-%d'))
                    log_details(df, day.strftime('%Y-%m-%d'))
                except Exception as e:
                    logging.error(f"日志输出出错: {e}")
            else:
                logging.info(f"{day.strftime('%Y-%m-%d')} 无数据")
                print(f"{day.strftime('%Y-%m-%d')} 无数据")

            day = day_end

        if any_data:
            logging.info(f"Excel 报表已生成：{filename}")
            print(f"Excel 报表已生成：{filename}")
        else:
            # 所有日期无数据，删除空文件（可能已创建但无Sheet）
            if os.path.exists(filename):
                os.remove(filename)
            logging.info("所有日期均无数据，未生成 Excel")
            print("所有日期均无数据，未生成 Excel")

if __name__ == '__main__':
    main()