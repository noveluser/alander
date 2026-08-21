#!/usr/bin/python
# coding=utf-8

"""
弃包行李统计 + 物品匹配分析（基于 WC_PACKAGEINFO）
功能：
1. 按天统计指定日期范围（起始日期至起始+7天或昨日，取较短者）。
2. 每天输出（输出文件均为英文命名、xlsx 格式）：
   - 日志：ISCID_EXIT × FINAL_ACTIVE_PROCESS 交叉表（Sheet1）
   - dumpbag_stats_<范围>.xlsx：明细/统计交叉表（DEREGISTER_REASON 分类，完整逻辑）
3. 物品匹配分析：将 manuallscan_data（手动扫描 ManualScan/SpecialDestination）与
   fetch_data（注销 Deregistration）两条 SQL 的查询结果映射到一个 xlsx 文件
   （item_match_<范围>.xlsx）。匹配规则：bag 在注销环节后约 7s~180s 才会进入手动
   扫描环节（时间太短/太长都不是同一件）；按顺序锚点算法建立连接：满足 7~180s 窗口
   的注销记录均为候选（尽量建立匹配），以 bag 顺序位为第一权重、时间差接近中位数
   29.5s 为第二权重选择候选；窗口内实在无候选才进入不匹配名单；存在多个候选（无法
   确认）的匹配用 match_confidence=AMBIGUOUS 标记出来；未匹配记录单独筛选输出。
4. 新增 reason_classification_<范围>.xlsx：参照之前的交叉表，对 DEREGISTER_REASON
   进行分类。以手动扫描结果为主体：建立连接的 bag 使用注销记录的
   DEREGISTER_REASON，未建立连接的才使用手动扫描自身的 DEREGISTER_REASON。
5. 支持命令行参数或交互输入日期。
"""

import sys
import os
import oracledb
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ==================== 配置 ====================
DB_HOST = '10.31.8.21'
DB_PORT = '1521'
DB_SERVICE = 'ORABPI'
DB_USER = r'owner_31_bpi_3_0'
DB_PASSWORD = 'owner31bpi'

# 物品匹配时间窗口：bag 在注销(Deregistration)环节后约 7s~180s 才会进入手动扫描环节，
# 时间太短(< 7s)或太长(> 180s)都不是同一件行李
MIN_MATCH_GAP = timedelta(seconds=7)
MAX_MATCH_GAP = timedelta(seconds=180)
# 多候选匹配权重：bag 顺序位为第一权重（位置靠前优先）；
# 时间差接近中位数 29.5s 为第二权重
MEDIAN_GAP = timedelta(seconds=29.5)

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


def derive_reason(row, circle_checker=None):
    """
    派生 DEREGISTER_REASON（完整 Splunk 逻辑）。
    1. ACTIVEPROCESS 为 Garbage/Lateral 且无 LPC → EMPTY（空框）
    2. ACTIVEPROCESS == 'Trace and Eject' → 中控操作主动弹出行李
    3. IDENTIFICATIONSTATE == 'DELETED_BAGDATA' → DEL BSM
    4. RECOGNITIONSTATE in ('NO_READ','MULTI_READ') → 未读/多读
    5. FLIGHTBUILDTIMELINESS == 'EARLY' → 早到
    6. ACTIVEPROCESS == 'Dump Flight Build' → 检查分拣机循环圈数（≥6 → Recirculations）
    7. ACTIVEPROCESS == 'Unplanned flight' → 太晚
    8. 其余默认判定为 ACTIVEPROCESS
    """
    if row['ACTIVEPROCESS'] in ['Lateral_41', 'Lateral_81', 'Garbage SAT', 'Garbage T3 East', 'Garbage T3 West'] and pd.isna(row['LPC']):
        return 'EMPTY'
    if row['ACTIVEPROCESS'] == 'Trace and Eject':
        return 'Trace and Eject'
    if row['IDENTIFICATIONSTATE'] == 'DELETED_BAGDATA':
        return 'DEL BSM'
    if row['RECOGNITIONSTATE'] in ('NO_READ', 'MULTI_READ'):
        return row['RECOGNITIONSTATE']
    if row['FLIGHTBUILDTIMELINESS'] == 'EARLY':
        return 'EARLY'
    if row['ACTIVEPROCESS'] == 'Dump Flight Build':
        if circle_checker is not None and pd.notna(row['LPC']):
            return circle_checker(row['LPC'])
        return 'Dump Flight Build'
    if row['ACTIVEPROCESS'] == 'Unplanned flight':
        return 'Unplanned flight'
    return row['ACTIVEPROCESS']


def make_circle_checker(start_ts, end_ts):
    """构造按 LPC 缓存的分拣机循环圈数判定函数（避免对同一 LPC 重复查库）"""
    cache = {}
    def checker(lpc):
        if lpc not in cache:
            cache[lpc] = circle_check(start_ts, end_ts, lpc)
        return cache[lpc]
    return checker



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
                AND EXECUTEDTASK IN ('ManualScan', 'SpecialDestination') 
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
    # 派生 DEREGISTER_REASON（完整 Splunk 逻辑，circle_check 按 LPC 缓存）
    checker = make_circle_checker(start_ts, end_ts)
    df['DEREGISTER_REASON'] = df.apply(lambda r: derive_reason(r, checker), axis=1)
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
    # 派生 DEREGISTER_REASON（完整 Splunk 逻辑，circle_check 按 LPC 缓存）
    checker = make_circle_checker(start_ts, end_ts)
    df['DEREGISTER_REASON'] = df.apply(lambda r: derive_reason(r, checker), axis=1)
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
    pivot_reason['Total'] = pivot_reason.sum(axis=1)
    pivot_reason.index.name = 'Station'
    return pivot_reason


# ==================== 物品匹配（手动扫描 × 注销）====================
def match_records(df_manual, df_fetch, min_gap=MIN_MATCH_GAP, max_gap=MAX_MATCH_GAP, median_gap=MEDIAN_GAP):
    """
    将手动扫描（ManualScan/SpecialDestination）与注销（Deregistration）记录按顺序匹配。

    物理规则：bag 在注销(Deregistration)环节之后约 7s~180s 才会进入手动扫描环节，
    时间太短(< 7s)或太长(> 180s)都不是同一件行李：
        7s <= manual.EVENTTS - fetch.EVENTTS <= 180s   （manual 晚于 fetch）

    顺序锚点算法：
    1. 候选范围：满足 7~180s 时间窗口的全部注销记录
       （第一个匹配从头顺序扫描；之后的匹配从锚点之后扫描）；
    2. 多候选权重（调整匹配函数，尽量建立匹配）：
       - 第一权重：bag 顺序位 —— 位置靠前的注销记录优先；
       - 第二权重：时间差越接近中位数 29.5s 越优先；
    3. 窗口内有候选 → 建立连接，该注销位置成为新的锚点，重新开始该流程；
       窗口内实在没有候选 → 才进入不匹配名单（unmatched_manual）；
    4. 多候选（无法确认）的匹配用 match_confidence = 'AMBIGUOUS' 标记，
       唯一候选标记 'CONFIRMED'；
    5. 未被任何手动扫描匹配的注销记录 → unmatched_fetch。

    返回：
      matched_df       匹配成功的手动扫描记录，并拼接对应注销记录字段（前缀 dereg_），
                       另含 time_diff_sec 列（manual.EVENTTS - fetch.EVENTTS，单位秒，恒在 [7,180]）
                       和 match_confidence 列（CONFIRMED 唯一候选 / AMBIGUOUS 多候选无法确认）
      unmatched_manual 未匹配的手动扫描记录（不匹配，筛选出来）
      unmatched_fetch  未被匹配的注销记录（不匹配，筛选出来）
    """
    if df_manual.empty or df_fetch.empty:
        return pd.DataFrame(), df_manual.copy(), df_fetch.copy()

    man = df_manual.copy()
    fet = df_fetch.copy()
    man['EVENTTS'] = pd.to_datetime(man['EVENTTS'])
    fet['EVENTTS'] = pd.to_datetime(fet['EVENTTS'])
    man = man.sort_values('EVENTTS').reset_index(drop=True)
    fet = fet.sort_values('EVENTTS').reset_index(drop=True)

    man_ts = man['EVENTTS'].to_numpy(dtype='datetime64[ns]')
    fet_ts = fet['EVENTTS'].to_numpy(dtype='datetime64[ns]')
    min_ns = np.timedelta64(min_gap)
    max_ns = np.timedelta64(max_gap)
    median_ns = np.timedelta64(median_gap)
    n_man = len(man)
    n_fet = len(fet)

    def in_window(m_ts, f_ts):
        dt = m_ts - f_ts
        return (dt >= min_ns) and (dt <= max_ns)

    def pick_best(cand_list, m_ts):
        # 第一权重：bag 顺序位（位置靠前优先）；第二权重：时间差接近中位数 29.5s
        return min(cand_list, key=lambda k: (k, abs((m_ts - fet_ts[k]) - median_ns)))

    assignments = []        # [(手动idx, 注销idx), ...]
    confidences = {}        # 手动idx -> 'CONFIRMED' / 'AMBIGUOUS'（多候选无法确认，标记出来）
    unmatched_man_idx = []
    anchor = None           # 上一匹配点在注销列表中的顺序位
    i = 0
    while i < n_man:
        m_ts = man_ts[i]
        # 满足 7~180s 时间窗口的注销记录区间 [lo, hi)（二分定位）
        lo = np.searchsorted(fet_ts, m_ts - max_ns, side='left')
        hi = np.searchsorted(fet_ts, m_ts - min_ns, side='right')
        if anchor is None:
            start_pos = lo
        else:
            # 从锚点（上一匹配 bag 顺序位）之后开始寻找，保证顺序一致
            start_pos = max(lo, anchor + 1)
        cands = [k for k in range(start_pos, min(hi, n_fet)) if in_window(m_ts, fet_ts[k])]
        if cands:
            best = pick_best(cands, m_ts)
            assignments.append((i, best))
            confidences[i] = 'AMBIGUOUS' if len(cands) >= 2 else 'CONFIRMED'
            anchor = best                # 建立新的锚点，重新开始该流程
        else:
            unmatched_man_idx.append(i)  # 实在不满足的才进入未匹配清单
        i += 1

    # 组装匹配结果（按手动扫描时间顺序）
    rows = []
    for i, j in assignments:
        mrow = man.loc[i]
        frow = fet.loc[j]
        row = {}
        for col in man.columns:
            row[f'manual_{col}'] = mrow[col]
        for col in fet.columns:
            row[f'dereg_{col}'] = frow[col]
        row['time_diff_sec'] = float((mrow['EVENTTS'] - frow['EVENTTS']).total_seconds())
        row['match_confidence'] = confidences[i]
        rows.append(row)

    matched_df = pd.DataFrame(rows)
    matched_fet_idx = sorted({j for _, j in assignments})
    unmatched_manual = man.loc[unmatched_man_idx] if unmatched_man_idx else man.iloc[0:0]
    unmatched_fetch = fet.drop(index=matched_fet_idx)
    return matched_df, unmatched_manual, unmatched_fetch


def write_match_excel(filename, day_results, df_manual_all, df_fetch_all):
    """
    将手动扫描/注销的 SQL 查询结果、匹配结果、未匹配记录写入一个 xlsx 文件。

    Sheet 结构（英文命名）：
      summary                每日 手动扫描数 / 注销数 / 匹配数 / 未匹配数 / 匹配率
      manualscan_detail      manuallscan_data 的 SQL 查询结果（原样映射）
      deregistration_detail  fetch_data 的 SQL 查询结果（原样映射）
      matched_YYYY-MM-DD     当天匹配成功的手动扫描×注销记录（同一物品，已建立连接）
      unmatched_manual_YYYY-MM-DD 当天未匹配到注销记录的手动扫描记录
      unmatched_dereg_YYYY-MM-DD 当天未被匹配的注销记录
    """
    # 汇总行
    summary_rows = []
    for date_str, matched, um, uf in day_results:
        n_man = len(um) + len(matched)
        n_fet = len(uf) + len(matched)
        n_confirmed = int((matched['match_confidence'] == 'CONFIRMED').sum()) if not matched.empty else 0
        n_ambiguous = int((matched['match_confidence'] == 'AMBIGUOUS').sum()) if not matched.empty else 0
        summary_rows.append({
            'date': date_str,
            'manual_count': n_man,
            'dereg_count': n_fet,
            'matched_count': len(matched),
            'confirmed_count': n_confirmed,
            'ambiguous_count': n_ambiguous,
            'unmatched_manual': len(um),
            'unmatched_dereg': len(uf),
            'match_rate': (len(matched) / n_man) if n_man else 0.0,
        })

    def _raw_with_date(df_raw):
        raw = df_raw.copy()
        raw.insert(0, 'date', pd.to_datetime(raw['EVENTTS']).dt.strftime('%Y-%m-%d'))
        return raw

    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name='summary', index=False)
        for label, df_raw in (('manualscan_detail', df_manual_all), ('deregistration_detail', df_fetch_all)):
            if not df_raw.empty:
                _raw_with_date(df_raw).to_excel(writer, sheet_name=label, index=False)
        for date_str, matched, um, uf in day_results:
            if not matched.empty:
                # 新增 final_reason 列
                matched['REASON'] = matched.apply(
                    lambda row: row['manual_DEREGISTER_REASON'] 
                    if row['manual_DEREGISTER_REASON'] == 'EMPTY' 
                    else row['dereg_DEREGISTER_REASON'],
                    axis=1
                )
                # matched.to_excel(writer, sheet_name=f'matched_{date_str}', index=False)
                match_column = ["manual_EVENTTS","manual_LPC","manual_PID","manual_DEREGISTER_REASON","dereg_EVENTTS","dereg_LPC","dereg_PID","dereg_DEREGISTER_REASON"]
                existing_cols = [col for col in match_column if col in matched.columns]  + ['REASON']
                matched[existing_cols].to_excel(writer, sheet_name=f'matched_{date_str}', index=False)
            if not um.empty:
                um['REASON'] = um['DEREGISTER_REASON']
                um_col = um.columns.tolist() + ['REASON']
                um[um_col].to_excel(writer, sheet_name=f'unmatched_manual_{date_str}', index=False)
            if not uf.empty:
                uf['REASON'] = uf['DEREGISTER_REASON']
                uf_col = um.columns.tolist() + ['REASON']
                uf[uf_col].to_excel(writer, sheet_name=f'unmatched_dereg_{date_str}', index=False)


def build_classification_df(matched, unmatched_manual):
    """
    以手动扫描(ManualScan)结果为主体构建 DEREGISTER_REASON 分类数据：
    建立连接的 bag 使用注销(Deregistration)记录的 DEREGISTER_REASON，
    未建立连接的才使用手动扫描自身的 DEREGISTER_REASON。
    """
    parts = []
    if matched is not None and not matched.empty:
        parts.append(pd.DataFrame({
            'manual_EVENTTS': matched['manual_EVENTTS'],
            'manual_LPC': matched['manual_LPC'],
            'manual_PID': matched['manual_PID'],
            'manual_CURRENTSTATIONID': matched['manual_CURRENTSTATIONID'],
            'manual_DEREGISTER_REASON': matched['manual_DEREGISTER_REASON'],
            'dereg_EVENTTS': matched['dereg_EVENTTS'],
            'dereg_LPC': matched['dereg_LPC'],
            'dereg_PID': matched['dereg_PID'],
            'dereg_CURRENTSTATIONID': matched['dereg_CURRENTSTATIONID'],
            'dereg_DEREGISTER_REASON': matched['dereg_DEREGISTER_REASON'],
            'REASON_SOURCE': 'MATCHED_DEREG',
            'MATCH_CONFIDENCE': matched['match_confidence'],
        }))
    if unmatched_manual is not None and not unmatched_manual.empty:
        parts.append(pd.DataFrame({
            'manual_EVENTTS': unmatched_manual['EVENTTS'],
            'manual_LPC': unmatched_manual['LPC'],
            'manual_PID': unmatched_manual['PID'],
            'manual_CURRENTSTATIONID': unmatched_manual['CURRENTSTATIONID'],
            'manual_DEREGISTER_REASON': unmatched_manual['DEREGISTER_REASON'],
            'REASON_SOURCE': 'UNMATCHED_MANUAL',
            'MATCH_CONFIDENCE': 'UNMATCHED',
        }))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def build_reason_pivot(df, index_col, value_col):
    """站点/日期 × DEREGISTER_REASON 交叉表（参照之前表格样式），带 Total 列"""
    pivot = pd.crosstab(df[index_col], df[value_col], margins=False)
    pivot['Total'] = pivot.sum(axis=1)
    pivot.index.name = index_col
    return pivot


# def write_reason_excel(filename, day_results):
#     """
#     新增输出文件（xlsx）：对 DEREGISTER_REASON 进行分类（参照之前的统计表格）。

#     Sheet 结构（英文命名）：
#       summary                 日期 × DEREGISTER_REASON 交叉表 + Total
#       station_reason_<date>   当天 站点 × DEREGISTER_REASON 交叉表 + Total
#       detail_<date>           当天逐条记录：EVENTTS/LPC/PID/STATION/原因/原因来源
#     """
#     summary_parts = []
#     for date_str, cls in day_results:
#         if cls is None or cls.empty:
#             continue
#         c = cls.copy()

#         c.insert(0, 'date', date_str)
#         summary_parts.append(c)

#     with pd.ExcelWriter(filename, engine='openpyxl') as writer:
#         if summary_parts:
#             all_df = pd.concat(summary_parts, ignore_index=True)
#             build_reason_pivot(all_df, 'date', 'dereg_DEREGISTER_REASON').to_excel(writer, sheet_name='summary')
#         for date_str, cls in day_results:
#             if cls is None or cls.empty:
#                 continue  

#             # 同样为当前 DataFrame 添加 REASON 列（复制一份，避免影响后续循环）
#             df = cls.copy()
#             df['REASON'] = np.where(
#                 df['manual_DEREGISTER_REASON'] == 'EMPTY',
#                 df['manual_DEREGISTER_REASON'],
#                 df['dereg_DEREGISTER_REASON']
#             )

#             build_reason_pivot(df, 'manual_CURRENTSTATIONID', 'REASON').to_excel(writer, sheet_name=f'station_reason_{date_str}')
#             df.to_excel(writer, sheet_name=f'detail_{date_str}', index=False)


def write_reason_excel(filename, day_results):
    """
    新增输出文件（xlsx）：对 DEREGISTER_REASON 进行分类（参照之前的统计表格）。

    Sheet 结构（英文命名）：
      summary                 日期 × DEREGISTER_REASON 交叉表 + Total
      station_reason_<date>   当天 站点 × DEREGISTER_REASON 交叉表 + Total
      detail_<date>           当天逐条记录：EVENTTS/LPC/PID/STATION/原因/原因来源
    """
    summary_parts = []
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for date_str, cls in day_results:
            if cls is None or cls.empty:
                continue

            # 统一复制并添加 REASON 列（向量化）
            df = cls.copy()
            df['REASON'] = np.where(
                df['manual_DEREGISTER_REASON'] == 'EMPTY',
                df['manual_DEREGISTER_REASON'],
                df['dereg_DEREGISTER_REASON']
            )

            # 1. 构建 summary 汇总数据（添加日期列）
            df_with_date = df.copy()
            df_with_date.insert(0, 'date', date_str)
            summary_parts.append(df_with_date)

            # 2. 写入 station_reason（基于 REASON 分类）
            build_reason_pivot(df, 'manual_CURRENTSTATIONID', 'REASON').to_excel(
                writer, sheet_name=f'station_reason_{date_str}'
            )

            # 3. 写入 detail（包含所有列，含 REASON）
            df.to_excel(writer, sheet_name=f'detail_{date_str}', index=False)

        # 汇总表（基于所有日期的 REASON）
        if summary_parts:
            all_df = pd.concat(summary_parts, ignore_index=True)
            build_reason_pivot(all_df, 'date', 'REASON').to_excel(
                writer, sheet_name='summary'
            )

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

    # 生成Excel文件名（输出文件不使用中文名）
    date_label = f"{start_ts.strftime('%Y-%m-%d')}_to_{end_ts.strftime('%Y-%m-%d')}"
    filename = f"dumpbag_stats_{date_label}.xlsx"

    # 物品匹配结果收集（手动扫描 × 注销，EVENTTS 相差 < 1 分钟视为同一物品）
    match_day_results = []      # [(日期str, 匹配df, 未匹配手动df, 未匹配注销df), ...]
    reason_day_results = []     # [(日期str, 分类df), ...]  以手动扫描结果为主体
    df_manual_all = pd.DataFrame()
    df_fetch_all = pd.DataFrame()

    # 逐日统计并写入Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        any_data = False
        day = start_ts
        while day < end_ts:
            day_end = day + timedelta(days=1)
            logging.info(f"统计日期: {day.strftime('%Y-%m-%d')}")
            print(f"正在统计 {day.strftime('%Y-%m-%d')} ...")
            df = fetch_data(day, day_end)          # 注销 Deregistration
            df2 = manuallscan_data(day, day_end)   # 手动扫描 ManualScan

            # ---- 物品匹配：手动扫描 EVENTTS 与注销 EVENTTS 相差 < 1 分钟即同一物品 ----
            matched, unmatched_manual, unmatched_fetch = match_records(df2, df)
            match_day_results.append((day.strftime('%Y-%m-%d'), matched, unmatched_manual, unmatched_fetch))

            # ---- DEREGISTER_REASON 分类（以手动扫描为主体）----
            classification_df = build_classification_df(matched, unmatched_manual)
            reason_day_results.append((day.strftime('%Y-%m-%d'), classification_df))

            if not df2.empty:
                df_manual_all = pd.concat([df_manual_all, df2], ignore_index=True)
            if not df.empty:
                df_fetch_all = pd.concat([df_fetch_all, df], ignore_index=True)
            logging.info(f"{day.strftime('%Y-%m-%d')} 物品匹配: 匹配 {len(matched)} 条, "
                         f"未匹配手动扫描 {len(unmatched_manual)} 条, 未匹配注销 {len(unmatched_fetch)} 条")

            if not df.empty:
                any_data = True
                sheet_name = day.strftime('%Y-%m-%d')

                df.to_excel(writer, sheet_name=f"detail_{sheet_name}", index=False)
                pivot_reason = build_sheet2(df)
                pivot_reason.to_excel(writer, sheet_name=f"stats_{sheet_name}")
                
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
                pivot_reason2.to_excel(writer, sheet_name=f"manualscan_stats_{sheet_name}")
                
                # 再输出日志（即使日志出错，Excel 已经写入）
                try:
                    log_sheet1(df2, day.strftime('%Y-%m-%d'))
                    log_details(df2, day.strftime('%Y-%m-%d'))
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

    # ---- 输出物品匹配 xlsx：映射两条 SQL 查询结果 + 匹配/未匹配筛选 ----
    if any_data or not df_manual_all.empty or not df_fetch_all.empty:
        match_filename = f"item_match_{date_label}.xlsx"
        write_match_excel(match_filename, match_day_results, df_manual_all, df_fetch_all)
        logging.info(f"物品匹配文件已生成：{match_filename}")
        print(f"物品匹配文件已生成：{match_filename}")

        # ---- 新增：DEREGISTER_REASON 分类输出文件 ----
        reason_filename = f"reason_classification_{date_label}.xlsx"
        write_reason_excel(reason_filename, reason_day_results)
        logging.info(f"DEREGISTER_REASON 分类文件已生成：{reason_filename}")
        print(f"DEREGISTER_REASON 分类文件已生成：{reason_filename}")

if __name__ == '__main__':
    main()