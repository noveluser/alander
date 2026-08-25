"""数据获取与 DEREGISTER_REASON 派生。

`fetch_package_events` 统一了手动扫描（ManualScan / SpecialDestination）与注销
（Deregistration）两条几乎相同的查询，仅参数不同的部分用 `_EVENT_FILTER` 区分，
从根本上消除了原 v4 中两大段重复 SQL。
"""
import pandas as pd

from . import db
from .derivation import DerivationPipeline  # 延迟导入避免环依赖

# 查询结果列（字段与 WC_PACKAGEINFO 一致，统一列名以便按 DataFrame 处理）。
BASE_COLUMNS = [
    "EVENTTS", "LPC", "PID", "ACTIVEPROCESS", "ASSIGNEDTASK",
    "CURRENTSTATIONID", "FLIGHTBUILDTIMELINESS", "IDENTIFICATIONSTATE",
    "MANUALIDTASK", "PROCESSPLANIDNAME", "PROCESSDEFINITIONNAME", "RECOGNITIONSTATE",
]

_TASK_CLAUSE = {
    "manual": "EXECUTEDTASK IN ('ManualScan', 'SpecialDestination')",
    "dereg": "EXECUTEDTASK = 'Deregistration'",
}
_STATION_CLAUSE = {
    "manual": "CURRENTSTATIONID IN (91, 92, 93, 94)",
    "dereg": "CURRENTSTATIONID IN (96, 97, 98, 99)",
}

_SELECT_SQL = """
    SELECT
        EVENTTS, lpc, pid, ACTIVEPROCESS, ASSIGNEDTASK, CURRENTSTATIONID,
        FLIGHTBUILDTIMELINESS, IDENTIFICATIONSTATE, MANUALIDTASK,
        PROCESSPLANIDNAME, PROCESSDEFINITIONNAME, RECOGNITIONSTATE
    FROM WC_PACKAGEINFO
    WHERE 1 = 1
        AND EVENTTS > :start_ts
        AND EVENTTS < :end_ts
        AND {task_clause}
        AND {station_clause}
        AND TARGETPROCESSID = 'BSIS_03997185'
    ORDER BY EVENTTS
"""


def fetch_package_events(kind: str, start_ts, end_ts) -> pd.DataFrame:
    """按类型查询指定时间段内的事件，派生 DEREGISTER_REASON 后返回。

    kind: 'manual'（手动扫描）或 'dereg'（注销）。start_ts/end_ts 为秒级 datetime，
    半开区间 [start_ts, end_ts)。
    """
    sql = _SELECT_SQL.format(
        task_clause=_TASK_CLAUSE[kind],
        station_clause=_STATION_CLAUSE[kind],
    )
    rows = db.fetch_all(sql, {"start_ts": start_ts, "end_ts": end_ts})
    df = pd.DataFrame(rows, columns=BASE_COLUMNS) if rows else pd.DataFrame(columns=BASE_COLUMNS)
    df["EVENTTS"] = pd.to_datetime(df["EVENTTS"])
    df["DEREGISTER_REASON"] = DerivationPipeline(start_ts, end_ts).derive(df)
    return df


def fetch_manual(start_ts, end_ts) -> pd.DataFrame:
    """手动扫描（ManualScan / SpecialDestination）。"""
    return fetch_package_events("manual", start_ts, end_ts)


def fetch_dereg(start_ts, end_ts) -> pd.DataFrame:
    """注销（Deregistration）。"""
    return fetch_package_events("dereg", start_ts, end_ts)


# ------------------------------------------------------------------- 机场名单 ----
# 机场侧白名单：MCS 手动扫描数据（来自 FACT_BAG_SUMMARIES_V）。
# 最终输出只保留「LPC 或 PID 命中该名单」的行李。
_AIRPORT_COLUMNS = ["MANUAL_SCAN_DT", "MANUAL_SCAN_LOCATION", "LPC", "XPID"]

_AIRPORT_SQL = """
    SELECT
        MANUAL_SCAN_DT,
        MANUAL_SCAN_LOCATION,
        LPC,
        XPID
    FROM
        FACT_BAG_SUMMARIES_V
    WHERE
        1 = 1
        AND MCS_RECOGNITION IS NOT NULL
        AND MANUAL_SCAN_DT > :start_ts
        AND MANUAL_SCAN_DT < :end_ts
        AND MANUAL_SCAN_LOCATION IN ('MCS01', 'MCS02', 'MCS03', 'MCS04')
    ORDER BY
        MANUAL_SCAN_DT
"""


def fetch_airport_events(start_ts, end_ts) -> pd.DataFrame:
    """执行机场名单查询，返回 [MANUAL_SCAN_DT, MANUAL_SCAN_LOCATION, LPC, XPID] 表。"""
    rows = db.fetch_all(_AIRPORT_SQL, {"start_ts": start_ts, "end_ts": end_ts})
    if not rows:
        return pd.DataFrame(columns=_AIRPORT_COLUMNS)
    return pd.DataFrame(rows, columns=_AIRPORT_COLUMNS)


def fetch_airport(start_ts, end_ts) -> pd.DataFrame:
    """机场名单（FACT_BAG_SUMMARIES_V 的 MCS 手动扫描白名单）。"""
    return fetch_airport_events(start_ts, end_ts)
