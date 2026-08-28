"""数据获取。

`fetch_package_events` 统一了手动扫描（ManualScan / SpecialDestination）与注销
（Deregistration）两条几乎相同的查询，仅参数不同的部分用 `_EVENT_FILTER` 区分，
从根本上消除了原 v4 中两大段重复 SQL。
"""
import pandas as pd

from . import db

# 查询结果列（字段与 WC_PACKAGEINFO 一致，统一列名以便按 DataFrame 处理）。
BASE_COLUMNS = [
    "EVENTTS", "LPC", "PID", "ACTIVEPROCESS", "ASSIGNEDTASK",
    "CURRENTSTATIONID", "FLIGHTBUILDTIMELINESS", "IDENTIFICATIONSTATE",
    "MANUALIDTASK", "PROCESSPLANIDNAME", "PROCESSDEFINITIONNAME", "RECOGNITIONSTATE",
]

_WHERE_CLAUSE = {

    "dereg": """
        (
            (EXECUTEDTASK IN ('RouteToMC') AND CURRENTSTATIONID IN (191, 192))
            OR (EXECUTEDTASK = 'Deregistration' AND CURRENTSTATIONID IN (96, 97, 98, 99))
        )
    """,

    "manual": """
        (
            (EXECUTEDTASK IN ('ManualScan', 'Deregistration') AND CURRENTSTATIONID IN (191, 192))
            OR (ASSIGNEDTASK = 'RouteToMC' AND EXECUTEDTASK = 'Deregistration' AND ACTIVEPROCESS = 'Stop T3 in SAT')
            OR (EXECUTEDTASK IN ('ManualScan', 'SpecialDestination') AND CURRENTSTATIONID IN (91, 92, 93, 94, 191,192))
        )
    """,
}

# _WHERE_CLAUSE = {

#     "dereg": """
#         (
            
#             EXECUTEDTASK = 'Deregistration' AND CURRENTSTATIONID IN (96, 97, 98, 99)
#         )
#     """,

#     "manual": """
#         (
#             EXECUTEDTASK IN ('ManualScan', 'SpecialDestination') AND CURRENTSTATIONID IN (91, 92, 93, 94)
#         )
#     """,
# }

_SELECT_SQL = """
    SELECT
        EVENTTS, lpc, pid, ACTIVEPROCESS, ASSIGNEDTASK, CURRENTSTATIONID,
        FLIGHTBUILDTIMELINESS, IDENTIFICATIONSTATE, MANUALIDTASK,
        PROCESSPLANIDNAME, PROCESSDEFINITIONNAME, RECOGNITIONSTATE
    FROM WC_PACKAGEINFO
    WHERE 1 = 1
        AND EVENTTS > :start_ts
        AND EVENTTS < :end_ts
        AND {where_clause}
        AND TARGETPROCESSID = 'BSIS_03997185'
    ORDER BY EVENTTS
"""


def fetch_package_events(kind: str, start_ts, end_ts) -> pd.DataFrame:
    """按类型查询指定时间段内的事件，派生 DEREGISTER_REASON 后返回。

    kind: 'manual'（手动扫描）或 'dereg'（注销）。start_ts/end_ts 为秒级 datetime，
    半开区间 [start_ts, end_ts)。
    """
    sql = _SELECT_SQL.format(
        where_clause=_WHERE_CLAUSE[kind],
    )
    rows = db.fetch_all(sql, {"start_ts": start_ts, "end_ts": end_ts})
    df = pd.DataFrame(rows, columns=BASE_COLUMNS) if rows else pd.DataFrame(columns=BASE_COLUMNS)
    df["EVENTTS"] = pd.to_datetime(df["EVENTTS"])
    # 需求：REASON 完全用 ACTIVEPROCESS 分类（不再用 8 条派生规则）。
    # 空框(EMPTY) 由分类阶段根据 ACTIVEPROCESS + LPC 判断，见 classification.py。
    df["DEREGISTER_REASON"] = df["ACTIVEPROCESS"]
    return df


def fetch_manual(start_ts, end_ts) -> pd.DataFrame:
    """手动扫描（ManualScan / SpecialDestination）。"""
    return fetch_package_events("manual", start_ts, end_ts)


def fetch_dereg(start_ts, end_ts) -> pd.DataFrame:
    """注销（Deregistration）。"""
    return fetch_package_events("dereg", start_ts, end_ts)
