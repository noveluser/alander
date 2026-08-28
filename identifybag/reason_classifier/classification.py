"""以注销(dereg)为主体的 reason_classification 数据构建。

需求口径：
  - 主体 = deregistration 结果；
  - 每条 dereg 记录都占一行：配到 manual 的带手动扫描信息，配不到的 dereg_* 正常、
    manual_* 置空；
  - REASON 完全用 ACTIVEPROCESS 分类：配对成功后 REASON 默认取 dereg 的
    ACTIVEPROCESS（即 dereg_DEREGISTER_REASON）；
  - 特例：若配对到的 manual 是空框（EMPTY），则最终 REASON 改为 EMPTY。
"""
import pandas as pd

# 空框类 ACTIVEPROCESS：这些流程 + 无 LPC 判定为空框(EMPTY)。
_EMPTY_ACTIVEPROCESS = {
    "Lateral_41", "Lateral_81", "Garbage SAT", "Garbage T3 East", "Garbage T3 West",
}

# 明细列顺序：dereg_*（主体）在前，manual_*（关联）在后，最后是结果列。
_DETAIL_COLUMNS = [
    "dereg_EVENTTS", "dereg_LPC", "dereg_PID", "dereg_CURRENTSTATIONID",
    "dereg_ACTIVEPROCESS", "dereg_DEREGISTER_REASON",
    "manual_EVENTTS", "manual_LPC", "manual_PID", "manual_CURRENTSTATIONID",
    "manual_ACTIVEPROCESS", "manual_DEREGISTER_REASON",
    "REASON", "REASON_SOURCE", "MATCH_CONFIDENCE",
]


def build_classification_df(matched, unmatched_dereg) -> pd.DataFrame:
    """以注销(dereg)结果为主体构建分类数据。

    matched 为配对结果（含 dereg_* 与 manual_* 两套列），unmatched_dereg 为未配到
    手动扫描的注销记录。
    """
    parts = []
    if matched is not None and not matched.empty:
        parts.append(_build_matched(matched))
    if unmatched_dereg is not None and not unmatched_dereg.empty:
        parts.append(_build_unmatched_dereg(unmatched_dereg))

    if not parts:
        return pd.DataFrame(columns=_DETAIL_COLUMNS)
    detail = pd.concat(parts, ignore_index=True)
    return detail[_DETAIL_COLUMNS]


def _is_empty_tray(ap: pd.Series, lpc: pd.Series) -> pd.Series:
    """空框判定：ACTIVEPROCESS 属空框类（或字面 EMPTY）且无 LPC。"""
    return (ap.isin(_EMPTY_ACTIVEPROCESS) | ap.eq("EMPTY")) & lpc.isna()


def _build_matched(matched) -> pd.DataFrame:
    df = pd.DataFrame({
        "dereg_EVENTTS": matched["dereg_EVENTTS"],
        "dereg_LPC": matched["dereg_LPC"],
        "dereg_PID": matched["dereg_PID"],
        "dereg_CURRENTSTATIONID": matched["dereg_CURRENTSTATIONID"],
        "dereg_ACTIVEPROCESS": matched["dereg_ACTIVEPROCESS"],
        "dereg_DEREGISTER_REASON": matched["dereg_DEREGISTER_REASON"],
        "manual_EVENTTS": matched["manual_EVENTTS"],
        "manual_LPC": matched["manual_LPC"],
        "manual_PID": matched["manual_PID"],
        "manual_CURRENTSTATIONID": matched["manual_CURRENTSTATIONID"],
        "manual_ACTIVEPROCESS": matched["manual_ACTIVEPROCESS"],
        "manual_DEREGISTER_REASON": matched["manual_DEREGISTER_REASON"],
    })
    # 默认 REASON = dereg 的 ACTIVEPROCESS
    df["REASON"] = df["dereg_DEREGISTER_REASON"]
    # 特例：关联的 manual 为空框 → REASON 改 EMPTY
    empty = _is_empty_tray(df["manual_ACTIVEPROCESS"], df["manual_LPC"])
    df.loc[empty, "REASON"] = "EMPTY"
    df["REASON_SOURCE"] = "MATCHED_DEREG"
    df["MATCH_CONFIDENCE"] = matched["match_confidence"]
    return df


def _build_unmatched_dereg(unmatched_dereg) -> pd.DataFrame:
    df = pd.DataFrame({
        "dereg_EVENTTS": unmatched_dereg["EVENTTS"],
        "dereg_LPC": unmatched_dereg["LPC"],
        "dereg_PID": unmatched_dereg["PID"],
        "dereg_CURRENTSTATIONID": unmatched_dereg["CURRENTSTATIONID"],
        "dereg_ACTIVEPROCESS": unmatched_dereg["ACTIVEPROCESS"],
        "dereg_DEREGISTER_REASON": unmatched_dereg["DEREGISTER_REASON"],
    })
    for col in ("manual_EVENTTS", "manual_LPC", "manual_PID", "manual_CURRENTSTATIONID",
                "manual_ACTIVEPROCESS", "manual_DEREGISTER_REASON"):
        df[col] = pd.NA
    # 未配到手动扫描：REASON 用 dereg 自身的 ACTIVEPROCESS
    df["REASON"] = df["dereg_DEREGISTER_REASON"]
    df["REASON_SOURCE"] = "UNMATCHED_DEREG"
    df["MATCH_CONFIDENCE"] = "UNMATCHED"
    return df


def build_reason_pivot(df: pd.DataFrame, index_col: str, value_col: str) -> pd.DataFrame:
    """index × REASON 交叉表（带分类总计列），仿照原统计表样式。"""
    pivot = pd.crosstab(df[index_col], df[value_col], margins=False)
    pivot["Total"] = pivot.sum(axis=1)
    pivot.index.name = index_col
    return pivot
