"""以手动扫描为主体的 reason_classification 数据构建。"""
import numpy as np
import pandas as pd

# 保留进 reason_classification 明细的列（统一列名，避免 v4 中两个分支列名不一致）。
_DETAIL_COLUMNS = [
    "manual_EVENTTS", "manual_LPC", "manual_PID", "manual_CURRENTSTATIONID",
    "manual_DEREGISTER_REASON",
    "dereg_EVENTTS", "dereg_LPC", "dereg_PID", "dereg_CURRENTSTATIONID",
    "dereg_DEREGISTER_REASON",
    "REASON", "REASON_SOURCE", "MATCH_CONFIDENCE",
]


def build_classification_df(matched, unmatched_manual) -> pd.DataFrame:
    """以手动扫描结果为主体构建分类数据。

    口径（与业务一致）：
      - 建立连接的 bag  -> 使用注销记录的 DEREGISTER_REASON；
        `manual_DEREGISTER_REASON == EMPTY` 时空框信息仍予以保留。
      - 未建立连接的 bag -> 使用手动扫描自身的 DEREGISTER_REASON。

    所有行统一含 manual_* 与 dereg_* 两套列（未匹配行 dereg_* 置空），
    并派生最终 REASON 一列，避免 v4 中因列缺失导致的取值错乱。
    """
    parts = []
    if matched is not None and not matched.empty:
        parts.append(_build_matched(matched))
    if unmatched_manual is not None and not unmatched_manual.empty:
        parts.append(_build_unmatched(unmatched_manual))

    if not parts:
        return pd.DataFrame(columns=_DETAIL_COLUMNS)
    detail = pd.concat(parts, ignore_index=True)
    return detail[_DETAIL_COLUMNS]


def _build_matched(matched) -> pd.DataFrame:
    df = pd.DataFrame({
        "manual_EVENTTS": matched["manual_EVENTTS"],
        "manual_LPC": matched["manual_LPC"],
        "manual_PID": matched["manual_PID"],
        "manual_CURRENTSTATIONID": matched["manual_CURRENTSTATIONID"],
        "manual_DEREGISTER_REASON": matched["manual_DEREGISTER_REASON"],
        "dereg_EVENTTS": matched["dereg_EVENTTS"],
        "dereg_LPC": matched["dereg_LPC"],
        "dereg_PID": matched["dereg_PID"],
        "dereg_CURRENTSTATIONID": matched["dereg_CURRENTSTATIONID"],
        "dereg_DEREGISTER_REASON": matched["dereg_DEREGISTER_REASON"],
    })
    # 建立连接的 bag：最终原因取注销原因；空框(EMPTY)保留手动侧信息。
    df["REASON"] = np.where(
        df["manual_DEREGISTER_REASON"].eq("EMPTY"),
        df["manual_DEREGISTER_REASON"],
        df["dereg_DEREGISTER_REASON"],
    )
    df["REASON_SOURCE"] = "MATCHED_DEREG"
    df["MATCH_CONFIDENCE"] = matched["match_confidence"]
    return df


def _build_unmatched(unmatched) -> pd.DataFrame:
    df = pd.DataFrame({
        "manual_EVENTTS": unmatched["EVENTTS"],
        "manual_LPC": unmatched["LPC"],
        "manual_PID": unmatched["PID"],
        "manual_CURRENTSTATIONID": unmatched["CURRENTSTATIONID"],
        "manual_DEREGISTER_REASON": unmatched["DEREGISTER_REASON"],
    })
    for col in ("dereg_EVENTTS", "dereg_LPC", "dereg_PID", "dereg_CURRENTSTATIONID", "dereg_DEREGISTER_REASON"):
        df[col] = pd.NA
    # 未建立连接的 bag：使用手动扫描自身原因。
    df["REASON"] = df["manual_DEREGISTER_REASON"]
    df["REASON_SOURCE"] = "UNMATCHED_MANUAL"
    df["MATCH_CONFIDENCE"] = "UNMATCHED"
    return df


def build_reason_pivot(df: pd.DataFrame, index_col: str, value_col: str) -> pd.DataFrame:
    """index × REASON 交叉表（带分类总计列），仿照原统计表样式。"""
    pivot = pd.crosstab(df[index_col], df[value_col], margins=False)
    pivot["Total"] = pivot.sum(axis=1)
    pivot.index.name = index_col
    return pivot
