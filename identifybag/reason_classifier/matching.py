"""手动扫描 × 注销的顺序锚点匹配算法。"""
import numpy as np
import pandas as pd

from . import config

CONFIRMED = "CONFIRMED"
AMBIGUOUS = "AMBIGUOUS"


def match_records(
    df_manual: pd.DataFrame,
    df_dereg: pd.DataFrame,
    min_gap=config.MIN_MATCH_GAP,
    max_gap=config.MAX_MATCH_GAP,
    median_gap=config.MEDIAN_GAP,
):
    """按顺序锚点算法将手动扫描与注销记录配对。

    物理规则：bag 在注销环节后约 7s~180s 才进入手动扫描，
    即 7s <= manual.EVENTTS - dereg.EVENTTS <= 180s（manual 晚于 dereg）。

    算法：
      1. 候选 = 满足 7~180s 窗口的全部注销记录（首个匹配从头扫描，其余从锚点之后扫描，
         保证顺序一致）；
      2. 多候选权重：第一权重=bag 顺序位（位置靠前优先），第二权重=时间差接近中位数 29.5s；
      3. 窗口内有候选 -> 配对并更新锚点；无候选 -> 进未匹配清单；
      4. 唯一候选标记 CONFIRMED，多候选（无法确认）标记 AMBIGUOUS。

    返回：
      (matched_df, unmatched_manual, unmatched_dereg)
      - matched_df:        手动字段带 manual_ 前缀、注销字段带 dereg_ 前缀，
                           另有 time_diff_sec 与 match_confidence 两列。
      - unmatched_manual:  未匹配的手动扫描记录。
      - unmatched_dereg:   未被任何手动扫描匹配的注销记录。
    """
    if df_manual.empty or df_dereg.empty:
        empty_manual = df_manual.copy()
        empty_dereg = df_dereg.copy()
        return pd.DataFrame(), empty_manual, empty_dereg

    man = df_manual.copy()
    fet = df_dereg.copy()
    # 与 v4 完全一致：显式转 datetime 后再排序（保证任何调用路径行为一致）。
    man["EVENTTS"] = pd.to_datetime(man["EVENTTS"])
    fet["EVENTTS"] = pd.to_datetime(fet["EVENTTS"])
    man = man.sort_values("EVENTTS").reset_index(drop=True)
    fet = fet.sort_values("EVENTTS").reset_index(drop=True)
    man_ts = man["EVENTTS"].to_numpy(dtype="datetime64[ns]")
    fet_ts = fet["EVENTTS"].to_numpy(dtype="datetime64[ns]")

    min_ns = np.timedelta64(min_gap)
    max_ns = np.timedelta64(max_gap)
    median_ns = np.timedelta64(median_gap)
    n_fet = len(fet)

    def in_window(m_ts, f_ts) -> bool:
        dt = m_ts - f_ts
        return (dt >= min_ns) and (dt <= max_ns)

    def pick_best(cands, m_ts) -> int:
        # 第一权重：顺序位靠前；第二权重：时间差接近中位数。用元组作排序键一次取最小。
        return min(cands, key=lambda k: (k, abs((m_ts - fet_ts[k]) - median_ns)))

    assignments = []   # [(manual_idx, dereg_idx)]
    confidences: dict = {}
    unmatched_man_idx = []
    anchor = None

    for i, m_ts in enumerate(man_ts):
        # 满足窗口的注销记录索引区间 [lo, hi)
        lo = int(np.searchsorted(fet_ts, m_ts - max_ns, side="left"))
        hi = int(np.searchsorted(fet_ts, m_ts - min_ns, side="right"))
        start = lo if anchor is None else max(lo, anchor + 1)

        cands = [k for k in range(start, min(hi, n_fet)) if in_window(m_ts, fet_ts[k])]
        if not cands:
            unmatched_man_idx.append(i)
            continue

        best = pick_best(cands, m_ts)
        assignments.append((i, best))
        confidences[i] = AMBIGUOUS if len(cands) >= 2 else CONFIRMED
        anchor = best  # 更新锚点，重新开始流程

    matched_df = _assemble(man, fet, assignments, confidences)
    matched_dereg_idx = sorted({j for _, j in assignments})
    unmatched_manual = man.loc[unmatched_man_idx] if unmatched_man_idx else man.iloc[0:0]
    unmatched_dereg = fet.drop(index=matched_dereg_idx)
    return matched_df, unmatched_manual, unmatched_dereg


def _assemble(man, fet, assignments, confidences) -> pd.DataFrame:
    """把 (manual_idx, dereg_idx) 配对组装为带前缀连接后的 DataFrame。"""
    rows = []
    for i, j in assignments:
        m = man.loc[i]
        f = fet.loc[j]
        row = {f"manual_{col}": m[col] for col in man.columns}
        row.update({f"dereg_{col}": f[col] for col in fet.columns})
        row["time_diff_sec"] = float((m["EVENTTS"] - f["EVENTTS"]).total_seconds())
        row["match_confidence"] = confidences[i]
        rows.append(row)
    return pd.DataFrame(rows)
