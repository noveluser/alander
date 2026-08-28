"""注销 × 手动扫描的顺序锚点匹配算法（以注销 dereg 为主体）。

需求：以 deregistration 的结果为主体。遍历每条注销记录，在 7~180s 窗口内为其
寻找对应的手动扫描记录并配对，锚点落在手动扫描列表上。
"""
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
    """按顺序锚点算法将注销（主体）与手动扫描配对。

    物理规则：bag 在注销环节后约 7s~180s 才进入手动扫描，
    即 7s <= manual.EVENTTS - dereg.EVENTTS <= 180s（manual 晚于 dereg）。

    算法（以 dereg 为主体）：
      1. 遍历每条注销记录（按时间顺序），候选 = 满足 7~180s 窗口的全部手动扫描记录
         （首个匹配从头扫描，其余从锚点之后扫描，保证顺序一致）；
      2. 多候选权重：第一权重=手动扫描顺序位（位置靠前优先），
         第二权重=时间差接近中位数 29.5s；
      3. 窗口内有候选 -> 配对并更新锚点；无候选 -> 进未匹配注销清单；
      4. 唯一候选标记 CONFIRMED，多候选（无法确认）标记 AMBIGUOUS。

    返回：
      (matched_df, unmatched_manual, unmatched_dereg)
      - matched_df:        注销字段带 dereg_ 前缀、手动扫描字段带 manual_ 前缀，
                           另有 time_diff_sec 与 match_confidence 两列。
      - unmatched_manual:  未被任何注销匹配的手动扫描记录。
      - unmatched_dereg:   未匹配到手动扫描的注销记录（主体侧未匹配）。
    """
    if df_manual.empty or df_dereg.empty:
        return pd.DataFrame(), df_manual.copy(), df_dereg.copy()

    dereg = df_dereg.copy()
    manual = df_manual.copy()
    dereg["EVENTTS"] = pd.to_datetime(dereg["EVENTTS"])
    manual["EVENTTS"] = pd.to_datetime(manual["EVENTTS"])
    dereg = dereg.sort_values("EVENTTS").reset_index(drop=True)
    manual = manual.sort_values("EVENTTS").reset_index(drop=True)

    dereg_ts = dereg["EVENTTS"].to_numpy(dtype="datetime64[ns]")
    manual_ts = manual["EVENTTS"].to_numpy(dtype="datetime64[ns]")

    min_ns = np.timedelta64(min_gap)
    max_ns = np.timedelta64(max_gap)
    median_ns = np.timedelta64(median_gap)
    n_manual = len(manual)

    def in_window(d_ts, m_ts) -> bool:
        dt = m_ts - d_ts  # manual 晚于 dereg
        return (dt >= min_ns) and (dt <= max_ns)

    def pick_best(cands, d_ts) -> int:
        # 第一权重：手动扫描顺序位靠前；第二权重：时间差接近中位数。
        return min(cands, key=lambda k: (k, abs((manual_ts[k] - d_ts) - median_ns)))

    assignments = []          # [(dereg_idx, manual_idx)]
    confidences: dict = {}    # dereg_idx -> CONFIRMED / AMBIGUOUS
    unmatched_dereg_idx = []
    anchor = None             # 上一匹配在手动扫描列表中的顺序位

    for j, d_ts in enumerate(dereg_ts):
        # 满足窗口的手动扫描记录索引区间 [lo, hi)：manual ∈ [dereg+7, dereg+180]
        lo = int(np.searchsorted(manual_ts, d_ts + min_ns, side="left"))
        hi = int(np.searchsorted(manual_ts, d_ts + max_ns, side="right"))
        start = lo if anchor is None else max(lo, anchor + 1)

        cands = [k for k in range(start, min(hi, n_manual)) if in_window(d_ts, manual_ts[k])]
        if not cands:
            unmatched_dereg_idx.append(j)
            continue

        best = pick_best(cands, d_ts)
        assignments.append((j, best))
        confidences[j] = AMBIGUOUS if len(cands) >= 2 else CONFIRMED
        anchor = best  # 更新锚点，重新开始流程

    matched_df = _assemble(dereg, manual, assignments, confidences)
    matched_manual_idx = sorted({k for _, k in assignments})
    unmatched_dereg = dereg.loc[unmatched_dereg_idx] if unmatched_dereg_idx else dereg.iloc[0:0]
    unmatched_manual = manual.drop(index=matched_manual_idx)
    return matched_df, unmatched_manual, unmatched_dereg


def _assemble(dereg, manual, assignments, confidences) -> pd.DataFrame:
    """把 (dereg_idx, manual_idx) 配对组装为带前缀连接后的 DataFrame。"""
    rows = []
    for j, k in assignments:
        d = dereg.loc[j]
        m = manual.loc[k]
        row = {f"dereg_{col}": d[col] for col in dereg.columns}
        row.update({f"manual_{col}": m[col] for col in manual.columns})
        row["time_diff_sec"] = float((m["EVENTTS"] - d["EVENTTS"]).total_seconds())
        row["match_confidence"] = confidences[j]
        rows.append(row)
    return pd.DataFrame(rows)
