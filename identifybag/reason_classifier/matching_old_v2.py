"""注销 × 手动扫描的顺序锚点匹配算法（以注销 dereg 为主体）。

匹配规则（新需求）：
  - 硬约束（前两条，必须同时满足）：
      1) 站点一一对应 _STATION_PAIR：dereg 站点 -> manual 站点；
      2) 时间差 7s <= manual.EVENTTS - dereg.EVENTTS <= 180s
         （中位数 29.5s 为偏好值，硬区间为 7~180s）。
  - 软匹配（多个候选时择优，后两条）：
      1) LPC 相同优先；
      2) 手动扫描顺序位 k 靠前优先。
  三级微调：|时间差 - 29.5s| 更小者优先。
"""
import numpy as np
import pandas as pd
import logging
from . import config

CONFIRMED = "CONFIRMED"
AMBIGUOUS = "AMBIGUOUS"

# 每个模块获取自己的 logger（模块名作为名称）
logger = logging.getLogger(__name__)

# 站点一一对应：dereg.CURRENTSTATIONID -> 可配对的 manual.CURRENTSTATIONID。
_STATION_PAIR = {96: 91, 97: 92, 98: 93, 99: 94, 191: 191, 192: 192}


def _paired_station(dereg_station):
    """dereg 站点对应的 manual 站点；不在映射表中则返回 None（无法配对）。"""
    try:
        return _STATION_PAIR.get(int(float(dereg_station)))
    except (TypeError, ValueError):
        return None


def _to_int(v):
    """尽力把站点转成整数；失败返回 None（视为无法比较）。"""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _same_item(a_lpc, a_pid, b_lpc, b_pid) -> bool:
    """判断两件是否同一件：LPC 相同 或 PID 相同（都非空且一致）即视为同一件。"""
    if not (pd.isna(a_lpc) or pd.isna(b_lpc)) and str(a_lpc).strip() == str(b_lpc).strip():
        return True
    if not (pd.isna(a_pid) or pd.isna(b_pid)) and str(a_pid).strip() == str(b_pid).strip():
        return True
    return False


def match_records(
    df_manual: pd.DataFrame,
    df_dereg: pd.DataFrame,
    min_gap=config.MIN_MATCH_GAP,
    max_gap=config.MAX_MATCH_GAP,
    t3_median_gap=config.T3_MEDIAN_GAP,
    sat_median_gap=config.SAT_MEDIAN_GAP,
):
    """按顺序锚点算法将注销（主体）与手动扫描配对。

    算法（以 dereg 为主体）：
      1. 遍历每条注销记录（按时间顺序），候选 = 同时满足「站点一一对应」与
         「7~180s 时间窗口」的全部手动扫描记录
         （首个匹配从头扫描，其余从锚点之后扫描，锚点落在手动扫描列表上）；
      2. 多候选择优（软匹配）：LPC 相同优先 -> 手动顺序位靠前优先 -> |时间差-29.5s| 更小；
      3. 窗口+站点内有候选 -> 配对并更新锚点；无候选 -> 进未匹配注销清单；
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

    min_ns = np.timedelta64(min_gap, 's')
    max_ns = np.timedelta64(max_gap, 's')
    t3_median_ns = np.timedelta64(t3_median_gap, 's')
    sat_median_ns = np.timedelta64(sat_median_gap, 's')
    n_manual = len(manual)

    def pick_best(cands, d_ts, dereg_lpc, dereg_pid, dereg_currentstation) -> int:
        # 软匹配择优：LPC 或 PID 相同优先 -> 顺序位靠前 -> |时间差-29.5s| 更小。
        if dereg_currentstation in (191,192):
            median_ns = sat_median_ns
        else:
            median_ns = t3_median_ns
        # logger.debug(f"cands={cands}, d_ts={d_ts}, lpc={dereg_lpc}, pid={dereg_pid}, station={dereg_currentstation}")
        return min(cands, key=lambda k: (
            not _same_item(manual.loc[k]["LPC"], manual.loc[k]["PID"], dereg_lpc, dereg_pid),
            k,
            abs((manual_ts[k] - d_ts) - median_ns),
        ))

    assignments = []          # [(dereg_idx, manual_idx)]
    confidences: dict = {}    # dereg_idx -> CONFIRMED / AMBIGUOUS
    unmatched_dereg_idx = []
    anchor = None             # 上一匹配在手动扫描列表中的顺序位
    one_sec = np.timedelta64(1, 's')

    for j, d_ts in enumerate(dereg_ts):
        logger.debug(f"全部deregsteion station={dereg.loc[j]["CURRENTSTATIONID"]}, pid={dereg.loc[j]["PID"]}")
        target = _paired_station(dereg.loc[j]["CURRENTSTATIONID"])
        if target is None:          # dereg 站点不在映射表 -> 无法配对
            unmatched_dereg_idx.append(j)
            continue

        # 根据目标站点类型决定时间窗口下界
        if target in (191, 192):
            start_offset = one_sec   # 允许 dt > 1s
        else:
            start_offset = min_ns    # 默认 7s

        # 时间窗口内的手动扫描索引区间 [lo, hi)：manual ∈ [dereg+5, dereg+120]
        lo = int(np.searchsorted(manual_ts, d_ts + start_offset, side="left"))
        hi = int(np.searchsorted(manual_ts, d_ts + max_ns, side="right"))
        start = lo if anchor is None else max(lo, anchor + 1)

        # --- 添加日志：窗口信息 ---
        logger.debug(f"j={j}, pid={dereg.loc[j]['PID']}, target={target}, lo={lo}, hi={hi}, start={start}, anchor={anchor}, n_manual={n_manual}")

        cands = []
        for k in range(start, min(hi, n_manual)):
            row = manual.loc[k]
            station = _to_int(row["CURRENTSTATIONID"])
            dt = manual_ts[k] - d_ts
            
            # --- 添加日志：每个候选检查 ---
            logger.debug(f"  checking k={k}, station={station}, dt={dt}")

            if station != target:
                continue

            if station in (191,192) :
                if not (one_sec < dt <= max_ns):
                    continue
            else:
                if not (min_ns <= dt <= max_ns):
                    continue
            cands.append(k)
            logger.debug(f"进入cands的bag station={dereg.loc[j]["CURRENTSTATIONID"]}, pid={dereg.loc[j]["PID"]}")

        if not cands:
            unmatched_dereg_idx.append(j)
            logger.debug(f"未cands的bag station={dereg.loc[j]["CURRENTSTATIONID"]}, pid={dereg.loc[j]["PID"]}")
            continue

        best = pick_best(cands, d_ts, dereg.loc[j]["LPC"], dereg.loc[j]["PID"], dereg.loc[j]["CURRENTSTATIONID"])
        assignments.append((j, best))
        # confidences[j] = AMBIGUOUS if len(cands) >= 2 else CONFIRMED
        # 检查最佳候选是否与当前记录在 LPC/PID 上一致
        is_same_item = _same_item(
            manual.loc[best]["LPC"], manual.loc[best]["PID"],
            dereg.loc[j]["LPC"], dereg.loc[j]["PID"]
        )
        if is_same_item:
            confidences[j] = CONFIRMED
        else:
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
