"""注销 × 手动扫描的顺序锚点匹配算法（两阶段策略）"""
import numpy as np
import pandas as pd
import logging
from . import config

CONFIRMED = "CONFIRMED"
AMBIGUOUS = "AMBIGUOUS"

logger = logging.getLogger(__name__)

_STATION_PAIR = {96: 91, 97: 92, 98: 93, 99: 94, 191: 191, 192: 192}


def _paired_station(dereg_station):
    try:
        return _STATION_PAIR.get(int(float(dereg_station)))
    except (TypeError, ValueError):
        return None


def _to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _same_pid(a, b):
    if pd.isna(a) or pd.isna(b):
        return False
    return str(a).strip() == str(b).strip()


def _same_lpc(a, b):
    if pd.isna(a) or pd.isna(b):
        return False
    return str(a).strip() == str(b).strip()


def match_records(
    df_manual: pd.DataFrame,
    df_dereg: pd.DataFrame,
    # 窄窗口参数（锚点确认）
    narrow_t3_min=7,
    narrow_t3_max=40,
    narrow_sat_min=1,
    narrow_sat_max=30,
    # 宽窗口参数（局部搜索）
    wide_t3_min=7,
    wide_t3_max=120,
    wide_sat_min=1,
    wide_sat_max=120,
    # 中位数
    t3_median_gap=config.T3_MEDIAN_GAP,
    sat_median_gap=config.SAT_MEDIAN_GAP,
    # 锚点窗口偏移量
    anchor_window=10,
):
    logger.info("开始匹配: dereg记录数=%d, manual记录数=%d", len(df_dereg), len(df_manual))

    if df_manual.empty or df_dereg.empty:
        logger.warning("输入数据为空，返回空结果")
        return pd.DataFrame(), df_manual.copy(), df_dereg.copy()

    # 预处理
    dereg = df_dereg.copy()
    manual = df_manual.copy()
    dereg["EVENTTS"] = pd.to_datetime(dereg["EVENTTS"])
    manual["EVENTTS"] = pd.to_datetime(manual["EVENTTS"])
    dereg = dereg.sort_values("EVENTTS").reset_index(drop=True)
    manual = manual.sort_values("EVENTTS").reset_index(drop=True)

    dereg_ts = dereg["EVENTTS"].to_numpy(dtype="datetime64[ns]")
    manual_ts = manual["EVENTTS"].to_numpy(dtype="datetime64[ns]")
    n_manual = len(manual)

    def to_td(sec):
        return np.timedelta64(sec, 's')

    # 窄窗口阈值
    n_min_t3 = to_td(narrow_t3_min)
    n_max_t3 = to_td(narrow_t3_max)
    n_min_sat = to_td(narrow_sat_min)
    n_max_sat = to_td(narrow_sat_max)

    # 宽窗口阈值
    w_min_t3 = to_td(wide_t3_min)
    w_max_t3 = to_td(wide_t3_max)
    w_min_sat = to_td(wide_sat_min)
    w_max_sat = to_td(wide_sat_max)

    t3_median = to_td(t3_median_gap)
    sat_median = to_td(sat_median_gap)
    one_sec = to_td(1)

    def get_window_params(station, is_narrow):
        if station in (191, 192):
            return (n_min_sat, n_max_sat) if is_narrow else (w_min_sat, w_max_sat)
        else:
            return (n_min_t3, n_max_t3) if is_narrow else (w_min_t3, w_max_t3)

    def pick_best(cands, d_ts, dereg_station):
        median = sat_median if dereg_station in (191, 192) else t3_median
        def key_func(k):
            dt = manual_ts[k] - d_ts
            return (k, abs(dt - median))
        return min(cands, key=key_func)

    # ========== 第一阶段 ==========
    assignments = []
    confidences = {}
    occupied_manual = set()
    confirmed_anchors = []

    # ---- 第一轮：窄窗口唯一候选 ----
    logger.info("第一阶段第一轮：窄窗口唯一候选匹配开始")
    for j, d_ts in enumerate(dereg_ts):
        target = _paired_station(dereg.loc[j]["CURRENTSTATIONID"])
        if target is None:
            continue

        min_delta, max_delta = get_window_params(target, is_narrow=True)
        lo = int(np.searchsorted(manual_ts, d_ts + min_delta, side="left"))
        hi = int(np.searchsorted(manual_ts, d_ts + max_delta, side="right"))

        cands = []
        for k in range(lo, min(hi, n_manual)):
            if k in occupied_manual:
                continue
            row = manual.loc[k]
            if _to_int(row["CURRENTSTATIONID"]) != target:
                continue
            dt = manual_ts[k] - d_ts
            if not (min_delta <= dt <= max_delta):
                continue
            cands.append(k)

        if len(cands) == 1:
            best = cands[0]
            assignments.append((j, best))
            confidences[j] = CONFIRMED
            occupied_manual.add(best)
            confirmed_anchors.append((j, best))
            logger.debug("第一轮配对: dereg[%d] -> manual[%d]", j, best)

    logger.info("第一轮完成，匹配 %d 对", len(confirmed_anchors))

    # ---- 第二轮：窄窗口 + PID/LPC 相同 ----
    logger.info("第二阶段第一轮：窄窗口 + PID/LPC 相同匹配开始")
    matched_dereg = {j for j, _ in assignments}
    round2_count = 0
    for j, d_ts in enumerate(dereg_ts):
        if j in matched_dereg:
            continue
        target = _paired_station(dereg.loc[j]["CURRENTSTATIONID"])
        if target is None:
            continue

        min_delta, max_delta = get_window_params(target, is_narrow=True)
        lo = int(np.searchsorted(manual_ts, d_ts + min_delta, side="left"))
        hi = int(np.searchsorted(manual_ts, d_ts + max_delta, side="right"))

        cands = []
        for k in range(lo, min(hi, n_manual)):
            if k in occupied_manual:
                continue
            row = manual.loc[k]
            if _to_int(row["CURRENTSTATIONID"]) != target:
                continue
            dt = manual_ts[k] - d_ts
            if not (min_delta <= dt <= max_delta):
                continue
            if _same_pid(dereg.loc[j]["PID"], row["PID"]) or _same_lpc(dereg.loc[j]["LPC"], row["LPC"]):
                cands.append(k)

        if len(cands) == 1:
            best = cands[0]
            assignments.append((j, best))
            confidences[j] = CONFIRMED
            occupied_manual.add(best)
            confirmed_anchors.append((j, best))
            round2_count += 1
            logger.debug("第二轮配对: dereg[%d] -> manual[%d]", j, best)

    logger.info("第二轮完成，匹配 %d 对", round2_count)
    logger.info("第一阶段总计匹配 %d 对，锚点数 %d", len(assignments), len(confirmed_anchors))

    # ========== 第二阶段 ==========
    matched_dereg = {j for j, _ in assignments}
    if not confirmed_anchors:
        logger.warning("没有确认锚点，第二阶段跳过")
    else:
        logger.info("第二阶段：局部搜索开始，锚点窗口偏移量 = %d", anchor_window)
        phase2_count = 0
        for j, d_ts in enumerate(dereg_ts):
            if j in matched_dereg:
                continue
            target = _paired_station(dereg.loc[j]["CURRENTSTATIONID"])
            if target is None:
                continue

            # 宽窗口
            min_delta, max_delta = get_window_params(target, is_narrow=False)
            lo = int(np.searchsorted(manual_ts, d_ts + min_delta, side="left"))
            hi = int(np.searchsorted(manual_ts, d_ts + max_delta, side="right"))

            # 找最近锚点
            closest_anchor = min(confirmed_anchors, key=lambda x: abs(x[0] - j))
            anchor_manual = closest_anchor[1]

            search_start = max(lo, anchor_manual)
            search_end = min(hi, anchor_manual + anchor_window)

            logger.debug("dereg[%d] 搜索范围: lo=%d, hi=%d, anchor_manual=%d, start=%d, end=%d",
                         j, lo, hi, anchor_manual, search_start, search_end)

            cands = []
            for k in range(search_start, min(search_end, n_manual)):
                if k in occupied_manual:
                    continue
                row = manual.loc[k]
                if _to_int(row["CURRENTSTATIONID"]) != target:
                    continue
                dt = manual_ts[k] - d_ts
                if not (min_delta <= dt <= max_delta):
                    continue
                cands.append(k)

            if not cands:
                logger.debug("dereg[%d] 在缩小范围内无候选", j)
                continue

            best = pick_best(cands, d_ts, target)
            assignments.append((j, best))
            occupied_manual.add(best)

            near_anchor = any(abs(j - a_d) <= anchor_window for a_d, _ in confirmed_anchors)
            if near_anchor and len(cands) >= 2:
                confidences[j] = AMBIGUOUS
            else:
                confidences[j] = CONFIRMED
            phase2_count += 1
            logger.debug("第二阶段配对: dereg[%d] -> manual[%d], 候选数=%d, 置信度=%s",
                         j, best, len(cands), confidences[j])

        logger.info("第二阶段完成，匹配 %d 对", phase2_count)

    # ========== 统计与组装 ==========
    logger.info("总匹配数: %d", len(assignments))
    matched_dereg_final = {j for j, _ in assignments}
    all_dereg_idx = set(range(len(dereg)))
    unmatched_dereg_idx = list(all_dereg_idx - matched_dereg_final)
    logger.info("未匹配dereg数: %d", len(unmatched_dereg_idx))

    matched_df = _assemble(dereg, manual, assignments, confidences)
    matched_manual_idx = sorted({k for _, k in assignments})
    unmatched_dereg = dereg.loc[unmatched_dereg_idx] if unmatched_dereg_idx else dereg.iloc[0:0]
    unmatched_manual = manual.drop(index=matched_manual_idx)
    logger.info("未匹配manual数: %d", len(unmatched_manual))

    return matched_df, unmatched_manual, unmatched_dereg


def _assemble(dereg, manual, assignments, confidences):
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