"""注销 × 手动扫描的顺序锚点匹配（两阶段策略）

两阶段策略：
  第一阶段（确认锚点）
    第一轮：窄窗口内「唯一候选」的直接配对；
    第二轮：窄窗口内仅「PID/LPC 相同」的未配 dereg 补配。
  第二阶段（局部搜索）
    以已确认锚点的 manual 位置为基准，在宽窗口内就近把剩余 dereg 配到 manual。

硬约束：站点一一对应 + 时间窗口；软匹配：PID/LPC 相同优先、顺序位靠前优先。
匹配参数（窗口、中位数、锚点偏移量）集中在 config.py，便于统一调整。
"""
import logging

import numpy as np
import pandas as pd

from . import config

CONFIRMED = "CONFIRMED"
AMBIGUOUS = "AMBIGUOUS"

logger = logging.getLogger(__name__)

# 站点一一对应：dereg 站点 -> 可配对的 manual 站点。
_STATION_PAIR = {96: 91, 97: 92, 98: 93, 99: 94, 191: 191, 192: 192}
# 站点类型：191/192 用 SAT 窗口，其余用 T3 窗口。
_SAT_STATIONS = (191, 192)


def _to_int(v):
    """尽力把值转成整数；失败返回 None。"""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _paired_station(dereg_station):
    """dereg 站点对应的 manual 站点；不在映射表中则返回 None（无法配对）。"""
    return _STATION_PAIR.get(_to_int(dereg_station))


def _same_item(a_lpc, a_pid, b_lpc, b_pid) -> bool:
    """同一件判定：LPC 相同 或 PID 相同（都非空且一致）即视为同一件。"""
    if not (pd.isna(a_lpc) or pd.isna(b_lpc)) and str(a_lpc).strip() == str(b_lpc).strip():
        return True
    if not (pd.isna(a_pid) or pd.isna(b_pid)) and str(a_pid).strip() == str(b_pid).strip():
        return True
    return False


def _td(sec):
    """把秒数（或 timedelta）统一转成 numpy 时间增量，便于与 datetime64 相减。"""
    return np.timedelta64(sec, "s")


class _Matcher:
    """一次匹配的全部状态与分阶段逻辑；match_records 只是它的薄封装。"""

    def __init__(self, df_manual, df_dereg, p):
        # ---------- 预处理：转时间 + 按 EVENTTS 排序 ----------
        self.manual = df_manual.copy()
        self.dereg = df_dereg.copy()
        self.manual["EVENTTS"] = pd.to_datetime(self.manual["EVENTTS"])
        self.dereg["EVENTTS"] = pd.to_datetime(self.dereg["EVENTTS"])
        self.manual = self.manual.sort_values("EVENTTS").reset_index(drop=True)
        self.dereg = self.dereg.sort_values("EVENTTS").reset_index(drop=True)
        self.manual_ts = self.manual["EVENTTS"].to_numpy(dtype="datetime64[ns]")
        self.dereg_ts = self.dereg["EVENTTS"].to_numpy(dtype="datetime64[ns]")
        self.n_manual = len(self.manual)

        # ---------- 窗口与中位数（按站点类型区分 T3 / SAT） ----------
        self.narrow = {
            "t3": (_td(p.narrow_t3_min), _td(p.narrow_t3_max)),
            "sat": (_td(p.narrow_sat_min), _td(p.narrow_sat_max)),
        }
        self.wide = {
            "t3": (_td(p.wide_t3_min), _td(p.wide_t3_max)),
            "sat": (_td(p.wide_sat_min), _td(p.wide_sat_max)),
        }
        self.median = {"t3": _td(p.t3_median_gap), "sat": _td(p.sat_median_gap)}
        self.anchor_window = p.anchor_window

        # ---------- 匹配状态（跨阶段共享） ----------
        self.assignments = []          # [(dereg_idx, manual_idx)]
        self.confidences = {}          # dereg_idx -> CONFIRMED / AMBIGUOUS
        self.occupied = set()          # 已被占用的 manual 索引
        self.anchors = []              # 第一阶段确认的锚点 [(dereg_idx, manual_idx)]

    # ---------------------------------------------------------------- 基础工具 ----
    def _station_kind(self, station) -> str:
        """站点类型：'sat' 或 't3'。"""
        return "sat" if station in _SAT_STATIONS else "t3"

    def _window(self, station, is_narrow):
        """取某站点对应 (min, max) 窗口（窄/宽）。"""
        return (self.narrow if is_narrow else self.wide)[self._station_kind(station)]

    def _window_bounds(self, d_ts, min_delta, max_delta):
        """时间窗口内的 manual 索引区间 [lo, hi)。"""
        lo = int(np.searchsorted(self.manual_ts, d_ts + min_delta, side="left"))
        hi = int(np.searchsorted(self.manual_ts, d_ts + max_delta, side="right"))
        return lo, hi

    def _pick_best(self, cands, d_ts, station) -> int:
        """软匹配择优：顺序位靠前优先，其次时间差接近中位数。"""
        median = self.median[self._station_kind(station)]

        def key(k):
            return (k, abs((self.manual_ts[k] - d_ts) - median))

        return min(cands, key=key)

    # ---------------------------------------------------------------- 候选与记录 ----
    def _candidates(self, j, d_ts, target, min_delta, max_delta, start_k, end_k, same_key=False):
        """收集候选 manual 索引：统一过滤「未占用 + 站点对 + 时间窗口」。

        start_k/end_k 限定搜索范围（时间窗口内或锚点局部范围）；
        same_key=True 时只保留 PID/LPC 与 dereg 相同的候选。
        """
        dereg_row = self.dereg.loc[j]
        cands = []
        for k in range(start_k, min(end_k, self.n_manual)):
            if k in self.occupied:
                continue
            row = self.manual.loc[k]
            if _to_int(row["CURRENTSTATIONID"]) != target:
                continue
            dt = self.manual_ts[k] - d_ts
            if not (min_delta <= dt <= max_delta):
                continue
            if same_key and not _same_item(dereg_row["LPC"], dereg_row["PID"], row["LPC"], row["PID"]):
                continue
            cands.append(k)
        return cands

    def _record(self, j, best, confidence, is_anchor):
        """记录一次配对：追加结果、标记占用、写置信度；is_anchor 时加入锚点列表。"""
        self.assignments.append((j, best))
        self.occupied.add(best)
        self.confidences[j] = confidence
        if is_anchor:
            self.anchors.append((j, best))

    # ---------------------------------------------------------------- 各阶段 ----
    def _phase1_round1(self):
        """第一轮：窄窗口唯一候选 -> 确认锚点。"""
        logger.info("第一阶段第一轮：窄窗口唯一候选匹配开始")
        for j, d_ts in enumerate(self.dereg_ts):
            target = _paired_station(self.dereg.loc[j]["CURRENTSTATIONID"])
            if target is None:
                continue
            min_delta, max_delta = self._window(target, is_narrow=True)
            lo, hi = self._window_bounds(d_ts, min_delta, max_delta)
            cands = self._candidates(j, d_ts, target, min_delta, max_delta, lo, hi)
            if len(cands) == 1:  # 唯一候选 -> 确认锚点
                self._record(j, cands[0], CONFIRMED, is_anchor=True)
                logger.debug("第一轮配对: dereg[%d] -> manual[%d]", j, cands[0])
        logger.info("第一轮完成，匹配 %d 对", len(self.anchors))

    def _phase1_round2(self):
        """第二轮：窄窗口 + PID/LPC 相同 -> 唯一候选补配（也作为锚点）。"""
        logger.info("第二阶段第一轮：窄窗口 + PID/LPC 相同匹配开始")
        matched_dereg = {j for j, _ in self.assignments}
        round2_count = 0
        for j, d_ts in enumerate(self.dereg_ts):
            if j in matched_dereg:
                continue
            target = _paired_station(self.dereg.loc[j]["CURRENTSTATIONID"])
            if target is None:
                continue
            min_delta, max_delta = self._window(target, is_narrow=True)
            lo, hi = self._window_bounds(d_ts, min_delta, max_delta)
            cands = self._candidates(j, d_ts, target, min_delta, max_delta, lo, hi, same_key=True)
            if len(cands) == 1:  # PID/LPC 相同且唯一候选 -> 确认锚点
                self._record(j, cands[0], CONFIRMED, is_anchor=True)
                round2_count += 1
                logger.debug("第二轮配对: dereg[%d] -> manual[%d]", j, cands[0])
        logger.info("第二轮完成，匹配 %d 对", round2_count)
        logger.info("第一阶段总计匹配 %d 对，锚点数 %d", len(self.assignments), len(self.anchors))

    def _phase2(self):
        """第二阶段：以最近锚点为准，在宽窗口内做局部搜索。"""
        if not self.anchors:
            logger.warning("没有确认锚点，第二阶段跳过")
            return
        logger.info("第二阶段：局部搜索开始，锚点窗口偏移量 = %d", self.anchor_window)
        matched_dereg = {j for j, _ in self.assignments}
        phase2_count = 0
        for j, d_ts in enumerate(self.dereg_ts):
            if j in matched_dereg:
                continue
            target = _paired_station(self.dereg.loc[j]["CURRENTSTATIONID"])
            if target is None:
                continue

            # 宽窗口
            min_delta, max_delta = self._window(target, is_narrow=False)
            lo, hi = self._window_bounds(d_ts, min_delta, max_delta)

            # 找最近锚点，在其 manual 位置附近做局部搜索
            closest_anchor = min(self.anchors, key=lambda x: abs(x[0] - j))
            anchor_manual = closest_anchor[1]
            search_start = max(lo, anchor_manual)
            search_end = min(hi, anchor_manual + self.anchor_window)

            logger.debug("dereg[%d] 搜索范围: lo=%d, hi=%d, anchor_manual=%d, start=%d, end=%d",
                         j, lo, hi, anchor_manual, search_start, search_end)

            cands = self._candidates(j, d_ts, target, min_delta, max_delta, search_start, search_end)
            if not cands:
                logger.debug("dereg[%d] 在缩小范围内无候选", j)
                continue

            best = self._pick_best(cands, d_ts, target)
            near_anchor = any(abs(j - a_d) <= self.anchor_window for a_d, _ in self.anchors)
            confidence = AMBIGUOUS if (near_anchor and len(cands) >= 2) else CONFIRMED
            self._record(j, best, confidence, is_anchor=False)
            phase2_count += 1
            logger.debug("第二阶段配对: dereg[%d] -> manual[%d], 候选数=%d, 置信度=%s",
                         j, best, len(cands), confidence)
        logger.info("第二阶段完成，匹配 %d 对", phase2_count)

    # ---------------------------------------------------------------- 收尾 ----
    def run(self):
        """执行两阶段匹配，返回 (matched_df, unmatched_manual, unmatched_dereg)。"""
        self._phase1_round1()
        self._phase1_round2()
        self._phase2()

        logger.info("总匹配数: %d", len(self.assignments))
        matched_dereg = {j for j, _ in self.assignments}
        unmatched_dereg_idx = list(set(range(len(self.dereg))) - matched_dereg)
        logger.info("未匹配dereg数: %d", len(unmatched_dereg_idx))

        matched_df = self._assemble()
        matched_manual_idx = sorted({k for _, k in self.assignments})
        unmatched_dereg = self.dereg.loc[unmatched_dereg_idx] if unmatched_dereg_idx else self.dereg.iloc[0:0]
        unmatched_manual = self.manual.drop(index=matched_manual_idx)
        logger.info("未匹配manual数: %d", len(unmatched_manual))
        return matched_df, unmatched_manual, unmatched_dereg

    def _assemble(self):
        """把配对组装为带 dereg_/manual_ 前缀的 DataFrame。"""
        rows = []
        for j, k in self.assignments:
            d = self.dereg.loc[j]
            m = self.manual.loc[k]
            row = {f"dereg_{col}": d[col] for col in self.dereg.columns}
            row.update({f"manual_{col}": m[col] for col in self.manual.columns})
            row["time_diff_sec"] = float((m["EVENTTS"] - d["EVENTTS"]).total_seconds())
            row["match_confidence"] = self.confidences[j]
            rows.append(row)
        return pd.DataFrame(rows)


def match_records(
    df_manual: pd.DataFrame,
    df_dereg: pd.DataFrame,
    narrow_t3_min=config.NARROW_T3_MIN,
    narrow_t3_max=config.NARROW_T3_MAX,
    narrow_sat_min=config.NARROW_SAT_MIN,
    narrow_sat_max=config.NARROW_SAT_MAX,
    wide_t3_min=config.WIDE_T3_MIN,
    wide_t3_max=config.WIDE_T3_MAX,
    wide_sat_min=config.WIDE_SAT_MIN,
    wide_sat_max=config.WIDE_SAT_MAX,
    t3_median_gap=config.T3_MEDIAN_GAP,
    sat_median_gap=config.SAT_MEDIAN_GAP,
    anchor_window=config.ANCHOR_WINDOW,
):
    """以注销(dereg)为主体做两阶段顺序锚点匹配。

    参数默认值均取自 config.py；如需临时调整可显式传入覆盖。

    返回：
      (matched_df, unmatched_manual, unmatched_dereg)
      - matched_df:        注销字段带 dereg_ 前缀、手动扫描字段带 manual_ 前缀，
                           另有 time_diff_sec 与 match_confidence 两列。
      - unmatched_manual:  未被任何注销匹配的手动扫描记录。
      - unmatched_dereg:   未匹配到手动扫描的注销记录（主体侧未匹配）。
    """
    logger.info("开始匹配: dereg记录数=%d, manual记录数=%d", len(df_dereg), len(df_manual))
    if df_manual.empty or df_dereg.empty:
        logger.warning("输入数据为空，返回空结果")
        return pd.DataFrame(), df_manual.copy(), df_dereg.copy()

    params = _Params(
        narrow_t3_min, narrow_t3_max, narrow_sat_min, narrow_sat_max,
        wide_t3_min, wide_t3_max, wide_sat_min, wide_sat_max,
        t3_median_gap, sat_median_gap, anchor_window,
    )
    return _Matcher(df_manual, df_dereg, params).run()


class _Params:
    """匹配参数的无类型小容器（避免 _Matcher 构造参数过长）。"""

    def __init__(self, narrow_t3_min, narrow_t3_max, narrow_sat_min, narrow_sat_max,
                 wide_t3_min, wide_t3_max, wide_sat_min, wide_sat_max,
                 t3_median_gap, sat_median_gap, anchor_window):
        self.narrow_t3_min, self.narrow_t3_max = narrow_t3_min, narrow_t3_max
        self.narrow_sat_min, self.narrow_sat_max = narrow_sat_min, narrow_sat_max
        self.wide_t3_min, self.wide_t3_max = wide_t3_min, wide_t3_max
        self.wide_sat_min, self.wide_sat_max = wide_sat_min, wide_sat_max
        self.t3_median_gap, self.sat_median_gap = t3_median_gap, sat_median_gap
        self.anchor_window = anchor_window
