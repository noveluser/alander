"""DEREGISTER_REASON 派生逻辑。

把 v4 中 scattered 的 `derive_reason` + LPC 缓存 + `make_circle_checker` 封装为
`DerivationPipeline`：按批计算、按 LPC 缓存分拣机循环判定，避免对同一 LPC 重复查库，
替代原先逐行 `apply(...axis=1)` 的慢实现。
"""
import numpy as np
import pandas as pd

from . import config, db

# 空框类 ACTIVEPROCESS（判断依据见派生规则第 1 条）。
_EMPTY_ACTIVEPROCESS = {
    "Lateral_41", "Lateral_81", "Garbage SAT", "Garbage T3 East", "Garbage T3 West",
}


# --------------------------------------------------------------------- 循环判定 ----
def _count_auto_scans(start_ts, end_ts, lpc) -> int:
    """统计 LPC 在时间范围内经过站点 580~590 的 AutoScan 次数（供分拣机循环判定）。"""
    sql = """
        SELECT COUNT(*)
        FROM WC_PACKAGEINFO
        WHERE lpc = :lpc
            AND EVENTTS > :start_ts
            AND EVENTTS < :end_ts
            AND TARGETPROCESSID = 'BSIS_03997185'
            AND CURRENTSTATIONID BETWEEN 580 AND 590
            AND EXECUTEDTASK = 'AutoScan'
    """
    rows = db.fetch_all(sql, {"start_ts": start_ts, "end_ts": end_ts, "lpc": lpc})
    return rows[0][0] if rows else 0


class DerivationPipeline:
    """按 LPC 缓存的分拣机循环判定 + 向量化 DEREGISTER_REASON 派生。"""

    def __init__(self, start_ts, end_ts):
        self._start_ts = start_ts
        self._end_ts = end_ts
        self._circle_cache: dict = {}

    def _circulation_reason(self, lpc) -> str:
        """某 LPC 是否循环：达到阈值判 Recirculations，否则 Dump Flight Build。"""
        if lpc not in self._circle_cache:
            count = _count_auto_scans(self._start_ts, self._end_ts, lpc)
            self._circle_cache[lpc] = (
                "Recirculations" if count >= config.CIRCLE_THRESHOLD else "Dump Flight Build"
            )
        return self._circle_cache[lpc]

    # ------------------------------------------------------------- 向量化规则 ----
    def derive(self, df: pd.DataFrame) -> pd.Series:
        """对 DataFrame 整体派生 DEREGISTER_REASON（按第 1~8 条规则优先级）。

        规则优先级（命中即返回）：
          1. ACTIVEPROCESS∈空框类 且 无 LPC   -> EMPTY
          2. ACTIVEPROCESS == Trace and Eject -> Trace and Eject
          3. IDENTIFICATIONSTATE == DELETED_BAGDATA -> DEL BSM
          4. RECOGNITIONSTATE ∈ {NO_READ, MULTI_READ} -> 该识别状态
          5. FLIGHTBUILDTIMELINESS == EARLY    -> EARLY
          6. ACTIVEPROCESS == Dump Flight Build -> 分拣机循环判定（按 LPC）
          7. ACTIVEPROCESS == Unplanned flight -> Unplanned flight
          8. 其余                          -> ACTIVEPROCESS
        """
        ap = df["ACTIVEPROCESS"].astype(str)
        reason = pd.Series(np.nan, index=df.index)
        # 凡已判定（reason 非空）的行不再被后续低优先级规则覆盖，忠实于 v4 的逐条优先返回。
        assigned = pd.Series(False, index=df.index)

        # 规则 1：空框类 && 无 LPC
        mask = ap.isin(_EMPTY_ACTIVEPROCESS) & df["LPC"].isna()
        reason[mask & ~assigned] = "EMPTY"
        assigned |= mask

        # 规则 2：中控主动弹出行李
        mask = ap.eq("Trace and Eject")
        reason[mask & ~assigned] = "Trace and Eject"
        assigned |= mask

        # 规则 3：DELETED_BAGDATA
        mask = df["IDENTIFICATIONSTATE"].eq("DELETED_BAGDATA")
        reason[mask & ~assigned] = "DEL BSM"
        assigned |= mask

        # 规则 4：未读 / 多读
        mask = df["RECOGNITIONSTATE"].isin(["NO_READ", "MULTI_READ"])
        reason[mask & ~assigned] = df.loc[mask & ~assigned, "RECOGNITIONSTATE"]
        assigned |= mask

        # 规则 5：早到
        mask = df["FLIGHTBUILDTIMELINESS"].eq("EARLY")
        reason[mask & ~assigned] = "EARLY"
        assigned |= mask

        # 规则 6：分拣机循环（仅对 Dump Flight Build 且非空 LPC 查库）
        mask = ap.eq("Dump Flight Build") & df["LPC"].notna()
        reason[mask & ~assigned] = df.loc[mask & ~assigned, "LPC"].map(self._circulation_reason)
        assigned |= mask
        mask = ap.eq("Dump Flight Build") & df["LPC"].isna()
        reason[mask & ~assigned] = "Dump Flight Build"
        assigned |= mask

        # 规则 7：太晚（航班计划外）
        mask = ap.eq("Unplanned flight")
        reason[mask & ~assigned] = "Unplanned flight"
        assigned |= mask

        # 规则 8：默认取 ACTIVEPROCESS
        reason[~assigned] = ap[~assigned]

        return reason.astype(str)
