"""reason_classification.xlsx 输出。

Sheet 结构（英文命名）：
  summary                 日期 × REASON 交叉表 + Total
  station_reason_<date>   当天 站点 × REASON 交叉表 + Total
  detail_<date>           当天逐条记录（含 REASON / REASON_SOURCE / MATCH_CONFIDENCE）
"""
import pandas as pd

from .classification import build_reason_pivot


def write_reason_excel(filename: str, day_results):
    """day_results: [(date_str, classification_df), ...]，写入单个 xlsx 文件。"""
    summary_parts = []
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        for date_str, cls in day_results:
            if cls is None or cls.empty:
                continue

            # 1) 逐日站点 × 原因交叉表（主体为 dereg，用 dereg 站点）
            build_reason_pivot(cls, "dereg_CURRENTSTATIONID", "REASON").to_excel(
                writer, sheet_name=f"station_reason_{date_str}"
            )

            # 2) 逐日明细
            cls.to_excel(writer, sheet_name=f"detail_{date_str}", index=False)

            # 3) 收集汇总数据（带日期列）
            dated = cls.copy()
            dated.insert(0, "date", date_str)
            summary_parts.append(dated)

        # 所有日期合并成日期 × 原因汇总表
        if summary_parts:
            all_df = pd.concat(summary_parts, ignore_index=True)
            build_reason_pivot(all_df, "date", "REASON").to_excel(writer, sheet_name="summary")
