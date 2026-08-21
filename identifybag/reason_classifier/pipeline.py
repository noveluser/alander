"""跨模块编排：给定日期，产出 reason_classification 的逐日数据。"""
import pandas as pd

from .classification import build_classification_df
from .data import fetch_dereg, fetch_manual
from .matching import match_records


def process_one_day(day_start, day_end) -> pd.DataFrame:
    """统计一天（[day_start, day_end)）的 reason_classification 数据。

    步骤：读取手动扫描与注销 -> 顺序锚点匹配 -> 以手动为主体构建分类 DataFrame。
    """
    dereg = fetch_dereg(day_start, day_end)
    manual = fetch_manual(day_start, day_end)
    matched, unmatched_manual, _ = match_records(manual, dereg)
    return build_classification_df(matched, unmatched_manual)
