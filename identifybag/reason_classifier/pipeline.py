"""跨模块编排：给定日期，产出 reason_classification 的逐日数据。"""
import pandas as pd
import logging
# 每个模块获取自己的 logger（模块名作为名称）
logger = logging.getLogger(__name__)

from .classification import build_classification_df
from .data import fetch_dereg, fetch_manual
from .matching import match_records


def process_one_day(day_start, day_end) -> pd.DataFrame:
    """统计一天（[day_start, day_end)）的 reason_classification 数据。

    步骤：读取手动扫描与注销 -> 顺序锚点匹配 -> 以注销为主体构建分类 DataFrame。
    """
    dereg = fetch_dereg(day_start, day_end)
    Before_deduplication_manual = fetch_manual(day_start, day_end)
    manual = Before_deduplication_manual.drop_duplicates(subset=['PID'], keep='first')
    logger.info(f"手动数据去重前: {len(Before_deduplication_manual)} 条, 去重后: {len(manual.drop_duplicates(subset=['PID'], keep='first'))} 条")
    logging.info(f"manual全记录 {manual.to_string()}")
    # 丢弃unmatch_mannual数据
    matched, unmatch_mannual, unmatched_dereg = match_records(manual, dereg)
    return build_classification_df(matched, unmatched_dereg)
