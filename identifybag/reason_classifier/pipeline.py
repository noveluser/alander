"""跨模块编排：给定日期，产出 reason_classification 的逐日数据。"""
import pandas as pd
import logging

from . import config
from .classification import build_classification_df, filter_by_airport
from .data import fetch_airport, fetch_dereg, fetch_manual
from .matching import match_records


# ---------- 日志配置 ----------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='mcsidentify.log',          # 与Excel同目录
                    filemode='a')


def process_one_day(day_start, day_end) -> pd.DataFrame:
    """统计一天（[day_start, day_end)）的 reason_classification 数据。

    步骤：读取手动扫描与注销 -> 顺序锚点匹配 -> 以手动为主体构建分类 DataFrame
    -> 按机场名单过滤（名单窗口终点需 +8 小时）。
    """
    dereg = fetch_dereg(day_start, day_end)
    manual = fetch_manual(day_start, day_end)
    matched, unmatched_manual, _ = match_records(manual, dereg)
    cls_df = build_classification_df(matched, unmatched_manual)
    # 剔除空框
    cls_df = cls_df[cls_df['manual_DEREGISTER_REASON'] != 'EMPTY']
    # 剔除重复扫描件
    cls_df = cls_df.drop_duplicates(subset=['manual_PID'], keep='first')
    logging.info(cls_df.to_string())
    return cls_df

    # 机场名单过滤：仅保留 LPC 或 PID 命中名单的行。
    # 取消这段代码，因为VIDI出问题时,summary表根本没数据
    # airport = fetch_airport(day_start + config.AIRPORT_END_OFFSET, day_end + config.AIRPORT_END_OFFSET)
    # logging.info(airport.to_string())
    # return filter_by_airport(cls_df, airport)
