"""主流程：解析日期 -> 计算运营日窗口 -> 逐日统计 -> 输出 reason_classification.xlsx。

同时提供可复用的 `run(start)`，供命令行入口或其它调用方使用。
"""
import sys
from datetime import datetime, timedelta

import logging
from .log_config import init_logger

# 1. 配置日志（必须最先执行）
init_logger()

from . import config
from .exporter import write_reason_excel
from .pipeline import process_one_day



_DAYS_BACK_LIMIT = 14      # 最早只能查最近 14 天
_MAX_RUN_DAYS = 8          # 从起始日起最多统计 8 天（起始日到起始+7天）
# 运营日窗口：前一天 16:00 -> 当天 16:00
_WINDOW_OFFSET_DAYS = 1
_WINDOW_HOUR = 16


# --------------------------------------------------------------- 日期处理 ----
def _parse_arg(args) -> datetime:
    """从命令行参数或交互输入解析起始日期（YYYYMMDD），输入为空/非法时回退到昨天。"""
    if len(args) > 1:
        try:
            return datetime.strptime(args[1], "%Y%m%d")
        except ValueError:
            print("命令行日期格式错误，请使用 YYYYMMDD，例如 20260819")
            sys.exit(1)

    print("请输入起始日期（YYYYMMDD），直接回车使用昨天：")
    try:
        text = input().strip()
    except EOFError:
        text = ""
    if not text:
        return datetime.now() - timedelta(days=1)
    try:
        return datetime.strptime(text, "%Y%m%d")
    except ValueError:
        print("输入日期格式错误，将使用昨天")
        return datetime.now() - timedelta(days=1)


def _start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _opening_window_start(input_date: datetime) -> datetime:
    """运营日起点：输入日 - 1 天 的 16:00。"""
    return (input_date - timedelta(days=_WINDOW_OFFSET_DAYS)).replace(
        hour=_WINDOW_HOUR, minute=0, second=0, microsecond=0
    )


def _window_end(start_ts: datetime) -> datetime:
    """窗口终点 = min(起始+7天, 昨日 16:00 + 1 天)。"""
    end_candidate = start_ts + timedelta(days=_MAX_RUN_DAYS)
    yesterday = (datetime.now() - timedelta(days=2)).replace(
        hour=_WINDOW_HOUR, minute=0, second=0, microsecond=0
    )
    return min(end_candidate, yesterday + timedelta(days=1))


# ---------------------------------------------------------------- 主流程 ----
def run(input_date: datetime):
    """以指定起始日期为运营日，输出 reason_classification.xlsx。"""
    # 最远限制
    today = _start_of_day(datetime.now())
    if input_date < today - timedelta(days=_DAYS_BACK_LIMIT):
        print(f"输入日期 {input_date:%Y%m%d} 超出数据库记录范围")
        return

    start_ts = _opening_window_start(input_date)
    end_ts = _window_end(start_ts)
    if start_ts >= end_ts:
        print("起始日期晚于昨日，无数据可统计")
        return
    print(f"统计范围: {start_ts:%Y-%m-%d} 至 {end_ts:%Y-%m-%d} (不含结束日期)")

    date_label = f"{start_ts:%Y-%m-%d}_to_{end_ts:%Y-%m-%d}"
    day_results = []
    day = start_ts
    while day < end_ts:
        day_end = day + timedelta(days=1)
        print(f"正在统计 {day:%Y-%m-%d} ...")
        day_results.append((day.strftime("%Y-%m-%d"), process_one_day(day, day_end)))
        day = day_end

    if not any(not cls.empty for _, cls in day_results):
        print("所选范围内无数据，未生成 Excel")
        return

    filename = f"{config.OUTPUT_PREFIX}_{date_label}.xlsx"
    write_reason_excel(filename, day_results)
    print(f"DEREGISTER_REASON 分类文件已生成：{filename}")


def main():
    # run(_parse_arg(sys.argv))
    run(datetime.strptime("20260827", "%Y%m%d"))


if __name__ == "__main__":
    main()
