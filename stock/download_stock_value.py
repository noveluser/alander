import random
import akshare as ak
import time
from datetime import datetime
import pandas as pd
from decimal import Decimal

from config import DB_URL, DB_CONFIG, LOG_CONFIG
from utils import init_logger
from db_operation import create_db_engine, db_read, batch_update, db_write

# 日志、数据库初始化
logger = init_logger(LOG_CONFIG)
create_db_engine(DB_URL, DB_CONFIG)


def to_decimal(value):
    """数值转为Decimal，保留4位小数"""
    if pd.isna(value):
        return None
    try:
        return Decimal(str(round(float(value), 4)))
    except (ValueError, TypeError):
        return None


def format_row_data(row):
    """格式化单行数据"""
    date_str = row.get("date", "").strip()
    sql_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None

    row_data = (
        sql_date,
        row.get("secucode"),
        to_decimal(row.get("estimate_pe")),
        to_decimal(row.get("pb")),
        to_decimal(row.get("share_capital"))
    )
    return row_data


def get_stock_listing_date(symbol):
    """获取代码上市日期，返回 YYYYMMDD"""
    try:
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        for _, row in stock_info.iterrows():
            item = str(row["item"]).strip()
            if "上市时间" in item:
                date_str = str(row["value"]).strip()
                digits = "".join(filter(str.isdigit, date_str))
                if len(digits) >= 8:
                    return digits[:8]
        return None
    except Exception as e:
        logger.error(f"获取{symbol}上市日期失败: {e}")
        return None


def get_stock_history(symbol, start_date, end_date):
    """获取代码历史估值数据"""
    stock_code = symbol.split(".")[0]

    # 自动修正起始日期
    listing_date = get_stock_listing_date(stock_code)
    if listing_date and listing_date > "20000101":
        start_date = listing_date

    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_value_em(symbol=stock_code)
            if df.empty:
                logger.warning(f"{symbol} 无行情数据")
                return None

            # 日期处理 & 字段重命名
            df["数据日期"] = pd.to_datetime(df["数据日期"])
            df["代码代码"] = symbol
            df = df.rename(
                columns={
                    "数据日期": "date",
                    "代码代码": "secucode",
                    "总股本": "share_capital",
                    "PE(TTM)": "estimate_pe",
                    "市净率": "pb"
                }
            )

            # 日期过滤
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)].copy()
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

            return df.sort_values("date").reset_index(drop=True)

        except Exception as e:
            logger.error(f"{symbol} 第{attempt+1}次拉取失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
    return None


def main():
    """主流程：读取待处理代码 -> 拉取数据 -> 批量更新库 -> 标记完成"""
    # 1. 读取未处理代码
    sql = "SELECT distinct secucode FROM daily_stock_price_list WHERE right(secucode,2) = :flag and share_capital = 0"
    df_unprocessed = db_read(sql, {"flag": "SZ"})
    if df_unprocessed.empty:
        logger.info("暂无待处理代码，任务结束")
        return

    symbols = df_unprocessed["secucode"].tolist()
    logger.info(f"开始处理，共 {len(symbols)} 只代码")

    # 更新SQL
    update_sql = """
        UPDATE daily_stock_price_list
        SET 
            estimate_pe = :estimate_pe,
            pb = :pb,
            share_capital = :share_capital
        WHERE 
            secucode = :secucode 
            AND date = :date
            AND (
                estimate_pe <=> :estimate_pe IS NOT TRUE
                OR pb <=> :pb IS NOT TRUE
                OR share_capital <=> :share_capital IS NOT TRUE
            )
    """

    success_cnt = 0
    fail_cnt = 0

    for idx, code in enumerate(symbols, 1):
        logger.info(f"[{idx}/{len(symbols)}] 处理 {code}")
        end_date = datetime.now().strftime("%Y%m%d")
        df = get_stock_history(code, start_date="20240101", end_date=end_date)

        if df is None or df.empty:
            fail_cnt += 1
            continue

        # 批量更新数据表
        affected = batch_update(update_sql, df=df)
        if affected > 0:
            # 更新完成标记
            db_write("UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode", {"secucode": code})
            success_cnt += 1
            logger.info(f"{code} 处理完成，更新 {affected} 条记录")
        else:
            fail_cnt += 1
            logger.warning(f"{code} 无数据更新")

        time.sleep(random.uniform(1, 2))

    logger.info(f"任务结束：成功 {success_cnt} 只，失败 {fail_cnt} 只")


if __name__ == "__main__":
    main()