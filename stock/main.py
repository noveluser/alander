#!/usr/bin/python3
# coding=utf-8
"""
主程序 - 仅做三大步：数据输入 → 数据处理 → 数据输出
核心：无具体实现细节，仅调度各标准化模块
"""
import random
import time
import logging
import pandas as pd
from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG
# 导入各标准化模块
from utils import init_logger, validate_dataframe
from db_operation import create_db_engine, db_read, batch_update, retry_row_update
from business_calc import calc_ttm_finance

# ---------------------- 初始化（仅日志+DB引擎） ----------------------
logger = init_logger(LOG_CONFIG)  # 初始化日志
create_db_engine(DB_URL, DB_CONFIG)  # 初始化DB引擎

# ---------------------- 主流程：数据输入→数据处理→数据输出 ----------------------
def main():
    """主函数：仅三大核心步骤，全程通过SQL实现数据输入/输出"""
    # 配置常量
    UNPROCESSED_FLAG = BUSINESS_CONFIG["unprocessed_flag"]
    REPORT_TIME_START = BUSINESS_CONFIG["report_time_start"]
    TARGET_DATE = BUSINESS_CONFIG["target_date"]
    SLEEP_MIN = BUSINESS_CONFIG["sleep_time_min"]
    SLEEP_MAX = BUSINESS_CONFIG["sleep_time_max"]
    
    # ---------------------- 步骤1：数据输入（SQL读取，全量未处理股票+财务数据） ----------------------
    # 1.1 读取stock_list表中未处理股票代码（SQL输入）
    sql_unprocessed = "SELECT secucode FROM stock_list WHERE secucode = :flag"
    params_unprocessed = {"flag": UNPROCESSED_FLAG}
    df_unprocessed = db_read(sql_unprocessed, params_unprocessed)
    symbols = df_unprocessed['secucode'].tolist() if not df_unprocessed.empty else []
    if not symbols:
        logger.info("ℹ️  stock_list表中无未处理股票，任务结束")
        return
    logger.info(f"🚀 开始批量处理，共{len(symbols)}只未处理股票，目标日期：{TARGET_DATE}")
    
    # 初始化统计
    successful_symbols = []
    failed_symbols = []

    # 遍历处理每只股票
    for i, symbol in enumerate(symbols, 1):
        logger.info(f"\n===== 处理进度：[{i}/{len(symbols)}] 股票代码：{symbol} =====")
        try:
            # 1.2 读取该股票的财务原始数据（SQL输入）
            sql_finance = """
                SELECT * FROM sort_finance 
                WHERE secucode = :secucode AND report_time > :start_time
            """
            params_finance = {"secucode": symbol, "start_time": REPORT_TIME_START}
            df_finance = db_read(sql_finance, params_finance)
            # 数据验证
            is_valid, msg = validate_dataframe(df_finance)
            if not is_valid:
                logger.warning(f"⚠️  财务数据验证失败: {msg}")
                failed_symbols.append(symbol)
                continue
            # 日期类型转换（为后续计算做准备）
            df_finance['report_time'] = pd.to_datetime(df_finance['report_time'])

            # ---------------------- 步骤2：数据处理（仅调用业务模块，计算TTM） ----------------------
            df_ttm = calc_ttm_finance(df_finance)
            # TTM数据验证
            is_ttm_valid, ttm_msg = validate_dataframe(df_ttm, ['secucode', 'report_time', 'ttm_revenue', 'ttm_netprofit'])
            if not is_ttm_valid:
                logger.warning(f"⚠️  TTM数据验证失败: {ttm_msg}")
                failed_symbols.append(symbol)
                continue

            # ---------------------- 步骤3：数据输出（SQL更新，批量更新+逐行重试） ----------------------
            # 3.1 批量更新sort_finance表的预估字段（SQL输出）
            sql_batch_update = """
                UPDATE sort_finance
                SET estimate_total_revenue = :ttm_revenue,
                    estimate_total_netprofit = :ttm_netprofit
                WHERE secucode = :secucode AND report_time = :report_time
            """
            affected_rows = batch_update(sql_batch_update, df=df_ttm)
            
            # 3.2 批量更新失败则逐行重试（SQL输出兜底）
            if affected_rows == 0:
                logger.warning(f"⚠️ {symbol} 批量更新失败，触发逐行重试")
                affected_rows = retry_row_update(symbol, sql_batch_update, df=df_ttm)

            successful_symbols.append(symbol)
            
            # # 3.3 更新处理成功标志位（SQL输出）
            # if affected_rows > 0:
            #     sql_update_flag = "UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode"
            #     db_read(sql_update_flag, params={"secucode": symbol})  # 复用读接口执行更新（无返回DF）
            #     successful_symbols.append(symbol)
            #     logger.info(f"✅ {symbol} 全流程处理完成，更新{affected_rows}条财务数据")
            # else:
            #     failed_symbols.append(symbol)
            #     logger.warning(f"⚠️ {symbol} 无有效数据更新，处理失败")

        except Exception as e:
            logger.error(f"❌ {symbol} 处理失败: {str(e)}", exc_info=True)
            failed_symbols.append(symbol)
        finally:
            # 随机延迟，避免频繁请求
            if i < len(symbols):
                sleep_time = random.uniform(SLEEP_MIN, SLEEP_MAX)
                logger.debug(f"ℹ️  等待{sleep_time:.2f}秒后处理下一只（DEBUG级）")
                time.sleep(sleep_time)

    # ---------------------- 任务汇总统计 ----------------------
    logger.info(f"\n" + "=" * 80)
    logger.info(f"📊 任务完成汇总 - 目标日期：{TARGET_DATE}")
    logger.info(f"✅ 成功处理：{len(successful_symbols)} 只 | {', '.join(successful_symbols) if successful_symbols else '无'}")
    logger.info(f"❌ 处理失败：{len(failed_symbols)} 只 | {', '.join(failed_symbols) if failed_symbols else '无'}")
    logger.info(f"📈 处理成功率：{len(successful_symbols)/len(symbols)*100:.2f}%")
    logger.info(f"=" * 80)

# ---------------------- 程序入口 ----------------------
if __name__ == "__main__":
    main()