#!/usr/bin/python3
# coding=utf-8
"""
主程序 - 仅业务调度：数据输入→TTM计算→数据输出
核心：无DB代码、无校验代码，全通过通用模块调用，保持极致干净
author:wangle
version:1.0
"""
import random
import time
import pandas as pd
from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG
# 导入通用模块，无自定义DB/校验逻辑
from utils import init_logger, validate_finance_data
from db_operation import create_db_engine, safe_db_operation, db_read, db_write, batch_update, retry_row_update

# ---------------------- 全局初始化（仅日志+DB引擎，通用操作） ----------------------
logger = init_logger(LOG_CONFIG)
create_db_engine(DB_URL, DB_CONFIG)

# ---------------------- 纯业务函数：TTM计算（无DB/无校验，仅数值计算） ----------------------
def calc_ttm_PE_PB(df):
    """
    【纯业务函数】矢量化计算TTM_PE,TTM_PB - 适配季度累积型财务数据
    入参：已通过utils.validate_finance_data校验的干净数据
    公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
    """
    # 备选修复：直接对原数据排序并重置索引，无任何拷贝，内存占用最低（推荐）
    df_ttm = df.sort_values('date', ascending=True).reset_index(drop=True)
    
    # 过滤无效计算值（预估净利润=0/空、收盘价≤0/空、总股本≤0/空，均无法计算PE）
    invalid_mask = (df_ttm['estimate_total_netprofit'].isna()| (df_ttm['estimate_total_netprofit'] == 0) |
                    df_ttm['close_price'].isna() | (df_ttm['close_price'] <= 0) |
                    df_ttm['share_capital'].isna() | (df_ttm['share_capital'] <= 0) |
                    df_ttm['total_equity'].isna() )  # PB计算补充净资产空值过滤
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        logger.warning(f"⚠️  发现{invalid_count}条无效数据（净利润/收盘价/总股本为空/≤0），跳过PE计算")
    
    # 核心公式：PE = 收盘价 * 总股本 / 预估净利润 （矢量化计算，批量处理效率高）
    df_ttm.loc[~invalid_mask, 'pe'] = (df_ttm.loc[~invalid_mask, 'close_price'] * 
                                        df_ttm.loc[~invalid_mask, 'share_capital']) / \
                                       df_ttm.loc[~invalid_mask, 'estimate_total_netprofit']
    df_ttm.loc[~invalid_mask, 'pb'] = (df_ttm.loc[~invalid_mask, 'close_price'] * 
                                        df_ttm.loc[~invalid_mask, 'share_capital']) / \
                                       df_ttm.loc[~invalid_mask, 'total_equity']
    df_ttm.loc[~invalid_mask, 'roe'] = (df_ttm.loc[~invalid_mask, 'estimate_total_netprofit']) / \
                                    df_ttm.loc[~invalid_mask, 'total_equity']
    # 无效行PE设为None
    df_ttm.loc[invalid_mask, 'pe'] = None
    df_ttm.loc[invalid_mask, 'pb'] = None
    df_ttm.loc[invalid_mask, 'roe'] = None
    
    # 保留核心字段并通过通用工具校验TTM结果
    df_ttm = df_ttm[['date', 'secucode', 'pe', 'pb', 'roe']]
    logger.debug(f"✅ TTM计算完成，有效数据{len(df_ttm)}条")
    return df_ttm


# ---------------------- 主流程：仅业务调度，无任何通用逻辑 ----------------------
def main():
    """主流程：批量读取→单股数据校验→TTM计算→批量更新DB→状态更新"""
    # 从配置读取常量，无硬编码
    CFG = BUSINESS_CONFIG
    UNPROCESSED_FLAG = CFG["unprocessed_flag"]
    DELAY_DAYS = CFG['delay_days']
    SLEEP_MIN = CFG["sleep_time_min"]
    SLEEP_MAX = CFG["sleep_time_max"]
    BATCH_SIZE = CFG.get("batch_size", 20)
    DB_RETRY_TIMES = CFG.get("db_retry_times", 2)

    # 数据配置（与业务强相关，仅此处定义）
    FINANCE_NUMERIC_COLS = ['estimate_total_netprofit', 'share_capital', 'total_equity']
    SELECT_SQL = """
        WITH stock AS (
            SELECT secucode, name
            FROM stock_list
            WHERE secucode = :secucode
        ),
        price AS (
            SELECT date, close_price, secucode
            FROM daily_stock_price_list
            WHERE secucode = :secucode
        )
        SELECT
            s.secucode,
            p.date,
            p.close_price,
            -- 关键：每一天都【只查1条最新财报】，性能爆炸
            (SELECT report_time FROM sort_finance f
            WHERE f.secucode = p.secucode
            AND f.report_time < DATE_SUB(p.date, INTERVAL :delay_days DAY)
            ORDER BY f.report_time DESC LIMIT 1) AS report_time,

            (SELECT total_equity FROM sort_finance f
            WHERE f.secucode = p.secucode
            AND f.report_time < DATE_SUB(p.date, INTERVAL :delay_days DAY)
            ORDER BY f.report_time DESC LIMIT 1) AS total_equity,

            (SELECT share_capital FROM sort_finance f
            WHERE f.secucode = p.secucode
            AND f.report_time < DATE_SUB(p.date, INTERVAL :delay_days DAY)
            ORDER BY f.report_time DESC LIMIT 1) AS share_capital,

            (SELECT estimate_total_netprofit FROM sort_finance f
            WHERE f.secucode = p.secucode
            AND f.report_time < DATE_SUB(p.date, INTERVAL :delay_days DAY)
            ORDER BY f.report_time DESC LIMIT 1) AS estimate_total_netprofit

        FROM stock s
        JOIN price p ON s.secucode = p.secucode
        ORDER BY p.date DESC;                   
    """
    UPDATE_SQL = """
        UPDATE daily_stock_price_list
        SET estimate_pe = :pe,
        PB = :pb,
        ROE = :roe
        WHERE secucode = :secucode AND date = :date
    """

    # 1. 批量读取未处理（调用db_operation安全方法）
    # sql_unprocessed = "SELECT secucode FROM stock_list WHERE secucode = :UNPROCESSED_SECUCODE"  # 测试语句
    sql_unprocessed = "SELECT secucode FROM stock_list WHERE flag = :UNPROCESSED_FLAG"  # 测试语句
    df_unprocessed = safe_db_operation(db_read, sql_unprocessed, params={"UNPROCESSED_FLAG": UNPROCESSED_FLAG}, retry_times=DB_RETRY_TIMES)
    if df_unprocessed is None or df_unprocessed.empty:
        logger.info("ℹ️  stock_list表中无未处理，任务结束")
        return
    symbols = df_unprocessed['secucode'].tolist()
    symbol_batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    logger.info(f"🚀 任务启动：共{len(symbols)}只，分{len(symbol_batches)}批处理，每批{BATCH_SIZE}只")

    # 初始化统计
    successful_symbols, failed_symbols = [], []

    # 2. 批量遍历处理
    for batch_idx, batch_syms in enumerate(symbol_batches, 1):
        logger.info(f"\n===== 批量进度：[{batch_idx}/{len(symbol_batches)}] ：{batch_syms} =====")
        batch_ttm_dfs, batch_success, batch_fail = [], [], []

        # 单股处理：仅调度校验+计算，无通用逻辑
        for sym in batch_syms:
            try:
                # 读取单股所需数据
                df = safe_db_operation(
                    db_read,
                    SELECT_SQL,
                    params={"secucode": sym, "delay_days": DELAY_DAYS},
                    retry_times=DB_RETRY_TIMES
                )
                if df is None or df.empty:
                    logger.warning(f"⚠️  {sym} 无符合条件的财务数据")
                    batch_fail.append(sym)
                    continue

                # 调用utils通用财务数据校验（一键校验，无需重复代码）
                is_valid, msg, df_valid = validate_finance_data(df, FINANCE_NUMERIC_COLS)
                if not is_valid:
                    logger.warning(f"⚠️  {sym} 财务数据校验失败: {msg}")
                    batch_fail.append(sym)
                    continue

                # TTM纯业务计算
                df_ttm = calc_ttm_PE_PB(df_valid)
                if df_ttm.empty:
                    logger.warning(f"⚠️  {sym} TTM计算无有效数据")
                    batch_fail.append(sym)
                    continue

                # 计算成功，加入批量待更新列表
                batch_ttm_dfs.append(df_ttm)
                batch_success.append(sym)
                logger.info(f"✅ {sym} 数据处理完成，待批量更新")

            except Exception as e:
                logger.error(f"❌ {sym} 单股处理异常: {str(e)}", exc_info=True)
                batch_fail.append(sym)
                continue

        # 3. 批量更新数据库（调用db_operation通用方法）
        if batch_ttm_dfs and batch_success:
            df_batch_ttm = pd.concat(batch_ttm_dfs, ignore_index=True)
            # 安全批量更新
            affected_rows = safe_db_operation(
                batch_update, UPDATE_SQL, df_batch_ttm,
                retry_times=DB_RETRY_TIMES
            )
            # 批量更新失败则逐行重试
            if affected_rows == 0:
                logger.warning(f"⚠️  第{batch_idx}批批量更新失败，触发逐行重试")
                for sym in batch_success:
                    df_sym_ttm = df_batch_ttm[df_batch_ttm['secucode'] == sym]
                    retry_aff = safe_db_operation(retry_row_update, sym, UPDATE_SQL, df_sym_ttm, retry_times=DB_RETRY_TIMES)
                    if retry_aff > 0:
                        # 安全更新处理状态
                        flag_aff = safe_db_operation(
                            db_write, "UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode",
                            params={"secucode": sym}, retry_times=DB_RETRY_TIMES
                        )
                        if flag_aff is not None and flag_aff > 0:
                            successful_symbols.append(sym)
                        else:
                            logger.error(f"❌ {sym} 数据更新成功，状态更新失败（需手动处理）")
                            batch_fail.append(sym)
                        
                        # 新增：无论状态更新是否成功，都从batch_success移除，避免重复处理/统计错误
                        if sym in batch_success:
                            batch_success.remove(sym)
                    else:
                        logger.warning(f"⚠️ {sym} 逐行重试仍失败")
                        batch_fail.append(sym)

            # 批量更新成功，批量更新状态（修复：单股循环→批量更新，1次DB请求）
            else:
                if batch_success:
                    # 批量更新flag SQL：用IN条件匹配整批股票代码
                    batch_flag_sql = "UPDATE stock_list SET flag = 'Y' WHERE secucode IN :secucodes"
                    # 传入批量参数（元组格式适配SQLAlchemy的IN条件）
                    batch_flag_params = {"secucodes": tuple(batch_success)}
                    # 1次安全批量更新，获取受影响行数
                    batch_flag_aff = safe_db_operation(
                        db_write, batch_flag_sql,
                        params=batch_flag_params, retry_times=DB_RETRY_TIMES
                    )
                    # 遍历校验每只股票状态更新结果，保留原统计逻辑
                    for sym in batch_success.copy():
                        if batch_flag_aff and batch_flag_aff > 0:
                            # 批量更新成功，计入成功列表
                            successful_symbols.append(sym)
                        else:
                            # 批量更新失败，标记为手动处理
                            logger.error(f"❌ {sym} 数据更新成功，状态批量更新失败（需手动处理）")
                            batch_fail.append(sym)
                        # 保留原移除逻辑，避免重复处理
                        if sym in batch_success:
                            batch_success.remove(sym)
        # 批量失败加入总统计
        failed_symbols.extend(batch_fail)

        # 批量间延迟，避免数据库压力
        if batch_idx < len(symbol_batches):
            sleep_t = random.uniform(SLEEP_MIN, SLEEP_MAX)
            logger.debug(f"ℹ️  等待{sleep_t:.2f}秒处理下一批")
            time.sleep(sleep_t)

    # 4. 任务汇总统计
    logger.info(f"\n" + "=" * 80)
    logger.info(f"📊 任务完成汇总 | 总数：{len(symbols)}")
    logger.info(f"✅ 成功处理：{len(successful_symbols)} 只 | {', '.join(successful_symbols) if successful_symbols else '无'}")
    logger.info(f"❌ 处理失败：{len(failed_symbols)} 只 | {', '.join(failed_symbols) if failed_symbols else '无'}")
    logger.info(f"📈 处理成功率：{len(successful_symbols)/len(symbols)*100:.2f}%")
    logger.info(f"=" * 80)

# ---------------------- 程序入口 ----------------------
if __name__ == "__main__":
    main()
