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
from utils import init_logger, validate_finance_data, validate_ttm_data
from db_operation import create_db_engine, safe_db_operation, db_read, db_write, batch_update, retry_row_update

# ---------------------- 全局初始化（仅日志+DB引擎，通用操作） ----------------------
logger = init_logger(LOG_CONFIG)
create_db_engine(DB_URL, DB_CONFIG)

# ---------------------- 纯业务函数：TTM计算（无DB/无校验，仅数值计算） ----------------------
def calc_ttm_finance(df):
    """
    【纯业务函数】矢量化计算TTM滚动净利润、总营收 - 适配季度累积型财务数据
    入参：已通过utils.validate_finance_data校验的干净数据
    公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
    """
    # 备选修复：直接对原数据排序并重置索引，无任何拷贝，内存占用最低（推荐）
    df_ttm = df.sort_values('report_time', ascending=True).reset_index(drop=True)
    # 提取年/季度
    df_ttm['year'] = df_ttm['report_time'].dt.year
    df_ttm['quarter'] = df_ttm['report_time'].dt.month.map({3:1, 6:2, 9:3, 12:4})
    # 过滤非季度报数据
    abnormal_quarter = df_ttm['quarter'].isna().sum()
    if abnormal_quarter > 0:
        logger.warning(f"⚠️  发现{abnormal_quarter}条非季度报数据，已过滤")
        df_ttm = df_ttm[df_ttm['quarter'].notna()]
    if df_ttm.empty:
        return pd.DataFrame()
    
    # 关联上一年年报和上一年同季度数据
    df_ttm['last_year'] = df_ttm['year'] - 1
    df_ttm['last_year_quarter'] = df_ttm['quarter']
    # 上一年年报数据
    last_year_annual = df_ttm[df_ttm['quarter'] == 4][[
        'year', 'deduct_parent_netprofit', 'operate_income'
    ]].rename(columns={
        'year': 'last_year',
        'deduct_parent_netprofit': 'last_year_annual_net',
        'operate_income': 'last_year_annual_rev'
    })
    # 上一年同季度数据
    last_year_same_quarter = df_ttm[[
        'year', 'quarter', 'deduct_parent_netprofit', 'operate_income'
    ]].rename(columns={
        'year': 'last_year',
        'quarter': 'last_year_quarter',
        'deduct_parent_netprofit': 'last_year_sq_net',
        'operate_income': 'last_year_sq_rev'
    })
    
    # 关联数据并计算TTM
    df_ttm = df_ttm.merge(last_year_annual, on='last_year', how='left')
    df_ttm = df_ttm.merge(last_year_same_quarter, on=['last_year','last_year_quarter'], how='left')

    # 新增：过滤TTM计算所需核心字段的空值，避免无效计算生成NaN
    calc_required_cols = ['last_year_annual_net', 'last_year_sq_net', 'deduct_parent_netprofit',
                        'last_year_annual_rev', 'last_year_sq_rev', 'operate_income']
    df_ttm = df_ttm.dropna(subset=calc_required_cols)
    if df_ttm.empty:
        logger.warning(f"⚠️  无有效关联数据，TTM计算终止")
        return pd.DataFrame()

    df_ttm['ttm_netprofit'] = df_ttm['last_year_annual_net'] - df_ttm['last_year_sq_net'] + df_ttm['deduct_parent_netprofit']
    df_ttm['ttm_revenue'] = df_ttm['last_year_annual_rev'] - df_ttm['last_year_sq_rev'] + df_ttm['operate_income']
    
    # 保留核心字段并通过通用工具校验TTM结果
    df_ttm = df_ttm[['report_time', 'secucode', 'ttm_netprofit', 'ttm_revenue']]
    logger.debug(f"✅ TTM计算完成，总数据{len(df_ttm)}条")
    df_ttm = validate_ttm_data(df_ttm)  # 调用utils通用校验
    logger.debug(f"✅ TTM计算完成，有效数据{len(df_ttm)}条")
    return df_ttm

# ---------------------- 主流程：仅业务调度，无任何通用逻辑 ----------------------
def main():
    """主流程：批量读取→单股数据校验→TTM计算→批量更新DB→状态更新"""
    # 从配置读取常量，无硬编码
    CFG = BUSINESS_CONFIG
    UNPROCESSED_FLAG = CFG["unprocessed_flag"]
    REPORT_TIME_START = CFG["report_time_start"]
    SLEEP_MIN = CFG["sleep_time_min"]
    SLEEP_MAX = CFG["sleep_time_max"]
    BATCH_SIZE = CFG.get("batch_size", 20)
    DB_RETRY_TIMES = CFG.get("db_retry_times", 2)
    # 财务数据配置（与业务强相关，仅此处定义）
    FINANCE_NUMERIC_COLS = ['deduct_parent_netprofit', 'operate_income']
    UPDATE_SQL = """
        UPDATE sort_finance
        SET estimate_total_revenue = :ttm_revenue,
            estimate_total_netprofit = :ttm_netprofit
        WHERE secucode = :secucode AND report_time = :report_time
    """

    # 1. 批量读取未处理（调用db_operation安全方法）
    sql_unprocessed = "SELECT secucode FROM stock_list WHERE secucode = :flag"  # 测试语句
    df_unprocessed = safe_db_operation(db_read, sql_unprocessed, params={"flag": UNPROCESSED_FLAG}, retry_times=DB_RETRY_TIMES)
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
                # 读取单股财务数据
                df_fin = safe_db_operation(
                    db_read,
                    sql="SELECT * FROM sort_finance WHERE secucode = :secucode AND report_time > :start_time",
                    params={"secucode": sym, "start_time": REPORT_TIME_START},
                    retry_times=DB_RETRY_TIMES
                )
                if df_fin is None or df_fin.empty:
                    logger.warning(f"⚠️  {sym} 无符合条件的财务数据")
                    batch_fail.append(sym)
                    continue

                # 调用utils通用财务数据校验（一键校验，无需重复代码）
                is_valid, msg, df_fin_valid = validate_finance_data(df_fin, FINANCE_NUMERIC_COLS)
                if not is_valid:
                    logger.warning(f"⚠️  {sym} 财务数据校验失败: {msg}")
                    batch_fail.append(sym)
                    continue

                # TTM纯业务计算
                df_ttm = calc_ttm_finance(df_fin_valid)
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

# #!/usr/bin/python3
# # coding=utf-8
# """
# 主程序 - 仅做三大步：数据输入 → 数据处理 → 数据输出
# 核心：无具体实现细节，仅调度各标准化模块
# author:wangle
# version:1.0
# """
# import random
# import time
# import pandas as pd
# from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG
# # 导入各标准化模块
# from utils import init_logger, validate_dataframe
# from db_operation import create_db_engine, db_read, db_write, batch_update, retry_row_update

# # ---------------------- 初始化（仅日志+DB引擎） ----------------------
# logger = init_logger(LOG_CONFIG)  # 初始化日志
# create_db_engine(DB_URL, DB_CONFIG)  # 初始化DB引擎


# def calc_ttm_finance(df):
#     """
#     【纯业务函数】矢量化计算每一期的TTM滚动净利润、总营收 - 适配季度累积型财务数据
#     公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
#     特性：仅处理数据计算，不涉及任何数据库操作，输入输出均为DataFrame
#     :param df: pd.DataFrame 财务原始数据
#     :return: pd.DataFrame 新增ttm_netprofit/ttm_revenue列的TTM数据
#     """
#     df_ttm = df.copy().sort_values('report_time', ascending=True).reset_index(drop=True)
#     required_cols = ['deduct_parent_netprofit', 'operate_income', 'secucode', 'report_time']
#     is_valid, msg = validate_dataframe(df_ttm, required_cols)
#     if not is_valid:
#         logger.warning(f"⚠️  TTM计算数据验证失败: {msg}")
#         return pd.DataFrame()
    
#     df_ttm['year'] = df_ttm['report_time'].dt.year
#     df_ttm['quarter'] = df_ttm['report_time'].dt.month.map({3:1, 6:2, 9:3, 12:4})
#     abnormal_quarter = df_ttm['quarter'].isna().sum()
#     if abnormal_quarter > 0:
#         logger.warning(f"⚠️  发现{abnormal_quarter}条非季度报数据，已过滤")
#         df_ttm = df_ttm[df_ttm['quarter'].notna()]
    
#     df_ttm['last_year'] = df_ttm['year'] - 1
#     df_ttm['last_year_quarter'] = df_ttm['quarter']
    
#     last_year_annual = df_ttm[df_ttm['quarter'] == 4][[
#         'year', 'deduct_parent_netprofit', 'operate_income'
#     ]].rename(columns={
#         'year': 'last_year',
#         'deduct_parent_netprofit': 'last_year_annual_net',
#         'operate_income': 'last_year_annual_rev'
#     })
    
#     last_year_same_quarter = df_ttm[[
#         'year', 'quarter', 'deduct_parent_netprofit', 'operate_income'
#     ]].rename(columns={
#         'year': 'last_year',
#         'quarter': 'last_year_quarter',
#         'deduct_parent_netprofit': 'last_year_sq_net',
#         'operate_income': 'last_year_sq_rev'
#     })
    
#     df_ttm = df_ttm.merge(last_year_annual, on='last_year', how='left')
#     df_ttm = df_ttm.merge(last_year_same_quarter, on=['last_year','last_year_quarter'], how='left')
    
#     # 核心TTM计算公式
#     df_ttm['ttm_netprofit'] = df_ttm['last_year_annual_net'] - df_ttm['last_year_sq_net'] + df_ttm['deduct_parent_netprofit']
#     df_ttm['ttm_revenue'] = df_ttm['last_year_annual_rev'] - df_ttm['last_year_sq_rev'] + df_ttm['operate_income']
    
#     # 保留核心字段
#     df_ttm = df_ttm[['report_time', 'secucode', 'ttm_netprofit', 'ttm_revenue']]
#     logger.debug(f"✅ TTM计算完成：共有{len(df_ttm)}条")
#     # 修复：过滤TTM计算结果为空的数据，避免更新数据库为NULL
#     df_ttm = df_ttm.dropna(subset=['ttm_netprofit', 'ttm_revenue'])
#     logger.debug(f"✅ TTM计算完成：有效{len(df_ttm)}条（已过滤空值）")
    
#     return df_ttm


# # ---------------------- 主流程：数据输入→数据处理→数据输出 ----------------------
# def main():
#     """主函数：仅三大核心步骤，全程通过SQL实现数据输入/输出"""
#     # 配置常量
#     UNPROCESSED_FLAG = BUSINESS_CONFIG["unprocessed_flag"]
#     REPORT_TIME_START = BUSINESS_CONFIG["report_time_start"]
#     SLEEP_MIN = BUSINESS_CONFIG["sleep_time_min"]
#     SLEEP_MAX = BUSINESS_CONFIG["sleep_time_max"]
    
#     # ---------------------- 步骤1：数据输入（SQL读取，全量未处理+财务数据） ----------------------
#     # 1.1 读取stock_list表中未处理代码（SQL输入）- 测试语句，保留原注释
#     sql_unprocessed = "SELECT secucode FROM stock_list WHERE secucode = :flag"   #测试语句，非错误
#     params_unprocessed = {"flag": UNPROCESSED_FLAG}
#     df_unprocessed = db_read(sql_unprocessed, params_unprocessed)
#     symbols = df_unprocessed['secucode'].tolist() if not df_unprocessed.empty else []
#     if not symbols:
#         logger.info("ℹ️  stock_list表中无未处理，任务结束")
#         return
#     logger.info(f"🚀 开始批量处理，共{len(symbols)}只未处理")
    
#     # 初始化统计
#     successful_symbols = []
#     failed_symbols = []
#     # 财务数据校验必选列（与业务强相关，定义在主流程便于维护）
#     finance_required_cols = ['deduct_parent_netprofit', 'operate_income', 'secucode', 'report_time']
#     # 遍历处理每只
#     for i, symbol in enumerate(symbols, 1):
#         logger.info(f"\n===== 处理进度：[{i}/{len(symbols)}] 代码：{symbol} =====")
#         try:
#             # 1.2 读取财务原始数据（通用DB操作）
#             sql_finance = """
#                 SELECT * FROM sort_finance 
#                 WHERE secucode = :secucode AND report_time > :start_time
#             """
#             df_finance = db_read(sql_finance, params={"secucode": symbol, "start_time": REPORT_TIME_START})
#             if df_finance.empty:
#                 logger.warning(f"⚠️  {symbol} 无符合条件的财务数据")
#                 failed_symbols.append(symbol)
#                 continue
            
#             # 数据预处理（通用数据转换）
#             df_finance['report_time'] = pd.to_datetime(df_finance['report_time'])
#             # 修复：传入必选列列表，完成有效数据校验（原问题：仅传DataFrame，校验失效）
#             is_valid, msg = validate_dataframe(df_finance, finance_required_cols)
#             if not is_valid:
#                 logger.warning(f"⚠️  {symbol} 财务数据格式验证失败: {msg}")
#                 failed_symbols.append(symbol)
#                 continue
            
#             # ---------------------- 步骤2：纯业务处理（仅调用TTM计算） ----------------------
#             df_ttm = calc_ttm_finance(df_finance)
#             if df_ttm.empty:
#                 logger.warning(f"⚠️  {symbol} TTM计算无有效数据")
#                 failed_symbols.append(symbol)
#                 continue
            
#             # ---------------------- 步骤3：通用数据输出（SQL更新） ----------------------
#             # 3.1 通用批量更新（无财务业务逻辑）
#             sql_batch_update = """
#                 UPDATE sort_finance
#                 SET estimate_total_revenue = :ttm_revenue,
#                     estimate_total_netprofit = :ttm_netprofit
#                 WHERE secucode = :secucode AND report_time = :report_time
#             """
#             affected_rows = batch_update(sql_batch_update, df=df_ttm)
#             # 3.2 通用逐行重试（兜底机制，无财务业务逻辑）
#             if affected_rows == 0:
#                 logger.warning(f"⚠️ {symbol} 批量更新失败，触发逐行重试")
#                 affected_rows = retry_row_update(symbol, sql_batch_update, df=df_ttm)
#             # 3.3 统一判断更新结果，更新处理状态（批量/逐行成功都处理）
#             if affected_rows > 0:
#                 db_write("UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode", params={"secucode": symbol})
#                 successful_symbols.append(symbol)
#                 logger.info(f"✅ {symbol} 全流程处理完成，更新{affected_rows}条财务数据")
#             else:
#                 failed_symbols.append(symbol)
#                 logger.warning(f"⚠️ {symbol} 无有效数据更新（批量+逐行重试均失败），处理失败")
#         except Exception as e:
#             logger.error(f"❌ {symbol} 处理失败: {str(e)}", exc_info=True)
#             failed_symbols.append(symbol)
#         finally:
#             # 随机延迟，避免频繁请求
#             if i < len(symbols):
#                 sleep_time = random.uniform(SLEEP_MIN, SLEEP_MAX)
#                 logger.debug(f"ℹ️  等待{sleep_time:.2f}秒后处理下一只（DEBUG级）")
#                 time.sleep(sleep_time)
    
#     # ---------------------- 任务汇总统计 ----------------------
#     logger.info(f"\n" + "=" * 80)
#     logger.info(f"📊 任务完成汇总")
#     logger.info(f"✅ 成功处理：{len(successful_symbols)} 只 | {', '.join(successful_symbols) if successful_symbols else '无'}")
#     logger.info(f"❌ 处理失败：{len(failed_symbols)} 只 | {', '.join(failed_symbols) if failed_symbols else '无'}")
#     logger.info(f"📈 处理成功率：{len(successful_symbols)/len(symbols)*100:.2f}%")
#     logger.info(f"=" * 80)


# # ---------------------- 程序入口 ----------------------
# if __name__ == "__main__":
#     main()