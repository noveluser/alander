#!/usr/bin/python3
# coding=utf-8
"""
主程序 - 仅做三大步：数据输入 → 数据处理 → 数据输出
核心：无具体实现细节，仅调度各标准化模块
author:wangle
version:1.0
"""
import random
import time
import pandas as pd
from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG
# 导入各标准化模块
from utils import init_logger, validate_dataframe
from db_operation import create_db_engine, db_read, db_write, batch_update, retry_row_update


# ---------------------- 初始化（仅日志+DB引擎） ----------------------
logger = init_logger(LOG_CONFIG)  # 初始化日志
create_db_engine(DB_URL, DB_CONFIG)  # 初始化DB引擎


def calc_ttm_finance(df):
    """
    【纯业务函数】矢量化计算每一期的TTM滚动净利润、总营收 - 适配季度累积型财务数据
    公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
    特性：仅处理数据计算，不涉及任何数据库操作，输入输出均为DataFrame
    :param df: pd.DataFrame 财务原始数据
    :return: pd.DataFrame 新增ttm_netprofit/ttm_revenue列的TTM数据
    """
    df_ttm = df.copy().sort_values('report_time', ascending=True).reset_index(drop=True)

    required_cols = ['deduct_parent_netprofit', 'operate_income', 'secucode', 'report_time']
    is_valid, msg = validate_dataframe(df_ttm, required_cols)
    if not is_valid:
        logger.warning(f"⚠️  TTM计算数据验证失败: {msg}")
        return pd.DataFrame()

    df_ttm['year'] = df_ttm['report_time'].dt.year
    df_ttm['quarter'] = df_ttm['report_time'].dt.month.map({3:1, 6:2, 9:3, 12:4})

    abnormal_quarter = df_ttm['quarter'].isna().sum()
    if abnormal_quarter > 0:
        logger.warning(f"⚠️  发现{abnormal_quarter}条非季度报数据，已过滤")
        df_ttm = df_ttm[df_ttm['quarter'].notna()]

    df_ttm['last_year'] = df_ttm['year'] - 1
    df_ttm['last_year_quarter'] = df_ttm['quarter']

    last_year_annual = df_ttm[df_ttm['quarter'] == 4][[
        'year', 'deduct_parent_netprofit', 'operate_income'
    ]].rename(columns={
        'year': 'last_year',
        'deduct_parent_netprofit': 'last_year_annual_net',
        'operate_income': 'last_year_annual_rev'
    })

    last_year_same_quarter = df_ttm[[
        'year', 'quarter', 'deduct_parent_netprofit', 'operate_income'
    ]].rename(columns={
        'year': 'last_year',
        'quarter': 'last_year_quarter',
        'deduct_parent_netprofit': 'last_year_sq_net',
        'operate_income': 'last_year_sq_rev'
    })

    df_ttm = df_ttm.merge(last_year_annual, on='last_year', how='left')
    df_ttm = df_ttm.merge(last_year_same_quarter, on=['last_year','last_year_quarter'], how='left')

    df_ttm['ttm_netprofit'] = df_ttm['last_year_annual_net'] - df_ttm['last_year_sq_net'] + df_ttm['deduct_parent_netprofit']
    df_ttm['ttm_revenue'] = df_ttm['last_year_annual_rev'] - df_ttm['last_year_sq_rev'] + df_ttm['operate_income']

    df_ttm = df_ttm[['report_time', 'secucode', 'ttm_netprofit', 'ttm_revenue']]

    logger.debug(f"✅ TTM计算完成：有效{len(df_ttm)}条")
    return df_ttm


# # ---------------------- 矢量化计算TTM滚动净利润/营收（核心业务） ----------------------
# def calc_ttm_finance(df):
#     """
#     矢量化计算每一期的TTM滚动净利润、总营收 - 适配季度累积型财务数据
#     公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
#     :param df: pd.DataFrame 财务原始数据（report_time为datetime类型）
#     :return: pd.DataFrame 新增ttm_netprofit/ttm_revenue列的TTM数据
#     """
#     # 深拷贝避免修改原数据，按报告时间升序排序（滚动计算核心前提）
#     df_ttm = df.copy().sort_values('report_time', ascending=True).reset_index(drop=True)
    
#     # 校验必要财务列
#     required_cols = ['deduct_parent_netprofit', 'operate_income', 'secucode']
#     is_valid, msg = validate_dataframe(df_ttm, required_cols)
#     if not is_valid:
#         logger.warning(f"⚠️  TTM计算数据验证失败: {msg}")
#         return df_ttm
    
#     # 新增年、季度列（Q1=3月,Q2=6月,Q3=9月,Q4=12月/年报）
#     df_ttm['year'] = df_ttm['report_time'].dt.year
#     df_ttm['quarter'] = df_ttm['report_time'].dt.month.map({3:1, 6:2, 9:3, 12:4})
#     # 过滤非季度报数据
#     abnormal_quarter = df_ttm[df_ttm['quarter'].isna()].shape[0]
#     if abnormal_quarter > 0:
#         logger.warning(f"⚠️  发现{abnormal_quarter}条非季度报数据，跳过TTM计算")
    
#     # 构造匹配字段：上一年度、上一年同季度
#     df_ttm['last_year'] = df_ttm['year'] - 1
#     df_ttm['last_year_quarter'] = df_ttm['quarter']
    
#     # 提取上一年年报数据（Q4）
#     last_year_annual = df_ttm[df_ttm['quarter'] == 4][['year', 'deduct_parent_netprofit', 'operate_income']].rename(
#         columns={'year': 'last_year', 'deduct_parent_netprofit': 'last_year_annual_net', 'operate_income': 'last_year_annual_rev'}
#     )
#     # 提取上一年同季度数据
#     last_year_same_quarter = df_ttm[['year', 'quarter', 'deduct_parent_netprofit', 'operate_income']].rename(
#         columns={'year': 'last_year', 'quarter': 'last_year_quarter', 'deduct_parent_netprofit': 'last_year_sq_net', 'operate_income': 'last_year_sq_rev'}
#     )
    
#     # 关联数据（左连接）
#     df_ttm = df_ttm.merge(last_year_annual, on='last_year', how='left')
#     df_ttm = df_ttm.merge(last_year_same_quarter, on=['last_year', 'last_year_quarter'], how='left')
    
#     # 核心公式计算TTM
#     df_ttm['ttm_netprofit'] = df_ttm['last_year_annual_net'] - df_ttm['last_year_sq_net'] + df_ttm['deduct_parent_netprofit']
#     df_ttm['ttm_revenue'] = df_ttm['last_year_annual_rev'] - df_ttm['last_year_sq_rev'] + df_ttm['operate_income']
    
#     # 清理临时列，保留核心字段
#     df_ttm = df_ttm.drop(columns=['year', 'quarter', 'last_year', 'last_year_quarter', 
#                                   'last_year_annual_net', 'last_year_annual_rev', 
#                                   'last_year_sq_net', 'last_year_sq_rev'])
#     df_ttm = df_ttm[['report_time', 'secucode', 'ttm_netprofit', 'ttm_revenue']]
    
#     # 统计有效TTM数据
#     ttm_valid_count = df_ttm[df_ttm['ttm_netprofit'].notna() & df_ttm['ttm_revenue'].notna()].shape[0]
#     logger.debug(f"✅ TTM矢量化计算完成：有效{ttm_valid_count}条，总报告期{len(df_ttm)}条（DEBUG级）")

#     df_ttm = df_ttm.dropna(subset=['ttm_revenue', 'ttm_netprofit'])   # 剔除结果为NAN的数据

#     return df_ttm

# ---------------------- 主流程：数据输入→数据处理→数据输出 ----------------------
def main():
    """主函数：仅三大核心步骤，全程通过SQL实现数据输入/输出"""
    # 配置常量
    UNPROCESSED_FLAG = BUSINESS_CONFIG["unprocessed_flag"]
    REPORT_TIME_START = BUSINESS_CONFIG["report_time_start"]
    SLEEP_MIN = BUSINESS_CONFIG["sleep_time_min"]
    SLEEP_MAX = BUSINESS_CONFIG["sleep_time_max"]
    
    # ---------------------- 步骤1：数据输入（SQL读取，全量未处理股票+财务数据） ----------------------
    # 1.1 读取stock_list表中未处理股票代码（SQL输入）
    sql_unprocessed = "SELECT secucode FROM stock_list WHERE secucode = :flag"   #测试语句，非错误
    params_unprocessed = {"flag": UNPROCESSED_FLAG}
    df_unprocessed = db_read(sql_unprocessed, params_unprocessed)
    symbols = df_unprocessed['secucode'].tolist() if not df_unprocessed.empty else []
    if not symbols:
        logger.info("ℹ️  stock_list表中无未处理股票，任务结束")
        return
    logger.info(f"🚀 开始批量处理，共{len(symbols)}只未处理股票")
    
    # 初始化统计
    successful_symbols = []
    failed_symbols = []

    # 遍历处理每只股票
    for i, symbol in enumerate(symbols, 1):
        logger.info(f"\n===== 处理进度：[{i}/{len(symbols)}] 股票代码：{symbol} =====")
        try:
            # 1.2 读取财务原始数据（通用DB操作）
            sql_finance = """
                SELECT * FROM sort_finance 
                WHERE secucode = :secucode AND report_time > :start_time
            """
            df_finance = db_read(sql_finance, params={"secucode": symbol, "start_time": REPORT_TIME_START})
            if df_finance.empty:
                logger.warning(f"⚠️  {symbol} 无符合条件的财务数据")
                failed_symbols.append(symbol)
                continue
            
            # 数据预处理（通用数据转换）
            df_finance['report_time'] = pd.to_datetime(df_finance['report_time'])
            # 数据验证（通用校验）
            if not validate_dataframe(df_finance)[0]:
                logger.warning(f"⚠️  {symbol} 财务数据格式验证失败")
                failed_symbols.append(symbol)
                continue

            # ---------------------- 步骤2：纯业务处理（仅调用TTM计算） ----------------------
            df_ttm = calc_ttm_finance(df_finance)
            if df_ttm.empty:
                logger.warning(f"⚠️  {symbol} TTM计算无有效数据")
                failed_symbols.append(symbol)
                continue

            # ---------------------- 步骤3：通用数据输出（SQL更新） ----------------------
            # 3.1 通用批量更新（无财务业务逻辑）
            sql_batch_update = """
                UPDATE sort_finance
                SET estimate_total_revenue = :ttm_revenue,
                    estimate_total_netprofit = :ttm_netprofit
                WHERE secucode = :secucode AND report_time = :report_time
            """
            affected_rows = batch_update(sql_batch_update, df=df_ttm)

            # 3.2 通用逐行重试（兜底机制，无财务业务逻辑）
            if affected_rows == 0:
                logger.warning(f"⚠️ {symbol} 批量更新失败，触发逐行重试")
                affected_rows = retry_row_update(symbol, sql_batch_update, df=df_ttm)

            # 3.3 统一判断更新结果，更新处理状态（批量/逐行成功都处理）
            if affected_rows > 0:
                db_write("UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode", params={"secucode": symbol})
                successful_symbols.append(symbol)
                logger.info(f"✅ {symbol} 全流程处理完成，更新{affected_rows}条财务数据")
            else:
                failed_symbols.append(symbol)
                logger.warning(f"⚠️ {symbol} 无有效数据更新（批量+逐行重试均失败），处理失败")

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
    logger.info(f"📊 任务完成汇总")
    logger.info(f"✅ 成功处理：{len(successful_symbols)} 只 | {', '.join(successful_symbols) if successful_symbols else '无'}")
    logger.info(f"❌ 处理失败：{len(failed_symbols)} 只 | {', '.join(failed_symbols) if failed_symbols else '无'}")
    logger.info(f"📈 处理成功率：{len(successful_symbols)/len(symbols)*100:.2f}%")
    logger.info(f"=" * 80)


# ---------------------- 程序入口 ----------------------
if __name__ == "__main__":
    main()