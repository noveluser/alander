#!/usr/bin/python3
# coding=utf-8
"""
核心业务计算模块 - 仅做财务指标计算（TTM/PE/PB）
"""
import pandas as pd
import logging

logger = logging.getLogger()

# ---------------------- 矢量化计算TTM滚动净利润/营收（核心业务） ----------------------
def calc_ttm_finance(df):
    """
    矢量化计算每一期的TTM滚动净利润、总营收 - 适配季度累积型财务数据
    公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
    :param df: pd.DataFrame 财务原始数据（report_time为datetime类型）
    :return: pd.DataFrame 新增ttm_netprofit/ttm_revenue列的TTM数据
    """
    # 深拷贝避免修改原数据，按报告时间升序排序（滚动计算核心前提）
    df_ttm = df.copy().sort_values('report_time', ascending=True).reset_index(drop=True)
    
    # 校验必要财务列
    required_cols = ['deduct_parent_netprofit', 'operate_income', 'secucode']
    is_valid, msg = validate_dataframe(df_ttm, required_cols)
    if not is_valid:
        logger.warning(f"⚠️  TTM计算数据验证失败: {msg}")
        return df_ttm
    
    # 新增年、季度列（Q1=3月,Q2=6月,Q3=9月,Q4=12月/年报）
    df_ttm['year'] = df_ttm['report_time'].dt.year
    df_ttm['quarter'] = df_ttm['report_time'].dt.month.map({3:1, 6:2, 9:3, 12:4})
    # 过滤非季度报数据
    abnormal_quarter = df_ttm[df_ttm['quarter'].isna()].shape[0]
    if abnormal_quarter > 0:
        logger.warning(f"⚠️  发现{abnormal_quarter}条非季度报数据，跳过TTM计算")
    
    # 构造匹配字段：上一年度、上一年同季度
    df_ttm['last_year'] = df_ttm['year'] - 1
    df_ttm['last_year_quarter'] = df_ttm['quarter']
    
    # 提取上一年年报数据（Q4）
    last_year_annual = df_ttm[df_ttm['quarter'] == 4][['year', 'deduct_parent_netprofit', 'operate_income']].rename(
        columns={'year': 'last_year', 'deduct_parent_netprofit': 'last_year_annual_net', 'operate_income': 'last_year_annual_rev'}
    )
    # 提取上一年同季度数据
    last_year_same_quarter = df_ttm[['year', 'quarter', 'deduct_parent_netprofit', 'operate_income']].rename(
        columns={'year': 'last_year', 'quarter': 'last_year_quarter', 'deduct_parent_netprofit': 'last_year_sq_net', 'operate_income': 'last_year_sq_rev'}
    )
    
    # 关联数据（左连接）
    df_ttm = df_ttm.merge(last_year_annual, on='last_year', how='left')
    df_ttm = df_ttm.merge(last_year_same_quarter, on=['last_year', 'last_year_quarter'], how='left')
    
    # 核心公式计算TTM
    df_ttm['ttm_netprofit'] = df_ttm['last_year_annual_net'] - df_ttm['last_year_sq_net'] + df_ttm['deduct_parent_netprofit']
    df_ttm['ttm_revenue'] = df_ttm['last_year_annual_rev'] - df_ttm['last_year_sq_rev'] + df_ttm['operate_income']
    
    # 清理临时列，保留核心字段
    df_ttm = df_ttm.drop(columns=['year', 'quarter', 'last_year', 'last_year_quarter', 
                                  'last_year_annual_net', 'last_year_annual_rev', 
                                  'last_year_sq_net', 'last_year_sq_rev'])
    df_ttm = df_ttm[['report_time', 'secucode', 'ttm_netprofit', 'ttm_revenue']]
    df_ttm = df_ttm.dropna(subset=['ttm_revenue', 'ttm_netprofit'])   # 剔除结果为NAN的数据
    
    # 统计有效TTM数据
    ttm_valid_count = df_ttm[df_ttm['ttm_netprofit'].notna() & df_ttm['ttm_revenue'].notna()].shape[0]
    logger.debug(f"✅ TTM矢量化计算完成：有效{ttm_valid_count}条，总报告期{len(df_ttm)}条（DEBUG级）")
    return df_ttm

# ---------------------- PE/PB计算（基于TTM数据） ----------------------
def calc_pe_pb(ttm_netprofit, ttm_revenue, total_shares=1):
    """
    计算市盈率(PE)/市净率(PB) - 通用模板，可按需补充股价/净资产数据
    :param ttm_netprofit: TTM滚动净利润
    :param ttm_revenue: TTM滚动总营收
    :param total_shares: 总股数，默认1
    :return: (pe, pb) 市盈率/市净率
    """
    if ttm_netprofit is None or ttm_netprofit <= 0:
        logger.warning(f"⚠️  TTM净利润为空/为负，无法计算PE")
        pe = None
    else:
        eps = ttm_netprofit / total_shares  # 每股收益
        stock_price = 10  # 示例：需从daily_stock_price_list表读取最新收盘价
        pe = stock_price / eps if eps != 0 else None
    # PB计算：需补充每股净资产（total_assets - total_liability）
    pb = None
    logger.debug(f"✅ PE/PB计算完成：PE={pe}, PB={pb}（DEBUG级）")
    return pe, pb

# 复用工具模块的DataFrame验证（避免重复代码）
from utils import validate_dataframe