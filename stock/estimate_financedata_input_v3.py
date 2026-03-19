#!/usr/bin/python3
# coding=utf-8
"""
PE,PB,ROE计算 - 最终优化版V3
核心优化：全SQLAlchemy+配置抽离+DF矢量化+日志分级+批量读未处理
author:wangle
version:3.0
"""
# 基础库
import akshare as ak
import pandas as pd
import logging
import time
import os
import random
from datetime import datetime
from decimal import Decimal

# SQLAlchemy核心库（全替代pymysql）
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

# 导入统一配置
from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG

# ---------------------- 全局变量：SQLAlchemy引擎单例 ----------------------
DB_ENGINE = None

# ---------------------- 日志初始化：分级配置（核心优化） ----------------------
def init_logger():
    """初始化日志 - 分级可控、文件+控制台双输出"""
    # 创建日志器
    logger = logging.getLogger()
    logger.setLevel(LOG_CONFIG["level"])
    logger.handlers.clear()  # 清除默认处理器，防止重复输出

    # 1. 文件日志处理器
    if not os.path.exists(os.path.dirname(LOG_CONFIG["file_path"])):
        os.makedirs(os.path.dirname(LOG_CONFIG["file_path"]))
    file_handler = logging.FileHandler(LOG_CONFIG["file_path"], mode=LOG_CONFIG["file_mode"], encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_CONFIG["format"], datefmt=LOG_CONFIG["datefmt"]))
    logger.addHandler(file_handler)

    # 2. 控制台日志处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(LOG_CONFIG["console_format"], datefmt=LOG_CONFIG["datefmt"]))
    logger.addHandler(console_handler)

    return logger

# 初始化日志
logger = init_logger()

# ---------------------- SQLAlchemy引擎单例创建（核心优化） ----------------------
def create_db_engine():
    """创建SQLAlchemy引擎单例，全程复用连接池"""
    global DB_ENGINE
    if DB_ENGINE is not None:
        return DB_ENGINE
    try:
        DB_ENGINE = create_engine(
            DB_URL,
            pool_size=DB_CONFIG["pool_size"],
            max_overflow=DB_CONFIG["max_overflow"],
            pool_recycle=DB_CONFIG["pool_recycle"],
            pool_pre_ping=DB_CONFIG["pool_pre_ping"]
        )
        # 测试引擎有效性
        with DB_ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ SQLAlchemy数据库引擎创建成功，连接池初始化完成")
        return DB_ENGINE
    except SQLAlchemyError as e:
        logger.critical(f"❌ SQLAlchemy引擎创建失败: {str(e)}", exc_info=True)
        raise SystemExit(1)
    except Exception as e:
        logger.critical(f"❌ 数据库连接异常: {str(e)}", exc_info=True)
        raise SystemExit(1)

# ---------------------- 原业务函数：保留+适配SQLAlchemy/DF ----------------------
def to_decimal(value):
    """将值转换为Decimal，保留4位小数（适配DF空值/异常值）"""
    if pd.isna(value) or value is None:
        return None
    try:
        return Decimal(str(round(float(value), 4)))
    except (ValueError, TypeError):
        logger.warning(f"值转换Decimal失败: {value}")
        return None

def format_row_data(row):
    """格式化单行数据（适配DF行），处理日期、类型转换"""
    # 日期转换：适配DF的date列/日期字符串
    date_val = row.get("日期", row.get("report_time", None))
    sql_date = None
    if pd.notna(date_val):
        if isinstance(date_val, (datetime, pd.Timestamp)):
            sql_date = date_val.date()
        elif isinstance(date_val, str):
            date_val = date_val.strip()
            if date_val:
                sql_date = datetime.strptime(date_val, "%Y-%m-%d").date()

    # 构造批量插入数据元组
    row_data = (
        sql_date,
        row.get("secucode", row.get("代码", None)),
        to_decimal(row.get("close")),
        to_decimal(row.get("volume"))
    )
    return row_data

def get_stock_listing_date(symbol):
    """获取上市日期（原逻辑保留，日志升级）"""
    try:
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        # 方法1：查找上市时间字段
        for _, row in stock_info.iterrows():
            item_str = str(row['item']).strip()
            if '上市时间' in item_str or 'listing_date' in item_str.lower():
                date_str = str(row['value']).strip()
                if date_str and len(date_str) >= 8:
                    digits = ''.join(filter(str.isdigit, date_str))
                    if len(digits) >= 8:
                        return digits[:8]
        # 方法2：备用字段提取
        if 'date' in stock_info.columns:
            date_col = stock_info[stock_info['item'].str.contains('时间|date', case=False, na=False)]
            if not date_col.empty:
                date_str = str(date_col.iloc[0]['value'])
                digits = ''.join(filter(str.isdigit, date_str))
                if len(digits) >= 8:
                    return digits[:8]
        logger.warning(f"⚠️  {symbol}未提取到上市日期")
        return None
    except Exception as e:
        logger.error(f"❌ 获取{symbol}上市日期失败: {str(e)}", exc_info=True)
        return None

def validate_dataframe(df, required_cols=None):
    """验证DataFrame的完整性（原逻辑保留，日志优化）"""
    if df is None or df.empty:
        return False, "数据为空"
    if required_cols:
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少必要列: {missing_cols}"
    if 'secucode' not in df.columns and '代码' not in df.columns:
        return False, "缺少代码列"
    return True, "数据验证通过"

# ---------------------- 核心优化：SQLAlchemy实现get_data（pd.read_sql） ----------------------
def get_data(secucode):
    """
    从数据库读取财务数据 - 全SQLAlchemy+pd.read_sql
    :param secucode: 代码
    :return: pd.DataFrame 财务数据（空数据返回空DF）
    """
    try:
        engine = create_db_engine()
        # SQL防注入：使用text+参数绑定（SQLAlchemy原生防注入）
        sql = text("""
            SELECT * FROM sort_finance 
            WHERE secucode = :secucode 
              AND report_time > :start_time
        """)
        # pd.read_sql直接对接SQLAlchemy，返回结构化DF，自动适配字段类型
        df = pd.read_sql(
            sql,
            engine,
            params={"secucode": secucode, "start_time": BUSINESS_CONFIG["report_time_start"]}
        )
        if df.empty:
            logger.warning(f"⚠️  {secucode}在sort_finance表中无匹配数据（起始时间：{BUSINESS_CONFIG['report_time_start']}）")
        else:
            # 提前转换日期类型，为后续矢量化计算做准备
            df['report_time'] = pd.to_datetime(df['report_time'])
            logger.debug(f"✅ {secucode}读取到{len(df)}条财务数据（DEBUG级）")
        return df
    except SQLAlchemyError as e:
        logger.error(f"❌ {secucode}读取数据失败(SQLAlchemy): {str(e)}", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ {secucode}读取数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# ---------------------- 核心优化：批量读取未处理（从stock_list表） ----------------------
def get_all_unprocessed_stocks():
    """
    从stock_list表批量读取未处理代码（flag=BUSINESS_CONFIG['unprocessed_flag']）
    :return: list 未处理代码列表
    """
    try:
        engine = create_db_engine()
        sql = text("""
            SELECT secucode FROM stock_list 
            WHERE secucode = :unprocessed_flag 
        """)
        df = pd.read_sql(sql, engine, params={"unprocessed_flag": BUSINESS_CONFIG["unprocessed_flag"]})
        if df.empty:
            logger.info("ℹ️  stock_list表中无未处理（flag={}）".format(BUSINESS_CONFIG["unprocessed_flag"]))
            return []
        stock_list = df['secucode'].tolist()
        logger.info(f"✅ 从stock_list表读取到{len(stock_list)}只未处理")
        return stock_list
    except SQLAlchemyError as e:
        logger.error(f"❌ 批量读取未处理失败: {str(e)}", exc_info=True)
        return []


# ---------------------- 核心优化：SQLAlchemy实现批量更新财务预估数据 ----------------------
def execute_batch_insert(stock_code, batch_data):
    """
    批量更新 sort_finance 表的预估营收/净利润字段
    :param stock_code: 代码
    :param batch_data: 计算好的TTM数据(DataFrame)，包含secucode/report_time/ttm_revenue/ttm_netprofit
    :return: 总影响行数, 更新行数, 0(无插入)
    """
    engine = create_db_engine()
    # SQLAlchemy 命名参数语法，仅更新2个目标字段
    sql = text("""
        UPDATE sort_finance
        SET estimate_total_revenue = :ttm_revenue,
            estimate_total_netprofit = :ttm_netprofit
        WHERE secucode = :secucode AND report_time = :report_time
    """)
    
    # 转换为字典列表，核心处理：清洗代码+空值兜底+日期统一
    dict_data = []
    for _, row in batch_data.iterrows():
        # 1. 清洗股票代码：匹配数据库格式
        secu_code = str(row['secucode']).strip()
        # 2. 统一报告期格式：转成date类型（匹配数据库report_time的日期类型）
        report_time = row['report_time']
        if isinstance(report_time, (datetime, pd.Timestamp)):
            report_time = report_time.date()
        # 3. TTM空值兜底为0，避免过滤有效行
        ttm_rev = row['ttm_revenue'] if pd.notna(row['ttm_revenue']) else 0
        ttm_net = row['ttm_netprofit'] if pd.notna(row['ttm_netprofit']) else 0
        
        # 【新增过滤】仅当 主键有效 + TTM值大于0 时，才加入更新列表
        if secu_code and report_time and (ttm_rev != 0 or ttm_net != 0):
            dict_data.append({
                "secucode": secu_code,
                "report_time": report_time,
                "ttm_revenue": ttm_rev,
                "ttm_netprofit": ttm_net
            })
            # 放在 dict_data.append 之后，执行 SQL 之前
            logger.info(f"📌 {stock_code} 待更新值示例：{[(d['secucode'], d['report_time'], d['ttm_revenue'], d['ttm_netprofit']) for d in dict_data[:5]]}")

    if not dict_data:
        logger.warning(f"⚠️ {stock_code}无有效批量更新数据（主键为空）")
        return 0, 0, 0

    try:
        with engine.connect() as conn:
            result = conn.execute(sql, dict_data)
            conn.commit()
        
        total_affected = result.rowcount
        logger.info(f"✅ {stock_code}批量更新完成，共更新{total_affected}条数据")
        return total_affected, 0, total_affected
    except SQLAlchemyError as e:
        logger.error(f"❌ {stock_code}批量更新失败: {str(e)}", exc_info=True)
        return 0, 0, 0


# # ---------------------- 核心优化：SQLAlchemy实现批量插入（替代pymysql executemany） ----------------------
# def execute_batch_insert(stock_code, batch_data):
#     """

#     """
#     engine = create_db_engine()
#     sql = text("""
#         UPDATE sort_finance
#         SET estimate_total_revenue = %s,
#             estimate_total_netprofit = %s
#         WHERE secucode = %s AND report_time = %s
#     """)
#     # 转换为字典列表（SQLAlchemy批量绑定更友好）
#     dict_data = [
#         {
#             "date": d[0], "secucode": d[1],
#             "close_price": d[2], "volume": d[3]
#         } for d in batch_data if all(pd.notna(d[:2]))  # 过滤空日期/空代码
#     ]
#     if not dict_data:
#         logger.warning(f"⚠️  {stock_code}无有效批量插入数据")
#         return 0, 0, 0

#     try:
#         with engine.connect() as conn:
#             # 批量执行（SQLAlchemy 2.0+原生批量插入）
#             result = conn.execute(sql, dict_data)
#             conn.commit()
#         total_affected = result.rowcount
#         total_rows = len(dict_data)
#         # 计算插入/更新数（ON DUPLICATE KEY UPDATE：插入返回1，更新返回2）
#         updated_count = total_affected - total_rows
#         inserted_count = total_rows - updated_count
#         logger.info(f"✅ {stock_code}批量处理{total_rows}条，插入{inserted_count}条，更新{updated_count}条")
#         return total_affected, inserted_count, updated_count
#     except SQLAlchemyError as e:
#         logger.error(f"❌ {stock_code}批量插入失败: {str(e)}", exc_info=True)
#         return 0, 0, 0


def retry_insert_row_by_row(stock_code, batch_data):
    """
    逐行重试更新sort_finance表的预估营收/净利润（批量更新失败时的兜底重试）
    :param stock_code: 代码
    :param batch_data: TTM计算结果DataFrame，包含secucode/report_time/ttm_revenue/ttm_netprofit
    :return: 成功更新行数, 失败行数
    """
    engine = create_db_engine()
    sql = text("""
        UPDATE sort_finance
        SET estimate_total_revenue = :ttm_revenue,
            estimate_total_netprofit = :ttm_netprofit
        WHERE secucode = :secucode AND report_time = :report_time
    """)
    
    success_count = 0
    fail_count = 0
    
    try:
        with engine.connect() as conn:
            for _, row in batch_data.iterrows():
                # 1. 清洗股票代码
                secu_code = str(row['secucode']).split('.')[0].strip()
                # 2. 统一报告期格式
                report_time = row['report_time']
                if isinstance(report_time, (datetime, pd.Timestamp)):
                    report_time = report_time.date()
                # 3. TTM空值兜底为0
                ttm_rev = row['ttm_revenue'] if pd.notna(row['ttm_revenue']) else 0
                ttm_net = row['ttm_netprofit'] if pd.notna(row['ttm_netprofit']) else 0

                # 过滤主键为空的无效行
                if not secu_code or pd.isna(report_time):
                    fail_count += 1
                    continue
                
                # 构造行参数
                row_param = {
                    "secucode": secu_code,
                    "report_time": report_time,
                    "ttm_revenue": ttm_rev,
                    "ttm_netprofit": ttm_net
                }
                
                try:
                    conn.execute(sql, row_param)
                    conn.commit()
                    success_count += 1
                except SQLAlchemyError as e:
                    conn.rollback()
                    logger.warning(f"⚠️ {stock_code} 报告期{report_time} 单条更新失败: {str(e)[:50]}...")
                    fail_count += 1
    except Exception as e:
        logger.error(f"❌ {stock_code}逐行更新流程异常: {str(e)}", exc_info=True)

    logger.info(f"🔄 {stock_code}逐行重试完成：成功{success_count}条，失败{fail_count}条")
    return success_count, fail_count


def batch_insert_optimized(stock_code, data):
    """
    优化后的批量更新调度 - 适配TTM财务DataFrame
    保留原有逻辑：批量更新影响行数=0则触发逐行重试
    """
    # 数据验证（保留原有逻辑）
    is_valid, msg = validate_dataframe(data, required_cols=['secucode', 'report_time', 'ttm_revenue', 'ttm_netprofit'])
    if not is_valid:
        logger.warning(f"⚠️  {stock_code}TTM数据验证失败: {msg}")
        return 0, 0, 0

    logger.debug(f"✅ {stock_code}TTM数据验证通过，共{len(data)}条")
    
    try:
        # 执行批量更新
        total_affected, inserted_count, updated_count = execute_batch_insert(stock_code, data)
        # 批量更新成功，直接返回
        if total_affected > 0:
            return total_affected, inserted_count, updated_count
        # 批量更新影响行数为0，主动抛出异常触发重试
        else:
            raise SQLAlchemyError("批量更新返回影响行数为0，触发逐行重试")
    
    # 批量更新失败，执行逐行重试
    except SQLAlchemyError as e:
        logger.warning(f"⚠️  {stock_code}批量更新失败，触发逐行重试: {str(e)}")
        # 调用逐行重试函数
        success_count, fail_count = retry_insert_row_by_row(stock_code, data)
        # 【核心修复】返回标准三元组，保证主函数统计正常
        return success_count, 0, success_count
    
    # 其他未知异常
    except Exception as e:
        logger.error(f"❌ {stock_code}批量更新调度失败: {str(e)}", exc_info=True)
        return 0, 0, 0


# ---------------------- 批量插入主函数：适配全SQLAlchemy ----------------------
# def batch_insert_optimized(stock_code, data):
#     """
#     优化后的批量更新调度 - 适配TTM财务DataFrame，全SQLAlchemy实现，保留失败重试逻辑
#     :param stock_code: 代码
#     :param data: pd.DataFrame TTM计算结果（含secucode/report_time/ttm_revenue/ttm_netprofit）
#     :return: (total_affected, inserted_count, updated_count)
#     """
#     # 针对TTM数据的专属验证（适配实际列结构）
#     is_valid, msg = validate_dataframe(data, required_cols=['secucode', 'report_time', 'ttm_revenue', 'ttm_netprofit'])
#     if not is_valid:
#         logger.warning(f"⚠️  {stock_code}TTM数据验证失败: {msg}")
#         return 0, 0, 0

#     logger.debug(f"✅ {stock_code}TTM数据验证通过，共{len(data)}条（DEBUG级）")
#     # 批量更新主逻辑：直接传DF，不做任何格式化
#     try:
#         total_affected, inserted_count, updated_count = execute_batch_insert(stock_code, data)
#         if total_affected > 0:
#             return total_affected, inserted_count, updated_count
#         else:
#             raise SQLAlchemyError("批量更新返回影响行数为0，触发逐行重试")
#     except SQLAlchemyError as e:
#         logger.warning(f"⚠️  {stock_code}批量更新失败，触发逐行重试: {str(e)}")
#         # 逐行重试也直接传DF
#         success, fail = retry_insert_row_by_row(stock_code, data)
#         return success, 0, success
#     except Exception as e:
#         logger.error(f"❌ {stock_code}批量更新调度失败: {str(e)}", exc_info=True)
#         return 0, 0, 0
    

# ---------------------- 兼容原函数：inputdata ----------------------
def inputdata(stock_code, data):
    """兼容性函数，调用批量插入"""
    total_affected, _, _ = batch_insert_optimized(stock_code, data)
    logger.info(f"ℹ️  {stock_code}最终插入/更新总条数: {total_affected}")
    return total_affected

# ---------------------- 核心优化：SQLAlchemy实现标志位更新 ----------------------
def download_sucess_flag(code):
    """
    更新处理成功标志位 - 全SQLAlchemy实现（替代原pymysql）
    :param code: 代码
    :return: bool 是否更新成功
    """
    try:
        engine = create_db_engine()
        sql = text("""
            UPDATE stock_list 
            SET flag = 'Y' 
            WHERE secucode = :secucode
        """)
        with engine.connect() as conn:
            result = conn.execute(sql, {"secucode": code})
            conn.commit()
        if result.rowcount > 0:
            logger.info(f"✅ {code}标志位更新为Y成功")
            return True
        else:
            logger.warning(f"⚠️  {code}标志位更新失败：无匹配记录")
            return False
    except SQLAlchemyError as e:
        logger.error(f"❌ {code}标志位更新失败(SQLAlchemy): {str(e)}", exc_info=True)
        return False


def calc_ttm_finance(df):
    """
    矢量化计算**每一期**的TTM滚动净利润、总营收 - 适配**季度累积型财务数据**，按公式：TTM = 上一年年度数据 - 上一年同季度数据 + 本年度季度数据
    :param df: pd.DataFrame 已处理的财务数据（report_time为datetime类型，含deduct_parent_netprofit/operate_income列）
    :return: pd.DataFrame 原DF新增ttm_netprofit/ttm_revenue列，为每一期对应的TTM值
    """
    # 深拷贝避免修改原数据，按报告时间升序排序（滚动计算核心前提）
    df_ttm = df.copy().sort_values('report_time', ascending=True).reset_index(drop=True)
    
    # 校验必要列是否存在
    required_cols = ['deduct_parent_netprofit', 'operate_income']
    missing_cols = [col for col in required_cols if col not in df_ttm.columns]
    if missing_cols:
        logger.warning(f"⚠️  缺少必要财务列，无法计算TTM: {missing_cols}")
        return df_ttm
    
    # 新增年、季度列（适配累积数据TTM计算，Q1=3月,Q2=6月,Q3=9月,Q4=12月/年报）
    df_ttm['year'] = df_ttm['report_time'].dt.year
    df_ttm['quarter'] = df_ttm['report_time'].dt.month.map({3:1, 6:2, 9:3, 12:4})
    # 处理异常月份（非季度报），设为NaN并过滤日志
    abnormal_quarter = df_ttm[df_ttm['quarter'].isna()].shape[0]
    if abnormal_quarter > 0:
        logger.warning(f"⚠️  发现{abnormal_quarter}条非季度报数据（非3/6/9/12月），将跳过TTM计算")
    
    # 构造匹配字段：上一年度、上一年同季度
    df_ttm['last_year'] = df_ttm['year'] - 1
    df_ttm['last_year_quarter'] = df_ttm['quarter']
    
    # 提取上一年年报数据（上一年Q4）
    last_year_annual = df_ttm[df_ttm['quarter'] == 4][['year', 'deduct_parent_netprofit', 'operate_income']].rename(
        columns={'year': 'last_year', 'deduct_parent_netprofit': 'last_year_annual_net', 'operate_income': 'last_year_annual_rev'}
    )
    # 提取上一年同季度数据
    last_year_same_quarter = df_ttm[['year', 'quarter', 'deduct_parent_netprofit', 'operate_income']].rename(
        columns={'year': 'last_year', 'quarter': 'last_year_quarter', 'deduct_parent_netprofit': 'last_year_sq_net', 'operate_income': 'last_year_sq_rev'}
    )
    
    # 关联上一年年报和同季度数据（左连接，无匹配则为NaN）
    df_ttm = df_ttm.merge(last_year_annual, on='last_year', how='left')
    df_ttm = df_ttm.merge(last_year_same_quarter, on=['last_year', 'last_year_quarter'], how='left')
    
    # 核心公式计算TTM：上一年年度数据 - 上一年同季度数据 + 本年度季度数据
    df_ttm['ttm_netprofit'] = df_ttm['last_year_annual_net'] - df_ttm['last_year_sq_net'] + df_ttm['deduct_parent_netprofit']
    df_ttm['ttm_revenue'] = df_ttm['last_year_annual_rev'] - df_ttm['last_year_sq_rev'] + df_ttm['operate_income']
    
    # 清理临时列，不污染原数据
    df_ttm = df_ttm.drop(columns=['year', 'quarter', 'last_year', 'last_year_quarter', 
                                  'last_year_annual_net', 'last_year_annual_rev', 
                                  'last_year_sq_net', 'last_year_sq_rev'])
    
    # 统一报告期格式为datetime，方便后续转date
    df_ttm['report_time'] = pd.to_datetime(df_ttm['report_time'])
    # 仅需要这些数据
    df_ttm = df_ttm[['report_time', 'secucode', 'ttm_netprofit', 'ttm_revenue']]
    
    # 统计有效TTM数据条数
    ttm_valid_count = df_ttm[df_ttm['ttm_netprofit'].notna() & df_ttm['ttm_revenue'].notna()].shape[0]
    logger.debug(f"✅ 矢量化计算每一期TTM完成：有效TTM数据{ttm_valid_count}条，总报告期{len(df_ttm)}条（DEBUG级）")

    return df_ttm


# def calc_ttm_finance(df):
#     """
#     矢量化计算**每一期**的TTM滚动净利润、总营收 - 替代原手动for循环，按报告期滚动4个季度计算
#     :param df: pd.DataFrame 已处理的财务数据（report_time为datetime类型，含deduct_parent_netprofit/operate_income列）
#     :return: pd.DataFrame 原DF新增ttm_netprofit/ttm_revenue列，为每一期对应的TTM值
#     """
#     # 深拷贝避免修改原数据，按报告时间升序排序（滚动计算核心前提）
#     df_ttm = df.copy().sort_values('report_time', ascending=True).reset_index(drop=True)
    
#     # 校验必要列是否存在
#     required_cols = ['deduct_parent_netprofit', 'operate_income']
#     missing_cols = [col for col in required_cols if col not in df_ttm.columns]
#     if missing_cols:
#         logger.warning(f"⚠️  缺少必要财务列，无法计算TTM: {missing_cols}")
#         return df_ttm
    
#     # 矢量化滚动4个季度计算TTM（min_periods=4：不足4期则为NaN，符合TTM计算规则）
#     # 财务报告为季度报，滚动4期即过去连续12个月（TTM）
#     df_ttm['ttm_netprofit'] = df_ttm['deduct_parent_netprofit'].rolling(window=4, min_periods=4).sum()
#     df_ttm['ttm_revenue'] = df_ttm['operate_income'].rolling(window=4, min_periods=4).sum()
    
#     # 过滤掉无TTM数据的行日志提示（可选）
#     ttm_valid_count = df_ttm[df_ttm['ttm_netprofit'].notna()].shape[0]
#     logger.debug(f"✅ 矢量化计算每一期TTM完成：有效TTM数据{ttm_valid_count}条，总报告期{len(df_ttm)}条（DEBUG级）")
    
#     return df_ttm


# ---------------------- 核心优化：DF矢量化计算TTM滚动净利润/营收（替代原for循环） ----------------------
# def calc_ttm_finance(df):
#     """
#     矢量化计算TTM滚动净利润、总营收 - 替代原手动for循环，性能提升10倍+
#     :param df: pd.DataFrame 已处理的财务数据（report_time为datetime类型）
#     :return: (estimate_total_netprofit, estimate_total_revenue) TTM净利润/营收
#     """
#     # 提取年份和月份，为矢量化筛选做准备
#     df['year'] = df['report_time'].dt.year
#     df['month'] = df['report_time'].dt.month

#     # 1. 筛选年报数据（12月）
#     annual_df = df[df['month'] == 12].copy()
#     # 2. 筛选最新一期非年报数据
#     non_annual_df = df[df['month'] != 12].sort_values('report_time').tail(1)

#     # 矢量化计算TTM：年报直接取，非年报=上一年年报+当期数据
#     if not non_annual_df.empty:
#         latest_non_annual = non_annual_df.iloc[0]
#         last_year_annual = annual_df[annual_df['year'] == latest_non_annual['year'] - 1]
#         if not last_year_annual.empty:
#             ttm_netprofit = last_year_annual['deduct_parent_netprofit'].iloc[0] + latest_non_annual['deduct_parent_netprofit']
#             ttm_revenue = last_year_annual['operate_income'].iloc[0] + latest_non_annual['operate_income']
#             logger.debug(f"✅ 矢量化计算TTM完成：非年报模式（DEBUG级）")
#             return ttm_netprofit, ttm_revenue
#     # 无最新非年报，取最新年报
#     if not annual_df.empty:
#         latest_annual = annual_df.sort_values('report_time').tail(1).iloc[0]
#         logger.debug(f"✅ 矢量化计算TTM完成：年报模式（DEBUG级）")
#         return latest_annual['deduct_parent_netprofit'], latest_annual['operate_income']

#     # 无有效数据
#     logger.warning(f"⚠️  无有效财务数据，无法计算TTM")
#     return None, None

# ---------------------- 核心优化：PE/PB计算（基于TTM数据） ----------------------
def calc_pe_pb(ttm_netprofit, ttm_revenue, total_shares=1):
    """
    计算PE/PB（可根据实际业务补充总股数、股价等字段，此处为通用模板）
    :param ttm_netprofit: TTM滚动净利润
    :param ttm_revenue: TTM滚动总营收
    :param total_shares: 总股数（默认1，需根据实际表字段补充）
    :return: (pe, pb) 市盈率/市净率
    """
    if ttm_netprofit is None or ttm_netprofit <= 0:
        logger.warning(f"⚠️  TTM净利润为空/为负，无法计算PE")
        pe = None
    else:
        eps = ttm_netprofit / total_shares  # 每股收益
        stock_price = 10  # 示例：需从daily_stock_price_list表读取最新收盘价，可根据实际补充
        pe = stock_price / eps if eps != 0 else None

    # PB计算：需补充每股净资产（可从sort_finance表读取total_assets/total_liability计算）
    pb = None
    logger.debug(f"✅ PE/PB计算完成：PE={pe}, PB={pb}（DEBUG级）")
    return pe, pb

# ---------------------- 主函数：批量处理+矢量化计算+全SQLAlchemy ----------------------
def main():
    """主函数：批量读取未处理→计算TTM→批量插入→更新标志位"""
    # 初始化SQLAlchemy引擎
    create_db_engine()
    # 批量读取未处理（替代原手动写的symbols列表）
    symbols = get_all_unprocessed_stocks()
    if not symbols:
        logger.info("ℹ️  无未处理，任务结束")
        return

    # 初始化统计
    successful_symbols = []
    failed_symbols = []
    target_date = BUSINESS_CONFIG["target_date"]
    logger.info(f"🚀 开始批量处理，共{len(symbols)}只，目标日期：{target_date}")

    # 遍历处理每只
    for i, symbol in enumerate(symbols, 1):
        logger.info(f"\n===== 处理进度：[{i}/{len(symbols)}] 代码：{symbol} =====")
        try:
            # 1. 读取财务数据（SQLAlchemy+pd.read_sql）
            df_finance = get_data(symbol)
            if df_finance.empty:
                failed_symbols.append(symbol)
                continue

            # 2. 矢量化计算TTM滚动净利润/营收（核心优化，替代for循环）
            ttm_data = calc_ttm_finance(df_finance)

            # df.to_excel("c:/work/log/300206.xlsx", index=False, engine='openpyxl')
            if ttm_data is None:
                failed_symbols.append(symbol)
                continue


            # # 3. 计算PE/PB
            # pe, pb = calc_pe_pb(ttm_netprofit, ttm_revenue)
            # logger.info(f"ℹ️  {symbol} TTM净利润：{ttm_netprofit:.2f}，TTM营收：{ttm_revenue:.2f}，PE：{pe:.2f}，PB：{pb:.2f}")

            # 4. 批量插入数据（全SQLAlchemy）
            inputdata(symbol, ttm_data)
            successful_symbols.append(symbol)
            # # 5. 更新处理成功标志位（全SQLAlchemy）
            # if download_sucess_flag(symbol):
            #     successful_symbols.append(symbol)
            # else:
            #     failed_symbols.append(symbol)

        except Exception as e:
            logger.error(f"❌ {symbol}处理失败: {str(e)}", exc_info=True)
            failed_symbols.append(symbol)
        finally:
            # 添加随机延迟，避免数据库/接口频繁请求
            if i < len(symbols):
                sleep_time = random.uniform(BUSINESS_CONFIG["sleep_time_min"], BUSINESS_CONFIG["sleep_time_max"])
                logger.debug(f"ℹ️  等待{sleep_time:.2f}秒后处理下一只（DEBUG级）")
                time.sleep(sleep_time)

    # 输出任务汇总
    logger.info(f"\n" + "=" * 80)
    logger.info(f"📊 任务完成汇总 - 目标日期：{target_date}")
    logger.info(f"✅ 成功处理：{len(successful_symbols)} 只 | {', '.join(successful_symbols) if successful_symbols else '无'}")
    logger.info(f"❌ 处理失败：{len(failed_symbols)} 只 | {', '.join(failed_symbols) if failed_symbols else '无'}")
    logger.info(f"📈 处理成功率：{len(successful_symbols)/len(symbols)*100:.2f}%")
    logger.info(f"=" * 80)



# ---------------------- 程序入口 ----------------------
if __name__ == "__main__":
    main()