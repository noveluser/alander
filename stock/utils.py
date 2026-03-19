#!/usr/bin/python3
# coding=utf-8
"""
通用工具模块 - 日志、数据转换、验证、日期处理等通用操作
新增：财务数据专属校验（数值+字段+时间），主程序直接调用
"""
import logging
import os
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal

# ---------------------- 日志初始化（分级配置，文件+控制台双输出） ----------------------
def init_logger(LOG_CONFIG):
    """初始化日志器，全局唯一"""
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

# ---------------------- 数据类型转换 ----------------------
def to_decimal(value):
    """将值转换为Decimal，保留4位小数（适配DF空值/异常值）"""
    if pd.isna(value) or value is None:
        return None
    try:
        return Decimal(str(round(float(value), 4)))
    except (ValueError, TypeError):
        logging.warning(f"值转换Decimal失败: {value}")
        return None

# ---------------------- 基础DataFrame数据验证 ----------------------
def validate_dataframe(df, required_cols=None):
    """验证DataFrame的完整性，返回验证结果和提示信息"""
    if df is None or df.empty:
        return False, "数据为空"
    if required_cols:
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少必要列: {missing_cols}"
    if 'secucode' not in df.columns and '代码' not in df.columns:
        return False, "缺少代码列"
    return True, "数据验证通过"

# ---------------------- 财务数据专属全量校验（主程序核心调用） ----------------------
def validate_finance_data(df, numeric_cols, time_col='report_time'):
    """
    财务数据全链路校验：字段完整性+数值类型+时间有效性+空值过滤
    :param df: 原始财务DataFrame
    :param numeric_cols: 需要校验的数值字段列表
    :param time_col: 时间字段名，默认report_time
    :return: (bool, str, pd.DataFrame) 校验结果、提示信息、校验后的df
    """
    logger = logging.getLogger()
    # 1. 基础字段校验
    required_cols = numeric_cols + [time_col, 'secucode']
    is_valid, msg = validate_dataframe(df, required_cols)
    if not is_valid:
        return False, msg, pd.DataFrame()
    
    df_validate = df.copy()
    # 2. 时间字段校验+转换
    df_validate[time_col] = pd.to_datetime(df_validate[time_col], errors='coerce')
    time_na_count = df_validate[time_col].isna().sum()
    if time_na_count > 0:
        logger.warning(f"⚠️  时间字段[{time_col}]发现{time_na_count}条无效数据，已过滤")
        df_validate = df_validate.dropna(subset=[time_col])
    if df_validate.empty:
        return False, "时间字段过滤后无有效数据", pd.DataFrame()
    
    # 3. 数值字段校验+转换（过滤非数值/空值/无穷值）
    for col in numeric_cols:
        df_validate[col] = pd.to_numeric(df_validate[col], errors='coerce')
        num_na_count = df_validate[col].isna().sum()
        if num_na_count > 0:
            logger.warning(f"⚠️  数值字段[{col}]发现{num_na_count}条非数值/空数据，已过滤")
    # 过滤数值空值+无穷值
    df_validate = df_validate.dropna(subset=numeric_cols)
    df_validate = df_validate[np.isfinite(df_validate[numeric_cols]).all(axis=1)]
    inf_filter_count = len(df) - len(df_validate) - time_na_count - sum([df[col].isna().sum() for col in numeric_cols])
    if inf_filter_count > 0:
        logger.warning(f"⚠️  发现{inf_filter_count}条无穷值数值数据，已过滤")
    
    if df_validate.empty:
        return False, "数值字段过滤后无有效数据", pd.DataFrame()
    
    return True, "财务数据校验通过", df_validate

# ---------------------- TTM结果数据校验（过滤空值/异常值） ----------------------
def validate_ttm_data(df, ttm_cols=['ttm_netprofit', 'ttm_revenue']):
    """
    TTM计算结果校验：过滤空值+无穷值，保证数据库更新有效性
    :param df: TTM计算后的DataFrame
    :param ttm_cols: TTM结果字段列表
    :return: pd.DataFrame 校验后的df
    """
    logger = logging.getLogger()
    if df is None or df.empty:
        return pd.DataFrame()
    df_validate = df.copy()
    # 过滤空值
    df_validate = df_validate.dropna(subset=ttm_cols)
    # 过滤无穷值
    df_validate = df_validate[np.isfinite(df_validate[ttm_cols]).all(axis=1)]
    filter_count = len(df) - len(df_validate)
    if filter_count > 0:
        logger.warning(f"⚠️  TTM结果发现{filter_count}条空值/无穷值数据，已过滤")
    return df_validate

# ---------------------- 日期格式化工具 ----------------------
def format_date(date_val):
    """统一日期格式化，适配DF/字符串/时间戳类型"""
    if pd.isna(date_val):
        return None
    if isinstance(date_val, (datetime, pd.Timestamp)):
        return date_val.date()
    elif isinstance(date_val, str):
        date_val = date_val.strip()
        if date_val:
            return datetime.strptime(date_val, "%Y-%m-%d").date()
    return None

# ---------------------- 股票上市日期获取 ----------------------
def get_stock_listing_date(symbol):
    """获取股票上市日期（原逻辑保留，日志升级）"""
    logger = logging.getLogger()
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

# #!/usr/bin/python3
# # coding=utf-8
# """
# 通用工具模块 - 日志、数据转换、验证、日期处理等通用操作
# """
# import logging
# import os
# import akshare as ak
# import pandas as pd
# from datetime import datetime
# from decimal import Decimal

# # ---------------------- 日志初始化（分级配置，文件+控制台双输出） ----------------------
# def init_logger(LOG_CONFIG):
#     """初始化日志器，全局唯一"""
#     logger = logging.getLogger()
#     logger.setLevel(LOG_CONFIG["level"])
#     logger.handlers.clear()  # 清除默认处理器，防止重复输出
#     # 1. 文件日志处理器
#     if not os.path.exists(os.path.dirname(LOG_CONFIG["file_path"])):
#         os.makedirs(os.path.dirname(LOG_CONFIG["file_path"]))
#     file_handler = logging.FileHandler(LOG_CONFIG["file_path"], mode=LOG_CONFIG["file_mode"], encoding="utf-8")
#     file_handler.setFormatter(logging.Formatter(LOG_CONFIG["format"], datefmt=LOG_CONFIG["datefmt"]))
#     logger.addHandler(file_handler)
#     # 2. 控制台日志处理器
#     console_handler = logging.StreamHandler()
#     console_handler.setFormatter(logging.Formatter(LOG_CONFIG["console_format"], datefmt=LOG_CONFIG["datefmt"]))
#     logger.addHandler(console_handler)
#     return logger

# # ---------------------- 数据类型转换 ----------------------
# def to_decimal(value):
#     """将值转换为Decimal，保留4位小数（适配DF空值/异常值）"""
#     if pd.isna(value) or value is None:
#         return None
#     try:
#         return Decimal(str(round(float(value), 4)))
#     except (ValueError, TypeError):
#         logging.warning(f"值转换Decimal失败: {value}")
#         return None

# # ---------------------- DataFrame数据验证 ----------------------
# def validate_dataframe(df, required_cols=None):
#     """验证DataFrame的完整性，返回验证结果和提示信息"""
#     if df is None or df.empty:
#         return False, "数据为空"
#     if required_cols:
#         missing_cols = [col for col in required_cols if col not in df.columns]
#         if missing_cols:
#             return False, f"缺少必要列: {missing_cols}"
#     if 'secucode' not in df.columns and '代码' not in df.columns:
#         return False, "缺少代码列"
#     return True, "数据验证通过"

# # ---------------------- 日期格式化工具 ----------------------
# def format_date(date_val):
#     """统一日期格式化，适配DF/字符串/时间戳类型"""
#     if pd.isna(date_val):
#         return None
#     if isinstance(date_val, (datetime, pd.Timestamp)):
#         return date_val.date()
#     elif isinstance(date_val, str):
#         date_val = date_val.strip()
#         if date_val:
#             return datetime.strptime(date_val, "%Y-%m-%d").date()
#     return None

# # ---------------------- 股票上市日期获取 ----------------------
# def get_stock_listing_date(symbol):
#     """获取股票上市日期（原逻辑保留，日志升级）"""
#     logger = logging.getLogger()
#     try:
#         stock_info = ak.stock_individual_info_em(symbol=symbol)
#         # 方法1：查找上市时间字段
#         for _, row in stock_info.iterrows():
#             item_str = str(row['item']).strip()
#             if '上市时间' in item_str or 'listing_date' in item_str.lower():
#                 date_str = str(row['value']).strip()
#                 if date_str and len(date_str) >= 8:
#                     digits = ''.join(filter(str.isdigit, date_str))
#                     if len(digits) >= 8:
#                         return digits[:8]
#         # 方法2：备用字段提取
#         if 'date' in stock_info.columns:
#             date_col = stock_info[stock_info['item'].str.contains('时间|date', case=False, na=False)]
#             if not date_col.empty:
#                 date_str = str(date_col.iloc[0]['value'])
#                 digits = ''.join(filter(str.isdigit, date_str))
#                 if len(digits) >= 8:
#                     return digits[:8]
#         logger.warning(f"⚠️  {symbol}未提取到上市日期")
#         return None
#     except Exception as e:
#         logger.error(f"❌ 获取{symbol}上市日期失败: {str(e)}", exc_info=True)
#         return None