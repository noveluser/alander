#!/usr/bin/python3
# coding=utf-8
"""
主程序 - 仅业务调度：数据输入→TTM计算→数据输出
核心：无DB代码、无校验代码，全通过通用模块调用，保持极致干净
author:wangle
version:1.0
"""

import os
import pandas as pd
from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG
# 导入通用模块，无自定义DB/校验逻辑
from utils import init_logger
from db_operation import create_db_engine, safe_db_operation, batch_update

# ---------------------- 全局初始化（仅日志+DB引擎，通用操作） ----------------------
logger = init_logger(LOG_CONFIG)
create_db_engine(DB_URL, DB_CONFIG)

def _add_exchange_suffix(code: str) -> str:
    """根据股票代码首位数字自动添加交易所后缀"""
    if len(code) == 6:
        if code.startswith(("0", "3")):
            return f"{code}.SZ"
        elif code.startswith("6"):
            return f"{code}.SH"
        elif code.startswith(("4", "8", "9")):
            return f"{code}.BJ"
    return code


def getdata() -> pd.DataFrame:
    """
    从Excel文件读取行业分类数据，处理后返回标准化DataFrame。

    Returns:
        pd.DataFrame: 包含 secucode（证券代码）、industry（行业分类）两列；
                      失败时返回空 DataFrame。
    """
    CFG = BUSINESS_CONFIG
    file_path  = CFG.get("industry_file_path")
    col_code   = CFG.get("industry_col_code",  "证券代码")
    col_name   = CFG.get("industry_col_name",  "中证二级行业分类简称")
    required_cols = [col_code, col_name]

    # 1. 文件存在性校验
    if not os.path.exists(file_path):
        logger.error(f"行业分类文件不存在: {file_path}")
        return pd.DataFrame()

    try:
        # 2. 读取 Excel
        logger.info(f"正在读取行业分类文件: {file_path}")
        df = pd.read_excel(file_path, usecols=lambda c: c in required_cols)

        # 3. 列完整性校验
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.warning(f"Excel 缺少必要列: {missing}")
            return pd.DataFrame()

        # 4. 只保留所需列
        df = df[required_cols].copy()

        # 5. 截取前6位代码并补交易所后缀
        df[col_code] = df[col_code].astype(str).str.slice(0, 6).apply(_add_exchange_suffix)

        # 6. 列名标准化：中文 → 英文
        df.rename(columns={col_code: "secucode", col_name: "industry"}, inplace=True)

        # 7. 去除空值行
        df.dropna(subset=["secucode", "industry"], inplace=True)

        if df.empty:
            logger.warning("行业分类数据处理后为空，请检查源文件")
            return pd.DataFrame()

        logger.info(f"行业分类数据读取完成，共 {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"读取行业分类数据失败: {e}", exc_info=True)
        return pd.DataFrame()


# ---------------------- 主流程：仅业务调度，无任何通用逻辑 ----------------------
def main():
    """主流程：批量读取→单股数据校验→TTM计算→批量更新DB→状态更新"""
    # 从配置读取常量，无硬编码
    CFG = BUSINESS_CONFIG
    DB_RETRY_TIMES = CFG.get("db_retry_times", 2)

    # 数据配置（与业务强相关，仅此处定义）
    UPDATE_SQL = """
        UPDATE stock_list
        SET industry = :industry 
        WHERE secucode = :secucode
    """

    df = getdata()

        # 3. 批量更新数据库（调用db_operation通用方法）
    if df is not None:
        # 安全批量更新
        affected_rows = safe_db_operation(
            batch_update, UPDATE_SQL, df,
            retry_times=DB_RETRY_TIMES
        )
        # 批量更新失败则逐行重试
        if affected_rows == 0:
            logger.warning(f"⚠️ 批量更新失败")


# ---------------------- 程序入口 ----------------------
if __name__ == "__main__":
    main()
