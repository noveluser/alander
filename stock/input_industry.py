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
from db_operation import create_db_engine, safe_db_operation,batch_update, retry_row_update

# ---------------------- 全局初始化（仅日志+DB引擎，通用操作） ----------------------
logger = init_logger(LOG_CONFIG)
create_db_engine(DB_URL, DB_CONFIG)

def getdata():
    """
    从Excel文件读取财务数据并合并，输出secucode和industry列
    
    Returns:
        pandas DataFrame: 包含secucode（证券代码）、industry（行业分类）的财务数据
    """
    try:
        # 构建文件路径
        base_path = rf"d:/1/1/"

        # 1. 下载资产负债表
        balance_path = os.path.join(base_path, f"行业分类.xlsx")
        if not os.path.exists(balance_path):
            logger.error(f"文件不存在: {balance_path}")
            return pd.DataFrame()
        
        print("正在读取：资产负债表")
        balance = pd.read_excel(balance_path)
        # 检查必要的列是否存在（原中文名）
        required_balance_cols = ["证券代码", "中证二级行业分类简称"]
        # 校验列存在性，不存在则返回空DF
        if not all(col in balance.columns for col in required_balance_cols):
            logger.warning("Excel文件缺少必要列：证券代码 或 中证二级行业分类简称")
            return pd.DataFrame()
        
        # 只保留需要的列并复制
        balance = balance[required_balance_cols].copy()

        # ====================== 改造开始 ======================       
        # 2. 只保留6位代码
        balance["证券代码"] = balance["证券代码"].str.slice(0, 6)
        
        # 3. 自动加后缀：0开头/3开头 → .SZ；6开头 → .SH
        def add_suffix(code):
            if len(code) == 6:
                if code.startswith(("0", "3")):
                    return f"{code}.SZ"
                elif code.startswith("6"):
                    return f"{code}.SH"
                elif code.startswith(('4', '8', '9')):
                    return f"{code}.BJ"
            return code  # 不符合则返回原码        
        balance["证券代码"] = balance["证券代码"].apply(add_suffix)
        
        # ====================== 列名替换 ======================
        # 将中文列名替换为英文：证券代码→secucode，中证二级行业分类简称→industry
        balance.rename(columns={
            "证券代码": "secucode",
            "中证二级行业分类简称": "industry"
        }, inplace=True)

        # 检查数据是否为空
        if balance.empty:
            logger.warning("处理后的数据表为空")
            return pd.DataFrame()
        
        return balance
        
    except Exception as e:
        logger.error(f"读取数据失败: {str(e)}")
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
