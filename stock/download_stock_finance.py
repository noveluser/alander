#!/usr/bin/python3
# coding=utf-8

"""
随机选择股票并下载财务数据脚本
从stock_list表随机选择2个股票代码，下载财务数据到c:\stock目录
作者: wangle
版本: v1.0
"""

import pymysql
from pymysql import Error
import pandas as pd
import numpy as np
import logging
import os
import time
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 添加控制台输出
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def get_db_connection():
    """创建数据库连接"""
    try:
        connection = pymysql.connect(
            host='10.31.9.24',
            port=3306,
            user='it',
            password='1111111',
            database='ics',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Error as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise


def download_sucess_flag(stock_code):
    """下载成功后写入标记"""
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        sql = "update stock_list set flag = 'Y' where code = %s ;"
        cursor.execute(sql,(stock_code,))

        # 提交事务       
        connection.commit()

        # 检查是否更新了记录
        if cursor.rowcount > 0:
            logging.info(f"股票代码 {stock_code} 已成功更新标志位为Y")
            return True
        else:
            logging.warning(f"stock_list表中没有找到代码为 {stock_code} 的记录")
            return False

    except Exception as e:
        logging.error(f"更新股票代码 {stock_code} 标志位失败: {str(e)}")
        if connection:
            connection.rollback()
        return False

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_all_stock_codes():
    """从stock_list表获取所有股票代码"""
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        sql = "SELECT secucode FROM stock_list where flag = 'N' ;"
        cursor.execute(sql)
        results = cursor.fetchall()

        if not results:
            logging.warning("stock_list表中没有数据")
            return []

        # 提取secucode列表
        stock_codes = [row['secucode'] for row in results]
        logging.info(f"从数据库获取到 {len(stock_codes)} 个股票代码")

        return stock_codes

    except Exception as e:
        logging.error(f"获取股票代码失败: {str(e)}")
        return []

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def download_finance_data(stock_code, max_retries=3):
    """
    下载指定股票的财务数据（参考get_finance_report.py的方式）
    优先尝试从akshare下载，失败则尝试从本地文件读取

    Args:
        stock_code: 股票代码（如"SZ300206"）
        max_retries: 最大重试次数

    Returns:
        pandas DataFrame: 合并后的财务数据，失败返回空DataFrame
    """
    # 尝试方法1: 从akshare下载（网络方式）
    online_data = download_from_akshare(stock_code, max_retries)
    if not online_data.empty:
        return online_data

    logging.warning(f"{stock_code} 所有数据获取方式都失败")
    return pd.DataFrame()


def download_from_akshare(stock_code, max_retries=3):
    """从akshare下载财务数据（网络方式）"""
    #东财code需转换
    if stock_code.endswith('.SH'):
        modify_code = 'SH' + stock_code.replace('.SH', '')
    elif stock_code.endswith('.SZ'):
        modify_code = 'SZ' + stock_code.replace('.SZ', '')
    else:
        modify_code = 'BJ' + stock_code.replace('.BJ', '')

    try:
        import akshare as ak
    except ImportError:
        logging.warning("akshare库未安装，跳过网络下载方式")
        return pd.DataFrame()

    for attempt in range(max_retries):
        try:
            logging.info(f"尝试从akshare下载 {stock_code} 的财务数据 (第{attempt+1}次尝试)")

            # 1. 下载资产负债表
            print(f"正在从akshare下载 {stock_code} 资产负债表...")
            try:
                balance = ak.stock_balance_sheet_by_report_em(symbol=modify_code)
                save_to_excel(balance, stock_code, filetype='balance')
            except Exception as e:
                logging.error(f"下载资产负债表失败: {str(e)}")
                balance = pd.DataFrame()

            time.sleep(1)

            # 2. 下载利润表
            print(f"正在从akshare下载 {stock_code} 利润表...")
            try:
                income = ak.stock_profit_sheet_by_report_em(symbol=modify_code)
                save_to_excel(income, stock_code, filetype='income')
            except Exception as e:
                logging.error(f"下载利润表失败: {str(e)}")
                income = pd.DataFrame()

            time.sleep(1)

            # 3. 下载现金流量表
            print(f"正在从akshare下载 {stock_code} 现金流量表...")
            try:
                cash = ak.stock_cash_flow_sheet_by_report_em(symbol=modify_code)
                save_to_excel(cash, stock_code, filetype='cash')
            except Exception as e:
                logging.error(f"下载现金流量表失败: {str(e)}")
                cash = pd.DataFrame()

            # 检查数据是否为空
            if balance.empty or income.empty or cash.empty:
                logging.warning(f"{stock_code} 部分数据表为空")
                if attempt < max_retries - 1:
                    time.sleep(2)  # 等待后重试
                    continue
                return pd.DataFrame()
            
            # ========== 修改点3：修复【永远返回空】，成功时返回非空数据 ==========
            # 原来这里没有return，导致无论成功失败都返回空
            logging.info(f"{stock_code} 财务数据下载成功")
            download_sucess_flag(stock_code)
            return balance  # 返回任意一个非空表即可，表示成功

        except Exception as e:
            logging.error(f"从akshare下载 {stock_code} 失败 (尝试{attempt+1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)  # 等待后重试
                continue

    # ========== 修改点2：修复【重试逻辑失效】，重试完所有次数才返回空 ==========
    # 原来重试结束后没有明确返回，现在明确返回空
    logging.error(f"{stock_code} 多次重试后下载失败")
    return pd.DataFrame()


def save_to_excel(data, stock_code, filetype,output_dir="d:\\stock"):
    """
    保存财务数据到Excel文件

    Args:
        data: pandas DataFrame 数据
        stock_code: 股票代码
        output_dir: 输出目录
    """
    try:
        # 拼接 股票代码 子文件夹路径
        stock_dir = os.path.join(output_dir, stock_code)
        
        # 自动创建目录（不存在则新建，已存在则忽略）
        os.makedirs(stock_dir, exist_ok=True)

        # 生成文件名
        filename = f"{filetype}.xlsx"

        # 最终文件路径：根目录\股票代码\文件名.xlsx
        filepath = os.path.join(stock_dir, filename)

        # 保存到Excel
        data.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"{stock_code} 财务数据已保存到: {filepath}")

        return filepath

    except Exception as e:
        logging.error(f"保存 {stock_code} 数据到Excel失败: {str(e)}")
        return None


def main():
    """主函数"""
    logging.info("开始下载财务数据")

    # 1. 从数据库获取所有股票代码
    stock_codes = get_all_stock_codes()

    if not stock_codes:
        logging.error("无法获取股票代码列表，程序退出")
        return

    # 只取前2个测试
    stock_codes = stock_codes[2:4]

    # 2. 定义需要下载的财务报表类型（这里统一管理，新增只需加一行）
    finance_types = [
        ("balance", "资产数据"),
        ("profit", "利润数据"),
        ("cash", "现金数据")
    ]

    success_count = 0

    # 3. 为每个选中的股票下载财务数据
    success_count = 0
    for stock_code in stock_codes:
        try:
            # 👉 这里只调用一次，内部已经下载3张表：balance / income / cash
            finance_data = download_from_akshare(stock_code)

            if finance_data.empty:
                logging.warning(f"{stock_code} 没有下载到有效数据")
                continue

            logging.info(f"{stock_code} 处理完成")
            success_count += 1

        except Exception as e:
            logging.error(f"处理 {stock_code} 时发生错误: {str(e)}")

    # 4. 输出总结
    logging.info(f"处理完成！成功下载 {success_count} 个股票的财务数据")
    logging.info(f"数据保存在: d:\\stock\\{stock_code} 目录下")


if __name__ == '__main__':
    main()