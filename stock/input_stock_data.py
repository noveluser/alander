#!/usr/bin/python3
# coding=utf-8


"""
财务数据导入脚本
从Excel文件读取财务数据并导入到MySQL数据库
作者: wangle
版本: v2.2
"""

import pandas as pd
import pymysql
from pymysql import Error
import numpy as np
import logging
import os
from datetime import datetime
# ====================== 修改1：把Decimal导入提到顶部（原在循环内） ======================
from decimal import Decimal


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/data/package/crontab/log/firstscanbags.log',
                    filename='c://work//log//1.log',
                    filemode='a')

# 添加控制台输出
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def get_db_connection():
    """创建数据库连接"""
    try:
        connection = pymysql.connect(
            # host='10.31.9.24',
            # port=3306,
            # user='it',
            # password='1111111',
            # database='ics',
            host='192.168.87.128',
            port=3306,
            user='root',
            password='example',
            database='stock',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Error as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise


# ====================== 修改2：抽取独立函数 —— 数值转Decimal ======================
def to_decimal(value):
    """将值转换为Decimal，保留4位小数"""
    if pd.isna(value):
        return None
    try:
        return Decimal(str(round(float(value), 4)))
    except (ValueError, TypeError):
        return None


# ====================== 修改3：抽取独立函数 —— 单行数据格式化 ======================
def format_row_data(row):
    """格式化单行数据，处理日期、类型转换"""
    # 处理日期格式
    report_date = row.get("REPORT_DATE")
    if pd.isna(report_date):
        raise ValueError("缺少报告日期")
    
    if isinstance(report_date, str):
        report_date_obj = datetime.strptime(report_date.strip().split()[0], '%Y-%m-%d').date()
    else:
        # 兼容Excel日期类型
        report_date_obj = pd.to_datetime(report_date).date()

    # 构造数据元组
    row_data = (
        report_date_obj,
        str(row.get("SECUCODE")),
        str(row.get("SECURITY_NAME_ABBR")),
        to_decimal(row.get("FIXED_ASSET")),
        to_decimal(row.get("TOTAL_ASSETS")),
        to_decimal(row.get("TOTAL_LIABILITIES")),
        to_decimal(row.get("SHARE_CAPITAL")),
        to_decimal(row.get("TOTAL_EQUITY")),
        to_decimal(row.get("OPERATE_INCOME")),
        to_decimal(row.get("DEDUCT_PARENT_NETPROFIT")),
        to_decimal(row.get("NETCASH_OPERATE")),
        to_decimal(row.get("CONSTRUCT_LONG_ASSET"))
    )
    return row_data


# ====================== 修改4：抽取独立函数 —— 批量插入执行 ======================
def execute_batch_insert(connection, cursor, batch_data):
    """执行批量插入SQL，返回影响行数"""
    sql = """
    INSERT INTO sort_finance 
    (report_time, secucode, name, fixed_asset, total_assets, 
     total_liabilities, share_capital, total_equity, operate_income, 
     deduct_parent_netprofit, netcash_operate, construct_long_asset) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    fixed_asset = VALUES(fixed_asset),
    total_assets = VALUES(total_assets),
    total_liabilities = VALUES(total_liabilities),
    share_capital = VALUES(share_capital),
    total_equity = VALUES(total_equity),
    operate_income = VALUES(operate_income),
    deduct_parent_netprofit = VALUES(deduct_parent_netprofit),
    netcash_operate = VALUES(netcash_operate),
    construct_long_asset = VALUES(construct_long_asset)
    """
    cursor.executemany(sql, batch_data)
    connection.commit()
    return cursor.rowcount


# ====================== 修改5：抽取独立函数 —— 逐行重试插入 ======================
def retry_insert_row_by_row(connection, cursor, batch_data):
    """批量插入失败后，逐行重试插入"""
    sql = """
    INSERT INTO sort_finance 
    (report_time, secucode, name, fixed_asset, total_assets, 
     total_liabilities, share_capital, total_equity, operate_income, 
     deduct_parent_netprofit, netcash_operate, construct_long_asset) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    fixed_asset = VALUES(fixed_asset),
    total_assets = VALUES(total_assets),
    total_liabilities = VALUES(total_liabilities),
    share_capital = VALUES(share_capital),
    total_equity = VALUES(total_equity),
    operate_income = VALUES(operate_income),
    deduct_parent_netprofit = VALUES(deduct_parent_netprofit),
    netcash_operate = VALUES(netcash_operate),
    construct_long_asset = VALUES(construct_long_asset)
    """
    success_count = 0
    inserted = 0
    updated = 0
    
    for i, row_data in enumerate(batch_data):
        try:
            result = cursor.execute(sql, row_data)
            connection.commit()
            success_count += 1
            if result == 1:
                inserted += 1
            elif result == 2:
                updated += 1
        except pymysql.Error as single_error:
            connection.rollback()
            logging.error(f"第{i+1}行插入/更新失败: {str(single_error)}")
    
    return success_count, inserted, updated


# ====================== 修改6：瘦身之后的核心函数（只负责调度） ======================
def batch_insert_optimized(stock_code, data):
    """
    优化后的批量插入方法 - 职责单一，只做流程调度
    数据转换、SQL执行、失败重试均由独立函数完成
    """
    connection = None
    cursor = None
    error_count = 0
    batch_data = []

    try:
        if data.empty:
            logging.warning(f" {stock_code} 没有可插入的数据")
            return 0, 0, 0
        
        connection = get_db_connection()
        cursor = connection.cursor()

        # ========== 只调用：数据格式化 ==========
        for index, row in data.iterrows():
            try:
                row_data = format_row_data(row)
                batch_data.append(row_data)
            except Exception as row_error:
                logging.error(f"处理第{index+1}行数据时出错: {str(row_error)}")
                error_count += 1
                continue
        
        if not batch_data:
            logging.warning("没有有效数据可插入")
            return 0, 0, 0

        # ========== 只调用：批量执行 ==========
        try:
            total_affected = execute_batch_insert(connection, cursor, batch_data)
            total_rows = len(batch_data)
            
            # 统计插入/更新
            if total_affected > total_rows:
                updated_count = total_affected - total_rows
                inserted_count = total_rows - updated_count
            else:
                inserted_count = total_rows
                updated_count = 0
            
            logging.info(f"批量插入/更新完成，处理 {total_rows} 条记录，插入 {inserted_count} 条，更新 {updated_count} 条，失败 {error_count} 行")
            result = (total_affected, inserted_count, updated_count)  # <-- 修改：保存结果
            final_success = True  # 标记主路径成功  # <-- 修改：设置成功标志
            # return total_affected, inserted_count, updated_count
                   
        # ========== 只调用：逐行重试 ==========
        except pymysql.Error as db_error:
            connection.rollback()
            logging.error(f"批量插入数据库失败: {str(db_error)}")
            logging.info("尝试逐行插入...")
            
            success_count, inserted, updated = retry_insert_row_by_row(connection, cursor, batch_data)
            logging.info(f"逐行处理完成，成功 {success_count} 行（插入 {inserted}，更新 {updated}）")
            result = (success_count, inserted, updated)  # <-- 修改：保存结果
            final_success = (success_count > 0)  # 如果逐行重试至少成功了一行，可以认为是部分成功。根据业务逻辑调整。  # <-- 修改：设置成功标志
            # return success_count, inserted, updated
        # finally:
        #     logging.info(f"{stock_code} 财务数据写入成功")
        #     download_sucess_flag(stock_code)
        
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"批量插入失败: {str(e)}", exc_info=True)
        result = (0, 0, 0)  # <-- 修改：保存结果
        final_success = False  # <-- 修改：设置失败标志
        
    finally:
        # 无论成功失败，都尝试关闭游标和连接
        try:  # <-- 修改：新增资源释放的try块
            if cursor:
                cursor.close()
        except Exception as e:
            logging.error(f"关闭游标时出错: {e}")
        try:  # <-- 修改：新增资源释放的try块
            if connection:
                connection.close()
        except Exception as e:
            logging.error(f"关闭数据库连接时出错: {e}")

        # 只有最终成功，才执行成功后的操作
        if final_success:  # <-- 修改：新增条件判断
            logging.info(f"{stock_code} 财务数据写入成功")
            download_sucess_flag(stock_code)
        else:  # <-- 修改：新增失败处理
            logging.warning(f"{stock_code} 财务数据写入未完成或完全失败。")

    return result  # <-- 修改：返回结果变量


def inputdata(stock_code, data):
    """
    兼容性函数，调用批量插入
    
    Args:
        stock_code: 代码
        data: pandas DataFrame，包含财务数据
        
    Returns:
        int: 成功插入的记录数
    """
    # 调用批量插入/更新函数，但只返回总处理记录数
    total_affected, _, _ = batch_insert_optimized(stock_code, data)
    return total_affected


def download_sucess_flag(stock_code):
    """下载成功后写入标记"""
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        sql = "update stock_list set flag = 'N' where secucode = %s ;"
        cursor.execute(sql,(stock_code,))

        # 提交事务       
        connection.commit()

        # 检查是否更新了记录
        if cursor.rowcount > 0:
            logging.info(f"股票代码 {stock_code} 已成功更新标志位为N")
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


def getdata(stock_code):
    """
    从Excel文件读取财务数据并合并
    
    Args:
        stock_code: 代码（如"SZ300206"）
    
    Returns:
        pandas DataFrame: 合并后的财务数据
    """
    try:
        # 构建文件路径
        base_path = rf"d:/stock/finance_list/{stock_code}/"

        # 1. 下载资产负债表
        balance_path = os.path.join(base_path, f"balance.xlsx")
        if not os.path.exists(balance_path):
            logging.error(f"资产负债表文件不存在: {balance_path}")
            return pd.DataFrame()
        
        print("正在读取：资产负债表")
        balance = pd.read_excel(balance_path)
        # 检查必要的列是否存在
        required_balance_cols = ["REPORT_DATE", "SECUCODE", "SECURITY_NAME_ABBR", "FIXED_ASSET", "TOTAL_ASSETS", 
                                "TOTAL_LIABILITIES", "SHARE_CAPITAL", "TOTAL_EQUITY"]
        balance = balance[required_balance_cols].copy() if all(col in balance.columns for col in required_balance_cols) else pd.DataFrame()

        # 2. 下载利润表
        income_path = os.path.join(base_path, f"income.xlsx")
        if not os.path.exists(income_path):
            logging.error(f"利润表文件不存在: {income_path}")
            return pd.DataFrame()
        
        print("正在读取：利润表")
        income = pd.read_excel(income_path)
        required_income_cols = ["REPORT_DATE", "SECUCODE", "OPERATE_INCOME", "DEDUCT_PARENT_NETPROFIT"]
        income = income[required_income_cols].copy() if all(col in income.columns for col in required_income_cols) else pd.DataFrame()

        # 3. 下载现金流量表
        cash_path = os.path.join(base_path, f"cash.xlsx")
        if not os.path.exists(cash_path):
            logging.error(f"现金流量表文件不存在: {cash_path}")
            return pd.DataFrame()
        
        print("正在读取：现金流量表")
        cash = pd.read_excel(cash_path)
        required_cash_cols = ["REPORT_DATE", "SECUCODE", "NETCASH_OPERATE", "CONSTRUCT_LONG_ASSET"]
        cash = cash[required_cash_cols].copy() if all(col in cash.columns for col in required_cash_cols) else pd.DataFrame()

        # 检查数据是否为空
        if balance.empty or income.empty or cash.empty:
            logging.warning("部分数据表为空，无法合并")
            return pd.DataFrame()

        print("正在合并数据...")
        merged_df = pd.merge(balance, income, on=["REPORT_DATE", "SECUCODE"], how="inner")
        merged_df = pd.merge(merged_df, cash, on=["REPORT_DATE", "SECUCODE"], how="inner")  
        return(merged_df)    

    except Exception as e:
        logging.error(f"读取数据失败: {str(e)}")
        return pd.DataFrame()  


def get_all_stock_codes():
    """从stock_list表获取所有股票代码"""
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 提取所有flag标记为Y的代码，意味着财报已经下载完成
        sql = "SELECT secucode FROM stock_list where flag = 'Y';"
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


def main():
    """主函数"""
    stock_codes = get_all_stock_codes()
    for stock_code in stock_codes:
    
        try:
            logging.info(f"开始处理 {stock_code}")
            
            # 获取数据
            data = getdata(stock_code)
            
            if data.empty:
                logging.error(f"{stock_code} 没有获取到有效数据")
                return
            
            # 批量插入数据
            inserted_count = inputdata(stock_code, data)  # 这里返回的是整数
            
            if inserted_count > 0:
                logging.info(f" {stock_code} 数据处理完成，成功插入 {inserted_count} 条记录")
            else:
                logging.warning(f" {stock_code} 没有成功插入任何记录")
                
        except Exception as e:
            logging.error(f"处理 {stock_code} 时发生错误: {str(e)}", exc_info=True)


if __name__ == '__main__':
    main()
