#!/usr/bin/python3
# coding=utf-8

"""
财务数据导入脚本
从Excel文件读取财务数据并导入到MySQL数据库
作者: wangle
版本: v2.1 修正版（自动加市场后缀.SZ/.SH/.BJ + 修复SQL逻辑错误）
"""

import pandas as pd
import pymysql
from pymysql import Error
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//1.log',
    filemode='a'
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

def format_row_data(row):
    """格式化单行数据，处理日期、类型转换 + 股票代码加市场后缀"""
    # 获取原始股票代码（已确保是字符串）
    code = str(row.get("code", "")).strip()

    # ====================== 自动加后缀 ======================
    if code.startswith(('0', '3')):
        secucode = code + ".SZ"   # 深市
    elif code.startswith('6'):
        secucode = code + ".SH"   # 沪市
    elif code.startswith(('4', '8', '9')):
        secucode = code + ".BJ"   # 北交所
    else:
        secucode = code           # 未知市场，保持原样

    # 构造数据元组（对应数据库字段 secucode, name）
    row_data = (
        secucode,
        str(row.get("name", "")).strip()
    )
    return row_data

def execute_batch_insert(connection, cursor, batch_data):
    """执行批量插入SQL，返回影响行数"""
    sql = """
    INSERT INTO stock_list 
    (secucode, name) 
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
    name = VALUES(name)  -- ✅ 修复：只更新名称，不修改股票代码！
    """
    cursor.executemany(sql, batch_data)
    connection.commit()
    return cursor.rowcount

def batch_insert_optimized(data):
    """
    优化后的批量插入方法
    """
    connection = None
    cursor = None
    error_count = 0
    batch_data = []

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

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

        total_affected = execute_batch_insert(connection, cursor, batch_data)
        total_rows = len(batch_data)
        
        if total_affected > total_rows:
            updated_count = total_affected - total_rows
            inserted_count = total_rows - updated_count
        else:
            inserted_count = total_rows
            updated_count = 0
        
        logging.info(f"批量插入/更新完成，处理 {total_rows} 条记录，插入 {inserted_count} 条，更新 {updated_count} 条，失败 {error_count} 行")
        return total_affected, inserted_count, updated_count
        
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"批量插入失败: {str(e)}", exc_info=True)
        return 0, 0, 0
        
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as close_error:
                logging.warning(f"关闭游标时出错: {str(close_error)}")
        if connection:
            try:
                connection.close()
            except Exception as close_error:
                logging.warning(f"关闭数据库连接时出错: {str(close_error)}")

def inputdata(data):
    total_affected, _, _ = batch_insert_optimized(data)
    return total_affected

def getdata():
    """从Excel文件读取股票数据"""
    try:
        base_path = "d:\\1\\1\\"
        balance_path = os.path.join(base_path, "A股.xlsx")
        
        if not os.path.exists(balance_path):
            logging.error(f"文件不存在: {balance_path}")
            return pd.DataFrame()
        
        print("正在读取")
        # ✅ 强制code列为字符串，避免000001变成1
        df = pd.read_excel(balance_path, dtype={"code": str})  
        return df    

    except Exception as e:
        logging.error(f"读取数据失败: {str(e)}")
        return pd.DataFrame()      


def main():
    """主函数"""
    try:
        logging.info("开始处理")
        
        data = getdata()
        
        if data.empty:
            logging.error("没有获取到有效数据")
            return
        
        inserted_count = inputdata(data)
        
        if inserted_count > 0:
            logging.info(f"数据处理完成，成功插入 {inserted_count} 条记录")
        else:
            logging.warning("没有成功插入任何记录")
            
    except Exception as e:
        logging.error(f"处理时发生错误: {str(e)}", exc_info=True)

if __name__ == '__main__':
    main()