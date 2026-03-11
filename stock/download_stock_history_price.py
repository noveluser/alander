#!/usr/bin/python3
# coding=utf-8

"""
股票历史数据下载工具
从akshare获取股票历史行情数据
author:wangle
version:2.0
"""

import akshare as ak
import pandas as pd
import logging
from datetime import datetime
import time
import random
import os
import pymysql
from pymysql import Error
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
            host='10.31.9.24',
            port=3306,
            user='it',
            password='1111111',
            database='ics',
            # host='192.168.87.128',
            # port=3306,
            # user='root',
            # password='example',
            # database='stock',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Error as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise


def to_decimal(value):
    """将值转换为Decimal，保留4位小数"""
    if pd.isna(value):
        return None
    try:
        return Decimal(str(round(float(value), 4)))
    except (ValueError, TypeError):
        return None


def format_row_data(row):
    """格式化单行数据，处理日期、类型转换"""
    # # ====================== 自动加后缀 ======================
    # if row.get("股票代码").startswith(('0', '3')):
    #     secucode = row.get("股票代码") + ".SZ"   # 深市
    # elif row.get("股票代码").startswith('6'):
    #     secucode = row.get("股票代码") + ".SH"   # 沪市
    # elif row.get("股票代码").startswith(('4', '8', '9')):
    #     secucode = row.get("股票代码") + ".BJ"   # 北交所
    # else:
    #     secucode = row.get("股票代码")           # 未知市场，保持原样


    #  转换str成sql的date格式
    date_str = row.get("日期", "").strip()  # 空值默认给空字符串
    sql_date = None

    if date_str:  # 只有字符串非空才转换
        sql_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    # 构造数据元组
    row_data = (
        sql_date,
        row.get("股票代码"),
        to_decimal(row.get("close")),
        to_decimal(row.get("amount"))
    )
    return row_data


def get_all_stock_codes():
    """从stock_list表获取所有股票代码"""
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 提取所有flag标记为Y的代码，意味着财报已经下载完成
        sql = "SELECT secucode FROM stock_list where flag = 'N';"
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


def get_stock_listing_date(symbol):
    """获取股票上市日期，增强解析逻辑"""
    try:
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        
        # 方法1：从DataFrame中查找上市时间
        for idx, row in stock_info.iterrows():
            item_str = str(row['item']).strip()
            if '上市时间' in item_str or 'listing_date' in item_str.lower():
                date_str = str(row['value']).strip()
                if date_str and len(date_str) >= 8:
                    # 提取数字部分
                    digits = ''.join(filter(str.isdigit, date_str))
                    if len(digits) >= 8:
                        return digits[:8]  # 返回YYYYMMDD格式
        
        # 方法2：尝试从其他字段获取
        if 'date' in stock_info.columns:
            date_col = stock_info[stock_info['item'].str.contains('时间|date', case=False, na=False)]
            if not date_col.empty:
                date_str = str(date_col.iloc[0]['value'])
                digits = ''.join(filter(str.isdigit, date_str))
                if len(digits) >= 8:
                    return digits[:8]
        
        return None
    except Exception as e:
        print(f"获取股票{symbol}上市日期失败: {e}")
        return None

def validate_dataframe(df, required_cols=None):
    """验证DataFrame的完整性"""
    if df is None or df.empty:
        return False, "数据为空"
    
    if required_cols:
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少必要列: {missing_cols}"
    
    # 检查日期列
    if '日期' not in df.columns:
        return False, "缺少日期列"
    
    # 检查数据行数
    if len(df) == 0:
        return False, "数据行数为0"
    
    return True, "数据验证通过"

def get_stock_history(symbol, start_date=None, end_date=None, adjust=""):
    """
    获取股票历史数据
    """
    # 去除股票代码后缀
    stock_code_clean = symbol.split('.')[0]

    # 设置日期范围
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    if not start_date:
        # 获取上市日期或使用默认
        listing_date = get_stock_listing_date(stock_code_clean)
        if listing_date and listing_date > "20000101":
            start_date = listing_date
        else:
            start_date = "20000101"
    
    print(f"获取股票 {symbol} 数据: {start_date} 至 {end_date}")
    
    # 重试机制
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_zh_a_hist(
                symbol=stock_code_clean,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            # 数据验证
            is_valid, msg = validate_dataframe(df, ['日期', '开盘', '收盘', '成交量'])
            if not is_valid:
                print(f"数据验证失败: {msg}")
                return None
            
            # 数据处理
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            df['股票代码'] = symbol
            
            # 重命名列
            column_mapping = {
                '开盘': 'open',
                '收盘': 'close', 
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change',
                '换手率': 'turnover'
            }
            
            for cn_name, en_name in column_mapping.items():
                if cn_name in df.columns:
                    df[en_name] = df[cn_name]
            
            # 选择并重排序列
            base_cols = ['日期', '股票代码', 'close', 'volume']
            available_cols = [col for col in base_cols if col in df.columns]
            
            # 添加其他可用列
            other_cols = [col for col in df.columns if col not in available_cols and col not in column_mapping.keys()]
            selected_cols = available_cols + other_cols
            
            result_df = df[selected_cols].copy()
            result_df = result_df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            print(f"成功获取 {len(result_df)} 条记录")
            return result_df
            
        except Exception as e:
            print(f"第{attempt+1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                wait_time = random.uniform(1, 3)
                time.sleep(wait_time)
            else:
                print(f"获取失败，已重试{max_retries}次")
                return None
            
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


def download_sucess_flag(code):
    """下载成功后写入标记"""
    connection = None
    cursor = None
    

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        sql = "update stock_list set flag = 'Y' where secucode = %s ;"
        cursor.execute(sql,(code,))

        # 提交事务       
        connection.commit()

        # 检查是否更新了记录
        if cursor.rowcount > 0:
            logging.info(f"股票代码 {code} 已成功更新标志位为Y")
            return True
        else:
            logging.warning(f"stock_list表中未能更新代码为 {code} 的记录，")
            return False

    except Exception as e:
        logging.error(f"更新股票代码 {code} 标志位失败: {str(e)}")
        if connection:
            connection.rollback()
        return False

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# ====================== 修改4：抽取独立函数 —— 批量插入执行 ======================
def execute_batch_insert(connection, cursor, batch_data):
    """执行批量插入SQL，返回影响行数"""
    sql = """
        INSERT INTO daily_stock_price_list 
        (date, secucode, close_price, volume) 
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        close_price = VALUES(close_price),
        volume = VALUES(volume)
    """
    cursor.executemany(sql, batch_data)
    connection.commit()
    return cursor.rowcount


# ====================== 修改5：抽取独立函数 —— 逐行重试插入 ======================
def retry_insert_row_by_row(connection, cursor, batch_data):
    """批量插入失败后，逐行重试插入"""
    sql = """
        INSERT INTO daily_stock_price_list 
        (date, secucode, close_price, volume) 
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        close_price = VALUES(close_price),
        volume = VALUES(volume)
    """
    success_count = 0
    inserted = 0
    updated = 0
    
    for i, row_data in enumerate(batch_data):
        try:
            cursor.execute(sql, row_data)
            connection.commit()
            success_count += 1
            row_affected = cursor.rowcount
            if row_affected == 1:
                inserted += 1
            elif row_affected == 2:
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


# def save_to_excel(df, stock_code, output_dir="."):
#     """保存数据到Excel"""
#     if df is None or df.empty:
#         print("无数据可保存")
#         return False
    
#     # 创建输出目录
#     os.makedirs(output_dir, exist_ok=True)
    
#     # 生成安全的文件名
#     filename = f"{stock_code}_history.xlsx"
#     filepath = os.path.join(output_dir, filename)
    
#     try:
#         # 保存到Excel
#         with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
#             df.to_excel(writer, index=False, sheet_name='历史数据')
        
#         print(f"数据已保存: {filepath}")
#         print(f"数据维度: {df.shape[0]}行 × {df.shape[1]}列")
#         return True
#     except Exception as e:
#         print(f"保存文件失败: {e}")
#         return False


def main():
    """主函数"""
    # 配置参数
    # symbols = ["600519", "000002"]  # 要获取的股票列表
    symbols = get_all_stock_codes()[:2]
    # output_dir = "d:/1/1"  # 输出目录
    adjust_type = ""  # 复权类型: ""(不复权), "qfq"(前复权), "hfq"(后复权)
    
    print("=" * 50)
    print("股票历史数据下载工具")
    print("=" * 50)
    
    successful_symbols = []
    failed_symbols = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] 处理股票: {symbol}")
        
        # 获取数据
        df = get_stock_history(
            symbol=symbol,
            start_date="20260101",  # 实际使用时可根据需要调整
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust=adjust_type
        )
        
        # 输出数据至daily_stock_price_list
        if df is not None and not df.empty:
            base_cols = ['日期', '股票代码', 'close', 'volume']
            available_cols = [col for col in base_cols if col in df.columns]
            df = df[available_cols].copy()
            try:
                inputdata(symbol, df)
                successful_symbols.append(symbol)
            except Exception as e:
                logging.error(f"处理{symbol}数据时出错: {str(e)}")
        else:
            failed_symbols.append(symbol)
        
        # 添加延迟避免频繁请求
        if i < len(symbols):
            time.sleep(random.uniform(1, 2))
    
    # 输出汇总结果
    print("\n" + "=" * 50)
    print("任务完成汇总:")
    print(f"成功下载: {len(successful_symbols)} 只股票")
    if successful_symbols:
        print(f"股票代码: {', '.join(successful_symbols)}")
    
    print(f"失败: {len(failed_symbols)} 只股票")
    if failed_symbols:
        print(f"股票代码: {', '.join(failed_symbols)}")
    print("=" * 50)


if __name__ == "__main__":
    main()