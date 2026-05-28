"""
主程序 - 仅业务调度：数据输入→TTM计算→数据输出
核心：无DB代码、无校验代码，全通过通用模块调用，保持极致干净
author:wangle
version:1.0
"""
import random
import akshare as ak
import time
from datetime import datetime
import pandas as pd
from config import DB_URL, DB_CONFIG, LOG_CONFIG, BUSINESS_CONFIG
# 导入通用模块，无自定义DB/校验逻辑
from utils import init_logger, validate_finance_data
from db_operation import create_db_engine, safe_db_operation, db_read, db_write, batch_update, retry_row_update
from decimal import Decimal


# ---------------------- 全局初始化（仅日志+DB引擎，通用操作） ----------------------
logger = init_logger(LOG_CONFIG)
create_db_engine(DB_URL, DB_CONFIG)


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

    #  转换str成sql的date格式
    date_str = row.get("日期", "").strip()  # 空值默认给空字符串
    sql_date = None

    if date_str:  # 只有字符串非空才转换
        sql_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    # 构造数据元组
    row_data = (
        sql_date,
        row.get("代码代码"),
        to_decimal(row.get("close")),
        to_decimal(row.get("volume"))
    )
    return row_data


# def get_all_stock_codes():
#     """从stock_list表获取所有代码代码"""
#     connection = None
#     cursor = None

#     try:
#         connection = get_db_connection()
#         cursor = connection.cursor()

#         # 提取所有flag标记为Y的代码，意味着财报已经下载完成
#         sql = "SELECT secucode FROM stock_list where flag = 'N';"
#         cursor.execute(sql)
#         results = cursor.fetchall()

#         if not results:
#             logging.warning("stock_list表中没有数据")
#             return []

#         # 提取secucode列表
#         stock_codes = [row['secucode'] for row in results]
#         logging.info(f"从数据库获取到 {len(stock_codes)} 个代码代码")

#         return stock_codes

#     except Exception as e:
#         logging.error(f"获取代码代码失败: {str(e)}")
#         return []

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()


def get_stock_listing_date(symbol):
    """获取代码上市日期，增强解析逻辑"""
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
        print(f"获取代码{symbol}上市日期失败: {e}")
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
    if '数据日期' not in df.columns:
        return False, "缺少日期列"
    
    # 检查数据行数
    if len(df) == 0:
        return False, "数据行数为0"
    
    return True, "数据验证通过"

def get_stock_history(symbol, start_date=None, end_date=None, adjust=""):
    """
    获取代码历史数据
    """
    # 去除代码代码后缀
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
    
    print(f"获取代码 {symbol} 数据: {start_date} 至 {end_date}")
    
    # 重试机制
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_value_em(
                symbol=stock_code_clean
            )
            # df.to_excel("c:/work/log/300206.xlsx", index=False, engine='openpyxl')


            # 数据验证
            is_valid, msg = validate_dataframe(df, ['数据日期', '总股本', 'PE(TTM)', '市净率'])
            if not is_valid:
                print(f"数据验证失败: {msg}")
                return None
            
            # 数据处理
            # df['数据日期'] = pd.to_datetime(df['数据日期']).dt.strftime('%Y-%m-%d')
            df['数据日期'] = pd.to_datetime(df['数据日期'])
            df['代码代码'] = symbol
            
            
            # 选择并重排序列
            base_cols = ['数据日期', '总股本', 'PE(TTM)', '市净率']
            available_cols = [col for col in base_cols if col in df.columns]
            
            # 添加其他可用列
            other_cols = ['代码代码']
            selected_cols =  other_cols + available_cols

            # 4. 按日期范围过滤
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df["数据日期"] >= start_dt) & (df["数据日期"] <= end_dt)].copy()
            df['数据日期'] = df['数据日期'].dt.strftime('%Y-%m-%d')

            result_df = df[selected_cols].copy()
            result_df = result_df.sort_values('数据日期', ascending=True).reset_index(drop=True)

            result_df = result_df.rename(
                columns={
                    "数据日期": "date",
                    "代码代码": "secucode",
                    "总股本": "share_capital",
                    "PE(TTM)": "estimate_pe",
                    "市净率": "pb"
                }
            )
            
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
            logging.info(f"代码代码 {code} 已成功更新标志位为Y")
            return True
        else:
            logging.warning(f"stock_list表中未能更新代码为 {code} 的记录，")
            return False

    except Exception as e:
        logging.error(f"更新代码代码 {code} 标志位失败: {str(e)}")
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
        (date, secucode, estimate_pe, pb) 
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
            final_success = (success_count > 0)  # 如果逐行重试至少成功了一行，可以认为是部分成功。根据业务逻辑调整。
        
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

    """主函数：仅三大核心步骤，全程通过SQL实现数据输入/输出"""
    # 配置常量
    # UNPROCESSED_FLAG = BUSINESS_CONFIG["unprocessed_flag"]
    UNPROCESSED_FLAG = "Y"
    # REPORT_TIME_START = BUSINESS_CONFIG["report_time_start"]
    # SLEEP_MIN = BUSINESS_CONFIG["sleep_time_min"]
    # SLEEP_MAX = BUSINESS_CONFIG["sleep_time_max"]
    # symbols = ["300206.SZ"]  # 要获取的代码列表
    # ---------------------- 步骤1：数据输入（SQL读取，全量未处理代码+财务数据） ----------------------
    # 1.1 读取stock_list表中未处理代码代码（SQL输入）
    sql_unprocessed = "SELECT secucode FROM stock_list WHERE flag = :flag"   #测试语句，非错误
    params_unprocessed = {"flag": UNPROCESSED_FLAG}
    df_unprocessed = db_read(sql_unprocessed, params_unprocessed)
    symbols = df_unprocessed['secucode'].tolist() if not df_unprocessed.empty else []
    if not symbols:
        logger.info("ℹ️  stock_list表中无未处理代码，任务结束")
        return
    logger.info(f"🚀 开始批量处理，共{len(symbols)}只未处理代码")
    
    successful_symbols = []
    failed_symbols = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] 处理代码: {symbol}")
        
        # 获取数据
        df = get_stock_history(
            symbol=symbol,
            start_date="20240101",  # 实际使用时可根据需要调整
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust=""
        )
        # df.to_excel("c:/work/log/300206.xlsx", index=False, engine='openpyxl')
        
        # # 输出数据至daily_stock_price_list
        # if df is not None and not df.empty:
        #     base_cols = ['数据日期', '代码代码', '总股本', 'PE(TTM)', '市净率']
        #     available_cols = [col for col in base_cols if col in df.columns]
        #     df = df[available_cols].copy()
        #     try:
        #         inputdata(symbol, df)
        #         successful_symbols.append(symbol)
        #     except Exception as e:
        #         logging.error(f"处理{symbol}数据时出错: {str(e)}")
        # else:
        #     failed_symbols.append(symbol)
        
        # # 添加延迟避免频繁请求

        # if i < len(symbols):
        #     time.sleep(random.uniform(1, 2))

        # ---------------------- 步骤3：通用数据输出（SQL更新） ----------------------
        # 3.1 通用批量更新（无业务逻辑）
        sql_batch_update = """
            UPDATE daily_stock_price_list
            SET estimate_pe = :estimate_pe,
                pb = :pb,
				share_capital = :share_capital
            WHERE secucode = :secucode AND date = :date
        """
        # logger.warning(f"测试输出: {sql_batch_update}")
        affected_rows = batch_update(sql_batch_update, df=df)


        # 3.3 统一判断更新结果，更新处理状态（批量/逐行成功都处理）
        if affected_rows > 0:
            db_write("UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode", params={"secucode": symbol})
            successful_symbols.append(symbol)
            logger.info(f"✅ {symbol} 全流程处理完成，更新{affected_rows}条财务数据")
        else:
            failed_symbols.append(symbol)
            logger.warning(f"⚠️ {symbol} 无有效数据更新（批量+逐行重试均失败），处理失败")
    
    # # 输出汇总结果
    # print("\n" + "=" * 50)
    # print("任务完成汇总:")
    # print(f"成功下载: {len(successful_symbols)} 只代码")
    # if successful_symbols:
    #     print(f"代码代码: {', '.join(successful_symbols)}")
    
    # print(f"失败: {len(failed_symbols)} 只代码")
    # if failed_symbols:
    #     print(f"代码代码: {', '.join(failed_symbols)}")
    # print("=" * 50)


if __name__ == "__main__":
    main()

# # ---------------------- 主流程：仅业务调度，无任何通用逻辑 ----------------------
# def main():
#     """主流程：批量读取→单股数据校验→TTM计算→批量更新DB→状态更新"""
#     # 从配置读取常量，无硬编码
#     CFG = BUSINESS_CONFIG
#     SLEEP_MIN = CFG["sleep_time_min"]
#     SLEEP_MAX = CFG["sleep_time_max"]
#     BATCH_SIZE = CFG.get("batch_size", 20)
#     DB_RETRY_TIMES = CFG.get("db_retry_times", 2)

#     # 数据配置（与业务强相关，仅此处定义）
#     FINANCE_NUMERIC_COLS = ['share_capital', 'total_equity']
#     UPDATE_daily_stock_price_SQL = """
#         UPDATE daily_stock_price_list
#         SET estimate_pe = IF(:pe IS NULL, NULL, :pe),
#         PB = IF(:pb IS NULL, NULL, :pb)
#         WHERE secucode = :secucode AND date = :date
#     """
#     UPDATE_sort_finance_SQL = """
#         UPDATE sort_finance
#         SET share_capital = IF(:share_capital IS NULL, NULL, :share_capital)
#         WHERE secucode = :secucode AND date = :date
#     """

#     # 1. 批量读取未处理（调用db_operation安全方法）
#     sql_unprocessed = "SELECT secucode FROM stock_list WHERE secucode = '688728.SH' OR SECUCODE = '689009.SH' ;"  # 测试语句
#     # sql_unprocessed = "SELECT DISTINCT secucode FROM daily_stock_price_list WHERE estimate_pe IS NULL and date < '2025-01-01'"  
#     df_unprocessed = safe_db_operation(db_read, sql_unprocessed, retry_times=DB_RETRY_TIMES)
#     if df_unprocessed is None or df_unprocessed.empty:
#         logger.info("ℹ️  stock_list表中无未处理，任务结束")
#         return
#     symbols = df_unprocessed['secucode'].tolist()
#     # symbols = ['600925.SH' , '600930.SH' , '601061.SH' , '601065.SH' , '601083.SH' , '601096.SH' , '601112.SH' , '601121.SH' , '601133.SH' , '603004.SH' , '603014.SH' , '603049.SH']

#     symbol_batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
#     logger.info(f"🚀 任务启动：共{len(symbols)}只，分{len(symbol_batches)}批处理，每批{BATCH_SIZE}只")

#     # 初始化统计
#     successful_symbols, failed_symbols = [], []

#     # 2. 批量遍历处理
#     for batch_idx, batch_syms in enumerate(symbol_batches, 1):
#         logger.info(f"\n===== 批量进度：[{batch_idx}/{len(symbol_batches)}] ：{batch_syms} =====")
#         batch_ttm_dfs, batch_success, batch_fail = [], [], []

#         # 单股处理：仅调度校验+计算，无通用逻辑
#         for sym in batch_syms:
#             try:
#                 # 读取单股所需数据
#                 df = safe_db_operation(
#                     db_read,
#                     SELECT_SQL,
#                     params={"secucode": sym, "delay_days": DELAY_DAYS},
#                     retry_times=DB_RETRY_TIMES
#                 )
#                 if df is None or df.empty:
#                     logger.warning(f"⚠️  {sym} 无符合条件的财务数据")
#                     batch_fail.append(sym)
#                     continue

#                 # 调用utils通用财务数据校验（一键校验，无需重复代码）
#                 is_valid, msg, df_valid = validate_finance_data(df, FINANCE_NUMERIC_COLS)
#                 if not is_valid:
#                     logger.warning(f"⚠️  {sym} 财务数据校验失败: {msg}")
#                     batch_fail.append(sym)
#                     continue

#                 # TTM纯业务计算
#                 df_ttm = calc_ttm_PE_PB(df_valid)
#                 if df_ttm.empty:
#                     logger.warning(f"⚠️  {sym} TTM计算无有效数据，跳过")
#                     # batch_fail.append(sym)
#                     continue

#                 # 计算成功，加入批量待更新列表
#                 batch_ttm_dfs.append(df_ttm)
#                 batch_success.append(sym)
#                 logger.info(f"✅ {sym} 数据处理完成，待批量更新")

#             except Exception as e:
#                 logger.error(f"❌ {sym} 单股处理异常: {str(e)}", exc_info=True)
#                 batch_fail.append(sym)
#                 continue

#         # 3. 批量更新数据库（调用db_operation通用方法）
#         if batch_ttm_dfs and batch_success:
#             df_batch_ttm = pd.concat(batch_ttm_dfs, ignore_index=True)
#             # ======================
#             # 新增：打印 2024-01-10 的执行 SQL
#             # ======================
#             import datetime
#             target_date = datetime.date(2024, 1, 10)
#             for _, row in df_batch_ttm.iterrows():
#                 if row['date'] == target_date:
#                     logger.info(f"📝 即将执行更新SQL | date=2024-01-10 | secucode={row['secucode']}")
#                     logger.info(f"SQL参数: {row.to_dict()}")
#             # 安全批量更新
#             affected_rows = safe_db_operation(
#                 batch_update, UPDATE_SQL, df_batch_ttm,
#                 retry_times=DB_RETRY_TIMES
#             )
#             # 批量更新失败则逐行重试
#             if affected_rows == 0:
#                 logger.warning(f"⚠️  第{batch_idx}批批量更新失败，触发逐行重试")
#                 for sym in batch_success:
#                     df_sym_ttm = df_batch_ttm[df_batch_ttm['secucode'] == sym]
#                     retry_aff = safe_db_operation(retry_row_update, sym, UPDATE_SQL, df_sym_ttm, retry_times=DB_RETRY_TIMES)
#                     if retry_aff > 0:
#                         # 安全更新处理状态
#                         flag_aff = safe_db_operation(
#                             db_write, "UPDATE stock_list SET flag = 'Y' WHERE secucode = :secucode",
#                             params={"secucode": sym}, retry_times=DB_RETRY_TIMES
#                         )
#                         if flag_aff is not None and flag_aff > 0:
#                             successful_symbols.append(sym)
#                         else:
#                             logger.error(f"❌ {sym} 数据更新成功，状态更新失败（需手动处理）")
#                             batch_fail.append(sym)
                        
#                         # 新增：无论状态更新是否成功，都从batch_success移除，避免重复处理/统计错误
#                         if sym in batch_success:
#                             batch_success.remove(sym)
#                     else:
#                         logger.warning(f"⚠️ {sym} 逐行重试仍失败")
#                         batch_fail.append(sym)

#             # 批量更新成功，批量更新状态（修复：单股循环→批量更新，1次DB请求）
#             else:
#                 if batch_success:
#                     # 批量更新flag SQL：用IN条件匹配整批代码代码
#                     batch_flag_sql = "UPDATE stock_list SET flag = 'Y' WHERE secucode IN :secucodes"
#                     # 传入批量参数（元组格式适配SQLAlchemy的IN条件）
#                     batch_flag_params = {"secucodes": tuple(batch_success)}
#                     # 1次安全批量更新，获取受影响行数
#                     batch_flag_aff = safe_db_operation(
#                         db_write, batch_flag_sql,
#                         params=batch_flag_params, retry_times=DB_RETRY_TIMES
#                     )
#                     # 遍历校验每只代码状态更新结果，保留原统计逻辑
#                     for sym in batch_success.copy():
#                         if batch_flag_aff and batch_flag_aff > 0:
#                             # 批量更新成功，计入成功列表
#                             successful_symbols.append(sym)
#                         else:
#                             # 批量更新失败，标记为手动处理
#                             logger.error(f"❌ {sym} 数据更新成功，状态批量更新失败（需手动处理）")
#                             batch_fail.append(sym)
#                         # 保留原移除逻辑，避免重复处理
#                         if sym in batch_success:
#                             batch_success.remove(sym)
#         # 批量失败加入总统计
#         failed_symbols.extend(batch_fail)

#         # 批量间延迟，避免数据库压力
#         if batch_idx < len(symbol_batches):
#             sleep_t = random.uniform(SLEEP_MIN, SLEEP_MAX)
#             logger.debug(f"ℹ️  等待{sleep_t:.2f}秒处理下一批")
#             time.sleep(sleep_t)

#     # 4. 任务汇总统计
#     logger.info(f"\n" + "=" * 80)
#     logger.info(f"📊 任务完成汇总 | 总数：{len(symbols)}")
#     logger.info(f"✅ 成功处理：{len(successful_symbols)} 只 | {', '.join(successful_symbols) if successful_symbols else '无'}")
#     logger.info(f"❌ 处理失败：{len(failed_symbols)} 只 | {', '.join(failed_symbols) if failed_symbols else '无'}")
#     logger.info(f"📈 处理成功率：{len(successful_symbols)/len(symbols)*100:.2f}%")
#     logger.info(f"=" * 80)

# # ---------------------- 程序入口 ----------------------
# if __name__ == "__main__":
#     main()