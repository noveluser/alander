#!/usr/bin/python3
# coding=utf-8

"""
股票历史数据下载工具
从akshare获取股票历史行情数据
"""

import akshare as ak
import pandas as pd
from datetime import datetime
import time
import random
import os

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
    # 设置日期范围
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    if not start_date:
        # 获取上市日期或使用默认
        listing_date = get_stock_listing_date(symbol)
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
                symbol=symbol,
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
            base_cols = ['日期', '股票代码', 'open', 'close', 'high', 'low', 'volume']
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

def save_to_excel(df, stock_code, output_dir="."):
    """保存数据到Excel"""
    if df is None or df.empty:
        print("无数据可保存")
        return False
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成安全的文件名
    filename = f"{stock_code}_history.xlsx"
    filepath = os.path.join(output_dir, filename)
    
    try:
        # 保存到Excel
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='历史数据')
        
        print(f"数据已保存: {filepath}")
        print(f"数据维度: {df.shape[0]}行 × {df.shape[1]}列")
        return True
    except Exception as e:
        print(f"保存文件失败: {e}")
        return False

def main():
    """主函数"""
    # 配置参数
    symbols = ["600519", "000001"]  # 要获取的股票列表
    output_dir = "d:/1"  # 输出目录
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
        
        # 保存数据
        if df is not None and not df.empty:
            if save_to_excel(df, symbol, output_dir):
                successful_symbols.append(symbol)
                # 显示前3行数据预览
                print("数据预览:")
                print(df[['日期', 'open', 'close', 'volume']].head(3).to_string())
            else:
                failed_symbols.append(symbol)
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
    
    print(f"数据保存目录: {os.path.abspath(output_dir)}")
    print("=" * 50)

if __name__ == "__main__":
    main()