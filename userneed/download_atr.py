import datetime
import subprocess
import pandas as pd
from io import StringIO
from colorama import Fore, Style, init
import os
import oracledb  # 修改1：替换 cx_Oracle
import logging

# 初始化colorama
init(autoreset=True)

# ==================== 配置常量 ====================
WINDOW_SIZE = 5
BASE_URL = "http://10.31.7.16/rawfiles"
OUTPUT_DIR = "d:/1/2"
OUTPUT_FILE = "filtered_data.xlsx"

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='test.log',
    filemode='a'
)

# 文件名映射字典
INVALID_DICT = {
    # "01": "ATR-E01",
    # "02": "ATR-E02",
    # "03": "ATR-E03",
    # "04": "ATR-E3B",
    # "05": "ATR-E04",
    # "06": "ATR-E05",
    # "07": "ATR-W01",
    # "08": "ATR-W02", 
    # "09": "ATR-W03",
    # "10": "ATR-W3B",
    # "11": "ATR-W04",
    # "12": "ATR-W05",
    # "64": "RFMS-CIP",
    "13": "SAT-3113",
    "14": "SAT-3114",
    # "15": "SAT-3144",
    # "16": "SAT-3143",
}

def accessOracle(query, params=None):
    # 修改2：使用 oracledb 连接
    dsn_tns = '10.31.8.21:1521/ORABPI'
    try:
        # oracledb 连接语法
        with oracledb.connect(
            user='owner_31_bpi_3_0',
            password='owner31bpi',
            dsn=dsn_tns
        ) as conn:
            with conn.cursor() as c:
                c.execute(query, params or {})
                return c.fetchall()
    except oracledb.DatabaseError as e:  # 修改3：异常类型
        logging.error(f"Database error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Error occurred while accessing Oracle: {e}")
        return None


def getflightcode(lpc):
    UrgencyPackageQuery = """
        SELECT FLIGHTNR 
        FROM FACT_BAG_SUMMARIES 
        WHERE xlpc = :lpc 
        AND ENTER_DT > SYSDATE - 1 
        AND rownum < 10
    """
    # 拿到二维列表: [(val1,), (val2,)] 或 None/[]
    rows = accessOracle(UrgencyPackageQuery, {'lpc': lpc})
    
    # 判空：数据库异常返回None 或 无数据
    if not rows:
        return None
    
    # 取第一条第一条字段值
    first_row = rows[0]
    flight_nr = first_row[0] if len(first_row) > 0 else None
    
    return flight_nr

# ==================== 其余函数保持不变 ====================
def ensure_directory_exists(directory):
    """确保输出目录存在"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"{Fore.GREEN}创建目录: {directory}{Style.RESET_ALL}")

def is_empty_value(value):
    """判断值是否为空"""
    if pd.isna(value):
        return True
    if isinstance(value, str):
        return value.strip() == ''
    return False

def has_value(value):
    """判断值是否不为空"""
    return not is_empty_value(value)

def check_file_exists(url):
    """检查远程文件是否存在"""
    try:
        result = subprocess.run(
            f'curl -s -I "{url}"',
            shell=True,
            capture_output=True,
            text=True,
            timeout=10
        )
        return "200" in result.stdout.splitlines()[0]
    except Exception as e:
        print(f"{Fore.RED}检查文件时出错: {str(e)}{Style.RESET_ALL}")
        return False

def download_file_content(url):
    """下载文件内容"""
    try:
        result = subprocess.run(
            f'curl -s "{url}"',
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.stdout
    except Exception as e:
        print(f"{Fore.RED}下载文件时出错: {str(e)}{Style.RESET_ALL}")
        return ""

def read_and_process_file(file_content, skip_rows=4, separator=';'):
    """读取文件内容并处理为DataFrame"""
    if not file_content.strip():
        return None
    
    try:
        file_stream = StringIO(file_content)
        df = pd.read_csv(file_stream, sep=separator, skiprows=skip_rows)
        return df
    except Exception as e:
        print(f"{Fore.RED}读取CSV文件时出错: {str(e)}{Style.RESET_ALL}")
        return None

def extract_and_filter_data(df, date_formatted, filename):
    """提取指定列并筛选数据"""
    try:
        required_columns = max([0, 23, 42]) + 1
        if len(df.columns) < required_columns:
            print(f"{Fore.RED}警告: {filename} 在 {date_formatted} 列数不足 "
                  f"(只有 {len(df.columns)} 列，需要至少 {required_columns} 列){Style.RESET_ALL}")
            return None
        
        selected_columns = [0, 23, 42]
        column_names = [df.columns[i] for i in selected_columns]
        print(f"  提取的列: {column_names}")
        
        df_selected = df.iloc[:, selected_columns].copy()
        df_selected.columns = ['time', 'BARCODE', 'RFID']
        
        total_rows = len(df_selected)
        barcode_not_empty = df_selected['BARCODE'].apply(has_value).sum()
        rfid_empty = df_selected['RFID'].apply(is_empty_value).sum()
        print(f"  数据统计: 总行数={total_rows}, BARCODE不为空={barcode_not_empty}, RFID为空={rfid_empty}")
        
        mask = df_selected['RFID'].apply(is_empty_value) & df_selected['BARCODE'].apply(has_value)
        df_filtered = df_selected[mask].copy()
        
        # 注意：这里需要修改列名大小写，因为上面重命名为 'BARCODE'（大写）
        # 而 getflightcode 期望的是小写 'barcode'？根据实际情况调整
        def safe_get_flightcode(x):
            if pd.isna(x) or str(x).strip() == '':
                return None
            try:
                # 尝试转换为整数
                lpc_int = int(float(str(x).strip()))  # 先转float再转int，避免科学计数法等问题
                return getflightcode(lpc_int)
            except (ValueError, TypeError):
                return None


        df_filtered['external_data'] = df_filtered['BARCODE'].apply(safe_get_flightcode)
        
        print(f"  筛选结果: {len(df_filtered)} 行符合条件 (RFID为空且BARCODE不为空)")
        return df_filtered
        
    except Exception as e:
        print(f"{Fore.RED}处理数据时出错: {str(e)}{Style.RESET_ALL}")
        return None

def save_to_excel(df_list, output_path):
    """保存多个DataFrame到同一个Excel文件的不同sheet"""
    if not df_list:
        print(f"{Fore.YELLOW}没有数据可保存{Style.RESET_ALL}")
        return False
    
    try:
        ensure_directory_exists(os.path.dirname(output_path))
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet_name, df in df_list:
                safe_sheet_name = sheet_name[:31] if len(sheet_name) > 31 else sheet_name
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                print(f"{Fore.GREEN}已保存sheet: {safe_sheet_name} ({len(df)} 行){Style.RESET_ALL}")
        
        print(f"{Fore.GREEN}数据已成功保存到: {output_path}{Style.RESET_ALL}")
        return True
        
    except Exception as e:
        print(f"{Fore.RED}保存Excel文件时出错: {str(e)}{Style.RESET_ALL}")
        return False

def process_daily_data(date_range_days=1):
    """处理指定天数范围内的数据"""
    print(f"{Fore.CYAN}开始处理前 {date_range_days} 天的数据...{Style.RESET_ALL}")
    
    now = datetime.datetime.now()
    date_range = [(now - datetime.timedelta(days=i)) for i in range(date_range_days)]
    all_results = []
    
    for current_date in date_range:
        date_str = current_date.strftime('%y%m%d')
        date_formatted = current_date.strftime('%Y-%m-%d')
        
        print(f"\n{Fore.YELLOW}处理日期: {date_formatted}{Style.RESET_ALL}")
        
        for code, filename in INVALID_DICT.items():
            print(f"  处理文件: {filename}")
            
            file_url = f"{BASE_URL}/{date_str}{code}rw.dat"
            
            if not check_file_exists(file_url):
                print(f"  {Fore.RED}跳过: {filename} 在 {date_formatted} 不存在{Style.RESET_ALL}")
                continue
            
            file_content = download_file_content(file_url)
            
            if not file_content.strip():
                print(f"  {Fore.RED}警告: {filename} 在 {date_formatted} 没有数据{Style.RESET_ALL}")
                continue
            
            df = read_and_process_file(file_content)
            if df is None:
                continue
            
            df_filtered = extract_and_filter_data(df, date_formatted, filename)
            
            if df_filtered is not None and len(df_filtered) > 0:
                sheet_name = f"{filename}_{date_str}"
                all_results.append((sheet_name, df_filtered))
    
    if all_results:
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        save_to_excel(all_results, output_path)
    else:
        print(f"{Fore.YELLOW}没有找到符合条件的数据{Style.RESET_ALL}")

if __name__ == "__main__":
    # 安装依赖：pip install oracledb pandas colorama openpyxl
    process_daily_data(date_range_days=1)

            
 

