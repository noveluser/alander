#!/usr/bin/python3
# coding=utf-8

# 查找特定航班行李
# wangle
# v0.8

import tkinter as tk
from tkinter import messagebox, filedialog
# import cx_Oracle
import oracledb
import logging
import pandas as pd
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='bag.log',
    filemode='a'
)

# 数据库连接参数
DB_CONFIG = {
    'user': 'owner_31_bpi_3_0',
    'password': 'owner31bpi',
    'dsn': '10.31.8.21:1521/ORABPI'
}

def accessOracle(query, params=None):
    """连接到 Oracle 数据库并执行查询."""
    try:
        with oracledb.connect(**DB_CONFIG) as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                return c.fetchall()
    except oracledb.DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Error occurred while accessing Oracle: {e}")
        return None

def packageinfo(airline, flightnt, date):
    """根据航空公司、航班号和日期获取行李信息."""
    bagQuery = f"""
        WITH bsmtime AS (
            SELECT DISTINCT
                lpc,
                (DEPAIRLINE || DEPFLIGHT) AS flightnr,
                max(eventts) as EVENTTS
            FROM
                WC_PACKAGEDATA 
            WHERE
                EVENTTS >= TO_DATE('{date} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '8' HOUR
                AND EVENTTS < TO_DATE('{date} 23:59:59', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '8' HOUR
                AND DEPAIRLINE = '{airline}'
                AND DEPFLIGHT = '{flightnt}'
                AND BAG_MESSAGE_ID = 'BSM'
                AND TARGETPROCESSID LIKE 'ODB%' 
            group by 
                lpc,(DEPAIRLINE || DEPFLIGHT)    
            ORDER BY
                EVENTTS 
        )
        SELECT
            fs.lpn,
            fs.FLIGHTNR,
            TO_CHAR(FROM_TZ(CAST(bt.eventts AS TIMESTAMP), 'UTC') AT TIME ZONE 'Asia/Shanghai', 'hh24:mi:ss') AS starttime,
            TO_CHAR(fs.EXITTIMESTAMP, 'hh24:mi:ss') AS endtime,
            fs.EXITUSERID	
        FROM
            FACT_BAG_SUMMARIES_GUI_V fs
        JOIN bsmtime bt ON fs.lpn = bt.lpc
        where ENTRYTIMESTAMP >= TO_DATE('{date} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
        AND ENTRYTIMESTAMP < TO_DATE('{date} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    """
    print(bagQuery)
    return accessOracle(bagQuery)

def save_to_excel(data, file_path):
    """将查询结果保存到 Excel 文件."""
    if data:
        columns = ['条码', '航班', '创建时间', '到达时间', '目的地']
        df = pd.DataFrame(data, columns=columns)
        with pd.ExcelWriter(file_path) as writer:
            df.to_excel(writer, index=False, header=True)
        logging.info("数据成功保存到 Excel 文件.")
        return True
    else:
        logging.info("未查询到结果.")
        return False

def run_query():
    """从文本文件读取航班号并执行查询."""
    file_path = filedialog.askopenfilename(title="选择航班号文件", filetypes=[("Text Files", "*.txt")])
    if not file_path:
        return

    all_data = []  # 存储所有航班的数据
    date_input = date_entry.get().strip()  # 获取日期输入
    if not date_input:
        date_input = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # 默认为昨天

    with open(file_path, 'r') as file:
        flight_lines = file.readlines()

    for flight_input in flight_lines:
        flight_input = flight_input.strip().upper()  # 处理每一行输入
        if len(flight_input) != 6 or not flight_input[:2].isalpha() or not flight_input[2:].isdigit():
            messagebox.showwarning("输入警告", f"无效航班号：{flight_input}")
            continue

        airline = flight_input[:2]
        flightnt = flight_input[2:]

        try:
            bagresult = packageinfo(airline, flightnt, date_input)
            print(bagresult)
            if bagresult:
                all_data.extend(bagresult)  # 添加到所有数据中
            else:
                logging.warning(f"未查询到航班 {flight_input} 的信息.")
        except Exception as e:
            logging.error(f"发生错误: {e}")
            messagebox.showerror("错误", f"查询航班 {flight_input} 时发生错误: {e}")

    if all_data:
        save_to_excel(all_data, 'flighbag.xlsx')
        messagebox.showinfo("成功", "所有航班的数据已保存到 Excel 文件。")
    else:
        messagebox.showwarning("无结果", "没有查询到任何行李信息。")


# 创建主窗口
root = tk.Tk()
root.title("航班行李查询")

# 创建日期输入框和标签
date_label = tk.Label(root, text="请输入日期 (YYYY-MM-DD, 默认为昨天)：")
date_label.pack(pady=10)
date_entry = tk.Entry(root, width=20)
date_entry.pack(pady=10)

# 设置默认日期为昨天
default_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_entry.insert(0, default_date)

# 创建执行按钮
execute_button = tk.Button(root, text="执行", command=run_query)
execute_button.pack(pady=20)


# 运行主循环
if __name__ == '__main__':
    root.mainloop()