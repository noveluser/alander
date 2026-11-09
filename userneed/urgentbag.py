#!/usr/bin/python3
# coding=utf-8


# 查找紧急行李
# wangle
# v0.4 (改为SQL直接排序 + 批量查询)


import tkinter as tk
from tkinter import messagebox, ttk
import oracledb
import logging
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='urgentbag.log',
    filemode='a'
)

# 定义文件名
filename = 'lpc.txt'


def accessOracle(query, params=None):
    dsn_tns = '10.31.8.21:1521/ORABPI'
    try:
        with oracledb.connect(user='owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                return c.fetchall()
    except oracledb.DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Error occurred while accessing Oracle: {e}")
        return None


# >>> SQL排序优化：函数改为接收LPC列表，SQL改为IN查询并直接ORDER BY
def packageinfo_batch(lpc_list):
    # 为 IN 子句生成占位符，如 :lpc0, :lpc1, :lpc2
    placeholders = ', '.join([f':lpc{i}' for i in range(len(lpc_list))])
    params = {f'lpc{i}': val for i, val in enumerate(lpc_list)}
    
    # >>> SQL排序优化：核心改动在这里
    # 1. TUBINFO 和 BAGINFO 都改为按 LPC IN (...) 批量查询，并用 ROW_NUMBER 取每个LPC的最新一条
    # 2. 主查询末尾加上 ORDER BY fs.CLOSE_DT，让数据库直接按资源关闭时间排序
    UrgencyPackageQuery = f"""
        WITH TUBINFO AS ( 
            SELECT LPC, L_CARRIER, L_DESTINATION, areaid || '.' || ZONEID || '.' || EQUIPMENTID as location 
            FROM (
                SELECT t.LPC, t.L_CARRIER, t.L_DESTINATION, t.areaid, t.ZONEID, t.EQUIPMENTID, t.IDEVENT,
                       ROW_NUMBER() OVER(PARTITION BY t.LPC ORDER BY t.IDEVENT DESC) as rn
                FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT t
                WHERE t.EVENTTS >= TRUNC(SYSDATE) - 1 AND t.lpc IN ({placeholders})
            ) WHERE rn = 1
        ), 
        BAGINFO AS ( 
            SELECT LPC, REPLACE(substr(NAME, 1, 7), '_', '') AS flightnr
            FROM (
                SELECT LPC, NAME, ROW_NUMBER() OVER(PARTITION BY LPC ORDER BY IDEVENT DESC) as rn
                FROM WC_PACKAGEINFO 
                WHERE LPC IN ({placeholders})
            ) WHERE rn = 1
        ) 
        SELECT bg.lpc, fs.flightnr, fs.CLOSE_DT, fs.INTIME_ALLOCATED_SORT, tubinfo.location, 
               substr(tubinfo.L_CARRIER, 1, instr(tubinfo.L_CARRIER, ',') - 1) AS tubid
        FROM FACT_FLIGHT_SUMMARIES_V fs 
        JOIN BAGINFO bg ON fs.flightnr = bg.flightnr 
        LEFT JOIN TUBINFO tubinfo ON bg.LPC = tubinfo.LPC
        LEFT JOIN DIM_STATIONS s ON tubinfo.L_DESTINATION = to_char(s.DESTINATION_ID)
        WHERE FLIGHTDATE = TO_CHAR(SYSDATE + INTERVAL '8' HOUR, 'YYYY-MM-DD')
        ORDER BY fs.CLOSE_DT  -- >>> SQL排序优化：直接在这里按资源关闭时间排序
    """
    return accessOracle(UrgencyPackageQuery, params)


def query_packages():
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lpc_input = file.read().strip().splitlines() 
    except FileNotFoundError:
        messagebox.showerror("错误", "文件 lpc.txt 未找到！")
        return
    
    # 提取所有有效的LPC编号
    lpc_list = []
    for raw_line in lpc_input:
        try:
            lpc = int(raw_line.strip()[-10:])
            lpc_list.append(lpc)
        except ValueError:
            logging.warning(f"跳过无效行: {raw_line}")
            continue
            
    if not lpc_list:
        messagebox.showinfo("提示", "没有有效的LPC编号可查询。")
        return
    
    # >>> SQL排序优化：只调用一次数据库，直接拿到排好序的结果
    progress_bar['maximum'] = 1
    progress_bar['value'] = 0
    start_time = time.time() 
    
    all_results = packageinfo_batch(lpc_list)
    
    progress_bar['value'] = 1
    root.update_idletasks()
    
    end_time = time.time()  
    logging.info("总查询时间: {:.2f}秒".format(end_time - start_time))
    
    output_text.delete(1.0, tk.END) 
    
    if all_results:
        # >>> SQL排序优化：因为SQL已经ORDER BY了，这里直接按返回顺序显示即可
        formatted_lines = []
        for row in all_results:
            close_dt_str = row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else 'N/A'
            message = (
                f"行李:'{row[0]}', "
                f"航班:'{row[1]}', "
                f"托盘:'{row[5]}', "
                f"资源关闭时间：'{close_dt_str}', "
                f"最近位置：'{row[4]}', "
                f"行李目的地：'{row[3]}'"
            )
            formatted_lines.append(message)
            logging.info(message)
        output_text.insert(tk.END, "\n".join(formatted_lines))
    else:
        output_text.insert(tk.END, "未查询到任何行李信息。")


if __name__ == '__main__':
    # 创建主窗口
    root = tk.Tk()
    root.title("紧急行李查询")

    # 创建按钮
    query_button = tk.Button(root, text="查询 LPC 信息", command=query_packages)
    query_button.pack(pady=10)

    # 创建进度条
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
    progress_bar.pack(pady=10)

    # 创建输出文本框
    output_text = tk.Text(root, height=15, width=150)
    output_text.pack(pady=10)

    # 运行主循环
    root.mainloop()