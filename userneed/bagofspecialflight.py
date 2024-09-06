#!/usr/bin/python3
# coding=utf-8

# 查找特定航班行李
# wangle
# v0.9

import tkinter as tk
from tkinter import messagebox, filedialog
import cx_Oracle
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
    dsn_tns = '10.31.8.21:1521/ORABPI'
    try:
        with cx_Oracle.connect(user='owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                return c.fetchall()
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Error occurred while accessing Oracle: {e}")
        return None


def packageinfo(depairline: str, depflight: str, date: str):
    bagQuery = f"""
        WITH bsmtime AS (
            SELECT DISTINCT
                lpc,
                (DEPAIRLINE || DEPFLIGHT) AS flightnr,
                MAX(eventts) AS EVENTTS 
            FROM
                WC_PACKAGEDATA 
            WHERE
                EVENTTS >= TO_DATE(:date_start, 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '8' HOUR 
                AND EVENTTS < TO_DATE(:date_end, 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '8' HOUR 
                AND DEPAIRLINE = :depairline 
                AND DEPFLIGHT = :depflight 
                AND BAG_MESSAGE_ID = 'BSM' 
                AND TARGETPROCESSID LIKE 'ODB%' 
            GROUP BY
                lpc,
                (DEPAIRLINE || DEPFLIGHT) 
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
        WHERE
            ENTRYTIMESTAMP >= TO_DATE(:date_start, 'YYYY-MM-DD HH24:MI:SS') 
            AND ENTRYTIMESTAMP < TO_DATE(:date_end, 'YYYY-MM-DD HH24:MI:SS')
    """
    
    # 设置参数字典
    params = {
        'date_start': f"{date} 00:00:00",
        'date_end': f"{date} 23:59:59",
        'depairline': depairline,  # 确保是字符串
        'depflight': depflight       # 确保是字符串
    }

    return accessOracle(bagQuery, params)


# def packageinfo(lpc):
#     bagQuery = f"""
#         WITH TUBINFO AS (
#             SELECT L_CARRIER 
#             FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT 
#             WHERE EVENTTS >= TRUNC(SYSDATE) 
#               AND lpc = :lpc 
#               AND L_CARRIER IS NOT NULL 
#             ORDER BY EVENTTS
#         ), 
#         BAGINFO AS (
#             SELECT LPC, 
#                    (DEPAIRLINE || DEPFLIGHT) AS flightnr, 
#                    ROW_NUMBER() OVER (ORDER BY IDEVENT DESC) AS rn  
#             FROM WC_PACKAGEINFO  
#             WHERE LPC = :lpc  
#               AND ROWNUM = 1  
#             ORDER BY IDEVENT DESC  
#         ) 
#         SELECT bg.lpc, 
#                fs.flightnr, 
#                fs.CLOSE_DT, 
#                fs.INTIME_ALLOCATED_SORT, 
#                SUBSTR(tubinfo.L_CARRIER, 1, INSTR(tubinfo.L_CARRIER, ',') - 1) AS tubid  
#         FROM FACT_FLIGHT_SUMMARIES_V fs 
#         JOIN BAGINFO bg ON fs.flightnr = bg.flightnr 
#         JOIN (SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1) tubinfo ON 1 = 1  
#         WHERE bg.rn = 1  
#           AND FLIGHTDATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD')
#     """
#     #     UrgencyPackageQuery ="WITH TUBINFO AS ( SELECT L_CARRIER FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TRUNC( SYSDATE ) AND lpc = :lpc AND L_CARRIER IS NOT NULL ORDER BY EVENTTS ), BAGINFO AS ( SELECT LPC, ( DEPAIRLINE || DEPFLIGHT ) AS flightnr, ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn  FROM WC_PACKAGEINFO  WHERE LPC = :lpc  AND ROWNUM = 1  ORDER BY IDEVENT DESC  ) SELECT bg.lpc, fs.flightnr, fs.CLOSE_DT, fs.INTIME_ALLOCATED_SORT, substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid  FROM FACT_FLIGHT_SUMMARIES_V fs JOIN BAGINFO bg ON fs.flightnr = bg.flightnr JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1  WHERE bg.rn = 1  AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )" 
#     # 执行查询并返回结果
#     result = accessOracle(bagQuery, {'lpc': lpc})
    
#     if result is None:
#         print("查询没有返回任何结果.")
#     else:
#         print("查询结果:", result)  # 打印结果以便调试
#     return result


# def packageinfo(lpc):
#     UrgencyPackageQuery ="WITH TUBINFO AS ( SELECT L_CARRIER FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TRUNC( SYSDATE ) AND lpc = :lpc AND L_CARRIER IS NOT NULL ORDER BY EVENTTS ), BAGINFO AS ( SELECT LPC, ( DEPAIRLINE || DEPFLIGHT ) AS flightnr, ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn  FROM WC_PACKAGEINFO  WHERE LPC = :lpc  AND ROWNUM = 1  ORDER BY IDEVENT DESC  ) SELECT bg.lpc, fs.flightnr, fs.CLOSE_DT, fs.INTIME_ALLOCATED_SORT, substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid  FROM FACT_FLIGHT_SUMMARIES_V fs JOIN BAGINFO bg ON fs.flightnr = bg.flightnr JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1  WHERE bg.rn = 1  AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )"
# #     UrgencyPackageQuery = """
# #         SELECT DISTINCT
# #             lpc,
# #             ( DEPAIRLINE || DEPFLIGHT ) AS flightnr,
# #             EVENTTS 
# #         FROM
# #             WC_PACKAGEDATA 
# #         WHERE
# #             EVENTTS >= TO_DATE( '2024-09-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS' ) - INTERVAL '8' HOUR 
# #             AND EVENTTS < TO_DATE( '2024-09-05 23:59:59', 'YYYY-MM-DD HH24:MI:SS' ) - INTERVAL '8' HOUR 
# #             AND lpc = 347950591124-09-05 23:59:59', 'YYYY-MM-DD HH24:MI:SS' ) - INTERVAL '8' HOUR 
# #                 and lpc = :lpc
# #    """
#     return accessOracle(UrgencyPackageQuery, {'lpc': lpc})


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
            # print(bagresult)
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