#!/usr/bin/python3
# coding=utf-8


# 查找紧急行李
# wangle
# v0.3


import tkinter as tk
from tkinter import messagebox, ttk
import cx_Oracle
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


def packageinfo(lpc):
    UrgencyPackageQuery ="WITH TUBINFO AS ( SELECT L_CARRIER FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TRUNC( SYSDATE ) AND lpc = :lpc AND L_CARRIER IS NOT NULL ORDER BY EVENTTS ), BAGINFO AS ( SELECT LPC, ( DEPAIRLINE || DEPFLIGHT ) AS flightnr, ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn  FROM WC_PACKAGEINFO  WHERE LPC = :lpc  AND ROWNUM = 1  ORDER BY IDEVENT DESC  ) SELECT bg.lpc, fs.flightnr, fs.CLOSE_DT, fs.INTIME_ALLOCATED_SORT, substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid  FROM FACT_FLIGHT_SUMMARIES_V fs JOIN BAGINFO bg ON fs.flightnr = bg.flightnr JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1  WHERE bg.rn = 1  AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )"
    return accessOracle(UrgencyPackageQuery, {'lpc': lpc})


def query_packages():
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lpc_input = file.read().strip().splitlines() 
    except FileNotFoundError:
        messagebox.showerror("错误", "文件 lpc.txt 未找到！")
        return
    results = []
    progress_bar['maximum'] = len(lpc_input)
    progress_bar['value'] = 0
    start_time = time.time() 
    for lpc in lpc_input:
        try:
            lpc = int(lpc.strip())
            bagresult = packageinfo(lpc)
            if bagresult:
                message = (
                    f"行李:'{bagresult[0][0]}', "
                    f"航班:'{bagresult[0][1]}', "
                    f"资源关闭时间：'{bagresult[0][2].strftime('%Y-%m-%d %H:%M:%S')}', "
                    f"目的地：'{bagresult[0][3]}', "
                    f"托盘:'{bagresult[0][4]}'"
                )
                results.append(message)
                logging.info(message)
            else:
                results.append(f"LPC '{lpc}' 未找到包信息！")
        except ValueError:
            results.append(f"输入 '{lpc}' 不是有效的 LPC 编号。")
        except Exception as e:
            logging.error(f"发生错误: {e}")
            results.append(f"发生错误: {e}")
        progress_bar['value'] += 1
        root.update_idletasks() 
    end_time = time.time()  
    total_time = end_time - start_time  
    logging.info("总查询时间: {}".format(total_time))
    output_text.delete(1.0, tk.END) 
    output_text.insert(tk.END, "\n".join(results))  


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
    output_text = tk.Text(root, height=15, width=120)
    output_text.pack(pady=10)

    # 运行主循环
    root.mainloop()