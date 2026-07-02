#!/usr/bin/python
# coding=utf-8

import oracledb
import logging
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading

# ---------- 数据库操作函数 ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//1.log',
    filemode='a'
)

def accessOracle(query, params=None):
    dsn_tns = oracledb.makedsn('10.31.8.21', '1521', service_name='ORABPI')
    conn = oracledb.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)
    c = conn.cursor()
    c.execute(query, params) if params else c.execute(query)
    result = c.fetchall()
    conn.close()
    return result

def searchbagfrommcs(yesterday, dest_condition):
    """
    根据日期和 DESTINATION 条件统计唯一 LPC 数量。
    """
    dt = datetime.strptime(yesterday, '%Y-%m-%d')
    start_ts = dt.strftime('%d-%m-%Y') + ' 00:00:00'
    end_ts = (dt + timedelta(days=1)).strftime('%d-%m-%Y') + ' 00:00:00'

    query = f"""
        SELECT
            COUNT(DISTINCT lpc) AS total 
        FROM
            WC_TRACKINGREPORT 
        WHERE
            1 = 1 
            AND EVENTTS > TO_TIMESTAMP(:start_ts, 'DD-MM-YYYY HH24:MI:SS')
            AND EVENTTS < TO_TIMESTAMP(:end_ts, 'DD-MM-YYYY HH24:MI:SS')
            AND {dest_condition}
            AND lpc IS NOT NULL
    """
    data = accessOracle(query, {'start_ts': start_ts, 'end_ts': end_ts})
    return data[0][0] if data else 0

def search_all_mcs(yesterday):
    """
    统计四个区间的行李数，返回字典。
    """
    dest_map = {
        '12': "DESTINATION BETWEEN 150 AND 162",
        '13': "DESTINATION BETWEEN 250 AND 262",
        '28': "DESTINATION BETWEEN 350 AND 362",
        '29': "DESTINATION BETWEEN 450 AND 462",
    }
    results = {}
    for name, condition in dest_map.items():
        count = searchbagfrommcs(yesterday, condition)
        results[name] = count
        logging.info(f"{name}: {count} 件")
    return results

# ---------- GUI 界面 ----------
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("早到行李统计 - 四区间")
        self.geometry("500x450")
        self.resizable(False, False)

        self.date_var = tk.StringVar()
        self.status_var = tk.StringVar()

        self.date_var.set((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        self.status_var.set("就绪")

        main_frame = ttk.Frame(self, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(main_frame, text="分析日期（YYYY-MM-DD）：").grid(row=0, column=0, sticky=tk.W, pady=3)
        ttk.Entry(main_frame, textvariable=self.date_var, width=15).grid(row=0, column=1, sticky=tk.W, padx=5)
        ttk.Label(main_frame, text="例如 2026-06-30", foreground="gray").grid(row=0, column=2, sticky=tk.W)

        ttk.Label(main_frame, text="统计结果：").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.log_text = scrolledtext.ScrolledText(main_frame, height=12, width=60, state='disabled')
        self.log_text.grid(row=2, column=0, columnspan=3, pady=5, padx=5)

        self.run_btn = ttk.Button(main_frame, text="开始统计所有早到线", command=self.start_analysis)
        self.run_btn.grid(row=3, column=0, pady=10, sticky=tk.W)

        ttk.Label(main_frame, textvariable=self.status_var, foreground="darkgreen").grid(row=3, column=1, columnspan=2, sticky=tk.W, padx=10)

    def log(self, msg):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, msg + '\n')
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def start_analysis(self):
        date_str = self.date_var.get().strip()
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            messagebox.showerror("输入错误", "日期格式无效，请使用 YYYY-MM-DD 格式")
            return

        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state='disabled')
        self.status_var.set("查询中，请稍候...")
        self.run_btn.config(state=tk.DISABLED)

        threading.Thread(target=self._run_task, args=(date_str,), daemon=True).start()

    def _run_task(self, date_str):
        try:
            results = search_all_mcs(date_str)
            self.after(0, lambda: self._finish(results))
        except Exception as e:
            self.after(0, lambda: self._error(str(e)))

    def _finish(self, results):
        """查询完成，显示四行结果，无弹窗"""
        self.log("===== 统计结果 =====")
        total_all = 0
        for name, count in results.items():
            self.log(f"{name}: {count} 件")
            total_all += count
        self.log(f"总计: {total_all} 件")
        self.status_var.set("统计完成")
        self.run_btn.config(state=tk.NORMAL)
        # 不再弹出 messagebox

    def _error(self, err_msg):
        self.status_var.set("发生错误，请查看日志")
        self.log(f"错误：{err_msg}")
        self.run_btn.config(state=tk.NORMAL)
        messagebox.showerror("错误", f"执行过程中发生异常：\n{err_msg}")
        logging.exception("GUI执行异常")

if __name__ == '__main__':
    app = Application()
    app.mainloop()