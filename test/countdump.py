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

def searchbagfrommcs(date_str, station_condition, station_name):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    start_ts = dt - timedelta(hours=8)
    end_ts = dt + timedelta(hours=16)

    # 公共基础条件（不含 ACTIVEPROCESS 限制）
    base_condition = f"""
        EVENTTS > :start_ts
        AND EVENTTS < :end_ts
        AND {station_condition}
        AND TARGETPROCESSID LIKE 'BSIS%'
        AND EXECUTEDTASK = 'Deregistration'
    """

    # 统计条件：对 In Time Build 要求 lpc 非空，其他类型不限
    count_condition = base_condition + """
        AND (ACTIVEPROCESS != 'In Time Build' OR (ACTIVEPROCESS = 'In Time Build' AND lpc IS NOT NULL))
    """

    # 日志条件：不加任何 ACTIVEPROCESS 限制
    log_condition = base_condition

    # 1. 统计查询
    query_count = f"""
        SELECT ACTIVEPROCESS, COUNT(*)
        FROM WC_PACKAGEINFO
        WHERE {count_condition}
        GROUP BY ACTIVEPROCESS
    """
    data_count = accessOracle(query_count, {'start_ts': start_ts, 'end_ts': end_ts})
    count_dict = {act: cnt for act, cnt in data_count}

    # 2. 补齐所有类型（含 0），并按固定顺序输出
    # all_types = [
    #     'Stop T3 in SAT', 'Garbage SAT', 'Garbage T3 East',
    #     'No Read', 'Dump System', 'Dump Identification',
    #     'In Time Build', 'Dump Flight Build',
    #     'Trace and Eject', 'Lateral_81', 'Multi Read', 'Lateral_41'
    # ]
    all_types = [
        'No Read', 'Dump Identification', 'In Time Build', 'Dump Flight Build','Multi Read','Unplanned flight'
    ]
    result = []
    for act in all_types:
        result.append((act, count_dict.get(act, 0)))

    # 3. 日志明细（记录所有符合条件的记录，不限 ACTIVEPROCESS）
    query_detail = f"""
        SELECT LPC, PID, ACTIVEPROCESS
        FROM WC_PACKAGEINFO
        WHERE {log_condition}
        ORDER BY ACTIVEPROCESS
    """
    data_detail = accessOracle(query_detail, {'start_ts': start_ts, 'end_ts': end_ts})
    for lpc, pid, act in data_detail:
        record_id = lpc if lpc is not None else pid
        logging.info(f"Station {station_name} - LPC/PID: {record_id}, ACTIVEPROCESS: {act}")

    return result

def search_all_mcs(date_str):
    station_map = {
        '41': "CURRENTSTATIONID IN (41)",
        '81': "CURRENTSTATIONID IN (81)",
        'SAT': "CURRENTSTATIONID IN (220, 221)"
    }
    results = {}
    for name, condition in station_map.items():
        results[name] = searchbagfrommcs(date_str, condition, name)
        total = sum(cnt for _, cnt in results[name])
        logging.info(f"{name} 站组总计: {total} 件")
    return results

# ---------- GUI 界面（未改动） ----------
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("弃包行李统计 - 分站组统计")
        self.geometry("550x500")
        self.resizable(False, False)

        self.date_var = tk.StringVar()
        self.status_var = tk.StringVar()
        self.date_var.set((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        self.status_var.set("就绪")

        main_frame = ttk.Frame(self, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(main_frame, text="分析日期（YYYY-MM-DD）：").grid(row=0, column=0, sticky=tk.W, pady=3)
        ttk.Entry(main_frame, textvariable=self.date_var, width=15).grid(row=0, column=1, sticky=tk.W, padx=5)
        ttk.Label(main_frame, text="例如 2026-07-10", foreground="gray").grid(row=0, column=2, sticky=tk.W)

        ttk.Label(main_frame, text="统计结果：").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.log_text = scrolledtext.ScrolledText(main_frame, height=14, width=70, state='disabled')
        self.log_text.grid(row=2, column=0, columnspan=3, pady=5, padx=5)

        self.run_btn = ttk.Button(main_frame, text="开始统计", command=self.start_analysis)
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
            err_msg = str(e)
            self.after(0, lambda: self._error(err_msg))

    def _finish(self, results):
        self.log("===== 统计结果 =====")
        grand_total = 0
        for station_name, data in results.items():
            self.log(f"【站组 {station_name}】")
            if not data:
                self.log("  无记录")
            else:
                station_total = 0
                for act, cnt in data:
                    self.log(f"  {act}: {cnt} 件")
                    station_total += cnt
                self.log(f"  小计: {station_total} 件")
                grand_total += station_total
            self.log("")
        self.log(f"总计: {grand_total} 件")
        self.status_var.set("统计完成")
        self.run_btn.config(state=tk.NORMAL)

    def _error(self, err_msg):
        self.status_var.set("发生错误，请查看日志")
        self.log(f"错误：{err_msg}")
        self.run_btn.config(state=tk.NORMAL)
        messagebox.showerror("错误", f"执行过程中发生异常：\n{err_msg}")
        logging.exception("GUI执行异常")

if __name__ == '__main__':
    app = Application()
    app.mainloop()