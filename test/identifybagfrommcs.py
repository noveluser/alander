#!/usr/bin/python
# coding=utf-8

import oracledb
import os
import logging
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
from collections import Counter
from openpyxl import Workbook

# ---------- 日志配置 ----------
log_dir = os.getcwd()  # 获取当前工作目录（即 result_merged.xlsx 保存位置）
log_file = os.path.join(log_dir, '1.log')
os.makedirs(log_dir, exist_ok=True)  # 确保目录存在（当前目录通常已存在，但安全起见）

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=log_file,
                    filemode='a')

# ---------- 数据库基础操作 ----------
def accessOracle(query, params=None):
    dsn_tns = oracledb.makedsn('10.31.8.21', '1521', service_name='ORABPI')
    with oracledb.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
        with conn.cursor() as c:
            c.execute(query, params) if params else c.execute(query)
            return c.fetchall()

# ---------- 数据获取函数 ----------
def get_mcs_data(yesterday, location):
    dt = datetime.strptime(yesterday, '%Y-%m-%d')
    start_ts = dt.strftime('%d-%m-%Y') + ' 00:00:00'
    end_ts = (dt + timedelta(days=1)).strftime('%d-%m-%Y') + ' 00:00:00'
    sql = """
        SELECT DISTINCT lpc, REGISTER_LOCATION
        FROM FACT_BAG_SUMMARIES_V
        WHERE MCS_RECOGNITION IS NOT NULL
          AND MANUAL_SCAN_LOCATION = :location
          AND REGISTER_DT > TO_TIMESTAMP(:start_ts, 'DD-MM-YYYY HH24:MI:SS')
          AND REGISTER_DT < TO_TIMESTAMP(:end_ts, 'DD-MM-YYYY HH24:MI:SS')
    """
    params = {'start_ts': start_ts, 'end_ts': end_ts, 'location': location}
    data = accessOracle(sql, params)
    logging.info(f"从MCS表查询到 {len(data)} 条记录（含REGISTER_LOCATION）")
    return data

def get_bag_data(yesterday, location):
    dt = datetime.strptime(yesterday, '%Y-%m-%d')
    dest_map = {'MCS01': "96", 'MCS02': "97", 'MCS03': "98", 'MCS04': "99"}
    condition = dest_map.get(location)
    if condition is None:
        logging.warning(f"未知的location: {location}，使用NULL条件")
    event_start = (dt - timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    event_end = (dt + timedelta(hours=16)).strftime('%Y-%m-%d %H:%M:%S')
    sql = """
        SELECT ACTIVEPROCESS, lpc
        FROM WC_PACKAGEINFO INFO
        LEFT JOIN DIM_STATIONS ON id = CURRENTSTATIONID
        WHERE INFO.EXECUTEDTASK = 'Deregistration'
          AND INFO.EVENTTS > to_timestamp(:event_start, 'YYYY-MM-DD HH24:MI:SS')
          AND INFO.EVENTTS < to_timestamp(:event_end, 'YYYY-MM-DD HH24:MI:SS')
          AND END_USER_ID = :condition
          AND INFO.TARGETPROCESSID = 'BSIS_03997185'
          AND lpc IS NOT NULL
        ORDER BY info.key
    """
    params = {'event_start': event_start, 'event_end': event_end, 'condition': condition}
    data = accessOracle(sql, params)
    logging.info(f"从包裹表查询到 {len(data)} 条记录")
    return data

def judgebag(lpn, yesterday):
    base = datetime.strptime(yesterday, '%Y-%m-%d')
    event_start = base - timedelta(hours=8)
    event_end = base + timedelta(hours=16)
    
    query = """
        SELECT IDEVENT, EXECUTEDTASK
        FROM (
            SELECT IDEVENT, EXECUTEDTASK,
                   ROW_NUMBER() OVER (PARTITION BY EXECUTEDTASK ORDER BY IDEVENT DESC) AS rn
            FROM WC_PACKAGEINFO
            WHERE EVENTTS >= :event_start
              AND EVENTTS < :event_end 
              AND TARGETPROCESSID LIKE 'BSIS%'
              AND LPC = :lpn
              AND EXECUTEDTASK IN ('ManualScan', 'Store')   
        )
        WHERE rn = 1
        ORDER BY EXECUTEDTASK
    """
    data = accessOracle(query, {'lpn': lpn, 'event_start': event_start, 'event_end': event_end})
    return data

def run_analysis(yesterday, location, progress_callback=None):
    mcs_data = get_mcs_data(yesterday, location)
    bag_data = get_bag_data(yesterday, location)

    LOCATION_MAP = {
        'ATR08': 'FSC',
        '442-24.3.1': '西3B-H岛下',
        'ATR04': 'FSC',
        '438-20.27.1': '西4-E岛下',
        '434-21.27.1': '西5-E岛上',
        'MCSIn407': 'MCS',
        '453-22.5.1': '西1-H岛上',
        'ATR01': 'FSC',
        'ATR02': 'FSC',
        'ATR03': 'FSC',
        'ATR07': 'FSC', 
        '20-6.5.1': '东1-A岛下',
        '16-6.22.1': '东2-B岛',
        '9-8.3.1': '东3B-C岛上', 
        '12-7.4.1': '东3-A岛', 
        '5-4.50.1': '东4-D岛上', 
        '1-5.27.1': '东5-D岛下', 
        '445-23.4.1': '西3-F岛',
        '449-22.22.1': '西2-G岛',
        '0013.20.01': '东FSC',
        'FSC': 'FSC',          # 映射自身，保留显示
        # 不再需要 'store' 键，因为早到会单独处理
    }

    # 构建 bag 映射（ACTIVEPROCESS）
    bag_map = {}
    for active, lpc in bag_data:
        key = int(lpc)
        if key not in bag_map:
            bag_map[key] = active

    lpc_map = {}  # key -> (display_lpc, active, original_reg_loc, is_early)
    total_mcs = len(mcs_data)

    # 处理 MCS 数据
    for idx, (lpc, reg_loc) in enumerate(mcs_data, 1):
        key = int(lpc)
        if progress_callback:
            progress_callback(str(key), idx, total_mcs, 0)

        # 早到判断
        result = judgebag(key, yesterday)
        is_early = (len(result) == 2 and result[1][0] < result[0][0])  # 早到标志

        if key not in lpc_map:
            active = bag_map.get(key, 'NOREAD')
            display = str(key)
            lpc_map[key] = (display, active, reg_loc, is_early)   # 保存原始 reg_loc 和早到标志

    # 补充 Bag 独有记录（无早到判断，默认非早到）
    for key, active in bag_map.items():
        if key not in lpc_map:
            display = str(key)
            lpc_map[key] = (display, active, "FSC", False)   # 原始 reg_loc 固定为 "FSC"，非早到

    # 生成最终记录列表：保留原始 reg_loc，新增 LOCATION_NAME
    combined = []
    for _, (display, active, reg_loc, is_early) in lpc_map.items():
        # 早到行李 -> LOCATION_NAME 固定为 "Store"
        if is_early:
            loc_name = "Store"
        else:
            loc_name = LOCATION_MAP.get(reg_loc, '未知')
        combined.append({
            'lpc': display,
            'ACTIVEPROCESS': active,          # Excel 中列名改为 TYPE
            'REGISTER_LOCATION': reg_loc,     # 保留原始值
            'LOCATION_NAME': loc_name
        })

    # ... 后续统计、日志、返回值不变 ...

    total = len(combined)
    stats = Counter(rec['ACTIVEPROCESS'] for rec in combined)

    # NOREAD 的 location 统计（使用映射后的名称）
    NOREAD_locs = Counter()
    for rec in combined:
        if rec['ACTIVEPROCESS'] == 'NOREAD' and rec['LOCATION_NAME']:
            NOREAD_locs[rec['LOCATION_NAME']] += 1

    logging.info(f"合并后总记录数: {total}")
    logging.info(f"各 ACTIVEPROCESS 统计: {dict(stats)}")
    logging.info(f"NOREAD 中 LOCATION_NAME 分布: {dict(NOREAD_locs)}")
    logging.info("完整记录（LPC, ACT, 原始REG, 映射名称）:")
    for rec in combined:
        logging.info(f"  {rec['lpc']} -> {rec['ACTIVEPROCESS']} (REG:{rec['REGISTER_LOCATION']}, NAME:{rec['LOCATION_NAME']})")

    if progress_callback:
        # 完成进度
        progress_callback("完成", total, total, 0)

    return combined, total, stats, NOREAD_locs

# ---------- GUI 界面 ----------
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MCS行李统计与分析")
        self.geometry("500x450")
        self.resizable(False, False)

        self.date_var = tk.StringVar(value=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        self.location_var = tk.StringVar(value="MCS04")
        self.current_lpn_var = tk.StringVar(value="等待开始...")
        self.status_var = tk.StringVar(value="就绪")

        main = ttk.Frame(self, padding=20)
        main.pack(fill=tk.BOTH, expand=True)

        # 日期
        ttk.Label(main, text="分析日期（YYYY-MM-DD）：").grid(row=0, column=0, sticky=tk.W, pady=3)
        ttk.Entry(main, textvariable=self.date_var, width=15).grid(row=0, column=1, sticky=tk.W, padx=5)
        tk.Label(main, text="例如 2026-06-30", fg="gray").grid(row=0, column=2, sticky=tk.W)

        # 位置
        ttk.Label(main, text="MANUAL_SCAN_LOCATION：").grid(row=1, column=0, sticky=tk.W, pady=3)
        ttk.Entry(main, textvariable=self.location_var, width=15).grid(row=1, column=1, sticky=tk.W, padx=5)
        tk.Label(main, text="如 MCS03, MCS04", fg="gray").grid(row=1, column=2, sticky=tk.W)

        # 当前处理
        ttk.Label(main, text="当前处理：").grid(row=2, column=0, sticky=tk.W, pady=5)
        tk.Label(main, textvariable=self.current_lpn_var, fg="blue", width=20).grid(row=2, column=1, columnspan=2, sticky=tk.W)

        # 日志
        ttk.Label(main, text="处理日志：").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.log_text = scrolledtext.ScrolledText(main, height=8, width=60, state='disabled')
        self.log_text.grid(row=4, column=0, columnspan=3, pady=5, padx=5)

        # 按钮和状态
        self.run_btn = ttk.Button(main, text="开始执行", command=self.start_analysis)
        self.run_btn.grid(row=5, column=0, pady=10, sticky=tk.W)
        tk.Label(main, textvariable=self.status_var, fg="darkgreen").grid(row=5, column=1, columnspan=2, sticky=tk.W, padx=10)

    def log(self, msg):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, msg + '\n')
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def start_analysis(self):
        date_str = self.date_var.get().strip()
        location = self.location_var.get().strip()
        if not location:
            messagebox.showerror("错误", "位置不能为空")
            return
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            messagebox.showerror("错误", "日期格式无效，应为 YYYY-MM-DD")
            return

        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state='disabled')
        self.current_lpn_var.set("正在启动...")
        self.status_var.set("运行中...")
        self.run_btn.config(state=tk.DISABLED)
        threading.Thread(target=self._run_task, args=(date_str, location), daemon=True).start()

    def _run_task(self, date_str, location):
        try:
            def cb(lpn, cur, total, found):
                self.after(0, lambda: self._update_progress(lpn, cur, total, found))
            combined, total, stats, NOREAD_locs = run_analysis(date_str, location, cb)
            self.after(0, lambda: self._finish(combined, total, stats, NOREAD_locs))
        except Exception as e:
            self.after(0, lambda: self._error(str(e)))

    def _update_progress(self, lpn, cur, total, found):
        self.current_lpn_var.set(f"{lpn} ({cur}/{total})")
        self.log(f"[{cur}/{total}] 处理 LPC={lpn}")
        self.status_var.set(f"处理中 {cur}/{total}")

    def _finish(self, combined, total, stats, NOREAD_locs):
        wb = Workbook()
        ws = wb.active
        ws.title = "LPC统计"
        # 表头：仅三列
        ws.append(["LPC", "TYPE", "LOCATION_NAME"])
        for rec in combined:
            ws.append([rec['lpc'], rec['ACTIVEPROCESS'], rec['LOCATION_NAME']])
        wb.save("result_merged.xlsx")

        self.current_lpn_var.set("已完成")
        self.status_var.set(f"完成，共 {total} 件，已保存 result_merged.xlsx")
        self.log(f"分析完成！总行李 {total} 件")
        self.log("各 TYPE（原ACTIVEPROCESS）统计：")
        for process, count in stats.items():
            self.log(f"  {process}: {count}")
        if NOREAD_locs:
            self.log("NOREAD 中 LOCATION_NAME 分布：")
            for loc, cnt in NOREAD_locs.items():
                self.log(f"  {loc}: {cnt}")
        self.log("结果已保存至 result_merged.xlsx")
        self.run_btn.config(state=tk.NORMAL)

    def _error(self, err_msg):
        self.current_lpn_var.set("错误")
        self.status_var.set("发生错误")
        self.log(f"错误：{err_msg}")
        self.run_btn.config(state=tk.NORMAL)
        messagebox.showerror("错误", f"执行异常：\n{err_msg}")
        logging.exception("GUI执行异常")

if __name__ == '__main__':
    app = Application()
    app.mainloop()