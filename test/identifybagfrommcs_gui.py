#!/usr/bin/python
# coding=utf-8

import oracledb
import logging
from datetime import datetime, timedelta
from collections import Counter
from openpyxl import Workbook
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import queue
import sys

# ---------- 自定义日志处理器：将日志消息放入队列 ----------
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

# ---------- 数据库操作 ----------
def accessOracle(query, params=None):
    dsn_tns = oracledb.makedsn('10.31.8.21', '1521', service_name='ORABPI')
    with oracledb.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
        with conn.cursor() as c:
            c.execute(query, params) if params else c.execute(query)
            return c.fetchall()

# ---------- 数据获取 ----------
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
    return accessOracle(sql, params)

def get_bag_data(yesterday, location):
    dt = datetime.strptime(yesterday, '%Y-%m-%d')
    dest_map = {'MCS01': "96", 'MCS02': "97", 'MCS03': "98", 'MCS04': "99"}
    condition = dest_map.get(location)
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
    return accessOracle(sql, params)

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
    return accessOracle(query, {'lpn': lpn, 'event_start': event_start, 'event_end': event_end})

# ---------- 核心分析 ----------
def run_analysis(yesterday, location):
    mcs_data = get_mcs_data(yesterday, location)
    bag_data = get_bag_data(yesterday, location)

    LOCATION_MAP = {
        '442-24.3.1': '西3B-H岛下',
        '438-20.27.1': '西4-E岛下',
        '434-21.27.1': '西5-E岛上',
        '453-22.5.1': '西1-H岛上',
        '20-6.5.1': '东1-A岛下',
        '16-6.22.1': '东2-B岛',
        '9-8.3.1': '东3B-C岛上',
        '12-7.4.1': '东3-A岛',
        '5-4.50.1': '东4-D岛上',
        '1-5.27.1': '东5-D岛下',
        '445-23.4.1': '西3-F岛',
        '449-22.22.1': '西2-G岛',
        '0013.20.01': '东FSC',
        'FSC': 'FSC',
    }

    def get_location_name(reg_loc, is_early):
        if is_early:
            return "Store"
        if reg_loc in LOCATION_MAP:
            return LOCATION_MAP[reg_loc]
        if str(reg_loc).upper().startswith("ATR"):
            return "FSC"
        if "MCS" in str(reg_loc).upper():
            return "MCS"
        return "未知"

    bag_map = {}
    for active, lpc in bag_data:
        key = int(lpc)
        if key not in bag_map:
            bag_map[key] = active

    lpc_map = {}
    total_mcs = len(mcs_data)
    for idx, (lpc, reg_loc) in enumerate(mcs_data, 1):
        key = int(lpc)
        logging.info(f"正在检查 LPC={key} ({idx}/{total_mcs})")
        result = judgebag(key, yesterday)
        is_early = (len(result) == 2 and result[1][0] < result[0][0])
        if key not in lpc_map:
            active = bag_map.get(key, 'NOREAD')
            lpc_map[key] = (str(key), active, reg_loc, is_early)

    # 补充 Bag 独有记录
    for key, active in bag_map.items():
        if key not in lpc_map:
            lpc_map[key] = (str(key), active, "FSC", False)

    combined = []
    for _, (display, active, reg_loc, is_early) in lpc_map.items():
        loc_name = get_location_name(reg_loc, is_early)
        combined.append({
            'lpc': display,
            'TYPE': active,
            'REGISTER_LOCATION': reg_loc,
            'LOCATION_NAME': loc_name
        })

    total = len(combined)
    stats = Counter(rec['TYPE'] for rec in combined)
    noread_locs = Counter()
    for rec in combined:
        if rec['TYPE'] == 'NOREAD' and rec['LOCATION_NAME']:
            noread_locs[rec['LOCATION_NAME']] += 1

    return combined, total, stats, noread_locs

# ---------- GUI 应用程序 ----------
class Application(tk.Tk):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        self.title("MCS行李统计与分析 (GUI)")
        self.geometry("700x500")
        self.resizable(True, True)

        # 主框架
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 顶部信息
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill=tk.X, pady=5)
        ttk.Label(info_frame, text="分析日期: 昨天 (自动)", font=('Arial', 10)).pack(side=tk.LEFT)
        ttk.Label(info_frame, text=" | 站点: MCS01~MCS04", font=('Arial', 10)).pack(side=tk.LEFT, padx=10)

        # 日志文本框
        self.log_text = scrolledtext.ScrolledText(main_frame, height=20, width=80, state='normal', font=('Courier New', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=5)

        # 按钮框架
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=5)

        self.start_btn = ttk.Button(btn_frame, text="开始分析", command=self.start_analysis)
        self.start_btn.pack(side=tk.LEFT, padx=5)

        self.clear_btn = ttk.Button(btn_frame, text="清空日志", command=self.clear_log)
        self.clear_btn.pack(side=tk.LEFT, padx=5)

        self.status_label = ttk.Label(btn_frame, text="就绪", foreground="green")
        self.status_label.pack(side=tk.RIGHT, padx=10)

        # 定时检查队列中的日志消息
        self.after(100, self.poll_log_queue)

    def log(self, msg):
        """向文本框追加日志"""
        self.log_text.insert(tk.END, msg + '\n')
        self.log_text.see(tk.END)

    def clear_log(self):
        self.log_text.delete(1.0, tk.END)

    def poll_log_queue(self):
        """从队列取出日志消息并显示"""
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log(msg)
        except queue.Empty:
            pass
        self.after(100, self.poll_log_queue)

    def start_analysis(self):
        """启动分析线程"""
        self.start_btn.config(state=tk.DISABLED)
        self.status_label.config(text="运行中...", foreground="blue")
        self.log("========== 开始分析 ==========")
        # 启动后台线程
        threading.Thread(target=self._run_task, daemon=True).start()

    def _run_task(self):
        """后台任务"""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            locations = ['MCS01', 'MCS02', 'MCS03', 'MCS04']
            all_station_data = []

            for loc in locations:
                logging.info(f"========== 开始处理 {loc} ==========")
                combined, total, stats, noread_locs = run_analysis(yesterday, loc)

                # 输出每条记录
                for rec in combined:
                    logging.info(f"{rec['lpc']} -> {rec['TYPE']} (REG:{rec['REGISTER_LOCATION']}, NAME:{rec['LOCATION_NAME']})")

                # 输出统计
                logging.info(f"--- {loc} 统计 ---")
                logging.info(f"总行李: {total}")
                logging.info("各TYPE分布:")
                for typ, cnt in stats.items():
                    logging.info(f"  {typ}: {cnt}")
                if noread_locs:
                    logging.info("NOREAD位置分布:")
                    for loc_name, cnt in noread_locs.items():
                        logging.info(f"  {loc_name}: {cnt}")

                # 保存站点Excel
                wb = Workbook()
                ws = wb.active
                ws.title = "LPC统计"
                ws.append(["LPC", "TYPE", "LOCATION_NAME"])
                for rec in combined:
                    ws.append([rec['lpc'], rec['TYPE'], rec['LOCATION_NAME']])
                wb.save(f"result_merged_{loc}_{yesterday}.xlsx")

                all_station_data.append((loc, combined))
                logging.info(f"========== {loc} 完成 ==========\n")

            # 生成NOREAD汇总
            logging.info("开始生成NOREAD汇总文件...")
            summary_data = []
            for loc, records in all_station_data:
                noread_records = [rec for rec in records if rec['TYPE'] == 'NOREAD']
                loc_counter = Counter(rec['LOCATION_NAME'] for rec in noread_records)
                for loc_name, cnt in loc_counter.items():
                    summary_data.append([loc, loc_name, cnt])

            if summary_data:
                summary_data.sort(key=lambda x: x[0])
                wb_summary = Workbook()
                ws_summary = wb_summary.active
                ws_summary.title = "NOREAD汇总"
                ws_summary.append(["MCS", "行李来源", "数量"])
                for row in summary_data:
                    ws_summary.append(row)
                summary_filename = f"NOREAD汇总_{yesterday}.xlsx"
                wb_summary.save(summary_filename)
                logging.info(f"NOREAD汇总文件已保存: {summary_filename}")
            else:
                logging.info("没有NOREAD记录，汇总文件未生成")

            logging.info("所有任务完成！")
            self.after(0, self._finish)
        except Exception as e:
            logging.exception("执行过程中发生异常")
            self.after(0, lambda: self._error(str(e)))

    def _finish(self):
        self.start_btn.config(state=tk.NORMAL)
        self.status_label.config(text="完成", foreground="green")
        self.log("========== 分析完成 ==========")

    def _error(self, err_msg):
        self.start_btn.config(state=tk.NORMAL)
        self.status_label.config(text="错误", foreground="red")
        self.log(f"错误: {err_msg}")
        messagebox.showerror("错误", f"执行异常:\n{err_msg}")

# ---------- 主入口 ----------
if __name__ == '__main__':
    # 创建日志队列
    log_queue = queue.Queue()
    # 配置根日志记录器，添加队列处理器
    root_logger = logging.getLogger()
    # 移除可能已有的处理器（避免重复）
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    # 添加文件处理器
    file_handler = logging.FileHandler('1.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'))
    root_logger.addHandler(file_handler)
    # 添加队列处理器
    queue_handler = QueueHandler(log_queue)
    queue_handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'))
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(logging.INFO)

    # 启动GUI
    app = Application(log_queue)
    app.mainloop()