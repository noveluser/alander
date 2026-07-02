#!/usr/bin/python
# coding=utf-8

import oracledb
import logging
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import queue

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

def searchbagfrommcs(yesterday, location):
    dt = datetime.strptime(yesterday, '%Y-%m-%d')
    start_ts = dt.strftime('%d-%m-%Y') + ' 00:00:00'
    end_ts = (dt + timedelta(days=1)).strftime('%d-%m-%Y') + ' 00:00:00'
    
    query = """
        SELECT DISTINCT lpc
        FROM FACT_BAG_SUMMARIES_V
        WHERE MCS_RECOGNITION IS NOT NULL
          AND MANUAL_SCAN_LOCATION = :location
          AND REGISTER_DT > TO_TIMESTAMP(:start_ts, 'DD-MM-YYYY HH24:MI:SS')
          AND REGISTER_DT < TO_TIMESTAMP(:end_ts, 'DD-MM-YYYY HH24:MI:SS')
    """
    data = accessOracle(query, {'start_ts': start_ts, 'end_ts': end_ts, 'location': location})
    logging.info("从MCS表中查询到 {} 个唯一LPC".format(len(data)))
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
    if data:
        logging.info("LPC {} 查询结果: {}".format(lpn, data))
    else:
        logging.info("LPC {} 未查询到符合条件的记录".format(lpn))
    return data

# ---------- 核心业务（支持进度回调） ----------
def run_analysis(yesterday, location, progress_callback=None):
    """
    执行完整分析，返回 (满足条件的LPC列表, 总行李数)
    """
    lpns = searchbagfrommcs(yesterday, location)
    to_mcs = []
    total = len(lpns)
    for idx, lpn_tuple in enumerate(lpns, 1):
        lpn = lpn_tuple[0]
        if progress_callback:
            progress_callback(lpn, idx, total, len(to_mcs))
        result = judgebag(lpn, yesterday)
        if len(result) == 2 and result[1][0] < result[0][0]:
            to_mcs.append(lpn)
    return to_mcs, total   # 返回元组

# ---------- GUI 界面 ----------
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("早到行李占比分析")
        self.geometry("500x450")
        self.resizable(False, False)
        
        # 变量
        self.date_var = tk.StringVar()
        self.location_var = tk.StringVar()
        self.current_lpn_var = tk.StringVar()
        self.status_var = tk.StringVar()
        
        # 默认值
        self.date_var.set((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        self.location_var.set("MCS03")
        self.current_lpn_var.set("等待开始...")
        self.status_var.set("就绪")
        
        # 布局
        main_frame = ttk.Frame(self, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 日期
        ttk.Label(main_frame, text="分析日期（YYYY-MM-DD）：").grid(row=0, column=0, sticky=tk.W, pady=3)
        ttk.Entry(main_frame, textvariable=self.date_var, width=15).grid(row=0, column=1, sticky=tk.W, padx=5)
        ttk.Label(main_frame, text="例如 2026-06-30", foreground="gray").grid(row=0, column=2, sticky=tk.W)
        
        # 位置
        ttk.Label(main_frame, text="MANUAL_SCAN_LOCATION：").grid(row=1, column=0, sticky=tk.W, pady=3)
        ttk.Entry(main_frame, textvariable=self.location_var, width=15).grid(row=1, column=1, sticky=tk.W, padx=5)
        ttk.Label(main_frame, text="如 MCS03, MCS04", foreground="gray").grid(row=1, column=2, sticky=tk.W)
        
        # 当前处理LPN显示
        ttk.Label(main_frame, text="当前处理：").grid(row=2, column=0, sticky=tk.W, pady=5)
        ttk.Label(main_frame, textvariable=self.current_lpn_var, foreground="blue", width=20).grid(row=2, column=1, columnspan=2, sticky=tk.W)
        
        # 进度日志文本框
        ttk.Label(main_frame, text="处理日志：").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.log_text = scrolledtext.ScrolledText(main_frame, height=8, width=60, state='disabled')
        self.log_text.grid(row=4, column=0, columnspan=3, pady=5, padx=5)
        
        # 按钮
        self.run_btn = ttk.Button(main_frame, text="开始执行", command=self.start_analysis)
        self.run_btn.grid(row=5, column=0, pady=10, sticky=tk.W)
        
        # 状态
        ttk.Label(main_frame, textvariable=self.status_var, foreground="darkgreen").grid(row=5, column=1, columnspan=2, sticky=tk.W, padx=10)
        
    def log(self, msg):
        """向日志文本框追加内容"""
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, msg + '\n')
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')
    
    def start_analysis(self):
        """按钮点击事件——启动后台线程"""
        date_str = self.date_var.get().strip()
        location = self.location_var.get().strip()
        if not location:
            messagebox.showerror("输入错误", "MANUAL_SCAN_LOCATION 不能为空")
            return
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            messagebox.showerror("输入错误", "日期格式无效，请使用 YYYY-MM-DD 格式")
            return
        
        # 清空日志
        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state='disabled')
        self.current_lpn_var.set("正在启动...")
        self.status_var.set("运行中，请稍候...")
        self.run_btn.config(state=tk.DISABLED)
        
        # 启动后台线程
        threading.Thread(target=self._run_task, args=(date_str, location), daemon=True).start()
    
    def _run_task(self, date_str, location):
        try:
            def progress_callback(lpn, current, total, found):
                self.after(0, lambda: self._update_progress(lpn, current, total, found))
            
            to_mcs, total = run_analysis(date_str, location, progress_callback)  # 接收总数
            self.after(0, lambda: self._finish(to_mcs, total))   # 传递总数
        except Exception as e:
            self.after(0, lambda: self._error(str(e)))
    
    def _update_progress(self, lpn, current, total, found):
        """更新当前LPN显示和日志"""
        self.current_lpn_var.set(f"{lpn}  ({current}/{total})")
        self.log(f"[{current}/{total}] 正在处理 LPC={lpn}")
        self.status_var.set(f"处理中 {current}/{total}")
    
    def _finish(self, to_mcs, total):
        """任务完成，写入文件并显示统计"""
        output_file = "result_to_mcs.txt"
        early_count = len(to_mcs)
        ratio = (early_count / total * 100) if total > 0 else 0.0
        
        with open(output_file, 'w') as f:
            # 写入汇总信息
            f.write(f"本次扫描总行李数: {total}, 早到行李: {early_count}, 占比: {ratio:.2f}%\n")
            # 逐行写入LPC
            for lpc in to_mcs:
                f.write(str(lpc) + '\n')
        
        self.current_lpn_var.set("已完成")
        self.status_var.set(f"完成，共 {early_count} 件早到行李（总{total}件，占比{ratio:.2f}%），已保存至 {output_file}")
        self.log(f"分析完成！总行李 {total} 件，早到 {early_count} 件，占比 {ratio:.2f}%")
        self.run_btn.config(state=tk.NORMAL)
        messagebox.showinfo("完成", f"分析完成！\n总行李：{total} 件\n早到行李：{early_count} 件\n占比：{ratio:.2f}%\n结果已保存至：{output_file}")
    
    def _error(self, err_msg):
        """发生错误"""
        self.current_lpn_var.set("错误")
        self.status_var.set("发生错误，请查看日志")
        self.log(f"错误：{err_msg}")
        self.run_btn.config(state=tk.NORMAL)
        messagebox.showerror("错误", f"执行过程中发生异常：\n{err_msg}")
        logging.exception("GUI执行异常")

if __name__ == '__main__':
    app = Application()
    app.mainloop()