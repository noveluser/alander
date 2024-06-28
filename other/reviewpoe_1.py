#!/usr/bin/python
# coding=utf-8

# 暂定
# wangle
# v0.2

import tkinter as tk
import sqlite3
from my_mysql import Database

def main():
    """程序主函数"""

    # 数据库连接
    # conn = sqlite3.connect('my_poetry_db.db')  # 连接数据库文件
    # cursor = conn.cursor()
    cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
    current_line = 0

    def display_line(event=None):
        """显示用户选择的诗句."""
        global current_line

        # 获取输入框中的内容
        line_text = entry.get()

        if line_text:
            try:
                line_num = int(line_text)
                if 1 <= line_num <= 4:
                    current_line = line_num - 1
                    cursor.execute("SELECT content FROM poem WHERE ID=?", (current_line + 1,))
                    result = cursor.fetchone()
                    result_label.config(text=result[0])
                else:
                    result_label.config(text="输入错误，请输入 1-4 之间的数字。")
            except ValueError:
                result_label.config(text="输入错误，请输入数字。")
        else:
            current_line = (current_line + 1) % 4
            cursor.execute("SELECT content FROM jingyeshi WHERE ID=?", (current_line + 1,))
            result = cursor.fetchone()
            result_label.config(text=result[0])

        entry.delete(0, tk.END)

    # 创建主窗口
    window = tk.Tk()
    window.title("静夜思")
    window.geometry("400x200")

    # 创建标签和输入框
    label = tk.Label(window, text="请输入想要显示的诗句行数（1-4），回车显示下一句：")
    label.pack()

    entry = tk.Entry(window)
    entry.pack()
    entry.bind("<Return>", display_line)

    # 创建结果显示标签
    result_label = tk.Label(window, text="", font=("Arial", 36))
    result_label.pack()

    # 初始化显示第一行诗句
    cursor.execute("SELECT content FROM jingyeshi WHERE ID=1")
    result = cursor.fetchone()
    result_label.config(text=result[0])

    # 进入消息循环
    window.mainloop()

    # 关闭数据库连接
    conn.close()

# 如果是直接运行文件，则调用 main() 函数
if __name__ == "__main__":
    main()