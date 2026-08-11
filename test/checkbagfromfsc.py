#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

def extract_fields(line):
    """
    从一行日志中提取两个目标数字（第3、4个管道分隔字段）。
    条件：包含 "] - 20|" 和 "|1|"
    """
    if "] - 20|" not in line:
        return None
    if "|1|" not in line:
        return None

    # 按管道符拆分，去除每个字段首尾空格
    parts = [part.strip() for part in line.split('|')]
    # 确保至少有4个字段（索引0~3）
    if len(parts) < 4:
        return None

    # 返回第3个和第4个字段（索引2和3）
    return parts[2], parts[3]

def main():
    log_file = "/home/yunwei/eal/cm_hsc1/messages.xml"
    output_file = "extracted.log"

    # 清空或创建输出文件
    with open(output_file, 'w', encoding='utf-8') as f_out:
        f_out.write("提取结果（第一个数字 | 第二个数字）\n")
        f_out.write("=" * 50 + "\n")

    try:
        with open(log_file, 'r', encoding='utf-8') as f_in:
            for line_num, line in enumerate(f_in, 1):
                result = extract_fields(line)
                if result:
                    num1, num2 = result
                    output_line = f"{num1} | {num2}\n"
                    print(output_line.strip())
                    with open(output_file, 'a', encoding='utf-8') as f_out:
                        f_out.write(output_line)
        print(f"\n处理完成，结果已保存到 {output_file}")
    except FileNotFoundError:
        print(f"错误：文件 {log_file} 不存在，请检查路径。")
    except Exception as e:
        print(f"发生错误：{e}")

if __name__ == "__main__":
    main()