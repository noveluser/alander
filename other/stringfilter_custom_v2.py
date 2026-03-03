#!/usr/bin/python
# coding=utf-8

# ATR log transfer
# author wangle
# v0.1

import os
import logging

# 配置日志输出
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//test.log',
    filemode='a'
)

# ===================== 可配置参数（根据需要修改） =====================
# 要搜索的目标目录（所有.log文件会被扫描）
SEARCH_DIR = r"D://1//2"
# 汇总输出的文件路径
OUTPUT_FILE = r"D://1//output//all.log"


def check_contain_keywords(line,keyword1,keyword2,keyword3):
    if keyword1 in line:
        if keyword2 in line or keyword3 in line:
            return line
    return ""

def get_all_log_files(dir_path):
    """
    获取指定目录下所有的.log文件（仅一级目录，不递归子目录）
    :param dir_path: 目标目录路径
    :return: 所有.log文件的完整路径列表
    """
    log_files = []
    # 检查目录是否存在
    if not os.path.isdir(dir_path):
        logging.error(f"指定的搜索目录不存在：{dir_path}")
        return log_files
    
    # 遍历目录下所有文件，筛选.log后缀
    for file_name in os.listdir(dir_path):
        file_full_path = os.path.join(dir_path, file_name)
        # 标准化路径（统一分隔符，解决\和/混用问题）
        file_full_path = os.path.normpath(file_full_path)
        # 只处理文件，且后缀为.log
        if os.path.isfile(file_full_path) and file_name.lower().endswith(".log"):
            log_files.append(file_full_path)
    
    logging.info(f"在目录 {dir_path} 下找到 {len(log_files)} 个.log文件")
    return log_files

def main():
    # 1. 获取目标目录下所有.log文件
    log_files = get_all_log_files(SEARCH_DIR)
    if not log_files:
        logging.warning("未找到任何.log文件，程序退出")
        return
    
    # 2. 确保输出目录存在（如果不存在则创建）
    output_dir = os.path.dirname(OUTPUT_FILE)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"创建输出目录：{output_dir}")
        
   # 3. 打开汇总输出文件（覆盖模式，每次运行清空原有内容；如需追加改为 'a'）
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_output:
        # 先写入一行标识，方便区分不同运行结果
        f_output.write(f"===== 搜索结果汇总 - {logging.Formatter('%(asctime)s').format(logging.LogRecord('', 0, '', 0, '', (), ()))} =====\n\n")
        
        # 4. 遍历所有.log文件，逐行检查并写入匹配内容
        for log_file in log_files:
            try:
                logging.info(f"开始处理文件：{log_file}")
                # 写入当前文件的标识（可选，方便溯源）
                f_output.write(f"--- 来自文件：{log_file} ---\n")
                
                # 读取log文件内容
                with open(log_file, "r", encoding='utf-8', errors='ignore') as f_read:
                    for line_num, line in enumerate(f_read, 1):
                        # 检查当前行是否包含关键词
                        matched_line = check_contain_keywords(line)
                        if matched_line:
                            # 可选：在输出行前添加文件名和行号（方便定位），不需要可删除这行
                            output_content = f"[{os.path.basename(log_file)}:行{line_num}] {matched_line}"
                            # 仅输出行内容（无额外标识）：output_content = matched_line
                            f_output.write(output_content)
                            f_output.flush()  # 立即刷新缓冲区
            
            except PermissionError:
                logging.error(f"无权限访问文件：{log_file}")
            except UnicodeDecodeError:
                logging.error(f"文件编码异常，无法读取：{log_file}（已跳过）")
            except Exception as e:
                logging.error(f"处理文件 {log_file} 出错：{str(e)}")
    
    logging.info(f"所有文件处理完成，匹配结果已输出到：{OUTPUT_FILE}")


if __name__ == "__main__":
    main()