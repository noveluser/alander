import os
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook, Workbook
import threading

# ========== 核心修改：获取脚本所在目录 ==========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # 脚本所在绝对路径
invalidDict ={
    "01":"RFID 6.5.90-P",
    "02":"SCAN 6.22.90-P",
    "03":"SCAN 7.4.90-P",
    "04":"SCAN 8.3.90-P",
    "05":"RFID 4.50.90-P",
    "06":"RFID 5.27.90-P",
    "07":"RFID 22.5.90-P",
    "08":"SCAN 22.22.90-P",
    "09":"SCAN 23.4.90-P",
    "10":"SCAN 24.3.90-P",
    "11":"RFID 20.27.90-P",
    "12":"RFID 21.27.90-P",
    "13":"BHS-3113-P",
    "14":"BHS-3114-P",
    "15":"BHS-3144-P",
    "16":"BHS-3143-P",
    "41":"SCAN 29.20.91 ATR270",
    "42":"SCAN 29.20.90 ATR270",
    "46":"SCAN 28.20.91 ATR270",
    "47":"SCAN 28.20.90 ATR270",
    "61":"RFMS-9-10-90",
    "62":"RFMS-14-17-90",
    "63":"RFMS-15-17-90",
    "64":"RFMS-33-23-90",
    "66":"SCAN 12.20.91 ATR270",
    "67":"SCAN 12.20.90 ATR270",
    "81":"RFMS-25-10-90",
    "82":"RFMS-30-17-90",
    "83":"RFMS-31-23-90",
    "85":"SCAN 13.11.90",
    "86":"SCAN 13.20.91 ATR270",
    "87":"SCAN 13.20.90 ATR270"
}

# 获取用户输入
date_str = input('请输入年月值（格式为YYYYMM）：')
year = int(date_str[:4])
month = int(date_str[4:])
month_str = datetime(year, month, 1).strftime('%Y年%m月')

# ========== 修改：RDT原始数据目录改为脚本所在目录 ==========
folder_path = os.path.join(SCRIPT_DIR, "RDT原始数据")
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

def download_data(i):
    values90 = pd.read_csv(
        r"http://10.31.7.16/statisticsfiles/sys{}statistics.dat".format(i), 
        header=5, sep=";")

    # 删除空格符号
    values90 = values90.applymap(lambda x: x.replace(' ', '') if isinstance(x, str) else x)

    # ========== 修改：下载文件保存到脚本目录下的RDT原始数据 ==========
    values90.to_excel(
        os.path.join(folder_path, "{}.xlsx".format(invalidDict[i])),
        index=False)
    print("已完成 {} ".format(invalidDict[i]))

# ========== 修改：读取脚本目录下RDT原始数据中的Excel文件 ==========
def get_excel_files():
    excel_files = []
    rdt_dir = os.path.join(SCRIPT_DIR, "RDT原始数据")
    for root, dirs, files in os.walk(rdt_dir):
        for file in files:
            if file.endswith('.xlsx'):
                excel_files.append(os.path.join(root, file))
    return excel_files

# 处理Excel文件
def process_excel_file(file_path, year, month):
    wb = load_workbook(file_path)
    ws = wb.active
    new_wb = Workbook()
    new_ws = new_wb.active
    
    # 复制首行
    for row in ws.iter_rows(min_row=1, max_row=1):
        for cell in row:
            new_ws[cell.coordinate].value = cell.value
    
    # 筛选日期为指定年月的数据
    for row in ws.iter_rows(min_row=2):
        date_str = row[0].value
        if date_str is not None:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            if date.year == year and date.month == month:
                new_row = []
                for cell in row:
                    # 根据单元格数据类型进行格式化输出
                    if isinstance(cell.value, str):
                        new_value = cell.value.replace(' ', '')
                    elif isinstance(cell.value, int):
                        new_value = cell.value
                    elif isinstance(cell.value, float):
                        new_value = "{:.2%}".format(cell.value)
                    else:
                        new_value = cell.value
                    new_row.append(new_value)
                new_ws.append(new_row)
    
    # ========== 修改：年月目录创建在脚本所在目录 ==========
    output_dir = os.path.join(SCRIPT_DIR, month_str)
    os.makedirs(output_dir, exist_ok=True)
    output_file_name = os.path.basename(file_path)
    output_file_path = os.path.join(output_dir, output_file_name)
    new_wb.save(output_file_path)
    print(f'{file_path} -> {output_file_path}')

# 创建线程
threads_download = []
for i in invalidDict:
    t = threading.Thread(target=download_data, args=(i,))
    threads_download.append(t)

# 启动所有线程
for t in threads_download:
    t.start()

# 等待所有线程执行完毕
for t in threads_download:
    t.join()

# 获取所有Excel文件
excel_files = get_excel_files()

# 处理每个Excel文件
threads_process = []
for file in excel_files:
    t = threading.Thread(target=process_excel_file, args=(file, year, month))
    threads_process.append(t)
    t.start()

# 等待所有线程执行完毕
for t in threads_process:
    t.join()

print("All threads finished")

import pandas as pd
import xlsxwriter

# 文件列表
rfid_list = [
    "RFID 4.50.90-P.xlsx",
    "RFID 5.27.90-P.xlsx",
    "RFID 6.5.90-P.xlsx",
    "SCAN 6.22.90-P.xlsx",
    "SCAN 7.4.90-P.xlsx",
    "SCAN 8.3.90-P.xlsx",
    "RFID 20.27.90-P.xlsx",
    "RFID 21.27.90-P.xlsx",
    "RFID 22.5.90-P.xlsx",
    "SCAN 22.22.90-P.xlsx",
    "SCAN 23.4.90-P.xlsx",
    "SCAN 24.3.90-P.xlsx"
]

# ========== 修改：RDT路径改为脚本目录下 ==========
RDT = os.path.join(SCRIPT_DIR, "RDT原始数据")
file_list_with_RDT = [os.path.join(RDT, file) for file in rfid_list]
# ========== 修改：年月目录路径改为脚本目录下 ==========
MON_DIR = os.path.join(SCRIPT_DIR, month_str)
file_list_with_MON = [os.path.join(MON_DIR, file) for file in rfid_list]

avreage_list = file_list_with_MON + file_list_with_RDT

# ========== 修改：rfid_list路径改为脚本目录下的年月目录 ==========
rfid_list = [os.path.join(MON_DIR, file) for file in rfid_list]

# ========== 修改：处理完毕数据目录创建在脚本所在目录 ==========
output_folder = os.path.join(SCRIPT_DIR, "处理完毕数据")
output_path = os.path.join(output_folder, month_str)
os.makedirs(output_path, exist_ok=True)

# 创建结果Excel文件
output_file = os.path.join(output_path, "BSO360综合读码.xlsx")
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 写入列名
columns = ['Address', 'Bags', 'Read_Rate', 'Las_Read_Rate', 'Las_Mult','Las1', 'Las2', 'Las3', 'Las4', 'Las5', 'Las6', 'Las7', 'Las8', 'Las9', 'Las10', 'Ant_Read_Rate','Ant_Mult','Ant1','Ant2','Ant3','Ant4']
columns_data = pd.DataFrame(columns=columns)
columns_data.to_excel(writer, sheet_name='Sheet1', index=False)

# 循环处理每个文件
for file in avreage_list:
    # 读取Excel文件
    df = pd.read_excel(file)

    # 计算总和
    Bags_sum = df.iloc[1:, [1, 3, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 39, 41, 62, 63, 64, 65]].sum()

    # 处理文件名
    if file in file_list_with_RDT:
        # 截取RDT目录后的文件名部分
        rel_path = os.path.relpath(file, RDT)
        List_name = 'Average-SHE-BSO-' + rel_path[:-7]
    elif file in file_list_with_MON:
        rel_path = os.path.relpath(file, MON_DIR)
        if rel_path.endswith("4.50.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (E4)'
        elif rel_path.endswith("5.27.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (E5)'
        elif rel_path.endswith("6.5.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (E1)'
        elif rel_path.endswith("6.22.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (E2)'
        elif rel_path.endswith("7.4.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (E3)'
        elif rel_path.endswith("8.3.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (E3B)'
        elif rel_path.endswith("20.27.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (W4)'
        elif rel_path.endswith("21.27.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (W5)'
        elif rel_path.endswith("22.5.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (W1)'
        elif rel_path.endswith("22.22.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (W2)'
        elif rel_path.endswith("23.4.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (W3)'
        elif rel_path.endswith("24.3.90-P.xlsx"):
               List_name = 'SHE-BSO-' + rel_path[:-7] + ' (W3B)'
    else:
        List_name = ''

    # 计算结果
    Bags = Bags_sum[0]
    Read_Rate = Bags_sum[1] / Bags_sum[0]
    Las_Read_Rate = Bags_sum[2] / Bags_sum[0]
    Las_Mult = Bags_sum[3] / Bags_sum[0]
    Las1 = Bags_sum[4] / Bags_sum[0]
    Las2 = Bags_sum[5] / Bags_sum[0]
    Las3 = Bags_sum[6] / Bags_sum[0]
    Las4 = Bags_sum[7] / Bags_sum[0]
    Las5 = Bags_sum[8] / Bags_sum[0]
    Las6 = Bags_sum[9] / Bags_sum[0]
    Las7 = Bags_sum[10] / Bags_sum[0]
    Las8 = Bags_sum[11] / Bags_sum[0]
    Las9 = Bags_sum[12] / Bags_sum[0]
    Las10 = Bags_sum[13] / Bags_sum[0]
    Ant_Read_Rate = Bags_sum[14] / Bags_sum[0]
    Ant_Mult = Bags_sum[15] / Bags_sum[0]
    Ant1 = Bags_sum[16] / Bags_sum[0]
    Ant2 = Bags_sum[17] / Bags_sum[0]
    Ant3 = Bags_sum[18] / Bags_sum[0]
    Ant4 = Bags_sum[19] / Bags_sum[0]

    # 构建数据
    data = pd.DataFrame({'Address': [List_name], 'Bags': [Bags],'Read_Rate': [Read_Rate],'Las_Read_Rate': [Las_Read_Rate], 'Las_Mult': [Las_Mult], 'Las1': [Las1], 'Las2': [Las2], 'Las3': [Las3], 'Las4': [Las4], 'Las5': [Las5], 'Las6': [Las6], 'Las7': [Las7], 'Las8': [Las8], 'Las9': [Las9], 'Las10': [Las10], 'Ant_Read_Rate': [Ant_Read_Rate], 'Ant_Mult': [Ant_Mult], 'Ant1': [Ant1], 'Ant2': [Ant2], 'Ant3': [Ant3], 'Ant4': [Ant4]})

    # 写入数据
    data.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=len(columns_data)+1)

    # 获取工作表对象
    worksheet = writer.sheets['Sheet1']

    # 设置第2列到最后一列的百分比格式
    percent_format = writer.book.add_format({'num_format': '0.00%'})
    worksheet.set_column(2, 20, None, percent_format)

    # 更新列名数据
    columns_data = pd.concat([columns_data, data])
    
    # 输出进度
    print(f"已处理文件: {file}")

# 保存并关闭Excel文件
writer.close()  # 替换writer._save()，更规范

print("RFID任务完成！")


# 定义循环列表
bso270_list = [
    "SCAN 12.20.90 ATR270.xlsx",
    "SCAN 12.20.91 ATR270.xlsx",
    "SCAN 13.20.90 ATR270.xlsx",
    "SCAN 13.20.91 ATR270.xlsx",
    "SCAN 28.20.90 ATR270.xlsx",
    "SCAN 28.20.91 ATR270.xlsx",
    "SCAN 29.20.90 ATR270.xlsx",
    "SCAN 29.20.91 ATR270.xlsx"
]

# ========== 修改：bso270_list路径改为脚本目录下的年月目录 ==========
bso270_list = [os.path.join(MON_DIR, file) for file in bso270_list]

# 创建新的Excel文件
output_file = os.path.join(output_path, "BSO270-月度.xlsx")
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 写入列名
columns = ['Address', 'Bags', 'Read-Rate', 'Las1', 'Las2', 'Las3', 'Las4', 'Las5', 'Las6', 'Las7', 'Las8', 'Las9', 'Las10']
columns_data = pd.DataFrame(columns=columns)
columns_data.to_excel(writer, sheet_name='Sheet1', index=False)

# 循环处理每个文件
for file in bso270_list:
    # 读取Excel文件
    df = pd.read_excel(file)
    
    # 计算总和
    total_sum = df.iloc[1:, [1, 3, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]].sum()
    
    # 处理文件名
    rel_path = os.path.relpath(file, MON_DIR)
    sheet_name = 'SHE-BSO-' + rel_path[:-11]
    
    # 计算结果
    total = total_sum[0]
    hybrid = total_sum[1] / total_sum[0]
    scan1 = total_sum[2] / total_sum[0]
    scan2 = total_sum[3] / total_sum[0]
    scan3 = total_sum[4] / total_sum[0]
    scan4 = total_sum[5] / total_sum[0]
    scan5 = total_sum[6] / total_sum[0]
    scan6 = total_sum[7] / total_sum[0]
    scan7 = total_sum[8] / total_sum[0]
    scan8 = total_sum[9] / total_sum[0]
    scan9 = total_sum[10] / total_sum[0]
    scan10 = total_sum[11] / total_sum[0]
    
    # 构建数据
    data = pd.DataFrame({'Address': [sheet_name], 'Bags': [total],'Read-Rate': [hybrid], 'Las1': [scan1], 'Las2': [scan2], 'Las3': [scan3], 'Las4': [scan4], 'Las5': [scan5], 'Las6': [scan6], 'Las7': [scan7], 'Las8': [scan8], 'Las9': [scan9], 'Las10': [scan10]})

    # 写入数据
    data.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=len(columns_data)+1)

    # 获取工作表对象
    worksheet = writer.sheets['Sheet1']

    # 设置第3到12列的百分比格式
    percent_format = writer.book.add_format({'num_format': '0.00%'})
    worksheet.set_column(2, 12, None, percent_format)

    # 更新列名数据
    columns_data = pd.concat([columns_data, data])
    
    # 输出进度
    print(f"已处理文件: {file}")

# 保存并关闭Excel文件
writer.close()

print("BSO270任务完成！")

# ========== 新增：生成BSO270.xlsx（每日读码明细+图表） ==========
# 定义bso270设备列表（与之前一致）
bso270_detail_list = [
    "SCAN 12.20.90 ATR270.xlsx",
    "SCAN 12.20.91 ATR270.xlsx",
    "SCAN 13.20.90 ATR270.xlsx",
    "SCAN 13.20.91 ATR270.xlsx",
    "SCAN 28.20.90 ATR270.xlsx",
    "SCAN 28.20.91 ATR270.xlsx",
    "SCAN 29.20.90 ATR270.xlsx",
    "SCAN 29.20.91 ATR270.xlsx"
]
# 拼接完整路径（年月目录下）
bso270_detail_list = [os.path.join(MON_DIR, file) for file in bso270_detail_list]

# 创建BSO270.xlsx文件
output_file = os.path.join(output_path, "BSO270.xlsx")
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 循环处理每个bso270设备文件
for file in bso270_detail_list:
    # 1. 处理工作表名称（参考BSO360命名规则）
    rel_path = os.path.relpath(file, MON_DIR)
    sheet_name = 'SHE-BSO-' + rel_path[:-11]  # 去掉.xlsx后缀（ATR270.xlsx共11个字符）
    
    # 2. 读取原始Excel文件
    df = pd.read_excel(file)
    
    # 3. 提取指定列：日期(0)、Bags(1)、Read-Rate(3)、Las1~Las10(13~22)
    columns_to_extract = [0, 1, 3] + list(range(13, 23))  # 0:日期 1:Bags 3:Read-Rate 13-22:Las1~Las10
    extracted_data = df.iloc[:, columns_to_extract]
    
    # 4. 列名重命名（统一格式）
    col_names = ['Date', 'Bags', 'Read-Rate'] + [f'Las{i}' for i in range(1, 11)]
    extracted_data.columns = col_names
    
    # 5. 计算百分比列（Las1~Las10 / Bags）
    for las_col in [f'Las{i}' for i in range(1, 11)]:
        extracted_data[las_col] = extracted_data[las_col] / extracted_data['Bags']
    # Read-Rate百分比转换
    extracted_data['Read-Rate'] = extracted_data['Read-Rate'] / extracted_data['Bags']
    
    # 6. 写入工作表
    extracted_data.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # 7. 设置百分比格式（Read-Rate + Las1~Las10列）
    worksheet = writer.sheets[sheet_name]
    percent_format = writer.book.add_format({'num_format': '0.00%'})
    worksheet.set_column(2, 12, None, percent_format)  # 2:Read-Rate 3-12:Las1~Las10
    
    # 8. 添加簇状柱形图（参考BSO360图表逻辑）
    chart = writer.book.add_chart({'type': 'column'})
    chart.set_title({'name': f'{sheet_name} 每日读码率'})
    chart.set_x_axis({'name': '日期', 'visible': True, 'name_visible': True})
    chart.set_y_axis({'name': '读码率', 'num_format': '0.00%'})
    
    # 添加数据系列（Read-Rate + Las1~Las10）
    for col_idx in range(2, 13):  # 2:Read-Rate 3-12:Las1~Las10
        chart.add_series({
            'name': [sheet_name, 0, col_idx],  # 列名
            'categories': [sheet_name, 1, 0, len(extracted_data), 0],  # x轴：日期
            'values': [sheet_name, 1, col_idx, len(extracted_data), col_idx],  # y轴：读码率
        })
    
    # 插入图表到工作表（G2位置）
    worksheet.insert_chart('G2', chart, {'x_scale': 2, 'y_scale': 1.5})  # 放大图表适配数据
    
    # 输出进度
    print(f"已处理BSO270明细文件: {file}")

# 保存并关闭文件
writer.close()

print("BSO270.xlsx生成完成！")

# 创建结果Excel文件
output_file = os.path.join(output_path, "BSO360.xlsx")
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 循环处理每个文件
for file in rfid_list:
    # 处理文件名作为工作表名
    rel_path = os.path.relpath(file, MON_DIR)
    if rel_path.endswith("4.50.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (E4)'
    elif rel_path.endswith("5.27.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (E5)'
    elif rel_path.endswith("6.5.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (E1)'
    elif rel_path.endswith("6.22.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (E2)'
    elif rel_path.endswith("7.4.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (E3)'
    elif rel_path.endswith("8.3.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (E3B)'
    elif rel_path.endswith("20.27.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (W4)'
    elif rel_path.endswith("21.27.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (W5)'
    elif rel_path.endswith("22.5.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (W1)'
    elif rel_path.endswith("22.22.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (W2)'
    elif rel_path.endswith("23.4.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (W3)'
    elif rel_path.endswith("24.3.90-P.xlsx"):
        sheet_name = 'SHE-BSO-' + rel_path[:-7] + ' (W3B)'
    else:
        sheet_name = ''
    
    # 读取Excel文件
    df = pd.read_excel(file)
    
    # 提取指定列的数据
    columns_to_extract = [0, 1, 3, 10, 39]
    extracted_data = df.iloc[:, columns_to_extract]

    #修改第二列列名为Bags
    extracted_data.columns.values[1] = "Bags"
    
    # 添加SCAN1列和SCAN2列
    extracted_data["Read-Rate"] = extracted_data.iloc[:, 2] / extracted_data.iloc[:, 1]
    extracted_data["Las"] = extracted_data.iloc[:, 3] / extracted_data.iloc[:, 1]
    extracted_data["Ant"] = extracted_data.iloc[:, 4] / extracted_data.iloc[:, 1]

    # 删除2、3、4列
    extracted_data = extracted_data.drop(columns=[extracted_data.columns[2], extracted_data.columns[3], extracted_data.columns[4]])

    # 将数据写入工作表
    extracted_data.to_excel(writer, sheet_name=sheet_name, index=False)

    # 获取工作表对象
    worksheet = writer.sheets[sheet_name]

    # 设置第3、4、5列的百分比格式
    percent_format = writer.book.add_format({'num_format': '0.00%'})
    worksheet.set_column(2, 4, None, percent_format)

    # 添加簇状柱形图 - 核心修改：去掉Bags列，只展示Read-Rate、Las、Ant
    chart = writer.book.add_chart({'type': 'column'})
    chart.set_title({'name': sheet_name})
    chart.set_x_axis({'name': extracted_data.columns[0], 'visible': True, 'name_visible': False})

    # 只添加Read-Rate、Las、Ant列的数据系列（对应列索引2、3、4）
    for i in range(2, 5):
        chart.add_series({
            'name': [sheet_name, 0, i],
            'categories': [sheet_name, 1, 0, len(extracted_data), 0],
            'values': [sheet_name, 1, i, len(extracted_data), i],
        })

    # 将图表插入工作表
    worksheet.insert_chart('G2', chart)

    # 输出脚本的工作进度
    print(f"已处理文件: {file}")
    
# 保存并关闭Excel文件
writer.close()

print("BSO360任务完成！")

# 定义循环列表
rfms_list = [
    "RFMS-9-10-90.xlsx",
    "RFMS-14-17-90.xlsx",
    "RFMS-15-17-90.xlsx",
    "RFMS-33-23-90.xlsx",
    "RFMS-25-10-90.xlsx",
    "RFMS-30-17-90.xlsx",
    "RFMS-31-23-90.xlsx"
]

# ========== 修改：rfms_list路径改为脚本目录下的年月目录 ==========
rfms_list = [os.path.join(MON_DIR, file) for file in rfms_list]

# 创建新的Excel文件
output_file = os.path.join(output_path, "RFMS-月度.xlsx")
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 写入列名
columns = ['Address', 'Bags', 'Read-Rate', 'Ant1', 'Ant2', 'Ant3', 'Ant4']
columns_data = pd.DataFrame(columns=columns)
columns_data.to_excel(writer, sheet_name='Sheet1', index=False)

# 循环处理每个文件
for file in rfms_list:
    # 读取Excel文件
    df = pd.read_excel(file)
    
    # 计算总和
    total_sum = df.iloc[1:, [1, 3, 62, 63, 64, 65]].sum()
    
    # 处理文件名
    rel_path = os.path.relpath(file, MON_DIR)
    sheet_name = 'SHE-BSO-' + rel_path[:-5]
    
    # 计算结果
    total = total_sum[0]
    hybrid = total_sum[1] / total_sum[0]
    rf1 = total_sum[2] / total_sum[0]
    rf2 = total_sum[3] / total_sum[0]
    rf3 = total_sum[4] / total_sum[0]
    rf4 = total_sum[5] / total_sum[0]
    
    # 构建数据
    data = pd.DataFrame({'Address': [sheet_name], 'Bags': [total],'Read-Rate': [hybrid], 'Ant1': [rf1], 'Ant2': [rf2], 'Ant3': [rf3], 'Ant4': [rf4]})

    # 写入数据
    data.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=len(columns_data)+1)

    # 获取工作表对象
    worksheet = writer.sheets['Sheet1']

    # 设置第3到6列的百分比格式
    percent_format = writer.book.add_format({'num_format': '0.00%'})
    worksheet.set_column(2, 6, None, percent_format)

    # 更新列名数据
    columns_data = pd.concat([columns_data, data])
    
    # 输出进度
    print(f"已处理文件: {file}")

# 保存并关闭Excel文件
writer.close()

print("所有任务完成！")