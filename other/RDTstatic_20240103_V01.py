import subprocess
import datetime
import pandas as pd
import ftplib
import io

# 构建URL并下载文件
def download_files(transformed_date):
    base_url = "http://10.31.7.16/rawfiles/"
    data_dict = {}
    
    # 文件索引与变量名的映射
    file_var_mapping = {
        "01": "E1", "02": "E2", "03": "E3", "04": "E3B",
        "05": "E4", "06": "E5", "07": "W1", "08": "W2",
        "09": "W3", "10": "W3B", "11": "W4", "12": "W5"
    }
    
    for file_index in file_var_mapping:
        file_name = f"{transformed_date}{file_index}rw.dat"
        url = f"{base_url}{file_name}"
        
        # 使用curl下载文件
        command = f"curl -s {url}"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            # 将数据存储到内存中
            data_dict[file_var_mapping[file_index]] = stdout.decode('utf-8').split('\n')[5:]  # 跳过前五行
        else:
            print(f"下载文件{file_name}失败: {stderr.decode('utf-8')}")
    
    return data_dict

# 格式化数据
def format_data(data_dict):
    formatted_data = {}
    for var_name, data in data_dict.items():
        # 初始化存储格式化后的数据的字典
        formatted_data[var_name] = {
            "LPN": [],  # X列
            "LAS_LIST": [],  # O列
            "ANT_LIST": []  # AH列
        }
        for line in data:
            columns = line.split(';')
            # 确保有足够的列
            if len(columns) > 33:  # AH列索引为33，因此至少需要34列
                formatted_data[var_name]["LPN"].append(columns[23].zfill(10))  # 假设X列索引为23
                formatted_data[var_name]["LAS_LIST"].append(columns[14].zfill(24))  # 假设O列索引为14
                formatted_data[var_name]["ANT_LIST"].append(columns[33].zfill(4))  # 假设AH列索引为33
                
    return formatted_data

# 统计和分类数据
def process_data(formatted_data):
    results_list = []

    for var_name, data in formatted_data.items():
        results = {
            "Addess": var_name,
            "Bags": len(data["LPN"]),
            "read": 0,
            "No read": 0,
            "LAS read": 0,
            "ANT read": 0,
            "Only LAS": 0,
            "Only ANT": 0
        }

        for las, ant in zip(data["LAS_LIST"], data["ANT_LIST"]):
            las_read = '1' in las
            ant_read = '1' in ant

            if las_read or ant_read:
                results["read"] += 1
                if las_read and not ant_read:
                    results["Only LAS"] += 1
                elif not las_read and ant_read:
                    results["Only ANT"] += 1
                if las_read:  # 如果LAS列包含1
                    results["LAS read"] += 1
                if ant_read:  # 如果ANT列包含1
                    results["ANT read"] += 1
            else:
                results["No read"] += 1

        results_list.append(results)

    return results_list

# 输出结果到Excel函数，以将文件上传到FTP服务器
def output_to_excel(results_list, file_date):
    # 使用pandas创建DataFrame
    df = pd.DataFrame(results_list)
    # 构建文件名，包括日期
    file_name = f"RTD_{file_date}.xlsx"
    # 将DataFrame保存到Excel文件对象
    excel_file = io.BytesIO()
    df.to_excel(excel_file, index=False)
    # 重置文件指针到开始位置
    excel_file.seek(0)

    # # 连接到FTP服务器并上传文件
    # ftp_address = '10.31.7.16'
    # username = 'it'
    # password = 'itlog'
    # ftp_directory = 'RDT_LOG'
    # try:
    #     with ftplib.FTP(ftp_address, username, password) as ftp:
    #         ftp.cwd(ftp_directory)  # 切换到目标目录
    #         ftp.storbinary(f'STOR {file_name}', excel_file)  # 上传文件
    #         print(f"结果已上传到FTP服务器：{ftp_directory}/{file_name}")
    # except ftplib.all_errors as e:
    #     print(f"FTP错误：{e}")

# 主函数
def main():
    # 获取当前日期
    current_date = datetime.datetime.now()
    # 计算昨天的日期
    yesterday = current_date - datetime.timedelta(days=1)
    # 转换成所需的格式
    transformed_date = yesterday.strftime("%y%m%d")
    input_date = yesterday.strftime("%Y%m%d")

    data_dict = download_files(transformed_date)
    formatted_data = format_data(data_dict)
    print("数据下载和格式化完成。")

    # 处理数据
    results_list = process_data(formatted_data)
    print("数据处理完成。")
    for result in results_list:
        print(result)  # 打印每个变量的结果，以便查看

    # 输出结果到Excel，并上传到FTP服务器，传递昨天的日期
    output_to_excel(results_list, input_date)

if __name__ == "__main__":
    main()
