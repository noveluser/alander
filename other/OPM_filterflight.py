import os
import configparser


# 日志文件目录
# OPM_DIRECTORY = 'C:\\1\\TEMP\\OPM'
# 输出的Excel文件名
# OUTPUT_EXCEL_FILE = 'C:\\1\\TEMP\\OPM_DATA.log'

# # 航班的ffid号码
# flight = '26002977'


def main():
    config = configparser.ConfigParser()
    config.read('c:\\work\\conf\\opm_fliterflight.ini')
    sections = config.sections()
    for item in sections:
        # 遍历OPM目录下的所有文件
        infile_path = config.get(item, 'infile_path')
        OUTPUT_EXCEL_FILE = config.get(item, 'outputfile')
        flight = config.get(item, 'keyword') 
        OPM_DIRECTORY = infile_path
        for filename in os.listdir(OPM_DIRECTORY):
            file_path = os.path.join(OPM_DIRECTORY, filename)
            # 确保是文件而不是目录
            print(file_path)
            if os.path.isfile(file_path):
                # 逐行读取文件内容
                # with open(file_path, 'r', encoding='utf8') as file:
                with open(file_path, 'r') as infile, open(OUTPUT_EXCEL_FILE, 'w') as outfile: 
                    for line in infile:
                        if flight in line:
                            outfile.write(line)


if __name__ == "__main__":
    main()
