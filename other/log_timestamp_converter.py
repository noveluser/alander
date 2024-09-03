#!/usr/bin/python
# coding=utf-8

# 转换FSC日志的时间戳为正常时间格式
# 
# 


from datetime import datetime, timezone
import os


# Specify input and output folder paths
input_folder = r"C:\1\temp"
output_folder = r"C:\1\temp\output"

# Ensure the output folder exists, create it if it doesn't
os.makedirs(output_folder, exist_ok=True)


def convert_timestamp(log_timestamp):
    try:
        timestamp_float = float(log_timestamp)
        datetime_obj = datetime.fromtimestamp(timestamp_float, timezone.utc)
        formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return formatted_datetime
    except ValueError:
        print(f"Warning: Unable to convert timestamp {log_timestamp} to float.")
        return log_timestamp

def process_log_file(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as input_file, \
         open(output_path, 'w', encoding='utf-8') as output_file:
        
        # Skip the first line
        next(input_file, None)
        
        # Process each line
        for line in input_file:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            parts = line.split(' ')
            log_timestamp = parts[0]
            formatted_datetime = convert_timestamp(log_timestamp)
            updated_line = line.replace(log_timestamp, formatted_datetime)
            output_file.write(updated_line + '\n')



if __name__ == "__main__":
    # Process each .log file
    for filename in os.listdir(input_folder):
        if filename.endswith('.log'):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)
            process_log_file(input_path, output_path)
    print("Conversion completed")
