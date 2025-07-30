import requests
import datetime

def send_xml_file(xml_filepath, output_file):
    """
    发送 XML 文件到指定 URL，并保存到指定目录
    """

    url = "http://10.31.8.23:9900/DUMMY"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; en-US) WindowsPowerShell/5.1.14393.3053",
        "Content-Type": "application/xml",
        "Expect": "100-continue",
        "Host": "10.31.8.23:9900"
    }

    try:
        with open(xml_filepath, "r", encoding="utf-8") as file:
            xml_data = file.read()

        # 获取当前时间并格式化
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # ESTR_time = (datetime.datetime.now()+ datetime.timedelta(minutes=15)).strftime("%Y%m%d%H%M%S")

        # 替换 XML 中的字段（假设要替换 <DDTM> 字段）
        xml_data = xml_data.replace("__LOCAL_EVENTTIME__", f"{current_time}")
        # xml_data = xml_data.replace("__LOCAL_EVENTTIME__", f"{current_time}")
        

        headers["Content-Length"] = str(len(xml_data))

        response = requests.post(url, headers=headers, data=xml_data)

        if response.status_code == 200:
            print("XML 文件发送成功！")
            print("响应内容：", response.text)

            with open(output_file, "w", encoding="utf-8") as outfile:
                outfile.write(response.text)
            print(f"响应已保存到：{output_file}")

        else:
            print("XML 文件发送失败！")
            print("状态码：", response.status_code)
            print("响应内容：", response.text)

    except FileNotFoundError:
        print(f"错误：文件 {xml_filepath} 未找到。")
    except Exception as e:
        print(f"发生错误：{e}")

if __name__ == "__main__":

    xml_file = r"c://1//1//1.xml"
    output_file = r"c://1//1//output.txt"  
    send_xml_file(xml_file, output_file)