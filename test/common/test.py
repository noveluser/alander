import xml.etree.ElementTree as ET

def correct_xml_string_formatted(xml_string):
    """
    将 XML 字符串更正为格式良好的 XML 文档字符串 (UTF-8 无 BOM)，并添加缩进。

    参数:
    xml_string (str): 输入的 XML 字符串。

    返回:
    str: 格式良好的 XML 文档字符串 (UTF-8 无 BOM)，带缩进。
         如果解析失败，则返回 None。
    """
    try:
        root = ET.fromstring(xml_string) # 解析 XML 字符串

        ET.indent(root, space="\t", level=0) # 添加缩进 (Python 3.9+)**

        corrected_xml_string = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8') # 序列化为 UTF-8 字符串 (无 BOM)
        return corrected_xml_string
    except ET.ParseError as e:
        print(f"XML 解析错误: {e}")
        print("请检查输入的 XML 字符串是否格式良好。")
        return None
    except Exception as e:
        print(f"发生未知错误: {e}")
        return None

# 示例 XML 字符串 (您提供的 XML 内容)
original_xml_string = """<?xml version="1.0" encoding="UTF-8"?><MSG><META><SNDR>HWCAROUSEL</SNDR><RCVR></RCVR><SEQN>1</SEQN><DDTM>20250303154024768</DDTM><TYPE>DFME</TYPE><STYP>BLLS</STYP><SORC>HWCAROUSEL</SORC><MGID>2d557d6e99534b84852202199c686ed7</MGID><RMID></RMID><APOT>ZGSZ</APOT></META><DFLT><FLID>28553361</FLID><FFID>ZH-240-20250303170500-A</FFID><FLTK>W/Z</FLTK><FATT>INT</FATT><BLLS><BELT><BTNO>1</BTNO><ID>9</ID><CODE>17</CODE><BTAT></BTAT><ESTR>20250303165500</ESTR><EEND>20250303175500</EEND><RSTR></RSTR><REND></REND><FBAG></FBAG><LBAG></LBAG><BTSC>T3</BTSC><EXNO></EXNO></BELT></BLLS></DFLT></MSG>"""

if __name__ == "__main__":
    corrected_xml = correct_xml_string_formatted(original_xml_string)

    if corrected_xml:
        print("更正后的 XML 文档 (带缩进):\n")
        print(corrected_xml)

        # 如果需要保存到文件，可以使用以下代码：
        output_file_path = "c://1//1//formatted_document.xml"
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(corrected_xml)
        print(f"\n更正后的 XML 已保存到文件: {output_file_path}")
    else:
        print("XML 字符串更正失败，请检查输入。")