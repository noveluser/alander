# import xml.etree.ElementTree as ET


# def print_xml_file_lines(xml_filepath):
#     """
#     读取 XML 文件并逐行打印到控制台。

#     参数:
#     xml_filepath (str): XML 文件的路径。
#     """
#     try:
#         with open(xml_filepath, 'r', encoding='utf-8') as file:
#             for line in file:
#                 xml_fragment = line.replace("&lt;", "<").replace("&gt;", ">")
#                 # 如果你需要将其保存到文件：
#                 with open("c://1//1//formatted.xml", "a", encoding="utf-8") as f:
#                     f.write(xml_fragment)
#     except FileNotFoundError:
#         print(f"错误: 文件未找到: {xml_filepath}")
#     except Exception as e:
#         print(f"发生错误: {e}")

# # 示例用法
# if __name__ == "__main__":
#     xml_file_path = "c://1//1//2.xml"  # 替换成您的 XML 文件路径
#     print_xml_file_lines(xml_file_path)

import xml.etree.ElementTree as ET # 仍然导入 ET 库，即使在此版本中未使用，为未来可能的扩展做准备

def process_xml_lines_and_save(input_filepath, output_filepath, perform_entity_replace=True, append_output=False):
    """
    读取 XML 文件，逐行处理 (可选实体替换)，并将处理后的行写入到输出文件。

    参数:
    input_filepath (str): 输入 XML 文件的路径。
    output_filepath (str): 输出文件的路径。
    perform_entity_replace (bool): 是否执行 XML 实体替换 (&lt; to <, &gt; to >)。 默认为 True。
    append_output (bool): 是否以追加模式写入输出文件。默认为 False (写入模式，会覆盖已存在的文件)。
    """
    try:
        write_mode = "a" if append_output else "w" # 根据 append_output 参数决定写入模式 'a' (追加) 或 'w' (写入)

        with open(input_filepath, 'r', encoding='utf-8') as infile:
            with open(output_filepath, write_mode, encoding='utf-8') as outfile:
                for line in infile:
                    xml_fragment = line
                    if perform_entity_replace:
                        xml_fragment = xml_fragment.replace("&lt;", "<").replace("&gt;", ">")
                    outfile.write(xml_fragment)

        print(f"XML 文件已处理并保存到: {output_filepath}")

    except FileNotFoundError:
        print(f"错误: 输入文件未找到: {input_filepath}")
    except Exception as e:
        print(f"发生错误: {e}")

# 示例用法
if __name__ == "__main__":
    input_xml_file = "c://1//1//2.xml"
    output_xml_file = "c://1//1//processed_lines.xml" # 输出文件名更具描述性

    process_xml_lines_and_save(input_xml_file, output_xml_file) # 使用默认设置 (实体替换开启，写入模式)
    # process_xml_lines_and_save(input_xml_file, output_xml_file, perform_entity_replace=False) # 关闭实体替换
    # process_xml_lines_and_save(input_xml_file, output_xml_file, append_output=True) # 使用追加模式写入
    # process_xml_lines_and_save(input_xml_file, "c://temp//output_xml.xml", perform_entity_replace=True, append_output=False) #  更灵活的输出路径