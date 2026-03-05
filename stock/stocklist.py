# import akshare as ak

# # 获取A股所有上市公司信息（code列是SECUCODE格式）
# stock_info = ak.stock_info_a_code_name()
# print(stock_info.head())

# # 查看数据结构
# print("列名：", stock_info.columns.tolist())
# print("\n示例数据：")
# for i in range(min(5, len(stock_info))):
#     print(f"代码: {stock_info.iloc[i]['code']}, 名称: {stock_info.iloc[i]['name']}")

# # 保存SECUCODE和名称
# result = stock_info[['code', 'name']]
# # result.to_csv('A股_SECUCODE列表.csv', index=False, encoding='utf-8-sig')

# file_path = r'd:\1\1\A股股票列表.xlsx'
# result.to_excel(file_path, index=False, engine='openpyxl')



import akshare as ak
import time
import pandas as pd

def get_stock_info_with_retry(max_retries=3, delay=5):
    for i in range(max_retries):
        try:
            print(f"第 {i+1} 次尝试获取数据...")
            df = ak.stock_info_a_code_name()
            return df
        except Exception as e:
            print(f"获取失败: {e}")
            if i < max_retries - 1:
                print(f"等待 {delay} 秒后重试...")
                time.sleep(delay)
            else:
                raise e

try:
    stock_info_a_code_name_df = get_stock_info_with_retry()
    print("数据获取成功！")
    
    # 保存到Excel
    file_path = r'd:\1\1\A股股票列表.xlsx'
    stock_info_a_code_name_df.to_excel(file_path, index=False, engine='openpyxl')
    print(f"数据已保存到：{file_path}")
except Exception as e:
    print(f"最终失败: {e}")