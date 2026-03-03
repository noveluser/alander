import akshare as ak
import pandas as pd

# 股票代码（去掉SZ，直接写数字）
stock_code = "SZ300206"

# stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol="SH600519")
# stock_profit_sheet_by_report_em_df = ak.stock_profit_sheet_by_report_em(symbol="SH600519")
# stock_cash_flow_sheet_by_report_em_df = ak.stock_cash_flow_sheet_by_report_em(symbol="SH600519")

# 1. 下载 资产负债表（取：净资产）
print("正在下载：资产负债表")
# balance = ak.stock_balance_sheet_by_report_em(symbol=stock_code)
balance = pd.read_excel(f"d:\\1\\1\\{stock_code}_季度资产数据.xlsx")
balance = balance[["REPORT_DATE", "FIXED_ASSET", "TOTAL_ASSETS", "TOTAL_LIABILITIES", "SHARE_CAPITAL", "TOTAL_EQUITY"]].copy()
# balance = ak.stock_financial_report_sina(stock=stock_code, symbol="资产负债表")
# balance = balance[["报告日", "固定资产净值", "资产总计", "负债合计", "实收资本(或股本)", "实收资本(或股本)", "所有者权益(或股东权益)合计"]].copy()

# 2. 下载 利润表（取：净利润）
print("正在下载：利润表")
income = pd.read_excel(f"d:\\1\\1\\{stock_code}_季度财务数据.xlsx")
# income = ak.stock_profit_sheet_by_report_em(symbol=stock_code)
# income = ak.stock_financial_report_sina(stock=stock_code, symbol="利润表")
income = income[["REPORT_DATE", "OPERATE_INCOME", "DEDUCT_PARENT_NETPROFIT"]].copy()

# # 3. 下载 现金流量表（取：经营现金流）
print("正在下载：现金流量表")
# cash =  ak.stock_cash_flow_sheet_by_report_em(symbol=stock_code)
cash = pd.read_excel(f"d:\\1\\1\\{stock_code}_季度现金数据.xlsx")
cash = cash[["REPORT_DATE", "NETCASH_OPERATE", "CONSTRUCT_LONG_ASSET"]].copy()
# cash = ak.stock_financial_report_sina(stock=stock_code, symbol="现金流量表")
# cash = cash[["报告日", "经营活动产生的现金流量净额", "购建固定资产、无形资产和其他长期资产所支付的现金"]].copy()


print("正在合并数据...")
merged_df = pd.merge(balance, income, on="REPORT_DATE", how="inner")
merged_df = pd.merge(merged_df, cash, on="REPORT_DATE", how="inner")
# merged_df = merged_df.sort_values("REPORT_DATE").reset_index(drop=True)

# # # 提取你要的：报告期 + 净利润 + 净资产
# # df = financial_report[["报告期", "净利润(万元)", "所有者权益(或股东权益)合计"]].copy()
# # df = df.sort_values("报告期").reset_index(drop=True)

# # df = financial_report

# 保存Excel
# balance.to_excel(f"d:\\1\\1\\{stock_code}_transfer_asset.xlsx", index=False)
# income.to_excel(f"d:\\1\\1\\{stock_code}_transfer_profit.xlsx", index=False)
# cash.to_excel(f"d:\\1\\1\\{stock_code}_transfer_cash.xlsx", index=False)
merged_df.to_excel(f"d:\\1\\1\\{stock_code}_transfer_data.xlsx", index=False)

