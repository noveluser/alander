
import akshare as ak

df = ak.stock_individual_info_em(symbol="300206")
df.to_excel("c:/work/log/300206.xlsx", index=False, engine='openpyxl')

