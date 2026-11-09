import pandas as pd
import os

# ============ 配置区 ============
BASE_DIR = r"D:\1\temp\demo"
FILE_2026 = None
FILE_MCS  = "early.xlsx"
OUT_FILE  = os.path.join(BASE_DIR, "20261102-atrvsearly-result.xlsx")
# ================================

def norm_2026(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return s[:10] if s else None

def norm_mcs(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.startswith("0"):
        s = s.lstrip("0")
    return s or None

# ---- 读文件 ----
if FILE_2026 is None:
    for f in os.listdir(BASE_DIR):
        if f.startswith("2026-11-02") and f.endswith((".xls", ".xlsx")):
            FILE_2026 = f
            break

path_2026 = os.path.join(BASE_DIR, FILE_2026)
engine_26 = "xlrd" if FILE_2026.endswith(".xls") else None
df_2026 = pd.read_excel(path_2026, engine=engine_26)

engine_mcs = "xlrd" if FILE_MCS.endswith(".xls") else None
df_mcs = pd.read_excel(os.path.join(BASE_DIR, FILE_MCS), engine=engine_mcs)

print(f"2026-11-03 原始列: {list(df_2026.columns)}")
print(f"MCS 原始列: {list(df_mcs.columns)}")

# ---- 提取需要的列并重命名，避免 merge 后列名冲突 ----
df_2026_sub = pd.DataFrame()
df_2026_sub["_key"] = df_2026.iloc[:, 5].map(norm_2026)
df_2026_sub["26_DATETIME"]      = df_2026.iloc[:, 0]
df_2026_sub["26_CODE_CONTENT"]  = df_2026.iloc[:, 5]
df_2026_sub["26_DEVICE"]        = df_2026.iloc[:, 7]

df_mcs_sub = pd.DataFrame()
df_mcs_sub["_key"] = df_mcs.iloc[:, 1].map(norm_mcs)
df_mcs_sub = df_mcs_sub.drop_duplicates(subset="_key", keep="first")

# df_mcs_sub["mcs_dereg_CURRENTSTATIONID"]  = df_mcs.iloc[:, 3]
# df_mcs_sub["mcs_REASON"]                  = df_mcs.iloc[:, 9]

# ---- 筛选命中行 ----
keys_2026 = set(df_2026_sub["_key"].dropna())
keys_mcs  = set(df_mcs_sub["_key"].dropna())
inter     = keys_2026 & keys_mcs

print(f"2026-11-03 去重key数: {len(keys_2026)}")
print(f"MCS 去重key数:       {len(keys_mcs)}")
print(f"交集数量:            {len(inter)}")

hit_2026 = df_2026_sub[df_2026_sub["_key"].isin(inter)].copy()
hit_mcs  = df_mcs_sub[df_mcs_sub["_key"].isin(inter)].copy()

# ---- 笛卡尔积展开 ----
merged = hit_2026.merge(hit_mcs, on="_key", how="inner")

print(f"展开后总行数: {len(merged)}")
print(f"merge后列名: {list(merged.columns)}")

# ---- 整理输出列顺序 ----
out_cols = [
    "_key",
    "26_DATETIME",
    "26_DEVICE",
]
out = merged[out_cols].copy()
out = out.rename(columns={"_key": "匹配Key"})

# ---- 输出 ----
out.to_excel(OUT_FILE, index=False, engine="openpyxl")
print(f"已输出: {OUT_FILE}")
print(f"输出列: {list(out.columns)}")