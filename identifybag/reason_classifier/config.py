"""全局配置：数据库连接、匹配窗口、输出设置。"""
from datetime import timedelta

# ---------------------------------------------------------------- 数据库 ----
DB_HOST = "10.31.8.21"
DB_PORT = "1521"
DB_SERVICE = "ORABPI"
DB_USER = r"owner_31_bpi_3_0"
DB_PASSWORD = "owner31bpi"

# ------------------------------------------------------------ 匹配时间窗口 ----
# bag 在注销(Deregistration)环节之后约 7s~70s 才会进入手动扫描环节，
# 时间太短(<7s)或太长(>70s)都不是同一件行李：manual.EVENTTS - fetch.EVENTTS ∈ [7,70]s。
MIN_MATCH_GAP = timedelta(seconds=7)
MAX_MATCH_GAP = timedelta(seconds=70)

# 多个候选匹配时的权重：
#   - 第一权重：bag 顺序位（注销列表中位置靠前者优先）；
#   - 第二权重：时间差越接近中位数 29.5s 越优先。
T3_MEDIAN_GAP = timedelta(seconds=29.5)
SAT_MEDIAN_GAP = timedelta(seconds=5)

# 分拣机循环判定：LPC 在站点 580~590 经过 AutoScan 的次数达到该值即判为 Recirculations。
CIRCLE_THRESHOLD = 6

# ------------------------------------------------------------ 输出文件 ----
OUTPUT_PREFIX = "reason_classification"
