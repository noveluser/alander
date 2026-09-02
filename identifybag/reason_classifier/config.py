"""全局配置：数据库连接、匹配窗口、输出设置。"""
from datetime import timedelta

# ---------------------------------------------------------------- 数据库 ----
DB_HOST = "10.31.8.21"
DB_PORT = "1521"
DB_SERVICE = "ORABPI"
DB_USER = r"owner_31_bpi_3_0"
DB_PASSWORD = "owner31bpi"

# ------------------------------------------------------------ 匹配时间窗口 ----
# 两阶段匹配：先「窄窗口」确认锚点，再「宽窗口」做局部搜索。
# 站点类型：191/192 走 SAT 窗口，其余走 T3 窗口。
# 窗口单位为秒。

# T3 站点（91~94 配对）窗口
NARROW_T3_MIN, NARROW_T3_MAX = 7, 40      # 窄窗口：第一阶段确认锚点
WIDE_T3_MIN, WIDE_T3_MAX = 7, 120          # 宽窗口：第二阶段局部搜索

# SAT 站点（191/192 自配对）窗口
NARROW_SAT_MIN, NARROW_SAT_MAX = 1, 30
WIDE_SAT_MIN, WIDE_SAT_MAX = 1, 120

# 软匹配偏好：时间差越接近该中位数越优先
T3_MEDIAN_GAP = timedelta(seconds=29.5)
SAT_MEDIAN_GAP = timedelta(seconds=5)

# 第二阶段：以最近锚点的 manual 位置为起点，向后搜索的偏移量（条数）
ANCHOR_WINDOW = 10

# 分拣机循环判定：LPC 在站点 580~590 经过 AutoScan 的次数达到该值即判为 Recirculations。
CIRCLE_THRESHOLD = 6

# ------------------------------------------------------------ 输出文件 ----
OUTPUT_PREFIX = "reason_classification"
