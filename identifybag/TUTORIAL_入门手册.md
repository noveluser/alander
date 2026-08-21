# 零基础入门手册：reason_classifier 弃包分类项目

> 本文专门写给**完全没接触过这段代码**的同学。
> 不要求你懂数据库、不懂 Python 也没关系，**跟着讲一遍就能看懂大概**。
> 我们全程用大白话。

---

## 0. 先认识它到底是干吗的

这是一套**机场行李分拣系统**相关的数据处理脚本。

想象一条行李传输带：行李从值机处被送进来，电脑根据显示器上的**条码**自动把它们分到各自的航班口去装飞机。

- 大多数行李自动就能读对条码、分对地方。
- 但总有一些行李"出问题"——条码没读出来、航班改了、行李是空的框子等。这些行李会被人为"弹出去"，进入一条**弃包（Dump）**通道集中处理。

这套程序要做的事就一件：

> **把每天的"问题行李"按"为什么被弃包"（原因）分门别类，整理成一个 Excel 表格。**

这个 Excel 就叫 `reason_classification_xxx.xlsx`。
我们看这个表，就能知道：今天有多少行李是因为"条码没读出来"、有多少是"空框"、有多少是"循环转圈"……方便机场排查。

---

## 1. 三个你必须先懂的关键词

程序里反复出现这几个词，先记住它们的意思：

| 英文词 | 大白话 |
|---|---|
| **LPC** | 行李的**条码**（一行字）。空框子、没条码的行李，这里就是空的。 |
| **PID** | 一个内部的**包裹编号**，电脑给每件东西分配的流水号。 |
| **EVENTTS** | 这件事**发生的时刻**（时间戳）。 |

还有两个"环节"，它们对应程序里的两大块数据：

| 环节 | 大白话 |
|---|---|
| **ManualScan（手动扫描）** | 条码自动读不出来时，人拿着扫码枪**人工扫一下**。发生在 91/92/93/94 号站点 |
| **Deregistration（注销/弃包）** | 这行李被**正式判死、弹出去**。发生在 96/97/98/99 号站点 |

**核心思路**：一件行李先被"注销"弹出，然后过了几秒到几分钟，又被"手动扫描"记录一次。如果我们能用时间把"同一条码的注销"和"手动扫描"对上，就知道这件弃包最后被分到哪个原因了。

---

## 2. 程序「一句话简介」

```
读数据库(注销 + 手动扫描两条数据)
   → 用"时间 + 顺序"把它们配对(匹配)
   → 给每个行李算出一个"弃包原因"(DEREGISTER_REASON)
   → 按原因分类，写成一个 Excel
```

下面我们把每一环拆开讲。

---

## 3. 怎么运行它（照着敲）

```bash
# 进入项目目录
cd /home/yunwei/program

# 指定一个运营日，比如 20260819
# 注意：系统里 Python 装在 .venv 这个专用环境里，要用它的 python
/home/yunwei/program/.venv/bin/python run_reason_classifier.py 20260819
```

跑完后，目录里会出现一个文件：

```
reason_classification_2026-08-18_to_2026-08-19.xlsx
```

> 为什么文件名是 `08-18` 而不是 `08-19`？
> 因为机场的"一天"是从**前一天下午 4 点到今天下午 4 点**算的（运营日）。
> 你输入 `20260819`，它统计的其实是 8/18 16:00 → 8/19 16:00 这段，文件名按起点写。

如果不想指定日期，直接回车，它会默认统计"昨天"。

---

## 4. 输出的 Excel 长什么样

excel 里有 3 张子表（sheet）：

### ① summary ——「一页总结」
行 = 每一天，列 = 各种原因，最后一列是 Total（合计）。
一眼看到每天每种原因各有多少件。

### ② station_reason_日期 ——「按站点看」
行 = 站点号（91/92/93/94），列 = 各原因。
看**哪个站点**抛出了哪些原因的弃包。

### ③ detail_日期 ——「逐条明细」
一行 = 一件弃包行李。

它里面这些列（重要）：

| 列名 | 大白话 |
|---|---|
| `manual_EVENTTS` / `manual_LPC` / `manual_PID` | 手动扫描那一刻 / 条码 / 编号 |
| `manual_DEREGISTER_REASON` | 这条"手动扫描"自己算出的原因 |
| `dereg_LPC` / `dereg_*` | **配对上的那条注销记录**的信息（没配对上就空着） |
| `dereg_DEREGISTER_REASON` | 注销记录算出的原因 |
| `REASON` | **最终采用的原因**（关键列） |
| `REASON_SOURCE` | 这个 REASON 是取自注销(`MATCHED_DEREG`) 还是手动扫描(`UNMATCHED_MANUAL`) |
| `MATCH_CONFIDENCE` | 匹配可靠度：`CONFIRMED`(唯一匹配，很确定) / `AMBIGUOUS`(多个候选，拿不准) / `UNMATCHED`(没配上) |

---

## 5. 「弃包原因」是怎么算出来的（8 条规则）

每个行李，代码会按顺序往下判断，**命中一条就停**：

1. 流程是"空框类"（Garbage/Lateral 等）**且没条码** → `EMPTY`（空框子）
2. 流程是 `Trace and Eject` → 中控台**主动弹出去**的
3. `IDENTIFICATIONSTATE` 是 `DELETED_BAGDATA` → 数据被删除了（DEL BSM）
4. 识别状态是 `NO_READ`/`MULTI_READ` → **没读到** / **读到好几个**
5. `FLIGHTBUILDTIMELINESS` 是 `EARLY` → 行李**到太早**了
6. 流程是 `Dump Flight Build` → 查一下这行李在分拣机上**转了几圈**：
   - 转 ≥6 圈 → `Recirculations`（一直循环没出去）
   - 否则 `Dump Flight Build`（就是主动弃包）
7. 流程是 `Unplanned flight` → **到太晚**/计划外航班
8. 以上都不是 → 直接用 `ACTIVEPROCESS` 当原因

这就是 `derivation.py` 里那一段代码在做的事（后文细讲）。

---

## 6. 「匹配」是怎么做的（最难，但值得懂）

我们有两条数据：**手动扫描**列表、**注销**列表。

**第一步：时间窗口**
一件弃包被注销后，要隔 7 秒到 180 秒才会被手动扫描。所以配对条件是：

```
7 秒 ≤ (手动扫描时间 − 注销时间) ≤ 180 秒
```

太快(还没走到)、太慢(早就过了)都**不是同一件**。

**第二步：顺序锚点**
行李在传送带上**是排着队一个接一个走的**，所以手动扫描的先后顺序，应该和注销的先后顺序**基本一致**。

代码是这样"聪明地"配对的：
1. 找到第一对能配上的，记住它在注销列表里的**位置**（这个位置叫"锚点"）。
2. 接下来，每一件手动扫描行李，**只在锚点之后**去找注销记录（不会回头去找之前用过的）。
3. 再配上一对，就把锚点**更新**到新的位置，继续往后。

**第三步：多个候选怎么办**
如果一件手动扫描行李，窗口内有**好几个**注销记录都能配上，怎么选？
- **第一优先**：选**排队位置靠前**的那个（顺序位）。
- **第二优先**：选时间差**最接近 29.5 秒**的那个。

实在一个都配不上 → 进"未匹配"清单。

---

## 7. 项目文件结构（先在脑子里放一张地图）

这套代码拆成了很多小文件，每个管一件事。看这张图理解"数据怎么流动"：

```
run_reason_classifier.py   ← 入口开关（你运行的就是它）
        │
        ▼
reason_classifier/main.py        ← 总指挥：定日期、喊开工
        │
        ▼
reason_classifier/pipeline.py    ← 一天的具体流程：取数→配对→分类
        │
   ┌────┴────┐              ┌─────────────────┐
   ▼         ▼              │                 ▼
data.py   matching.py       │           classification.py
取数据    配对               │           做分类/算REASON
(调db.py)                    │         并集交叉表
   ▲                        ▼
   │               exporter.py
derivation.py        写出 xlsx
算弃包原因
(调db.py)

config.py  ← 所有"常数"集中放在这儿（数据库地址、时间窗口等）
db.py      ← 怎么连数据库、怎么查（供 data、derivation 共用）
```

> 小知识：把代码拆成多个小文件，就像把一篇长文章拆成好几章，
> 每章只讲一个主题，读起来轻松，改起来也安全（改 `matching.py` 不会碰坏 `data.py`）。

---

## 8. 逐个文件，大白话讲一遍

### 8.0 `run_reason_classifier.py`（入口开关）

很短。它唯一作用是：**告诉你 Python 到哪去找我们的代码包**。

```python
sys.path.insert(0, str(Path(__file__).resolve().parent))  # 把"这个文件所在目录"加进搜索路径
from reason_classifier.main import main
if __name__ == "__main__":
    main()   # 真正的工作交给 main.main() 去做
```

> `if __name__ == "__main__"` 是 Python 的惯用法：意思是"只有当我被当程序直接运行时才执行"。这样这个文件也能被别的文件"import 引用"而不乱跑。

---

### 8.1 `config.py`（本子上的"常数"）

把所有"写死的数值"集中放在这，方便改。比如：

```python
DB_HOST = "10.31.8.21"                 # 数据库在哪台机器
MIN_MATCH_GAP = timedelta(seconds=7)    # 匹配窗口最小值：7 秒
MAX_MATCH_GAP = timedelta(seconds=180)  # 匹配窗口最大值：180 秒
MEDIAN_GAP = timedelta(seconds=29.5)    # 多候选时的"第二加权"偏向
CIRCLE_THRESHOLD = 6                    # 转几圈算"循环"
```

> 好处：以后想改"把窗口放大到 300 秒"，只要改这里一个数，其它文件不用动。

---

### 8.2 `db.py`（怎么和数据库对话）

Python 不会直接看 Oracle 的表，得靠一个"司机"（库叫 `oracledb`）。

```python
def connect():   # 用 config 里的地址、账号、密码创建连接
    ...

def fetch_all(query, params=None):   # 执行一句 SQL，把结果全取回来
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params or {})
            return cursor.fetchall()   # 一堆行的列表
```

> `with ... as conn` 是"用完自动关闭"的语法，避免忘记关连接、漏资源。初学者记住：**这是打开资源的正确姿势**。

---

### 8.3 `data.py`（把数据读出来，变成表格）

数据库返回的是"一堆散的记录"，程序要把它变成**表格(DataFrame)**才方便算。

```python
BASE_COLUMNS = ["EVENTTS", "LPC", "PID", "ACTIVEPROCESS", ...]  # 表头(列名)顺序

def fetch_package_events(kind, start_ts, end_ts):
    # 根据 kind 选不同的 SQL 条件
    #   kind='manual' → 手动扫描(91-94) + 原因派生
    #   kind='dereg'  → 注销(96-99) + 原因派生
    rows = db.fetch_all(...)
    df = pd.DataFrame(rows, columns=BASE_COLUMNS)   # 变成表格
    df["EVENTTS"] = pd.to_datetime(df["EVENTTS"])   # 时间列转成"能算差值"的类型
    df["DEREGISTER_REASON"] = DerivationPipeline(...).derive(df)  # 算弃包原因
    return df
```

> 小课堂：DataFrame 是 pandas 提供的**表格数据**类型，行为、列为维度，非常方便做统计。
> 想看表格内容，在代码里写 `print(df)` 就行。
> `pd.to_datetime` 把"字符串时间"变成真正的**时间对象**，这样两行才做得了"两时间相减"。

---

### 8.4 `derivation.py`（算弃包原因，最重要的一环）

把第 5 节的 8 条规则用代码写出来。它内部有个小"查库"和一个"缓存"：

```python
def _count_auto_scans(start_ts, end_ts, lpc) -> int:
    # 专门问数据库：这个条码在自动扫描站转了 580~590 号多少次
    ...

class DerivationPipeline:
    def _circulation_reason(self, lpc):
        # 如果这个条码之前算过，直接拿缓存结果，不再查库
        count = <查库得到次数>
        return "Recirculations" if count >= 6 else "Dump Flight Build"

    def derive(self, df):   # 对整张表一次性算
        # 用掩码(mask)一列一列地打勾
        reason = 全是"空"的一列
        mask = 是空框类 且 没条码
        reason[mask] = "EMPTY"        # 命中规则1
        mask = 流程是 Trace and Eject
        reason[mask] = "Trace and Eject"   # 命中规则2
        ...   # 依次处理 3~8
        return reason
```

> **"掩码 mask"是 pandas 的核心技巧**：它是一列 True/False，像一张"打勾表"。
> `reason[mask] = 值` 意思是"只给打勾的那些行填上这个值"。比一行一行循环快得多。

> 小知识：为什么用"缓存"？因为同一个条码可能被当成多个记录，每次都去数据库问"它转了几圈"太浪费。记下来第一次的结果，后面直接抄。

---

### 8.5 `matching.py`（配对，逻辑上最亮眼）

这正是第 6 节讲的"时间窗口 + 顺序锚点 + 多候选排序"。

```python
def match_records(df_manual, df_dereg, ...):
    # 1. 先把两张表按时间排好序
    man = df_manual.sort_values("EVENTTS").reset_index(drop=True)
    fet = df_dereg.sort_values("EVENTTS").reset_index(drop=True)

    # 2. 一个循环，一件手动扫描一件手动扫描地处理
    for i, m_ts in enumerate(man_ts):
        # 算出"可能的注销记录"区间 [lo, hi)
        lo = np.searchsorted(fet_ts, m_ts - max_ns, side="left")
        hi = np.searchsorted(fet_ts, m_ts - min_ns, side="right")
        cands = [k for k in range(start, ...) if 能配上(时间在窗口内)]

        if 没有候选:
            丢进"未匹配"清单; continue
        best = 按"顺序位优先,其次接近29.5秒"选出最合适的
        记录配对; 更新锚点 = best
    ...
```

> 几个小词：
> - `sort_values` 按某列排序；
> - `reset_index(drop=True)` 排完序把行号重新从 0 编号（保持索引干净）；
> - `np.searchsorted` 在**已排序**的列表里快速"二分查找"某个值该插在哪——比逐个找快很多。
> - 返回的是三个东西：`(配对成功表, 未匹配手动, 未匹配注销)`。

---

### 8.6 `classification.py`（把配对结果整理成"给 excel 用的表"）

配对好了，但原始配对表里有 `manual_*` 和 `dereg_*` 两套列很杂。这里负责按**业务口径**定出最终 `REASON`：

```python
def build_classification_df(matched, unmatched_manual):
    # matched 行：REASON 一般取注销的 dereg_DEREGISTER_REASON；
    #           但如果是 EMPTY(空框)就保留 manual 的 EMPTY
    # unmatched 行：REASON 用手动扫描自己的 manual_DEREGISTER_REASON
    ...
    return 整理好列顺序的表
```

统一的列顺序（也决定了 excel detail 表的长相）：
`manual_* → dereg_* → REASON_SOURCE → MATCH_CONFIDENCE → REASON`

另一个函数 `build_reason_pivot` 负责做"行×列"的交叉统计：

```python
def build_reason_pivot(df, index_col, value_col):
    pivot = pd.crosstab(df[index_col], df[value_col])  # 行列交叉计数
    pivot["Total"] = pivot.sum(axis=1)                 # 每行加个合计
    pivot.index.name = index_col
    return pivot
```

> `pd.crosstab` 就是"交叉表"：左边选一行维度(比如站点)，上面一列维度(比如原因)，格子=计数。

---

### 8.7 `exporter.py`（写 Excel）

pandas 提供了 `pd.ExcelWriter`，能一次写多张子表：

```python
def write_reason_excel(filename, day_results):
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        for date_str, cls in day_results:
            build_reason_pivot(cls, "manual_CURRENTSTATIONID", "REASON")\
                .to_excel(writer, sheet_name=f"station_reason_{date_str}")
            cls.to_excel(writer, sheet_name=f"detail_{date_str}", index=False)
            ...
        # 最后汇总所有日期 → summary 表
```

> `engine="openpyxl"` 是说用 openpyxl 这个库来**写 xlsx 文件**。
> `to_excel(..., index=False)` 里的 index 是表格左边那列行号，`False` 意思是**不要把行号也写进去**，只要数据。

---

### 8.8 `pipeline.py`（一天的总流程）

把上面这些"零件"串起来，完成**一天**的处理：

```python
def process_one_day(day_start, day_end):
    dereg = fetch_dereg(day_start, day_end)      # 读注销
    manual = fetch_manual(day_start, day_end)    # 读手动扫描
    matched, unmatched_manual, _ = match_records(manual, dereg)  # 配对
    return build_classification_df(matched, unmatched_manual)    # 分类
```

> 一个函数只做"一天"，这样 `main` 想统计 8 天就循环调用 8 次，想改逻辑只改这一处。

---

### 8.9 `main.py`（总指挥：定哪天、跑几天、写文件）

```python
def _opening_window_start(input_date):
    # 运营日起点 = (输入日期 - 1天) 的 16:00
    return (input_date - timedelta(days=1)).replace(hour=16, ...)

def run(input_date):
    if input_date 太早(超过14天): 打印提示; return
    start_ts = 运营日起点
    end_ts   = 结束时间(最多8天, 且不超过昨天)
    for day in 每天:
        收齐每天的 classification → day_results
    write_reason_excel(filename, day_results)   # 最后一次性写出
```

> `main.py` 里还有一个 `_parse_arg`，负责读命令行/交互输入的那个日期，格式不对就给出友好提示。

---

## 9. 数据从数据库到 Excel 的完整旅程（一图流）

```
         Oracle 数据库
             │
   ┌─────────┴─────────────────────────────┐
   │ db.py 负责“对话”                      │
   │   - 注销数据(dereg)                    │
   │   - 手动扫描数据(manual)               │
   └───────────────────────────────────────┘
             │
   data.py: 变成 DataFrame 表格，并调 derivation 算出
            每条记录的 DEREGISTER_REASON
             │
   matching.py: 用“时间窗口+顺序锚点”把两手扫描/注销配对
             │
   classification.py: 定 REASON/来源/可靠度，整理列
             │
   exporter.py: 写成 3 张子表的 xlsx
```

---

## 10. 常见疑问（FAQ）

**Q1：为什么一个项目拆这么多个文件？**
方便读、方便改、方便分工。你想改"算原因的逻辑"只动 `derivation.py`，想改"配对规则"只动 `matching.py`，不用在整个大文件里找。

**Q2：`sorted_values` 和排序为什么重要？**
配对要按先后顺序走（锚点），必须先按时间排好队，顺序才有意义。

**Q3：`evntTS` 为什么要 `to_datetime`？**
文本类型的"时间"不能相减。转成时间对象后，`A - B` 就能得到一个"时长"，我们才能判断是不是在 7~180 秒内。

**Q4：什么叫 `ACTIVEPROCESS`？**
就是这行李当前走的是哪条"处理流程"，相当于给它贴的一个**当前状态标签**。很多原因判断都看它。

**Q5：`match_confidence` 三档分别什么意思？**
- `CONFIRMED`：窗口里只有一个候选，非常确定是它；
- `AMBIGUOUS`：窗口里好几个候选都能配上，程序只能猜一个（按顺序位+时间），所以**拿不准**，标记出来让人工复查；
- `UNMATCHED`：完全没配上。

**Q6：我想看中间某一步的结果怎么办？**
在对应文件里加一行 `print(df)` 或 `print(len(df))`，跑完看命令行输出。或用 pandas 的 `df.head(10)` 只看前 10 行。

**Q7：改了代码但运行报错 "No module named 'reason_classifier'" **
说明没从项目根目录运行，或没通过 `run_reason_classifier.py` 启动。记得用：

```bash
cd /home/yunwei/program
/home/yunwei/program/.venv/bin/python run_reason_classifier.py 20260819
```

---

## 11. 想自己动手练（建议顺序）

1. **先在目录里翻**：打开 `reason_classifier/config.py`，看看有几个常数，试着把 `CIRCLE_THRESHOLD` 从 6 改成 3，看运行结果怎么变（跑之前先存好原始结果对比）。
2. **看一张表**：运行一次拿到 xlsx，用 Excel 打开 `detail` 子表，找几行 `MATCH_CONFIDENCE = AMBIGUOUS` 的，对照 `manual_LPC` 和 `dereg_LPC` 是不是同一件。
3. **改一个规则**：在 `derivation.py` 的 `derive` 里，试着加一条规则（比如"某流程直接判某原因"），体会"加规则不影响别处"。
4. **读最短的文件**：`pipeline.py` 只有 17 行，是最容易"读懂一个完整函数是干嘛的"的范本。

---

## 12. 一句话总结

> 这套程序 = **读数据库 → 按"7~180 秒+顺序"把注销和手动扫描配对 → 按 8 条规则算出弃包原因 → 交叉统计写进 Excel**。
> 拆成多个小文件是为了**好读好改**；掌握几个 pandas 小技巧（DataFrame、mask、crosstab、to_datetime、ExcelWriter）你就会读会改了。
