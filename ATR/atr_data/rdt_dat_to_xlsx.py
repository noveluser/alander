#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RDT400 RFID 读卡器日志 (.dat) -> Excel (.xlsx) 批量提取工具

用法
----
  # 处理一个目录下所有 .dat，每个源文件产出一个同名 xlsx
  python rdt_dat_to_xlsx.py "D:/1/temp" -o "D:/1/temp/xlsx_out"

  # 合并成一本工作簿，每个源文件一个 sheet
  python rdt_dat_to_xlsx.py "D:/1/temp" -o out.xlsx --combine

  # 只处理单个文件
  python rdt_dat_to_xlsx.py "D:/1/temp/26110308rw.dat" -o out/

源文件结构（RDT400 Server V3.30）
--------------------------------
  第 1 行 : RDT400 Server V3.30
  第 2 行 : Site: <站点>
  第 3 行 : System: <设备> ; System type: <类型>
  第 4 行 : Date: <DD-MM-YYYY>  <HH:MM:SS>        <- 日志起始日期
  第 5 行 : 表头（以 ; 分隔，74 段）
  之后    : 每行一条读卡事件，同样以 ; 分隔，字段数与表头一致

这些 .dat 由 RDT400 服务器（10.31.7.16）提供，原始 URL 形如
    http://10.31.7.16/rawfiles/{JJ}{MM}{TT}{ID}rw.dat
命名规则见服务器自带下载页。下载请用 rdt_raw_download.py。

表头的语义分组（74 段）
----------------------
  固定段 12 : Date / ID / CT / IDX / TLE / TGA / V / DEVICE LIST / ERROR LIST / OUTPUT / ENR / R
  CODE1 段 19 : CODE1 ID, LENGTH, READ LIST, RES, CLV ID, XPOS, YPOS, ZPOS, XMIN, XMAX, CS,
                 CODE CONTENT, XPOS, YPOS, ZPOS, XMIN, XMAX, CS, CODE CONTENT
  CODE2 段 19 : 同上（CODE2）
  CODE3 段 19 : 同上（CODE3）
  尾部    4 : INFO1 / INFO2 / CUSTOM1 / CUSTOM2

  同一组内第二个 XPOS..CODE CONTENT 块代表"同一次事件里读到的第二张标签"（多标签）。
  尾部 INFO1/INFO2/CUSTOM2 内嵌 key:value 与 <RDT> 标记，脚本会额外拆成独立列。

输出工作簿结构
--------------
  说明   : 源文件元信息 + 字段字典（输出列名 / 原始表头 / 列序号 / 说明）
  明细   : 每行一条读卡事件，原始列（原文文本，不丢前导零）+ 解析出的关键词列
"""

import argparse
import glob
import os
import re
import sys
from datetime import datetime, timedelta

try:
    import openpyxl
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "openpyxl>=3.1.0"])
    import openpyxl
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter


# --------------------------------------------------------------------------
# 样式
# --------------------------------------------------------------------------
def xl_color(css_hex: str) -> str:
    value = css_hex.removeprefix("#").upper()
    if len(value) != 6:
        raise ValueError("Expected #RRGGBB, got: %s" % css_hex)
    return "FF" + value


XL_PRIMARY = xl_color("#4472C4")
XL_BORDER = xl_color("#BFBFBF")
XL_WHITE = xl_color("#FFFFFF")

THIN = Side(style="thin", color=XL_BORDER)
BORDER = Border(bottom=THIN)

HEADER_FONT = Font(bold=True, color=XL_WHITE, size=10)
HEADER_FILL = PatternFill("solid", fgColor=XL_PRIMARY)
HEADER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)


# --------------------------------------------------------------------------
# 解析
# --------------------------------------------------------------------------
GROUP_RE = re.compile(r"^(CODE\d+)\s+ID$", re.IGNORECASE)
TAIL_RE = re.compile(r"^(INFO\d+|CUSTOM\d+)$", re.IGNORECASE)
KV_RE = re.compile(r"([A-Za-z][A-Za-z0-9_]*):([^\s]*)")
RDT_RE = re.compile(r"<RDT>(.*?)<C1>(.*?)<C2>(.*?)<C3>(.*?)</RDT>", re.DOTALL)

# 固定段表头 -> 输出列名
BASE_NAME = {
    "Date": "TIME",
    "DEVICE LIST": "DEVICE_LIST",
    "ERROR LIST": "ERROR_LIST",
    "OUTPUT": "OUTPUT",
    "CODE CONTENT": "CONTENT",
}

INVALID_SHEET = re.compile(r"[\[\]:*?/\\]")


def sniff_decode(raw: bytes):
    """按可能性顺序尝试解码，返回 (文本, 编码名)。"""
    for enc in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8(replace)"


def parse_date_token(token: str):
    """
    把 '03-11-2026' / '2026-11-03' 解析为 date；无法确定则返回 None。

    日月顺序判定：>12 的那个数只能是"日"，因此 '03-11-2026' 只能是 3 日 11 月（日-月-年）。
    两者都 <=12 时按日-月-年处理——本项目源文件即为该格式（文件名 26110308 与之互证）。
    """
    m = re.match(r"^(\d{1,4})[-/.](\d{1,2})[-/.](\d{1,4})$", token.strip())
    if not m:
        return None
    p1, p2, p3 = int(m.group(1)), int(m.group(2)), int(m.group(3))

    if len(m.group(1)) == 4:          # YYYY-MM-DD：年月顺序已确定
        y, mo, d = p1, p2, p3
        try:
            return datetime(y, mo, d).date()
        except ValueError:
            return None

    if len(m.group(3)) == 4:          # DD-MM-YYYY / MM-DD-YYYY
        y, x, z = p3, p1, p2
    else:                             # DD-MM-YY
        y, x, z = 2000 + p3, p1, p2

    if x > 12 and z <= 31:
        d, mo = x, z                  # 日-月
    elif z > 12 and x <= 31:
        d, mo = z, x                  # 月-日
    else:
        d, mo = x, z                  # 日-月（默认）

    try:
        return datetime(y, mo, d).date()
    except ValueError:
        return None


def build_columns(tokens):
    """
    由表头 token 生成输出列定义。

    返回 list of dict:
      name  输出列名
      src   原始列序号
      raw   原始表头文本
      kind  'fixed' | 'group' | 'tail'
    """
    toks = [t.strip() for t in tokens]
    # 去掉末尾空列（源文件以 ; 结尾会产生一个空字段）
    while toks and toks[-1] == "":
        toks.pop()

    # 尾部段（INFO1 / INFO2 / CUSTOM1 / CUSTOM2）从第一个匹配处开始，
    # 不能被算进最后一个 CODE 分组，否则尾列会变成 CODE3_INFO1 这种错名。
    tail_start = len(toks)
    for i, t in enumerate(toks):
        if TAIL_RE.match(t):
            tail_start = i
            break

    group_starts = [i for i, t in enumerate(toks) if GROUP_RE.match(t)]
    groups = []
    for n, start in enumerate(group_starts):
        end = group_starts[n + 1] if n + 1 < len(group_starts) else tail_start
        name = GROUP_RE.match(toks[start]).group(1).upper()
        groups.append((name, start, end))
    first_group_start = group_starts[0] if group_starts else len(toks)

    cols = []
    occ = {}          # (组名, 基础名) -> 第几次出现
    plain_occ = {}    # 固定/尾部段同名计数
    for i, t in enumerate(toks):
        if not t:
            continue
        grp = None
        for name, start, end in groups:
            if start <= i < end:
                grp = name
                break
        if grp:
            base = BASE_NAME.get(t, t.replace(" ", "_"))
            # 组标签自身（"CODE1 ID"）已经含组名，避免变成 CODE1_CODE1_ID
            if GROUP_RE.match(t):
                base = "ID"
            key = (grp, base)
            occ[key] = occ.get(key, 0) + 1
            idx = occ[key]
            out_name = "%s_%s" % (grp, base) if idx == 1 else "%s_%d_%s" % (grp, idx, base)
            kind = "group"
        else:
            base = BASE_NAME.get(t, t.replace(" ", "_"))
            plain_occ[base] = plain_occ.get(base, 0) + 1
            out_name = base if plain_occ[base] == 1 else "%s_%d" % (base, plain_occ[base])
            kind = "fixed" if i < first_group_start else "tail"
        cols.append({"name": out_name, "src": i, "raw": t, "kind": kind})
    return cols


def parse_tail_keywords(fields):
    """
    从 INFO1 / INFO2 / CUSTOM1 / CUSTOM2 四段里拆出关键词。

    返回 {源列名: {关键词: 值}}；列顺序由调用方统一维护。
    """
    result = {}

    def add(src, key, val):
        result.setdefault(src, {})[key] = val

    # INFO1 / INFO2 / CUSTOM1：纯 key:value
    for idx in (69, 70, 71):
        if idx >= len(fields):
            continue
        raw = fields[idx].strip()
        if not raw:
            continue
        src = "INFO%d" % (idx - 68)
        for k, v in KV_RE.findall(raw):
            add(src, k, v)

    # CUSTOM2（序号 72）：<RDT>..<C1>..<C2>..<C3>..</RDT> IncStart:x IncEnd:y TM:z ICD:d OX:e
    if len(fields) > 72:
        raw = fields[72].strip()
        if raw:
            m = RDT_RE.search(raw)
            if m:
                add("CUSTOM2", "RDT", m.group(1))
                add("CUSTOM2", "C1", m.group(2))
                add("CUSTOM2", "C2", m.group(3))
                add("CUSTOM2", "C3", m.group(4))
            for k, v in KV_RE.findall(raw.split("</RDT>")[-1]):
                add("CUSTOM2", k, v)
    return result


def parse_file(path):
    """
    解析单个 .dat 文件。

    返回 dict:
      meta         文件元信息
      cols         列定义
      parsed_order 解析关键词列顺序 [(源列名, 关键词), ...]
      records      行列表，每行 = {'dt': datetime|None, 'raw': [str], 'kw': {src:{k:v}}}
    """
    with open(path, "rb") as fh:
        raw_bytes = fh.read()
    text, encoding = sniff_decode(raw_bytes)
    lines = text.splitlines()

    meta = {
        "source": os.path.basename(path),
        "size_kb": round(len(raw_bytes) / 1024, 1),
        "encoding": encoding,
        "app": "",
        "site": "",
        "system": "",
        "system_type": "",
        "log_date": "",
        "log_start": "",
        "total_lines": len(lines),
    }
    cur_date = None
    header_tokens = None
    cols = None
    records = []
    prev_dt = None
    date_bumped = 0
    data_from = None
    data_to = None

    for line in lines:
        s = line.strip()
        if not s:
            continue

        # ---- 元信息行 ----
        if s.startswith("RDT400"):
            meta["app"] = s
            continue
        if s.startswith("Site:"):
            meta["site"] = s.split(":", 1)[1].strip()
            continue
        if s.startswith("System:"):
            body = s.split(":", 1)[1]
            if "System type:" in body:
                sys_part, type_part = body.split("System type:", 1)
                meta["system_type"] = type_part.strip()
            else:
                sys_part = body
            meta["system"] = sys_part.strip().rstrip(";").strip()
            continue
        if s.startswith("Date:") and header_tokens is None:
            body = s.split(":", 1)[1].strip()
            parts = body.split()
            if parts:
                d = parse_date_token(parts[0])
                if d:
                    cur_date = d
                    meta["log_date"] = d.strftime("%Y-%m-%d")
                if len(parts) > 1:
                    meta["log_start"] = parts[1]
            continue

        # ---- 表头行 ----
        if header_tokens is None:
            if ";" in s:
                cand = s.split(";")
                if len(cand) >= 20 and any(GROUP_RE.match(t.strip()) for t in cand):
                    header_tokens = cand
                    cols = build_columns(cand)
                    continue
            continue

        # ---- 数据行 ----
        parts = s.split(";")
        n = len(cols)
        if len(parts) < n:
            parts += [""] * (n - len(parts))
        elif len(parts) > n:
            parts = parts[:n]

        # 日期 + 时间
        dt = None
        time_token = parts[0].strip()
        if cur_date and re.match(r"^\d{1,2}:\d{2}:\d{2}$", time_token):
            h, mi, sec = (int(x) for x in time_token.split(":"))
            dt = datetime.combine(cur_date, datetime.min.time()) + timedelta(hours=h, minutes=mi, seconds=sec)
            # 跨零点：时间回退则日期 +1
            if prev_dt is not None and dt < prev_dt:
                cur_date = cur_date + timedelta(days=1)
                dt = datetime.combine(cur_date, datetime.min.time()) + timedelta(hours=h, minutes=mi, seconds=sec)
                date_bumped += 1
            prev_dt = dt
            if data_from is None:
                data_from = dt
            data_to = dt

        records.append({"dt": dt, "raw": parts, "kw": parse_tail_keywords(parts)})

    # ---- 解析关键词的统一列顺序（取所有行键的并集，保证全表列一致）----
    parsed_order = []
    seen = set()
    for rec in records:
        for src in rec["kw"]:
            for key in rec["kw"][src]:
                if (src, key) not in seen:
                    seen.add((src, key))
                    parsed_order.append((src, key))

    meta["records"] = len(records)
    meta["cols"] = len(cols) if cols else 0
    meta["date_bumped"] = date_bumped
    meta["data_from"] = data_from.strftime("%Y-%m-%d %H:%M:%S") if data_from else ""
    meta["data_to"] = data_to.strftime("%Y-%m-%d %H:%M:%S") if data_to else ""
    return {"meta": meta, "cols": cols or [], "parsed_order": parsed_order, "records": records}


# --------------------------------------------------------------------------
# 字段说明
# --------------------------------------------------------------------------
COL_DOC = {
    "TIME": "事件时刻（仅时分秒），与日志日期合成 DATETIME 列",
    "ID": "读卡事件流水号",
    "CT": "事件类型计数",
    "IDX": "读卡器/天线索引",
    "TLE": "标签天线端口",
    "TGA": "标签天线组/增益档位",
    "V": "协议版本",
    "DEVICE_LIST": "设备在线状态位图（24 位）",
    "ERROR_LIST": "设备错误状态位图（24 位）",
    "OUTPUT": "输出端口状态位图",
    "ENR": "事件序号/使能计数",
    "R": "读卡结果标志",
    "LENGTH": "标签数据长度",
    "READ LIST": "读取到的天线位图（24 位）",
    "RES": "读取结果位图",
    "CLV ID": "标签所属读卡器通道号（01-10）",
    "XPOS": "标签 X 坐标（毫米）",
    "YPOS": "标签 Y 坐标（毫米）",
    "ZPOS": "标签 Z 坐标（毫米）",
    "XMIN": "读取范围 X 最小值",
    "XMAX": "读取范围 X 最大值",
    "CS": "校验/置信度值",
    "CONTENT": "标签原始数据（十进制串，按原文保留，不丢前导零）",
    "INFO1": "设备事件计数原文",
    "INFO2": "设备运行参数原文",
    "CUSTOM1": "自定义区 1 原文",
    "CUSTOM2": "RDT 帧标记与增量区间原文",
}

GROUP_DOC = "同一次事件读到的第 %s 组标签数据"
SECOND_BLOCK_DOC = "同一次事件里读到的第 %s 张标签（多标签）"
GROUP_ID_DOC = "该组标签的类型标识（e / R 等）"

PARSED_DOC = {
    "ON": "事件累计计数",
    "L": "读标签标志",
    "W": "写标签标志",
    "H": "HMI 交互标志",
    "RV": "复位/版本计数",
    "LF": "故障标志（N=正常）",
    "LS": "扫描状态",
    "RDT": "RDT 帧序号",
    "C1": "RDT 通道 1",
    "C2": "RDT 通道 2",
    "C3": "RDT 通道 3",
    "IncStart": "增量数据起始字节",
    "IncEnd": "增量数据结束字节",
    "TM": "传送方向（FWD=正向）",
    "ICD": "编码方向（UP=上行）",
    "OX": "输出标志",
}


def field_doc(col):
    name = col["name"]
    if col["kind"] == "group":
        parts = name.split("_")
        grp = parts[0]
        if len(parts) > 2 and parts[1].isdigit():
            doc = SECOND_BLOCK_DOC % grp
            base = "_".join(parts[2:])
        elif len(parts) == 2 and parts[1] == "ID":
            return GROUP_DOC % grp, GROUP_ID_DOC
        else:
            doc = GROUP_DOC % grp
            base = "_".join(parts[1:])
        return doc, COL_DOC.get(base, base)
    return "固定段" if col["kind"] == "fixed" else "尾部原文段", COL_DOC.get(name, name)


# --------------------------------------------------------------------------
# 写 Excel
# --------------------------------------------------------------------------
def make_header_cells(ws, headers):
    out = []
    for h in headers:
        c = WriteOnlyCell(ws, value=h)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
        c.alignment = HEADER_ALIGN
        c.border = BORDER
        out.append(c)
    return out


def col_widths(headers, sample_rows):
    """按内容估算列宽。"""
    widths = []
    for i, h in enumerate(headers):
        w = len(str(h)) * 1.7 + 3
        for r in sample_rows:
            v = r[i] if i < len(r) else ""
            if v:
                w = max(w, min(len(str(v)) * 1.05 + 2, 46))
        widths.append(round(min(max(w, 8), 46), 1))
    return widths


def detail_headers(res):
    parsed_names = []
    for src_col, key in res["parsed_order"]:
        n = "%s_%s" % (src_col, key)
        if n not in parsed_names:
            parsed_names.append(n)
    return parsed_names


def write_detail_sheet(ws, res):
    """把一个源文件的明细写进给定 sheet。"""
    meta = res["meta"]
    cols = res["cols"]
    parsed_order = res["parsed_order"]
    parsed_names = detail_headers(res)

    headers = ["DATETIME", "SOURCE_FILE"] + [c["name"] for c in cols] + parsed_names
    for i, w in enumerate(col_widths(headers, [])):
        ws.column_dimensions[get_column_letter(i + 1)].width = w
    ws.column_dimensions["A"].width = 19
    ws.column_dimensions["B"].width = 20
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = "A1:%s1" % get_column_letter(len(headers))
    ws.append(make_header_cells(ws, headers))

    for rec in res["records"]:
        row = [rec["dt"], meta["source"]] + list(rec["raw"])
        for src_col, key in parsed_order:
            row.append(rec["kw"].get(src_col, {}).get(key, ""))
        ws.append(row)
    return headers


def write_note_sheet(ws, res):
    """说明 sheet：元信息 + 字段字典。"""
    meta = res["meta"]
    parsed_names = detail_headers(res)

    meta_rows = [
        ["字段", "值"],
        ["源文件", meta["source"]],
        ["文件大小(KB)", meta["size_kb"]],
        ["生成器", meta["app"]],
        ["站点(Site)", meta["site"] or "(空)"],
        ["设备(System)", meta["system"]],
        ["系统类型", meta["system_type"]],
        ["日志日期", meta["log_date"]],
        ["日志起始时刻", meta["log_start"]],
        ["数据起始时刻", meta["data_from"]],
        ["数据结束时刻", meta["data_to"]],
        ["记录条数", meta["records"]],
        ["原始列数", meta["cols"]],
        ["解析关键词列数", len(parsed_names)],
        ["文本编码", meta["encoding"]],
        ["跨零点修正次数", meta["date_bumped"]],
        ["提取时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        [],
    ]
    ws.append(make_header_cells(ws, meta_rows[0]))
    for r in meta_rows[1:]:
        ws.append(r)

    ws.append(make_header_cells(ws, ["输出列名", "原始表头", "列序号", "类别", "说明"]))
    for col in res["cols"]:
        doc, sub = field_doc(col)
        ws.append([col["name"], col["raw"], col["src"] + 1, col["kind"], "%s；%s" % (doc, sub)])
    for src_col, key in res["parsed_order"]:
        name = "%s_%s" % (src_col, key)
        ws.append([name, "(由 %s 原文拆分)" % src_col, "-", "parsed", PARSED_DOC.get(key, key)])


def write_workbook(path, results, title="RDT400 读卡日志提取"):
    """每个源文件两个 sheet：说明 / 明细。"""
    wb = openpyxl.Workbook(write_only=True)
    wb.properties.title = title
    wb.properties.creator = "RDT dat extractor"

    for src, res in results:
        write_note_sheet(wb.create_sheet("说明"), res)
        write_detail_sheet(wb.create_sheet("明细"), res)

    wb.save(path)
    return path


def combine_workbook(path, results):
    """合并模式：每源文件一个明细 sheet，另有总览 sheet。"""
    wb = openpyxl.Workbook(write_only=True)
    wb.properties.title = "RDT400 读卡日志提取（合并）"
    wb.properties.creator = "RDT dat extractor"

    ws0 = wb.create_sheet("总览")
    hdr = ["源文件", "设备(System)", "系统类型", "日志日期", "记录条数", "原始列数", "文件大小(KB)", "数据起始", "数据结束"]
    for i, w in enumerate([26, 22, 12, 14, 12, 12, 14, 20, 20]):
        ws0.column_dimensions[get_column_letter(i + 1)].width = w
    ws0.freeze_panes = "A2"
    ws0.append(make_header_cells(ws0, hdr))
    for src, res in results:
        m = res["meta"]
        ws0.append([
            m["source"], m["system"], m["system_type"], m["log_date"], m["records"], m["cols"], m["size_kb"],
            m["data_from"], m["data_to"],
        ])

    used = set()
    for src, res in results:
        base = INVALID_SHEET.sub("_", res["meta"]["source"])[:31] or "Sheet"
        name, n = base, 1
        while name in used:
            n += 1
            suffix = "_%d" % n
            name = base[:31 - len(suffix)] + suffix
        used.add(name)
        write_detail_sheet(wb.create_sheet(name), res)

    wb.save(path)
    return path


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def collect_inputs(target, pattern):
    if os.path.isdir(target):
        files = sorted(glob.glob(os.path.join(target, "**", pattern), recursive=True))
        return [f for f in files if os.path.isfile(f)]
    return [target]


def main():
    ap = argparse.ArgumentParser(
        description="RDT400 .dat 日志批量提取为 xlsx",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("target", help="待处理的 .dat 文件或所在目录")
    ap.add_argument("-o", "--output", required=True, help="输出 xlsx 文件（合并模式）或输出目录（分文件模式）")
    ap.add_argument("--pattern", default="*.dat", help="目录模式下的文件匹配规则，默认 *.dat")
    ap.add_argument("--combine", action="store_true", help="把目录下所有文件合并成一本工作簿")
    ap.add_argument("--name-from", choices=["stem", "system"], default="stem",
                    help="分文件模式的输出文件名来源：stem=源文件名，system=设备名")
    args = ap.parse_args()

    files = collect_inputs(args.target, args.pattern)
    if not files:
        print("未找到匹配文件：%s" % args.target)
        return 1

    results = []
    for f in files:
        try:
            res = parse_file(f)
        except Exception as exc:                       # noqa: BLE001
            print("跳过 %s：%s" % (os.path.basename(f), exc))
            continue
        if not res["records"]:
            print("跳过 %s：未解析到数据行" % os.path.basename(f))
            continue
        results.append((f, res))
        print("已解析 %-28s 记录=%-6d 列=%-3d 设备=%s" % (
            os.path.basename(f), res["meta"]["records"], res["meta"]["cols"], res["meta"]["system"]))

    if not results:
        print("没有可输出的数据")
        return 1

    if args.combine:
        out = args.output
        if not out.lower().endswith(".xlsx"):
            out = os.path.join(out, "RDT400_合并提取.xlsx")
        os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
        combine_workbook(out, results)
        print("\n已生成合并工作簿：%s（总览 + %d 个明细 sheet）" % (out, len(results)))
        return 0

    outdir = args.output
    if os.path.isfile(outdir):
        outdir = os.path.dirname(outdir)
    os.makedirs(outdir, exist_ok=True)
    for src, res in results:
        m = res["meta"]
        if args.name_from == "system":
            base = re.sub(r'[\\/:*?"<>|]', "_", m["system"] or os.path.splitext(os.path.basename(src))[0])
        else:
            base = os.path.splitext(os.path.basename(src))[0]
        out = os.path.join(outdir, base + ".xlsx")
        if os.path.exists(out):
            out = os.path.join(outdir, "%s_%s.xlsx" % (base, datetime.now().strftime("%Y%m%d%H%M%S")))
        write_workbook(out, [(src, res)])
        print("已生成 %s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
