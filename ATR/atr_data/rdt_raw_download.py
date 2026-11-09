#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RDT400 读卡器原始数据（rw）批量下载器
======================================

服务器：10.31.7.16  （SICK RDT400 Visualization，IIS 10.0）

关键事实（已实测验证，勿凭猜测改动）
--------------------------------------
1) 原始数据 URL 形如::

       http://10.31.7.16/rawfiles/{JJ}{MM}{TT}{ID}{TYP}.dat

   官方命名规则（设备自带下载页 "ShowDownloadData" 原文）::

       File name: *(JJ)(MM)(TT)(ID)(TYP).dat
         (JJ): Two characters for year
         (MM): Two characters for month
         (TT): Two characters for day
         (ID): Two characters for device-ID
         (TYP): Two characters for file-type
                ('rw'=Raw Data File, 'if'=Info File, 'dy'=Daily Statistics File)

   例：26110308rw.dat = 2026-11-03 + 设备ID 08 + 原始数据(rw)

2) URL 末尾的 ?-1297994371 **与日期无关，也不需要**
   --------------------------------------------------
   * 它是 RDT400 网页在渲染下载列表时生成的"破缓存"计数：同一页里每往后一个文件减 1，
     每次重新渲染页面整体值又会漂移（实测约 +1~3 /秒），因此**无法由日期推算**。
   * 服务器**完全忽略**这个查询串：实测 ?0 / ?123 / ?abc / ?-1297994371 / 不带参数
     返回的字节**完全一致**（MD5 相同）。
   * 结论：直接用不带参数的裸 URL 下载即可；非要带就用"当前时间戳"占位也无妨。

3) 三种文件类型分别在三个目录（服务器实测）：
       /rawfiles/     {JJ}{MM}{TT}{ID}rw.dat   原始逐条读码事件（本脚本的目标）
       /infofiles/    {JJ}{MM}{TT}{ID}if.dat   设备消息 info 文件
       /dailyfiles/   {JJ}{MM}{TT}{ID}dy.dat   当日统计（服务器本地二进制格式）
   另有 /statisticsfiles/sys{NN}statistics.dat 是**按槽位**给出的文本统计表，
   其槽位与 rawfiles 的 ID 一一对应（已用事件数指纹逐日核对验证）。

4) 保留窗口（实测）：
       rawfiles 原始数据约保留 **30 天**（如 2026-11-04 时仅 261006~261104 可取）
       statisticsfiles 统计表保留 **92 天**
   超出窗口返回 HTTP 404。

5) 设备 ID ↔ 设备名：服务器 "System List" 页（GetSystemList）里的 `[ID: N]`
   就是 rawfiles 文件名里的 ID，且与 .dat 文件内部第 2 列的 ID 值一致。
   注意：/statisticsfiles/sysNNstatistics.dat 文件头部的 "System:" 行是**旧标签**，
   不可信；以 System List 的 [ID: N] 为准。

用法
----
  # 下载 2026-11-03 设备 08 与 09 的原始数据
  python rdt_raw_download.py --date 261103 --ids 08,09 -o D:/1/temp/raw

  # 下载某天全部设备（ID 由服务器 System List 自动发现）
  python rdt_raw_download.py --date 261103 --ids all -o D:/1/temp/raw

  # 只列出服务器当前的设备 ID 对照表
  python rdt_raw_download.py --list-devices

  # 下载后顺带转成 xlsx（复用 rdt_dat_to_xlsx.py）
  python rdt_raw_download.py --date 261103 --ids 08,09 -o D:/1/temp/raw --extract
"""

import argparse
import hashlib
import os
import re
import sys
import time
import urllib.error
import urllib.request

BASE = "http://10.31.7.16"
RAW_DIR = "rawfiles"
TIMEOUT = 60

# 用户原脚本 invalidDict 的设备名（2023-10-15 当时的服务端标签，仅作对照参考）
LEGACY_NAMES = {
    "01": "RFID 6.5.90-P", "02": "SCAN 6.22.90-P", "03": "SCAN 7.4.90-P",
    "04": "SCAN 8.3.90-P", "05": "RFID 4.50.90-P", "06": "RFID 5.27.90-P",
    "07": "RFID 22.5.90-P", "08": "SCAN 22.22.90-P", "09": "SCAN 23.4.90-P",
    "10": "SCAN 24.3.90-P", "11": "RFID 20.27.90-P", "12": "RFID 21.27.90-P",
    "13": "BHS-3113-P", "14": "BHS-3114-P", "15": "BHS-3144-P", "16": "BHS-3143-P",
}


def http_get(url, timeout=TIMEOUT):
    """返回 (状态码, 字节内容)；404 返回 (404, b'')。"""
    req = urllib.request.Request(url, headers={"User-Agent": "rdt-raw-downloader/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, b""


# --------------------------------------------------------------------------
# 设备表
# --------------------------------------------------------------------------
def fetch_device_map():
    """
    从服务器 System List 解析 ID -> (设备名, IP)。
    这是 ID 的权威来源（rawfiles 文件名里的 ID 与 [ID: N] 一致）。
    """
    url = "%s/scripts/isalis.dll?GetSystemList?OPS=10&VMS=10&VMSFirst=0" % BASE
    status, raw = http_get(url)
    if status != 200:
        raise RuntimeError("获取设备列表失败：HTTP %s" % status)
    text = raw.decode("gbk", errors="replace")

    dev = {}
    for tr in re.findall(r"<TR[^>]*>(.*?)</TR>", text, re.S | re.I):
        cells = [re.sub(r"<[^>]*>", "", c).replace("&nbsp;", " ").strip()
                 for c in re.findall(r"<TD[^>]*>(.*?)</TD>", tr, re.S | re.I)]
        if len(cells) < 3 or "[ID:" not in cells[2]:
            continue
        m = re.search(r"\[ID:\s*(\d+)\]", cells[2])
        if not m:
            continue
        ident = m.group(1).zfill(2)
        name = cells[2].split("(")[0].strip()
        ipm = re.search(r"\((\d+\.\d+\.\d+\.\d+)\)", cells[2])
        dev[ident] = (name, ipm.group(1) if ipm else "")
    return dev


def safe_name(s):
    return re.sub(r'[\\/:*?"<>|]', "_", s).strip()


# --------------------------------------------------------------------------
# 下载
# --------------------------------------------------------------------------
def build_url(date6, ident, typ="rw"):
    """
    构造裸 URL（不带破缓存参数）。
    JJ = date6[0:2], MM = date6[2:4], TT = date6[4:6], ID = 两位设备号, TYP = 文件类型
    """
    return "%s/%s/%s%s%s.dat" % (BASE, RAW_DIR, date6, ident, typ)


def download_one(date6, ident, outdir, devmap, typ="rw", retries=2, verify=True):
    """下载单个文件。返回 (状态, 消息, 保存路径)。"""
    url = build_url(date6, ident, typ)
    last = None
    for attempt in range(retries + 1):
        try:
            status, body = http_get(url)
        except Exception as exc:                       # 网络抖动
            last = "网络错误：%s" % exc
            time.sleep(1.5 * (attempt + 1))
            continue
        if status == 404:
            return "missing", "服务器无此文件（HTTP 404，可能超出保留窗口）", None
        if status != 200:
            last = "HTTP %s" % status
            time.sleep(1.5 * (attempt + 1))
            continue
        if not body:
            last = "响应为空"
            continue

        # 校验：必须是 RDT400 日志（防止把 404 的 HTML 错误页当成数据存下来）
        if body[:9] != b"RDT400 Se":
            return "bad", "内容不是 RDT400 日志（前 9 字节=%r）" % body[:20], None

        # 校验：文件内部自报的 ID 必须与请求的 ID 一致
        if verify:
            head = body[:400].decode("gbk", errors="replace").split("\r\n")
            sysname = ""
            for ln in head[:4]:
                if ln.lower().startswith("system:"):
                    sysname = ln.split(":", 1)[1].split(";")[0].strip()
            if sysname:
                expect = devmap.get(ident, (None,))[0]
                if expect and sysname != expect:
                    return "bad", "设备不符：请求 ID=%s(期望 %s)，文件自报 %s" % (
                        ident, expect, sysname), None

        devname = devmap.get(ident, (ident, ""))[0]
        fname = "%s_%s_%s.dat" % (date6, ident, safe_name(devname))
        path = os.path.join(outdir, fname)
        with open(path, "wb") as fh:
            fh.write(body)
        md5 = hashlib.md5(body).hexdigest()

        # 统计数据行（文件前 5 行是元信息+表头）
        nrec = max(0, body.count(b"\n") - 5)
        note = "" if nrec else "   [该设备当日无读码记录：文件只有表头]"
        return "ok", "%s  %d 字节  %d 条记录  md5=%s%s" % (
            devname, len(body), nrec, md5[:12], note), path

    return "error", last or "未知错误", None


def main():
    ap = argparse.ArgumentParser(
        description="RDT400 rawfiles 原始数据批量下载器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--date", help="日期，YYMMDD 或 YYYY-MM-DD，如 261103")
    ap.add_argument("--ids", default="08,09",
                    help="设备 ID，逗号分隔；'all' 表示服务器上全部设备。默认 08,09")
    ap.add_argument("-o", "--outdir", default=".", help="保存目录")
    ap.add_argument("--type", default="rw", choices=["rw", "if", "dy"],
                    help="文件类型：rw=原始读码事件, if=设备信息, dy=当日统计。默认 rw")
    ap.add_argument("--list-devices", action="store_true", help="仅打印设备 ID 对照表后退出")
    ap.add_argument("--extract", action="store_true", help="下载后调用 rdt_dat_to_xlsx.py 转 xlsx")
    ap.add_argument("--extract-dir", help="xlsx 输出目录（默认与 --outdir 相同）")
    args = ap.parse_args()

    try:
        devmap = fetch_device_map()
    except Exception as exc:
        print("无法连接服务器 %s：%s" % (BASE, exc))
        return 2

    if args.list_devices:
        print("服务器当前设备表（ID 即 rawfiles 文件名中的 ID，权威来源）")
        print("-" * 68)
        print("%-5s %-30s %-16s %s" % ("ID", "设备名", "IP", "2023 年脚本里的旧标签"))
        for k in sorted(devmap):
            name, ip = devmap[k]
            legacy = LEGACY_NAMES.get(k, "")
            flag = "" if not legacy or legacy == name else "  <-- 旧标签:%s" % legacy
            print("%-5s %-30s %-16s%s" % (k, name, ip, flag))
        return 0

    if not args.date:
        print("请用 --date 指定日期，例如 --date 261103")
        return 2

    d = args.date.replace("-", "").replace("/", "")
    if len(d) == 8 and d.isdigit():      # YYYYMMDD -> 取后 6 位
        d = d[2:]
    if len(d) != 6 or not d.isdigit():
        print("日期格式应为 YYMMDD / YYYYMMDD / YYYY-MM-DD，当前：%s" % args.date)
        return 2
    date6 = d

    if args.ids.strip().lower() == "all":
        targets = sorted(devmap)
    else:
        targets = []
        for tok in args.ids.replace(" ", "").split(","):
            if not tok:
                continue
            if not tok.isdigit():
                print("非法设备 ID：%s" % tok)
                return 2
            targets.append(tok.zfill(2))

    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    print("日期 %s-%s-%s   文件类型 %s   目标设备 %d 个" %
          ("20" + date6[0:2], date6[2:4], date6[4:6], args.type, len(targets)))
    print("保存目录 %s" % outdir)
    print("-" * 78)

    ok_paths = []
    stat = {"ok": 0, "missing": 0, "bad": 0, "error": 0}
    for ident in targets:
        state, msg, path = download_one(date6, ident, outdir, devmap, args.type)
        stat[state] = stat.get(state, 0) + 1
        mark = {"ok": "OK    ", "missing": "缺失  ", "bad": "异常  ", "error": "错误  "}.get(state, "????? ")
        print("%s ID=%s  %s" % (mark, ident, msg))
        if state == "ok" and path:
            ok_paths.append(path)

    print("-" * 78)
    print("成功 %d   缺失 %d   异常 %d   错误 %d" %
          (stat["ok"], stat["missing"], stat["bad"], stat["error"]))

    if args.extract and ok_paths:
        print("\n正在转换为 xlsx ...")
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from rdt_dat_to_xlsx import parse_file, write_workbook
        except ImportError:
            print("找不到 rdt_dat_to_xlsx.py，跳过转换")
            return 0
        exdir = os.path.abspath(args.extract_dir or args.outdir)
        os.makedirs(exdir, exist_ok=True)
        for p in ok_paths:
            res = parse_file(p)
            if not res["records"]:
                print("  跳过（无数据行）%s" % os.path.basename(p))
                continue
            name = os.path.splitext(os.path.basename(p))[0]
            out = os.path.join(exdir, name + ".xlsx")
            write_workbook(out, [(p, res)])
            print("  %s  ->  %s  (%d 行)" %
                  (os.path.basename(p), os.path.basename(out), res["meta"]["records"]))

    return 0


if __name__ == "__main__":
    sys.exit(main())
