#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RDT400 原始读码数据 一站式流水线
================================

输入日期 -> 自动下载该日全部 .dat（设备清单可配置）-> 存盘 -> 逐个转 xlsx
-> 另出一份跨设备整合表。

用法
----
  # 交互式（不传日期会提示输入）
  python rdt_pipeline.py

  # 指定日期
  python rdt_pipeline.py --date 261103

  # 指定日期 + 输出目录
  python rdt_pipeline.py --date 261103 --outdir "D:/1/temp/RDT下载"

  # 只处理本地已有的 dat，不重新下载
  python rdt_pipeline.py --date 261103 --outdir ... --skip-download

  # 临时只跑某几台设备（覆盖配置文件里的清单）
  python rdt_pipeline.py --date 261103 --ids 08,09,41

  # 维护设备清单配置
  python rdt_pipeline.py --sync-config        # 按服务器当前设备表刷新配置文件
  python rdt_pipeline.py --show-config        # 打印清单及启用状态

设备清单配置（rdt_devices.json）
------------------------------
控制"下载哪些设备的 dat"，首次运行自动生成，之后直接编辑即可：

    {
      "devices": [
        {"id": "08", "name": "RFID 22.22.90-P", "enabled": true},
        {"id": "27", "name": "（服务器无此设备）", "enabled": false},
        ...
      ]
    }

  * id      设备 ID，即 rawfiles 文件名里的两位数字
  * name    设备名，仅作可读性标注（以服务器 System List 为准）
  * enabled 是否下载该设备的 dat；置 false 即跳过

输出目录结构
------------
    <outdir>/
      raw/20261103/261103_08_RFID 22.22.90-P.dat      原始下载文件
      xlsx/20261103/261103_08_RFID 22.22.90-P.xlsx     单设备明细（说明+明细）
      xlsx/20261103/20261103_整合读码表.xlsx            跨设备整合表

整合表列
--------
    DATETIME, ID, IDX, CODE1_CONTENT, CODE1_2_CONTENT,
    CODE2_CONTENT, CODE2_2_CONTENT, DEVICE, SOURCE_FILE

  CODE1_CONTENT / CODE1_2_CONTENT 分别是同一次事件里读到的第 1 张、第 2 张标签
  （CODE2_* 同理对应第二种标签类型）。
"""

import argparse
import json
import os
import re
import sys
import threading
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from rdt_raw_download import (BASE, build_url, fetch_device_map, http_get,  # noqa: E402
                               safe_name)
from rdt_dat_to_xlsx import parse_file, write_workbook  # noqa: E402

CONFIG_PATH = os.path.join(SCRIPT_DIR, "rdt_devices.json")

# 整合表需要的原始列名 -> 输出列名
CONSOLIDATE_MAP = [
    ("CODE1_CONTENT", "CODE1_CONTENT"),
    ("CODE1_2_CONTENT", "CODE1_2_CONTENT"),
    ("CODE2_CONTENT", "CODE2_CONTENT"),
    ("CODE2_2_CONTENT", "CODE2_2_CONTENT"),
]


# --------------------------------------------------------------------------
# 设备清单配置
# --------------------------------------------------------------------------
def load_config(path=CONFIG_PATH):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as exc:
        print("配置文件读取失败（%s），将按服务器设备表重建：%s" % (path, exc))
        return None
    if not isinstance(cfg.get("devices"), list):
        return None
    return cfg


def save_config(cfg, path=CONFIG_PATH):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)
    return path


def sync_config(devmap, path=CONFIG_PATH):
    """按服务器当前设备表刷新配置；保留已有设备的 enabled 状态。"""
    old = load_config(path) or {}
    old_by_id = {d.get("id"): d for d in old.get("devices", []) if isinstance(d, dict)}

    devices = []
    for ident in sorted(devmap):
        name = devmap[ident][0]
        prev = old_by_id.get(ident)
        enabled = bool(prev.get("enabled", True)) if prev else True
        devices.append({"id": ident, "name": name, "enabled": enabled})

    # 配置里存在但服务器上已消失的设备：保留但默认关闭，并标注
    for ident, prev in old_by_id.items():
        if ident not in devmap:
            devices.append({
                "id": ident,
                "name": prev.get("name", "(服务器已无此设备)"),
                "enabled": False,
            })
    devices.sort(key=lambda d: d["id"])
    cfg = {
        "_说明": "控制 rdt_pipeline.py 下载哪些设备的 dat。id=设备ID，enabled=true 才下载。"
                 "改完直接保存即可；服务器新增设备可用 --sync-config 刷新。",
        "devices": devices,
    }
    save_config(cfg, path)
    return cfg


def enabled_ids(cfg, devmap):
    """从配置里取出要下载的设备 ID 列表。"""
    out = []
    for d in cfg.get("devices", []):
        if not isinstance(d, dict):
            continue
        if d.get("enabled", True):
            ident = str(d.get("id", "")).zfill(2)
            if ident.isdigit():
                out.append(ident)
    return sorted(set(out))


# --------------------------------------------------------------------------
# 下载
# --------------------------------------------------------------------------
def download_batch(date6, idents, devmap, datdir, workers=6, retries=2):
    """并发下载。返回 (成功列表[(ident, path)], 统计字典)。"""
    if not os.path.isdir(datdir):
        os.makedirs(datdir, exist_ok=True)

    results = {}
    lock = threading.Lock()
    counter = {"done": 0}
    total = len(idents)

    def one(ident):
        url = build_url(date6, ident, "rw")
        state, msg, path = "error", "", None
        for attempt in range(retries + 1):
            try:
                status, body = http_get(url)
            except Exception as exc:
                state, msg = "error", "网络错误：%s" % exc
                time.sleep(1.2 * (attempt + 1))
                continue
            if status == 404:
                state, msg = "missing", "服务器无此文件（HTTP 404）"
                break
            if status != 200:
                state, msg = "error", "HTTP %s" % status
                time.sleep(1.2 * (attempt + 1))
                continue
            if not body:
                state, msg = "error", "响应为空"
                continue
            if body[:9] != b"RDT400 Se":
                state, msg = "bad", "内容不是 RDT400 日志"
                break

            devname = devmap.get(ident, (ident, ""))[0]
            fname = "%s_%s_%s.dat" % (date6, ident, safe_name(devname))
            fpath = os.path.join(datdir, fname)
            with open(fpath, "wb") as fh:
                fh.write(body)
            nrec = max(0, body.count(b"\n") - 5)
            state = "ok"
            msg = "%s  %d 条记录  %.1f MB" % (devname, nrec, len(body) / 1048576.0)
            path = fpath
            break

        with lock:
            results[ident] = (state, msg, path)
            counter["done"] += 1
            done = counter["done"]
        mark = {"ok": "OK", "missing": "缺失", "bad": "异常", "error": "错误"}.get(state, "?")
        print("  [%3d/%3d] %-4s ID=%s  %s" % (done, total, mark, ident, msg))
        return state

    threads = []
    for ident in idents:
        t = threading.Thread(target=one, args=(ident,))
        threads.append(t)
    # 简单分批启动，避免瞬间打满
    batch = max(1, workers)
    for i in range(0, len(threads), batch):
        for t in threads[i:i + batch]:
            t.start()
        for t in threads[i:i + batch]:
            t.join()

    stat = {"ok": 0, "missing": 0, "bad": 0, "error": 0}
    ok_list = []
    for ident in idents:
        state, msg, path = results.get(ident, ("error", "未执行", None))
        stat[state] = stat.get(state, 0) + 1
        if state == "ok" and path:
            ok_list.append((ident, path))
    return ok_list, stat


# --------------------------------------------------------------------------
# 整合表
# --------------------------------------------------------------------------
def col_positions(cols):
    """列名 -> 在 rec["raw"] 中的下标。"""
    return {c["name"]: i for i, c in enumerate(cols)}


def build_consolidated(path, items):
    """
    items : [(ident, datpath, res), ...]
    生成跨设备整合表。
    """
    import openpyxl
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    XL_PRIMARY = "FF4472C4"
    XL_LIGHT = "FFD9E2F3"
    XL_BORDER = "FFBFBFBF"
    thin = Side(style="thin", color=XL_BORDER)

    wb = openpyxl.Workbook(write_only=True)
    wb.properties.title = "RDT400 整合读码表"
    wb.properties.creator = "RDT pipeline"

    headers = ["DATETIME", "ID", "IDX"] + [o for _, o in CONSOLIDATE_MAP] + \
              ["DEVICE", "SOURCE_FILE"]

    ws = wb.create_sheet("整合读码表")
    widths = [19, 7, 9, 16, 16, 16, 16, 24, 30]
    for i, w in enumerate(widths):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = "A1:%s1" % openpyxl.utils.get_column_letter(len(headers))

    hcells = []
    for h in headers:
        c = WriteOnlyCell(ws, value=h)
        c.font = Font(bold=True, color="FFFFFFFF", size=10)
        c.fill = PatternFill("solid", fgColor=XL_PRIMARY)
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = Border(bottom=thin)
        hcells.append(c)
    ws.append(hcells)

    total = 0
    for ident, datpath, res in items:
        pos = col_positions(res["cols"])
        id_idx = pos.get("ID")
        idx_idx = pos.get("IDX")
        tgt = [(pos.get(src), out) for src, out in CONSOLIDATE_MAP if pos.get(src) is not None]
        devname = res["meta"].get("system") or ""
        srcname = res["meta"].get("source") or os.path.basename(datpath)

        for rec in res["records"]:
            raw = rec["raw"]
            row = [rec["dt"]]
            row.append(raw[id_idx] if id_idx is not None and id_idx < len(raw) else "")
            row.append(raw[idx_idx] if idx_idx is not None and idx_idx < len(raw) else "")
            for i, _ in tgt:
                row.append(raw[i] if i < len(raw) else "")
            row.append(devname)
            row.append(srcname)
            ws.append(row)
            total += 1

    wb.save(path)
    return total


def build_device_summary(path, items):
    """单设备清单速览：每个设备一行的汇总。"""
    import openpyxl
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    XL_PRIMARY = "FF4472C4"
    thin = Side(style="thin", color="FFBFBFBF")
    wb = openpyxl.Workbook(write_only=True)
    wb.properties.title = "设备汇总"

    headers = ["ID", "设备名", "记录数", "CODE1数", "CODE2数", "多标签行数",
               "首条时刻", "末条时刻", "数据源"]
    ws = wb.create_sheet("设备汇总")
    for i, w in enumerate([7, 26, 11, 11, 11, 13, 12, 12, 32]):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = w
    ws.freeze_panes = "A2"

    hcells = []
    for h in headers:
        c = WriteOnlyCell(ws, value=h)
        c.font = Font(bold=True, color="FFFFFFFF", size=10)
        c.fill = PatternFill("solid", fgColor=XL_PRIMARY)
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = Border(bottom=thin)
        hcells.append(c)
    ws.append(hcells)

    for ident, datpath, res in items:
        pos = col_positions(res["cols"])
        recs = res["records"]
        c1i = pos.get("CODE1_CONTENT")
        c2i = pos.get("CODE2_CONTENT")
        c12i = pos.get("CODE1_2_CONTENT")
        c1 = sum(1 for r in recs if c1i is not None and r["raw"][c1i].strip())
        c2 = sum(1 for r in recs if c2i is not None and r["raw"][c2i].strip())
        mc = sum(1 for r in recs if c12i is not None and r["raw"][c12i].strip())
        dts = [r["dt"] for r in recs if r["dt"]]
        ws.append([
            ident,
            res["meta"].get("system", ""),
            len(recs), c1, c2, mc,
            dts[0].strftime("%H:%M:%S") if dts else "",
            dts[-1].strftime("%H:%M:%S") if dts else "",
            os.path.basename(datpath),
        ])
    wb.save(path)
    return path


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="RDT400 原始读码数据：一站式下载 + 转 xlsx + 整合表",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--date", help="日期 YYMMDD / YYYYMMDD / YYYY-MM-DD；不填则交互询问")
    ap.add_argument("--outdir", default=os.path.join(SCRIPT_DIR, "RDT下载"),
                    help="输出根目录")
    ap.add_argument("--config", default=CONFIG_PATH, help="设备清单配置文件路径")
    ap.add_argument("--ids", help="临时指定设备 ID（逗号分隔），覆盖配置文件清单")
    ap.add_argument("--workers", type=int, default=6, help="并发下载线程数，默认 6")
    ap.add_argument("--skip-download", action="store_true", help="不下载，只处理本地已有的 dat")
    ap.add_argument("--skip-extract", action="store_true", help="只下载，不转 xlsx")
    ap.add_argument("--sync-config", action="store_true", help="按服务器刷新设备清单配置后退出")
    ap.add_argument("--enable-all", action="store_true",
                    help="把配置里所有设备置为启用（配合 --sync-config 可一键全开）")
    ap.add_argument("--show-config", action="store_true", help="打印设备清单后退出")
    args = ap.parse_args()

    # ---- 服务器设备表 ----
    try:
        devmap = fetch_device_map()
    except Exception as exc:
        print("无法连接服务器 %s：%s" % (BASE, exc))
        return 2
    print("服务器设备表：%d 台设备" % len(devmap))

    # ---- 配置维护 ----
    if args.sync_config or args.enable_all:
        if args.enable_all:
            cfg = load_config(args.config) or {"devices": []}
            for d in cfg.get("devices", []):
                d["enabled"] = True
            for ident, (name, _ip) in devmap.items():
                if not any(x.get("id") == ident for x in cfg.get("devices", [])):
                    cfg.setdefault("devices", []).append(
                        {"id": ident, "name": name, "enabled": True})
            cfg["devices"].sort(key=lambda d: d["id"])
            save_config(cfg, args.config)
            print("已启用全部 %d 台设备：%s" % (len(cfg["devices"]), args.config))
        else:
            cfg = sync_config(devmap, args.config)
            print("已刷新设备清单：%s（%d 条）" % (args.config, len(cfg["devices"])))
        return 0

    cfg = load_config(args.config)
    if cfg is None:
        cfg = sync_config(devmap, args.config)
        print("已自动生成设备清单配置：%s" % args.config)

    if args.show_config:
        print("\n设备清单（enabled=true 才会下载）")
        print("-" * 72)
        print("%-5s %-6s %-28s %s" % ("ID", "下载", "设备名", "备注"))
        for d in cfg["devices"]:
            ident = d.get("id", "")
            on = "是" if d.get("enabled", True) else "否"
            note = ""
            if ident not in devmap:
                note = "服务器无此设备"
            print("%-5s %-6s %-28s %s" % (ident, on, d.get("name", ""), note))
        en = len(enabled_ids(cfg, devmap))
        print("-" * 72)
        print("共 %d 台，启用 %d 台" % (len(cfg["devices"]), en))
        return 0

    # ---- 日期 ----
    date6 = args.date
    if not date6:
        try:
            date6 = input("请输入日期（格式 YYMMDD，如 261103）：").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n已取消")
            return 1
    d = (date6 or "").replace("-", "").replace("/", "")
    if len(d) == 8 and d.isdigit():
        d = d[2:]
    if len(d) != 6 or not d.isdigit():
        print("日期格式应为 YYMMDD / YYYYMMDD / YYYY-MM-DD，当前：%s" % date6)
        return 2
    date6 = d
    iso = "%s-%s-%s" % ("20" + date6[0:2], date6[2:4], date6[4:6])

    # ---- 目标设备 ----
    if args.ids:
        targets = sorted({t.strip().zfill(2) for t in args.ids.replace(" ", "").split(",")
                          if t.strip().isdigit()})
        src_note = "命令行指定"
    else:
        targets = enabled_ids(cfg, devmap)
        src_note = "配置文件 %s" % os.path.basename(args.config)

    outroot = os.path.abspath(args.outdir)
    rawdir = os.path.join(outroot, "raw", iso)
    xlsdir = os.path.join(outroot, "xlsx", iso)

    print("\n日期 %s   目标设备 %d 台（来源：%s）" % (iso, len(targets), src_note))
    print("输出根目录 %s" % outroot)

    # ---- 1. 下载 ----
    ok_list = []
    if args.skip_download:
        print("\n[1/3] 跳过下载，扫描本地已有文件 ...")
        if os.path.isdir(rawdir):
            for fn in sorted(os.listdir(rawdir)):
                if not fn.lower().endswith(".dat"):
                    continue
                # 文件名格式 {日期}_{ID}_{设备名}.dat，取出 ID
                bits = fn.split("_")
                ident = bits[1] if len(bits) > 1 else ""
                if ident not in targets:
                    continue          # 只处理本次选中的设备
                ok_list.append((ident, os.path.join(rawdir, fn)))
        print("  本地匹配到 %d 个 dat（目标设备 %d 台）" % (len(ok_list), len(targets)))
    else:
        print("\n[1/3] 下载原始数据 ...")
        ok_list, stat = download_batch(date6, targets, devmap, rawdir, workers=args.workers)
        print("  -> 成功 %d   缺失 %d   异常 %d   错误 %d" % (
            stat["ok"], stat["missing"], stat["bad"], stat["error"]))

    if not ok_list:
        print("\n没有可用数据，结束。")
        return 1

    # ---- 2. 逐个转 xlsx ----
    print("\n[2/3] 解析并生成单设备明细 xlsx ...")
    items = []
    for ident, path in ok_list:
        try:
            res = parse_file(path)
        except Exception as exc:
            print("  解析失败 %s：%s" % (os.path.basename(path), exc))
            continue
        if not res["records"]:
            print("  %-44s 无数据行，跳过" % os.path.basename(path))
            continue
        items.append((ident, path, res))
        print("  %-44s %5d 条  设备=%s" % (
            os.path.basename(path), res["meta"]["records"], res["meta"]["system"]))

    if args.skip_extract:
        print("\n[3/3] 已按 --skip-extract 跳过转换")
        return 0

    if not items:
        print("\n没有可转换的数据，结束。")
        return 1

    os.makedirs(xlsdir, exist_ok=True)
    print("\n[3/3] 生成 xlsx ...")
    for ident, path, res in items:
        name = os.path.splitext(os.path.basename(path))[0]
        out = os.path.join(xlsdir, name + ".xlsx")
        write_workbook(out, [(path, res)])
        print("  %s" % os.path.basename(out))

    cons = os.path.join(xlsdir, "%s_整合读码表.xlsx" % iso)
    n = build_consolidated(cons, items)
    print("  %s   整合表 %d 行" % (os.path.basename(cons), n))

    summ = os.path.join(xlsdir, "%s_设备汇总.xlsx" % iso)
    build_device_summary(summ, items)
    print("  %s" % os.path.basename(summ))

    print("\n完成。输出目录：%s" % xlsdir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
