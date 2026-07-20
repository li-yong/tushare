#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""us_weekly_report.py — 合并 us_weekly_run.sh 产出的各子报告为一份当周总报告。

us_weekly_run.sh 每周日收盘后跑多个慢变量筛子，各自把报告写到 /home/ryan/DATA/result/
（us_undervalue_* / us_watchlist_suggest_* / us_news_top_* / us_signal_attrib_* /
us_tide_gauge_* / us_wind_class_* / us_swell_*）。本脚本把它们读回来，抽取其中
可执行的【风险提示 / 候选发现 / 复盘素材】，注明来源，汇成一份 Markdown 总报告
us_weekly_report_<date>.md，并在末尾原样附上各子报告全文。

与 us_daily_report.py 的分工：
  · 日报管【今天怎么动手】——买/卖/止损，来源是日频扫描。
  · 周报管【这周想什么】——水文环境(潮/风)、慢筛候选、信号账本的自我审计、
    周日复盘清单。周频筛子没有当日执行力，所以周报没有"就绪入场"段。
  · 同一只票被多个周频筛子命中，越值得排进下周研究队列 —— 做一张共振计票表。
  · 解析全部包在 try/except 里：单个子报告格式异常不影响其余，原文照样附在末尾。

用法:
    python us_weekly_report.py                # 今天
    python us_weekly_report.py --date 2026-07-19
    python us_weekly_report.py --stdout       # 同时打印到屏幕
"""

import argparse
import datetime as dt
import glob
import os
import re
import sys

RESULT_DIR = "/home/ryan/DATA/result"

# 每个来源: (key, 文件名前缀, 扩展名, 中文标签)
SOURCES = [
    ("undervalue",   "us_undervalue_",        "txt", "低估优质(主)"),
    ("watchlist",    "us_watchlist_suggest_", "csv", "观察名单建议"),
    ("news_top",     "us_news_top_",          "md",  "新闻见顶/启动"),
    ("signal_attrib","us_signal_attrib_",     "txt", "信号归因"),
    ("tide_gauge",   "us_tide_gauge_",        "txt", "潮位仪·真水量"),
    ("wind_class",   "us_wind_class_",        "txt", "阵风/季风分类"),
    ("swell",        "us_swell_",             "txt", "无风有涌"),
]

STOPOUT_CSV = "/home/ryan/DATA/result/us_signal_log/stopout_autopsy.csv"

_DATE_RE = re.compile(r"(\d{4})-?(\d{2})-?(\d{2})\.(?:txt|md|csv)$")

FLOAT = re.compile(r"-?\d+\.?\d*")


def find_report(prefix, ext, target, result_dir):
    """返回 prefix 系列里日期 <= target 的最新一个文件路径，找不到返回 (None, None)。

    自动兼容 YYYYMMDD 与 YYYY-MM-DD 两种命名（周频脚本两种都有）。
    """
    best, best_d = None, None
    # Recursive: finds the report whether it sits flat in result/ or under a
    # per-script subfolder (result/<script>/<prefix><date>.<ext>).
    for path in glob.glob(os.path.join(result_dir, "**", prefix + "*." + ext),
                          recursive=True):
        m = _DATE_RE.search(os.path.basename(path))
        if not m:
            continue
        d = dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d <= target and (best_d is None or d > best_d):
            best, best_d = path, d
    return best, best_d


def read(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


# ── 各子报告解析器 ──────────────────────────────────────────────────────────
# 每个解析器接收原始文本，返回结构化结果；失败时调用方兜底为 {}。


def parse_undervalue(text):
    """低估优质：抓 命中数 / ◎真低估名单（Futu 佐证真便宜）/ 非◎的"看着便宜"名。"""
    out = {"pool": None, "hits": None, "true_cnt": None, "rows": []}
    m = re.search(r"股票池:\s*(\S+)\s*\((\d+)\s*只\)\s*\|\s*命中\s*(\d+)\s*只", text)
    if m:
        out["pool"] = f"{m.group(1)} ({m.group(2)}只)"
        out["hits"] = int(m.group(3))
    m = re.search(r"◎真低估\s*(\d+)\s*只", text)
    if m:
        out["true_cnt"] = int(m.group(1))
    for ln in text.splitlines():
        mt = re.match(r"^(◎ )?([A-Z][A-Z.]{0,5})\s{2,}(\S.*)$", ln)
        if not mt or mt.group(2) in ("US",):
            continue
        rest = mt.group(3)
        matches = list(FLOAT.finditer(rest))
        if len(matches) < 4:
            continue
        nums = [m2.group(0) for m2 in matches]
        # 列: Sector Price Cap$B 1y% ROE3y [PE分位] [晨星★] [P/FV] flags
        # 中间列可空(无 Futu 数据), 但 P/FV 恒为最后一个数字, flags 是其后的文字。
        sector = re.match(r"^([A-Za-z /]+?)\s{2,}", rest)
        out["rows"].append({
            "ticker": mt.group(2), "true": bool(mt.group(1)),
            "sector": sector.group(1).strip() if sector else "?",
            "oneyr": nums[2], "roe": nums[3],
            "pfv": nums[-1] if len(nums) >= 5 else "?",
            "flags": rest[matches[-1].end():].strip(),
        })
    return out


def parse_watchlist(text):
    """观察名单建议：CSV → ADD 候选 / DROP 死票 / KEEP 计数。PRINT-ONLY，
    真正改 select.yml 是手工 --apply。"""
    adds, drops, keep = [], [], 0
    for ln in text.splitlines():
        cells = ln.strip().split(",")
        if len(cells) < 6 or cells[0] == "ticker":
            continue
        tk, action = cells[0], cells[5]
        if action == "ADD":
            adds.append({"ticker": tk, "premium": cells[3], "delivery": cells[4]})
        elif action.startswith("DROP"):
            drops.append(tk)
        elif action == "KEEP":
            keep += 1
    return {"adds": adds, "drops": drops, "keep": keep}


def _news_entries(block):
    """news_top md 的一个命中段落 → [{ticker, level, source, 结论, 明细行…}]。"""
    entries = []
    cur = None
    for ln in block.splitlines():
        m = re.match(r"^### (\S+) \*\*(\w+)\*\* \[(\w+)\]", ln)
        if m:
            cur = {"ticker": m.group(1), "level": m.group(2),
                   "source": m.group(3), "lines": []}
            entries.append(cur)
        elif cur is not None and ln.startswith("- "):
            cur["lines"].append(ln[2:].strip())
    return entries


def parse_news_top(text):
    """新闻见顶/启动：抓 稻草命中(风险) 与 火种命中(启动候选) 两段 + 概况行。"""
    out = {"summary": None, "straws": [], "sparks": [], "price_only": False}
    m = re.search(r"见顶\s*(\d+)\s*只\s*·\s*启动候选\s*(\d+)\s*只.*?"
                  r"market state=(\w+).*?\$\s*([\d.]+)", text)
    if m:
        out["summary"] = (f"见顶扫描 {m.group(1)} 只 · 启动候选 {m.group(2)} 只 · "
                          f"market state={m.group(3)} · Grok 成本 ≈ ${m.group(4)}")
    if "新闻层不可用" in text:
        out["price_only"] = True
    ms = re.search(r"## ⚠️ 稻草命中.*?\n(.*?)(?=\n## )", text, re.S)
    if ms:
        out["straws"] = _news_entries(ms.group(1))
    mf = re.search(r"## 🔥 火种命中.*?\n(.*?)(?=\n## )", text, re.S)
    if mf:
        out["sparks"] = _news_entries(mf.group(1))
    return out


def parse_attrib(text):
    """信号归因：episode 概况 + BY SOURCE 表(样本进度/stop-hit) + 尸检待办数。"""
    out = {"summary": None, "rows": [], "autopsy": None, "regime_warn": None}
    m = re.search(r"episodes:\s*(\d+)\s*total\s*·\s*(\d+)\s*mature[^·]*·\s*(\d+)\s*pending", text)
    if m:
        out["summary"] = (f"{m.group(1)} episodes · {m.group(2)} 成熟可归因 · "
                          f"{m.group(3)} 窗口未满")
    ms = re.search(r"\[ BY SOURCE × SIGNAL TYPE \].*?\n(.*?)\n\s*\n", text, re.S)
    if ms:
        for ln in ms.group(1).splitlines():
            mt = re.match(r"^(\w+)\s+(\S+)( ⚠)?\s+(\d+)\s+(\d+)\s+(.*)$", ln)
            if not mt:
                continue
            tail = mt.group(6).split()
            out["rows"].append({
                "source": mt.group(1), "type": mt.group(2),
                "small": bool(mt.group(3)), "n": int(mt.group(4)),
                "n63": int(mt.group(5)),
                "stop_hit": tail[-1] if tail else "—",
            })
    ma = re.search(r"(\d+)\s*笔已登记\s*·\s*(\d+)\s*笔待尸检", text)
    if ma:
        out["autopsy"] = {"total": int(ma.group(1)), "pending": int(ma.group(2))}
    if "单一 regime 警告" in text:
        out["regime_warn"] = ("样本几乎全来自同一市场体制, 结论仅在该体制内有效")
    return out


def parse_tide(text):
    """潮位仪：水量潮向 + 与价格潮位的对照(背离才有新信息) + 净流动性/市值/ETF流。"""
    out = {"direction": None, "cross": None, "netliq": None,
           "cap": None, "inflow": None, "outflow": None}
    for ln in text.splitlines():
        s = re.sub(r"\s+", " ", ln).strip()
        if s.startswith("→ 水量潮向"):
            out["direction"] = s.lstrip("→ ").strip()
        elif s.startswith("潮向对照:"):
            out["cross"] = s
        elif s.startswith("净流动性"):
            out["netliq"] = s
        elif s.startswith("TOTAL"):
            out["cap"] = s
        elif s.startswith("Δ since") and out["cap"]:
            out["cap"] += "　" + s
        elif s.startswith("流入前5:"):
            out["inflow"] = s
        elif s.startswith("流出前5:"):
            out["outflow"] = s
    return out


def parse_wind(text):
    """阵风/季风：价格端表里 分类≠无风 的板块 + 叙事端主导叙事行。"""
    out = {"rows": [], "narratives": [], "young_log": False}
    in_narr = False
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if ln.startswith("[ 叙事端"):
            in_narr = True
            continue
        if ln.startswith("[ 读法"):
            in_narr = False
        mt = re.match(r"^\s{2}([A-Z]{2,5})\s+(\S+)\s+(\S+)\s+(\d+)%\s+(\d+日前)\s+"
                      r"([+\-][\d.]+%)\s+(.*)$", ln)
        if mt and not in_narr:
            if mt.group(3) == "无风":
                continue
            out["rows"].append({
                "etf": mt.group(1), "sector": mt.group(2), "cls": mt.group(3),
                "persist": mt.group(4) + "%", "hi": mt.group(5),
                "rel21": mt.group(6), "resp": mt.group(7).strip() or "—",
            })
        elif in_narr:
            mn = re.match(r"^\s{2}(\S+)\s+·第(\d+)周\s*(\([^)]*\))?\s*(.+)$", ln)
            if mn:
                desc = ""
                if i + 1 < len(lines) and lines[i + 1].startswith("         "):
                    desc = lines[i + 1].strip()
                out["narratives"].append({
                    "cls": mn.group(1), "weeks": mn.group(2),
                    "dir": (mn.group(3) or "").strip("()"),
                    "name": mn.group(4).strip(), "desc": desc,
                })
            elif "日志积累" in ln and "别当真" in ln:
                out["young_log"] = True
    return out


def parse_swell(text):
    """无风有涌：Grok 复核段的 SWELL / 有名之风 标注 + 价格漏斗命中数。"""
    out = {"funnel_cnt": None, "swells": [], "named": []}
    m = re.search(r"共\s*(\d+)\s*个命中", text)
    if m:
        out["funnel_cnt"] = int(m.group(1))
    for ln in text.splitlines():
        ms = re.match(r"^\s*🌊 SWELL\s+([A-Z.]{1,6})\s+\[(\w+)\]\s*(.*)$", ln)
        if ms:
            out["swells"].append({"ticker": ms.group(1), "news": ms.group(2),
                                  "note": ms.group(3).strip()})
            continue
        mn = re.match(r"^\s*有名之风\s+([A-Z.]{1,6})\s+\[(\w+)\]\s*(.*)$", ln)
        if mn:
            out["named"].append({"ticker": mn.group(1), "news": mn.group(2),
                                 "note": mn.group(3).strip()})
    return out


# ── 组装总报告 ──────────────────────────────────────────────────────────────

def build_report(target, result_dir):
    found = {}          # key -> (path, date)
    raw = {}            # key -> 原文
    for key, prefix, ext, _label in SOURCES:
        path, d = find_report(prefix, ext, target, result_dir)
        found[key] = (path, d)
        if path:
            raw[key] = read(path)

    def safe(parser, key):
        if key not in raw:
            return {}
        try:
            return parser(raw[key])
        except Exception as e:                         # 单报告解析失败不致命
            sys.stderr.write(f"[warn] 解析 {key} 失败: {e}\n")
            return {}

    under = safe(parse_undervalue, "undervalue")
    watch = safe(parse_watchlist, "watchlist")
    news = safe(parse_news_top, "news_top")
    attrib = safe(parse_attrib, "signal_attrib")
    tide = safe(parse_tide, "tide_gauge")
    wind = safe(parse_wind, "wind_class")
    swell = safe(parse_swell, "swell")

    L = []  # 输出行
    A = L.append
    A(f"# US 每周汇总报告 — {target.isoformat()}")
    A("")
    A(f"_生成时间 {dt.datetime.now():%Y-%m-%d %H:%M}　·　合并自 `us_weekly_run.sh` 各子报告。"
      "分工：日报管今天怎么动手，周报管这周想什么 —— 环境(潮/风)、慢筛候选、账本审计、复盘清单。_")
    A("")

    # ── 一、水文环境 潮 / 风 ──
    A("## 一、水文环境（潮=流动性 · 风=叙事；提示不门控）")
    A("")
    if tide.get("direction"):
        _, td = found["tide_gauge"]
        A(f"- **水量潮向**（数据日 {td}）：{tide['direction']}　[来源 us_tide_gauge]")
        if tide.get("cross"):
            tag = "✅ 一致" if tide["cross"].rstrip().endswith("一致") else "⚠️ **背离——才是有新信息的读数**"
            A(f"- {tide['cross']}　{tag}")
        if tide.get("netliq"):
            A(f"- {tide['netliq']}")
        if tide.get("cap"):
            A(f"- 水位（潮×浪合成，只看趋势别当水量）：{tide['cap']}")
        if tide.get("inflow"):
            A(f"- ETF 创赎（真水搬家）：{tide['inflow']}；{tide.get('outflow') or ''}")
    else:
        A("- （无潮位仪数据）")
    A("")
    wrows = wind.get("rows", [])
    if wrows:
        A("**风况（相对线持续性×新鲜度；季风才值得仓位级响应，阵风只做单浪）**　[来源 us_wind_class]")
        A("")
        A("| ETF | 板块 | 分类 | 持续 | RS新高 | rel21 | 响应档位 |")
        A("|-----|------|------|------|--------|-------|----------|")
        for r in wrows:
            A(f"| {r['etf']} | {r['sector']} | **{r['cls']}** | {r['persist']} "
              f"| {r['hi']} | {r['rel21']} | {r['resp']} |")
        A("")
    for n in wind.get("narratives", []):
        warn = "（日志<3周, 周数被低估, 分类先别当真）" if wind.get("young_log") else ""
        A(f"- **主导叙事**：{n['name']}　{n['cls']}·第{n['weeks']}周"
          + (f"({n['dir']})" if n["dir"] else "") + f"{warn}　[来源 us_wind_class · Grok]")
        if n.get("desc"):
            A(f"  - {n['desc']}")
    A("")

    # ── 二、风险提示 ──
    A("## 二、🔴 见顶稻草 / 持仓风险　（先管风险；减仓仍走价格系统 L0/L1.5/L2）")
    A("")
    straws = news.get("straws", [])
    if news.get("summary"):
        _, nd = found["news_top"]
        A(f"- 新闻扫描概况（数据日 {nd}）：{news['summary']}"
          + ("　⚠️ 本期仅价格侧(xAI 不可用)" if news.get("price_only") else "")
          + "　[来源 us_news_top]")
        A("")
    if straws:
        for s in straws:
            A(f"### {s['ticker']} 稻草 **{s['level']}** [{s['source']}]")
            for ln in s["lines"]:
                A(f"- {ln}")
            A("")
        A("> 稻草命中 = L1 减仓待价格确认信号：新闻主语迁往生态 + §2.1 衰竭bar 才动手，"
          "单独的新闻稻草不是卖出指令。")
    else:
        A("_本周无稻草命中。_")
    A("")

    # ── 三、候选发现 + 共振计票 ──
    A("## 三、🟢 候选发现　（周频筛子无当日执行力：入场仍走关键K线 entry/stop + 日报确认）")
    A("")

    # ticker -> {source_label: 原因短语}
    conf = {}

    def add(ticker, label, why):
        ticker = ticker.strip().upper()
        if not ticker:
            return
        conf.setdefault(ticker, {})[label] = why

    A("### A. 新闻启动火种（价格半 + 叙事半都亮）")
    A("")
    sparks = news.get("sparks", [])
    if sparks:
        for s in sparks:
            A(f"### {s['ticker']} 火种 **{s['level']}** [{s['source']}]")
            for ln in s["lines"]:
                A(f"- {ln}")
            A("")
            add(s["ticker"], "启动火种", f"{s['level']}·主语回归")
    else:
        A("_本周无火种命中。_")
        A("")

    A("### B. 低估优质（超跌×质地；候选清单非买入信号）")
    A("")
    urows = under.get("rows", [])
    true_rows = [r for r in urows if r["true"]]
    if urows:
        _, ud = found["undervalue"]
        A(f"命中 {under.get('hits', '?')} 只（池 {under.get('pool', '?')}），"
          f"其中 ◎真低估 {under.get('true_cnt', len(true_rows))} 只"
          f"（Futu 佐证 PE分位≤50 且不高于公允价值 且晨星★≥3）。数据日 {ud}。")
        A("")
        show = true_rows[:15]
        A("| 标的 | 板块 | 1y% | ROE3y | P/FV | flags |")
        A("|------|------|-----|-------|------|-------|")
        for r in show:
            A(f"| **{r['ticker']}** | {r['sector']} | {r['oneyr']} | {r['roe']} "
              f"| {r['pfv']} | {r['flags'] or '—'} |")
        if len(true_rows) > len(show):
            A(f"| … | 还有 {len(true_rows) - len(show)} 只 ◎ | | | | 见附录原文 |")
        A("")
        A("> 逐只过笔记 §二核心三问：① 为什么便宜(暂时 vs 结构)？② 三年后更强吗？"
          "③ 比买 QQQ 好吗？非◎的（PE分位高/高于公允价值）是 MU 式周期陷阱候选。")
        for r in true_rows:
            add(r["ticker"], "低估优质◎", f"1y {r['oneyr']}% · P/FV {r['pfv']}")
    else:
        A("_本周无低估优质命中。_")
    A("")

    A("### C. 无风有涌（碾磨式跑赢 + 查无催化剂 = 能量来自远方）")
    A("")
    swells = swell.get("swells", [])
    if swells or swell.get("named"):
        if swell.get("funnel_cnt"):
            A(f"价格漏斗命中 {swell['funnel_cnt']} 只，Grok 复核前 8 名：")
            A("")
        for r in swells:
            A(f"- 🌊 **{r['ticker']}** SWELL [{r['news']}]　{r['note']}")
            add(r["ticker"], "无风有涌🌊", f"SWELL·新闻{r['news']}")
        for r in swell.get("named", []):
            A(f"- 有名之风 {r['ticker']} [{r['news']}]　{r['note']}")
        A("")
        A("> SWELL 标注不门控（registry #24, n 待积累）：假说是 SWELL 后续优于同涨幅"
          "有名之风；样本在 swell_log.csv 前向积累。")
    else:
        A("_本周无 SWELL 标注。_")
    A("")

    A("### D. 观察名单建议（print-only；改 select.yml 永远手工 --apply）")
    A("")
    adds = watch.get("adds", [])
    drops = watch.get("drops", [])
    if adds or drops:
        for r in adds:
            A(f"- ➕ ADD 候选 **{r['ticker']}**（贵气 {r['premium']} · 兑现 {r['delivery']}）")
            add(r["ticker"], "观察名单ADD", f"贵气{r['premium']}·兑现{r['delivery']}")
        if drops:
            A(f"- ➖ DROP 死票：{', '.join(drops)}")
        A("")
    elif watch:
        A(f"_无 ADD/DROP 建议（KEEP {watch.get('keep', 0)} 只）。_")
        A("")
    else:
        A("_无观察名单数据。_")
        A("")

    A("### E. 周频筛子共振计票")
    A("")
    if conf:
        ranked = sorted(conf.items(), key=lambda kv: (-len(kv[1]), kv[0]))
        A("| 标的 | 命中数 | 命中来源（=原因 + 来自哪个筛子） |")
        A("|------|:------:|------|")
        for tk, srcs in ranked:
            detail = "；".join(f"**{lab}**（{why}）" for lab, why in srcs.items())
            A(f"| {tk} | {len(srcs)} | {detail} |")
        A("")
        multi = [tk for tk, s in ranked if len(s) >= 2]
        if multi:
            A(f"> **多筛子共振（≥2）**：{', '.join(multi)} —— 排进下周研究队列，"
              "等日报的关键K线择时 + 写止损再动手。")
    else:
        A("_各周频筛子本周无候选命中。_")
    A("")

    # ── 四、信号账本审计 ──
    A("## 四、📒 信号账本审计　（哪个筛子真有 edge —— ledger 是知识, huice 只是假说）")
    A("")
    if attrib.get("summary"):
        _, ad = found["signal_attrib"]
        A(f"- **归因概况**（数据日 {ad}）：{attrib['summary']}　[来源 us_signal_attrib]")
        if attrib.get("regime_warn"):
            A(f"- ⚠️ 单一 regime 警告：{attrib['regime_warn']}。")
        arows = attrib.get("rows", [])
        if arows:
            A("")
            A("| source | type | n | n63d | stop-hit≤63d |")
            A("|--------|------|---|------|--------------|")
            for r in arows:
                warn = " ⚠" if r["small"] else ""
                A(f"| {r['source']} | {r['type']}{warn} | {r['n']} | {r['n63']} | {r['stop_hit']} |")
            A("")
            A("> ⚠ = n<5 别读EV；n63d<n 的差值是窗口未满的新 episode，样本每周长大。"
              "medNd%/α 列成熟后看原文。")
    else:
        A("_无信号归因数据。_")
    A("")

    # ── 五、周日复盘清单 ──
    A("## 五、☑️ 周日复盘清单　（周脉冲）")
    A("")
    ap = attrib.get("autopsy")
    if ap and ap["pending"]:
        A(f"- [ ] **止损尸检：{ap['pending']} 笔待填 `falsified_assumption`**"
          f"（四选一 pool/regime/context/tail）→ `{STOPOUT_CSV}`")
    elif ap:
        A(f"- [x] 止损尸检：{ap['total']} 笔已全部填写。")
    A("- [ ] 过最新日报的「近5日信号自证榜」：⏳没动 名单到时间预算即换仓候选；"
      "✗走弱 归止损纪律管。")
    A("- [ ] 本周复盘若产出结论，按认识等级制收尾：**已改哪个门控/参数**，或**明确不改+理由**，"
      "并更新 `docs/rule_registry.md`。")
    A("- [ ] 主要矛盾 banner（US_PRINCIPAL_CONTRADICTION, 10 天保质期）过期则重写。")
    A("- [ ] 上表共振计票 ≥2 的票：排研究队列，逐只跑 `t_us_key_kline.py --ticker SYM`。")
    A("")

    # ── 数据源清单 ──
    A("## 六、数据源 / 子报告清单")
    A("")
    A("| 子报告 | 标签 | 文件 |")
    A("|--------|------|------|")
    for key, _prefix, _ext, label in SOURCES:
        path, d = found[key]
        fname = os.path.basename(path) if path else "（缺失）"
        A(f"| {key} | {label} | {fname} |")
    A("")

    # ── 附：各子报告原文 ──
    A("---")
    A("")
    A("## 附录：各子报告原文")
    A("")
    for key, _prefix, _ext, label in SOURCES:
        path, _d = found[key]
        if not path:
            continue
        A(f"### {label}　`{os.path.basename(path)}`")
        A("")
        A("```text")
        A(raw[key].rstrip("\n"))
        A("```")
        A("")

    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser(description="合并 us_weekly_run.sh 各子报告为当周总报告")
    ap.add_argument("--date", help="目标日期 YYYY-MM-DD 或 YYYYMMDD（默认今天）")
    ap.add_argument("--result-dir", default=RESULT_DIR, help=f"子报告目录（默认 {RESULT_DIR}）")
    ap.add_argument("--out", help="输出文件路径（默认 <result-dir>/weekly_report/us_weekly_report_<date>.md）")
    ap.add_argument("--stdout", action="store_true", help="同时把总报告打印到屏幕")
    args = ap.parse_args()

    if args.date:
        s = args.date.replace("-", "")
        target = dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    else:
        target = dt.date.today()

    report = build_report(target, args.result_dir)

    if args.out:
        out = args.out
    else:
        out_dir = os.path.join(args.result_dir, "weekly_report")
        os.makedirs(out_dir, exist_ok=True)
        out = os.path.join(out_dir, f"us_weekly_report_{target:%Y%m%d}.md")
    with open(out, "w", encoding="utf-8") as f:
        f.write(report)
    sys.stderr.write(f"[ok] 周报 → {out}\n")

    if args.stdout:
        sys.stdout.write(report)


if __name__ == "__main__":
    main()
