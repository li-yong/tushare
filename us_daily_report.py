#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""us_daily_report.py — 合并 us_daily_run.sh 产出的各子报告为一份当日总报告。

us_daily_run.sh 每天在收盘后跑多个脚本，各自把报告写到 /home/ryan/DATA/result/
（us_tech_swing_*.txt / us_resonance_*.txt / us_breakout_screen_*.txt / …）。
本脚本把它们读回来，抽取其中可执行的【买/卖/持仓管理】信号，注明：
    · 信号(BUY / SELL / MANAGE) · 标的 · 原因 · 来自哪个子报告
汇成一份 Markdown 总报告 us_daily_report_<date>.md，并在末尾原样附上各子报告全文。

设计要点：
  · 真正的买卖信号只有 t_us_tech_swing 给得出（有持仓、有止损、有 R:R）——它是主报告。
  · 其它筛子（共振 / 突破 / 缺口 / 关键K线 / 稳涨 / 池外侦察）给的是【候选池】，
    一只票被越多筛子同时命中，越值得看 —— 脚本做一张「共振计票」表，按命中来源数排序。
  · 解析全部包在 try/except 里：单个子报告格式异常不影响其余，原文照样附在末尾。

用法:
    python us_daily_report.py                # 今天
    python us_daily_report.py --date 2026-06-28
    python us_daily_report.py --stdout       # 同时打印到屏幕
"""

import argparse
import datetime as dt
import glob
import os
import re
import sys

RESULT_DIR = "/home/ryan/DATA/result"

# 每个来源: (key, 文件名前缀, 中文标签)
SOURCES = [
    ("tech_swing",  "us_tech_swing_",       "技术Swing(主)"),
    ("regime",      "us_regime_monitor_",   "体制监控"),
    ("resonance",   "us_resonance_",        "三层共振"),
    ("breakout",    "us_breakout_screen_",  "横盘突破"),
    ("steady",      "us_steady_climb_",     "小步慢涨"),
    ("searchlight", "us_searchlight_",      "池外侦察"),
    ("key_kline",   "us_key_kline_scan_",   "关键K线"),
    ("gap_scan",    "us_gap_scan_",         "向上缺口"),
    ("gap_activity","us_gap_activity_",     "缺口活跃度"),
    ("signal_attrib","us_signal_attrib_",   "信号归因(周)"),
]

_DATE_RE = re.compile(r"(\d{4})-?(\d{2})-?(\d{2})\.txt$")


def find_report(prefix, target, result_dir):
    """返回 prefix 系列里日期 <= target 的最新一个 .txt 文件路径，找不到返回 None。

    自动兼容 YYYYMMDD 与 YYYY-MM-DD 两种命名；regime 监控常落后一两个交易日，
    这样能取到 <=target 的最近一份，而不是漏掉。
    """
    best, best_d = None, None
    # Recursive: finds the report whether it sits flat in result/ or under a
    # per-script subfolder (result/<script>/<prefix>_<date>.txt).
    for path in glob.glob(os.path.join(result_dir, "**", prefix + "*.txt"),
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

FLOAT = re.compile(r"-?\d+\.?\d*")


def parse_tech_swing(text):
    """主报告：抽 市场态(含粘滞带/GATE) / 入场信号(BUY, 含⛔ER静默期) / 持仓止损
    (SELL: BREACHED 周线 + STOP HIT 日线Layer1.5) / OPEN HEAT / 财报前决策 /
    持仓管理(EXIT/MOVE/TRIM) / AI capex 链布局线索(买半导体, 非买云厂商)。"""
    out = {"market_state": None, "entries": [], "sells": [], "manage": [],
           "capex_rows": [], "capex_note": None,
           "sticky": [], "gate_note": None, "breadth": None,
           "approaching": [], "open_heat": None, "over_cap": False,
           "er_decisions": [], "no_init": None}

    m = re.search(r"\[ MARKET STATE:\s*([A-Z]+)\s*\]", text)
    if m:
        out["market_state"] = m.group(1)

    lines = text.splitlines()
    section = None
    for ln in lines:
        if ln.startswith("["):
            if "MARKET STATE" in ln:
                section = "market"
            elif "ENTRY SIGNALS" in ln:
                section = "entry"
            elif ln.startswith("[ HOLDINGS"):
                section = "hold"
            elif "POSITION MANAGEMENT" in ln:
                section = "manage"
            elif "AI CAPEX CHAIN" in ln:
                section = "capex"
            else:
                section = None
            continue

        if section == "market":
            # barometer 在 ±3% 带内、沿用前次判定 (hysteresis) → 提示震荡临界
            if "带内粘滞" in ln:
                mt = re.match(r"^([A-Z]{1,6})\s", ln)
                if mt:
                    out["sticky"].append(mt.group(1))
            elif "Leadership breadth" in ln:
                out["breadth"] = re.sub(r"\s+", " ", ln).strip()
            elif "GATE:" in ln:
                out["gate_note"] = re.sub(r"\s+", " ", ln).strip()

        elif section == "entry":
            mt = re.match(r"^([A-Z]{1,6})\s+(\d.*)$", ln)
            if not mt:
                continue
            tk, rest = mt.group(1), mt.group(2)
            nums = FLOAT.findall(rest)
            entry = nums[1] if len(nums) > 1 else "?"
            stop = nums[2] if len(nums) > 2 else "?"
            target = nums[3] if len(nums) > 3 else "?"
            rr = re.search(r"(\d+\.\d:1)\s*([✓✗])", rest)
            typ = re.search(r"(BREAKOUT|PULLBACK|FIRST_KISS|POCKET_PIVOT)", rest)
            # 取 Notes（Shares 之后那段说明）作为原因
            note = re.sub(r"\s+", " ", rest).strip()
            out["entries"].append({
                "ticker": tk, "entry": entry, "stop": stop, "target": target,
                "rr": rr.group(1) if rr else "?",
                "rr_ok": (rr.group(2) == "✓") if rr else False,
                "type": typ.group(1) if typ else "?",
                "blackout": "⛔ER" in rest,   # 财报静默期: ≤5d 不进新仓
                "note": note,
            })

        elif section == "hold":
            row = re.match(r"^([A-Z]{1,6})\s", ln)
            if row and "STOP HIT" in ln:
                # Layer 1.5: 日收盘跌破 初始止损/保本线 → 今日离场, 不等周五
                out["sells"].append({
                    "ticker": row.group(1), "kind": "STOP HIT",
                    "reason": re.sub(r"\s+", " ", ln).strip(),
                })
            elif row and "BREACHED" in ln and "EXIT" in ln:
                out["sells"].append({
                    "ticker": row.group(1), "kind": "BREACHED",
                    "reason": re.sub(r"\s+", " ", ln).strip(),
                })
            elif row and "APPROACHING" in ln:
                dist = re.search(r"([+\-][\d.]+%)", ln)
                basis = re.search(r"\s(init|BE|20w)\s", ln)
                out["approaching"].append({
                    "ticker": row.group(1),
                    "dist": dist.group(1) if dist else "?",
                    "basis": basis.group(1) if basis else "?",
                })
            elif "OPEN HEAT" in ln:
                out["open_heat"] = re.sub(r"\s+", " ", ln).strip()
            elif "OVER HEAT CAP" in ln:
                out["over_cap"] = True
            elif ln.strip().startswith("📅"):
                out["er_decisions"].append(re.sub(r"\s+", " ", ln).strip())
            elif "未登记初始止损" in ln:
                out["no_init"] = re.sub(r"\s+", " ", ln).strip()

        elif section == "manage":
            mt = re.match(r"^\s*([A-Z]{1,6})\s+cost\s+([\d.]+).*?P/L\s*([+\-][\d.]+%)", ln)
            if mt:
                section_cur = {"ticker": mt.group(1), "cost": mt.group(2),
                               "pl": mt.group(3), "action": None}
                out["manage"].append(section_cur)
            elif ln.strip().startswith("→") and out["manage"]:
                out["manage"][-1]["action"] = ln.strip().lstrip("→ ").strip()

        elif section == "capex":
            # 云厂商行: TICKER  close  ER in Nd  [← LAYOUT WINDOW (buy semis now)]
            mt = re.match(r"^([A-Z]{1,6})\s+([\d.]+)\s+(ER in \d+d)(.*)$", ln)
            if mt:
                action = mt.group(4).strip().lstrip("←").strip()
                out["capex_rows"].append({
                    "ticker": mt.group(1), "close": mt.group(2),
                    "er": mt.group(3), "window": action or None,
                })
            elif "Capex guidance" in ln:
                out["capex_note"] = re.sub(r"\s+", " ", ln).strip()

    return out


def parse_resonance(text):
    """三层共振：抓「真共振」名单（爆发前夜候选）。"""
    m = re.search(r"三层全亮.*?:\s*([A-Z, ]+)", text)
    tickers = []
    if m:
        tickers = [t.strip() for t in m.group(1).split(",") if t.strip()]
    return {"true_resonance": tickers}


def parse_searchlight(text):
    """池外侦察：抓【双高】名单（贵气×兑现都达标）。"""
    m = re.search(r"池外【双高】.*?:\s*([A-Z, ]+)", text)
    tickers = []
    if m:
        tickers = [t.strip() for t in m.group(1).split(",") if t.strip()]
    return {"dual_high": tickers}


def parse_steady(text):
    """小步慢涨：抓通过的票（编号表），保留 SCORE。"""
    rows = []
    for ln in text.splitlines():
        mt = re.match(r"^\s*\d+\s+([A-Z]{1,6})\s+.*\s([\d.]+)\s*$", ln)
        if mt:
            rows.append({"ticker": mt.group(1), "score": mt.group(2)})
    return {"rows": rows}


def parse_breakout(text):
    """横盘突破：抓 HIT 放量突破段的票（标注 R:R 是否达标）。"""
    rows = []
    in_hit = False
    for ln in text.splitlines():
        if "HIT 放量突破" in ln:
            in_hit = True
            continue
        if in_hit and ("WATCH" in ln or ln.startswith("──")):
            break
        if in_hit:
            mt = re.match(r"^([A-Z]{1,6})\s+\d", ln)
            if mt:
                rows.append({"ticker": mt.group(1), "rr_ok": ln.rstrip().endswith("✓")})
    return {"hits": rows}


def _md_table_rows(text):
    """逐行 yield Markdown 表格的数据行(已 split 成 cell 列表)，跳过表头/分隔。"""
    for ln in text.splitlines():
        if not ln.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        if not cells or set("".join(cells)) <= set("-: "):  # 分隔行
            continue
        yield cells


def parse_gap_scan(text):
    """向上缺口：Tier A(新生·止损紧) / Tier B(存活确认·偏晚)，附原因列。"""
    a, b = [], []
    for cells in _md_table_rows(text):
        if len(cells) < 11 or cells[0] in ("ticker",):
            continue
        tk, tier, reason = cells[0], cells[1], cells[-1]
        if tier == "A":
            a.append({"ticker": tk, "reason": reason})
        elif tier == "B":
            b.append({"ticker": tk, "reason": reason})
    return {"tierA": a, "tierB": b}


def parse_key_kline(text):
    """关键K线：只取『新鲜入场』(几bar前<=2) 且 类型为 BREAKOUT/FIRST_KISS、
    趋势为 多头排列/偏多 的名 —— 孤立 POCKET_PIVOT 按方法论当背景，不计入。"""
    rows = []
    for cells in _md_table_rows(text):
        if len(cells) < 8 or cells[0] == "代码":
            continue
        code, trend, typ, risk, bars = cells[0], cells[1], cells[2], cells[3], cells[4]
        try:
            bars_n = int(bars)
        except ValueError:
            continue
        if bars_n > 2:
            continue
        if typ not in ("BREAKOUT", "FIRST_KISS"):
            continue
        if trend not in ("多头排列", "偏多"):
            continue
        rows.append({"ticker": code, "type": typ, "trend": trend,
                     "risk": risk, "stop": cells[6] if len(cells) > 6 else "?"})
    return {"rows": rows}


def parse_attrib(text):
    """信号归因(周)：抓 episode 概况行 —— 账本积累进度 + 成熟样本数。"""
    out = {"summary": None}
    m = re.search(r"episodes:\s*(\d+)\s*total\s*·\s*(\d+)\s*mature[^·]*·\s*(\d+)\s*pending", text)
    if m:
        out["summary"] = (f"{m.group(1)} episodes · {m.group(2)} 成熟可归因 · "
                          f"{m.group(3)} 窗口未满")
    return out


def parse_regime(text):
    """体制监控：抓状态行 + SPY/VIX/宽度 概要。"""
    out = {"state": None, "summary": None}
    m = re.search(r"市场状态:\s*【([^】]+)】\s*(\S+)?", text)
    if m:
        out["state"] = (m.group(1) + (" " + m.group(2) if m.group(2) else "")).strip()
    spy = re.search(r"SPY\s+([\d.]+)", text)
    vix = re.search(r"VIX\s+([\d.]+)", text)
    bre = re.search(r"宽度\(%[^)]*\)\s*(\d+%)", text)
    parts = []
    if spy:
        parts.append(f"SPY {spy.group(1)}")
    if vix:
        parts.append(f"VIX {vix.group(1)}")
    if bre:
        parts.append(f"宽度 {bre.group(1)}")
    if parts:
        out["summary"] = " · ".join(parts)
    return out


# ── 组装总报告 ──────────────────────────────────────────────────────────────

def build_report(target, result_dir):
    found = {}          # key -> (path, date)
    raw = {}            # key -> 原文
    for key, prefix, _label in SOURCES:
        path, d = find_report(prefix, target, result_dir)
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

    swing = safe(parse_tech_swing, "tech_swing")
    attrib = safe(parse_attrib, "signal_attrib")
    regime = safe(parse_regime, "regime")
    reson = safe(parse_resonance, "resonance")
    sight = safe(parse_searchlight, "searchlight")
    steady = safe(parse_steady, "steady")
    breakout = safe(parse_breakout, "breakout")
    gap = safe(parse_gap_scan, "gap_scan")
    kkl = safe(parse_key_kline, "key_kline")

    L = []  # 输出行
    A = L.append
    A(f"# US 每日汇总报告 — {target.isoformat()}")
    A("")
    A(f"_生成时间 {dt.datetime.now():%Y-%m-%d %H:%M}　·　合并自 `us_daily_run.sh` 各子报告_")
    A("")

    # ── 市场背景 ──
    A("## 一、市场背景")
    A("")
    if regime.get("state"):
        _, rd = found["regime"]
        A(f"- **体制监控**（数据日 {rd}）：状态 **{regime['state']}**"
          + (f"　{regime['summary']}" if regime.get("summary") else "")
          + "　[来源 us_regime_monitor]")
    if swing.get("market_state"):
        sticky = swing.get("sticky") or []
        sticky_s = (f"；⚠ {'/'.join(sticky)} 在20周线±3%带内·沿用前判（震荡临界, 距换向一步）"
                    if sticky else "")
        A(f"- **扫描市场态**（技术Swing）：**{swing['market_state']}**"
          f"（STRONG=QQQ与SOXX均在20周线上方, ±3%粘滞带防抖）{sticky_s}　[来源 us_tech_swing]")
        if swing.get("breadth"):
            A(f"- {swing['breadth']}　[来源 us_tech_swing]")
        if swing.get("gate_note"):
            A(f"- **{swing['gate_note']}**　[来源 us_tech_swing]")
    if attrib.get("summary"):
        _, ad = found["signal_attrib"]
        A(f"- **信号归因账本**（周更, 数据日 {ad}）：{attrib['summary']}"
          "　——12个月后这张表回答\"哪个筛子真有edge\"　[来源 us_signal_attrib]")
    if not regime.get("state") and not swing.get("market_state"):
        A("- （无体制/市场态数据）")
    A("")

    # ── 卖出 / 离场 ──
    A("## 二、🔴 卖出 / 离场信号　（先管风险）")
    A("")
    sells = swing.get("sells", [])
    exits = [m for m in swing.get("manage", []) if m.get("action") and "EXIT" in m["action"]]
    if sells or exits:
        A("| 标的 | 信号 | 原因 | 来源 |")
        A("|------|------|------|------|")
        done = set()
        for s in sells:
            if s.get("kind") == "STOP HIT":
                why = "日收盘跌破 初始止损/保本线 (Layer1.5 STOP HIT → EXIT TODAY, 不等周五)"
            else:
                why = "周收盘跌破20周线止损 (BREACHED → EXIT TODAY)"
            A(f"| **{s['ticker']}** | SELL 今日离场 | {why} | us_tech_swing · 持仓止损 |")
            done.add(s["ticker"])
        for m in exits:
            if m["ticker"] in done:
                continue
            A(f"| **{m['ticker']}** | SELL 清仓 | {m['action']}（P/L {m['pl']}） | us_tech_swing · 持仓管理 |")
        A("")
        A("> 跌破止损的持仓按 swing 退出规则：**次日开盘离场**。")
    else:
        A("_无卖出信号。_")
    A("")

    # ── 持仓管理 ──
    A("## 三、🟡 持仓管理　（已有仓位的加固动作，非清仓）")
    A("")

    # 组合风险速览: OPEN HEAT(全部有效止损同日打穿的损失) + 财报前决策 + 逼近止损
    risk_lines = []
    if swing.get("open_heat"):
        heat = swing["open_heat"].lstrip("⚠ ").strip()
        if swing.get("over_cap"):
            risk_lines.append(f"- 🔥 **{heat}**　→ **⛔ 超出主题风险预算：不进新仓，先上移弱仓止损/减仓**")
        else:
            risk_lines.append(f"- 🔥 {heat}")
    if swing.get("no_init"):
        risk_lines.append(f"- {swing['no_init']}")
    for d in swing.get("er_decisions", []):
        risk_lines.append(f"- **{d}**")
    if risk_lines:
        A("**组合风险速览**　[来源 us_tech_swing · HOLDINGS]")
        A("")
        L.extend(risk_lines)
        A("")

    approaching = swing.get("approaching", [])
    if approaching:
        A("**逼近止损（≤3%，未触发）**：")
        A("")
        A("| 标的 | 距有效止损 | 止损依据 |")
        A("|------|------|------|")
        basis_cn = {"init": "初始技术止损", "BE": "保本线", "20w": "20周线"}
        for h in approaching:
            A(f"| {h['ticker']} | {h['dist']} | {basis_cn.get(h['basis'], h['basis'])} |")
        A("")

    manage = [m for m in swing.get("manage", []) if m.get("action") and "EXIT" not in m["action"]]
    if manage:
        A("| 标的 | 动作 | 浮盈 | 来源 |")
        A("|------|------|------|------|")
        for m in manage:
            A(f"| {m['ticker']} | {m['action']} | {m['pl']} | us_tech_swing · 持仓管理 |")
    elif not risk_lines and not approaching:
        A("_无持仓管理动作。_")
    A("")

    # ── 买入 / 入场 ──
    A("## 四、🟢 买入 / 入场信号")
    A("")
    A("### A. 已就绪入场（technical Swing 主报告，有止损/目标/盈亏比）")
    A("")
    entries = swing.get("entries", [])
    if entries:
        A("| 标的 | 类型 | 入场 | 止损 | 目标 | R:R | 备注 | 来源 |")
        A("|------|------|------|------|------|-----|------|------|")
        for e in entries:
            rr = e["rr"] + ("✓" if e["rr_ok"] else "✗")
            tk = f"**{e['ticker']}**" if (e["rr_ok"] and not e.get("blackout")) else e["ticker"]
            note = "⛔ 财报静默期(≤5d)·不进" if e.get("blackout") else "—"
            A(f"| {tk} | {e['type']} | {e['entry']} | {e['stop']} | {e['target']} | {rr} | {note} | us_tech_swing · 入场信号 |")
        A("")
        A("> R:R 带 ✗ 者盈亏比不足 2:1，仅观察、不建议直接做。"
          "⛔ 静默期者距财报 ≤5 天，**不进新仓**——财报后跳空确认再看。")
    else:
        A("_主报告今日无就绪入场信号。_")
    A("")

    # ── B. 候选池 共振计票 ──
    A("### B. 候选池 · 多筛子共振计票")
    A("")
    A("一只票被越多筛子同时命中，越值得看。下表汇总各筛子命中（按命中来源数降序）：")
    A("")

    # ticker -> {source_label: 原因短语}
    conf = {}

    def add(ticker, label, why):
        ticker = ticker.strip().upper()
        if not ticker:
            return
        conf.setdefault(ticker, {})[label] = why

    for e in entries:
        bo = "·⛔ER静默期" if e.get("blackout") else ""
        add(e["ticker"], "技术Swing入场", f"{e['type']} R:R{e['rr']}{'✓' if e['rr_ok'] else '✗'}{bo}")
    for tk in reson.get("true_resonance", []):
        add(tk, "三层共振★★★", "贵气×兑现×技术 全亮")
    for tk in sight.get("dual_high", []):
        add(tk, "池外双高", "贵气×兑现 双达标")
    for r in steady.get("rows", []):
        add(r["ticker"], "小步慢涨", f"台阶式上行 score{r['score']}")
    for r in breakout.get("hits", []):
        add(r["ticker"], "横盘突破", "放量突破" + ("·R:R✓" if r["rr_ok"] else ""))
    for r in gap.get("tierA", []):
        add(r["ticker"], "缺口A", r["reason"] + "(新生·止损紧)")
    for r in gap.get("tierB", []):
        add(r["ticker"], "缺口B", r["reason"] + "(存活确认·偏晚)")
    for r in kkl.get("rows", []):
        add(r["ticker"], "关键K线", f"{r['type']}/{r['trend']} 风险{r['risk']}%")

    if conf:
        ranked = sorted(conf.items(), key=lambda kv: (-len(kv[1]), kv[0]))
        A("| 标的 | 命中数 | 命中来源（=原因 + 来自哪个子报告） |")
        A("|------|:------:|------|")
        for tk, srcs in ranked:
            detail = "；".join(f"**{lab}**（{why}）" for lab, why in srcs.items())
            A(f"| {tk} | {len(srcs)} | {detail} |")
        A("")
        multi = [tk for tk, s in ranked if len(s) >= 2]
        if multi:
            A(f"> **多筛子共振（≥2）**：{', '.join(multi)} —— 优先在这些里做关键K线择时 + 写止损。")
    else:
        A("_各候选筛子今日无命中。_")
    A("")

    # ── C. AI capex 链布局线索 ──
    A("### C. AI capex 链布局线索　（买半导体，**不是**买云厂商）")
    A("")
    A("云厂商财报当半导体的领先指标：财报前是埋伏上游半导体的窗口，财报上调 capex 即兑现。"
      "下表的云厂商只是**计时器**（用其财报倒计时界定窗口），不是交易标的；属博弈型线索，"
      "无止损/目标，风险高于上面 A 段。")
    A("")
    crows = swing.get("capex_rows", [])
    if crows:
        A("| 云厂商(计时器) | 现价 | 财报 | 布局窗口 |")
        A("|------|------|------|------|")
        for r in crows:
            win = r["window"] if r.get("window") else "—"
            A(f"| {r['ticker']} | {r['close']} | {r['er']} | {win} |")
        A("")
        windows = [r["ticker"] for r in crows if r.get("window")]
        if windows:
            A(f"> **当前处于布局窗口**：{', '.join(windows)} 临近财报 → 现在是埋伏半导体的窗口。")
        if swing.get("capex_note"):
            A(f"> 操作：{swing['capex_note']}　[来源 us_tech_swing · AI CAPEX CHAIN]")
    else:
        A("_主报告无 capex 链数据。_")
    A("")

    # ── 数据源清单 ──
    A("## 五、数据源 / 子报告清单")
    A("")
    A("| 子报告 | 标签 | 文件 |")
    A("|--------|------|------|")
    for key, _prefix, label in SOURCES:
        path, d = found[key]
        fname = os.path.basename(path) if path else "（缺失）"
        A(f"| {key} | {label} | {fname} |")
    A("")

    # ── 附：各子报告原文 ──
    A("---")
    A("")
    A("## 附录：各子报告原文")
    A("")
    for key, _prefix, label in SOURCES:
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
    ap = argparse.ArgumentParser(description="合并 us_daily_run.sh 各子报告为当日总报告")
    ap.add_argument("--date", help="目标日期 YYYY-MM-DD 或 YYYYMMDD（默认今天）")
    ap.add_argument("--result-dir", default=RESULT_DIR, help=f"子报告目录（默认 {RESULT_DIR}）")
    ap.add_argument("--out", help="输出文件路径（默认 <result-dir>/us_daily_report_<date>.md）")
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
        out_dir = os.path.join(args.result_dir, "daily_report")
        os.makedirs(out_dir, exist_ok=True)
        out = os.path.join(out_dir, f"us_daily_report_{target:%Y%m%d}.md")
    with open(out, "w", encoding="utf-8") as f:
        f.write(report)
    sys.stderr.write(f"[ok] 总报告 → {out}\n")

    if args.stdout:
        sys.stdout.write(report)


if __name__ == "__main__":
    main()
