#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ndx_predictor.py — 纳斯达克100指数成分股调整预测器
====================================================

基于纳斯达克官方方法论（2026年5月1日生效版本）：
https://indexes.nasdaq.com/docs/Methodology_NDX.pdf

核心规则（季度再平衡，3/6/9月）：
  1. 参考日（Reference Date）：2月/5月/8月最后一个交易日
  2. 所有合格公司按"全市值"(Full Market Cap, 含未上市股份) 排名
  3. 排名跌出前125的现有成分股 → 移除
  4. 由排名最高的合格非成分股替补至100只
  5. 全市值进入"现有成分股前40名"的非成分股 → Fast Entry 直接加入
     （不需要移除其他股票，成分股数可暂时超过100）

年度重构（12月，参考日=11月最后交易日）：
  1. 前75名直接入选
  2. 排名76-100的现有成分股保留
  3. 排名101-125的现有成分股按排名保留（须上次重构时在前100内）
  4. 剩余名额由前100内的非成分股按排名补足

合格标准：
  - 纳斯达克主上市（Global Select / Global Market，不含 Capital Market）
  - 非金融行业（ICB分类）；REIT、SPAC 不合格
  - 三个月日均成交额(ADVT) >= $500万
  - 上市满3个完整日历月（Seasoning）

用法：
  python ndx_predictor.py                 # 实跑：拉取真实数据并预测
  python ndx_predictor.py --demo          # 演示：用合成数据验证规则引擎
  python ndx_predictor.py --top 200       # 限制候选池大小（加速）
  python ndx_predictor.py --out report.md # 输出报告文件

依赖：pip install yfinance pandas requests lxml
"""

import argparse
import calendar
import datetime as dt
import json
import time
from dataclasses import dataclass, field, asdict
from io import StringIO
from pathlib import Path

import pandas as pd

# ----------------------------------------------------------------------------
# 常量与配置
# ----------------------------------------------------------------------------

ADVT_MIN_USD = 5_000_000          # 三个月日均成交额下限
REMOVAL_RANK_CUTOFF = 125         # 季度再平衡：跌出前125即移除
FAST_ENTRY_TOP_N = 40             # 全市值进入现有成分股前40名 → Fast Entry
INDEX_TARGET_COUNT = 100

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
WIKI_NDX_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"

CACHE_DIR = Path("ndx_cache")
SNAPSHOT_FILE = CACHE_DIR / "snapshots.json"

# 同一公司多个股份类别 → 合并为公司级全市值（官方规则要求按"公司"排名）
SHARE_CLASS_MAP = {
    "GOOG": "GOOGL", "GOOGL": "GOOGL",
    "FOXA": "FOX",   "FOX":   "FOX",
    "LBTYA": "LBTYA", "LBTYK": "LBTYA",
    "CMCSA": "CMCSA",
    "TCOM":  "TCOM",
}

# Yahoo 的行业分类(GICS风格)与官方使用的 ICB 不一致。
# 这些公司在 Yahoo 被标为 Financial Services，但 ICB 分类下并非金融业，
# 官方认定为合格（例：PYPL 历史上长期是 NDX 成分股）。
ICB_NON_FINANCIAL_OVERRIDES = {
    "PYPL",  # ICB: Industrial Support Services
    "COIN",  # 需人工复核（加密交易所，ICB分类有争议）
}

# Yahoo 标为非金融、但 ICB 视为金融/不合格的已知例外（极少）
ICB_FINANCIAL_OVERRIDES: set = set()


# ----------------------------------------------------------------------------
# 指数日历：参考日 / 公告日 / 生效日
# ----------------------------------------------------------------------------

def _last_business_day(year: int, month: int) -> dt.date:
    """某月最后一个交易日（简化：跳过周末，未处理节假日——参考日精度足够）"""
    day = calendar.monthrange(year, month)[1]
    d = dt.date(year, month, day)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def _third_friday(year: int, month: int) -> dt.date:
    fridays = [d for d in range(1, 29)
               if dt.date(year, month, d).weekday() == 4]
    return dt.date(year, month, fridays[2])


def next_rebalance_schedule(today: dt.date | None = None) -> dict:
    """返回下一次调整事件的关键日期"""
    today = today or dt.date.today()
    events = []
    for y in (today.year, today.year + 1):
        for m, kind in ((3, "季度再平衡"), (6, "季度再平衡"),
                        (9, "季度再平衡"), (12, "年度重构")):
            ref_month = m - 1  # 参考日 = 前一个月的最后交易日
            ref = _last_business_day(y, ref_month)
            tf = _third_friday(y, m)
            effective = tf + dt.timedelta(days=1)
            while effective.weekday() >= 5:
                effective += dt.timedelta(days=1)
            # 公告：生效日前第6个交易日收盘后（近似：前8个自然日）
            announce = effective - dt.timedelta(days=8)
            events.append({
                "kind": kind,
                "reference_date": ref.isoformat(),
                "announcement_approx": announce.isoformat(),
                "effective_date": effective.isoformat(),
            })
    future = [e for e in events
              if dt.date.fromisoformat(e["effective_date"]) >= today]
    return future[0]


# ----------------------------------------------------------------------------
# 数据获取
# ----------------------------------------------------------------------------

def fetch_nasdaq_universe() -> pd.DataFrame:
    """
    从 nasdaqtrader.com 官方符号目录拉取全部纳斯达克上市证券。
    Market Category: Q = Global Select, G = Global Market, S = Capital Market
    官方规则排除 Capital Market(S)，并排除 ETF / 测试代码。
    """
    import requests
    print("  ↳ 下载纳斯达克官方上市证券目录 ...")
    r = requests.get(NASDAQ_LISTED_URL, timeout=30)
    r.raise_for_status()
    lines = [l for l in r.text.splitlines()
             if "|" in l and not l.startswith("File Creation")]
    df = pd.read_csv(StringIO("\n".join(lines)), sep="|")
    df = df[(df["Test Issue"] == "N") & (df["ETF"] == "N")]
    df = df[df["Market Category"].isin(["Q", "G"])]  # 排除 Capital Market
    df = df[~df["Security Name"].str.contains(
        r"Warrant|Right|Unit|Preferred|Depositary Shs|%|Notes",
        case=False, na=False)]
    df = df.rename(columns={"Symbol": "ticker", "Security Name": "name"})
    print(f"  ↳ 合格交易层级证券: {len(df)} 只")
    return df[["ticker", "name"]].reset_index(drop=True)


def fetch_current_constituents() -> list[str]:
    """从 Wikipedia 拉取当前 NDX 成分股（更新及时、可机器解析）"""
    print("  ↳ 获取当前纳斯达克100成分股 ...")
    tables = pd.read_html(WIKI_NDX_URL)
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if any("ticker" in c or "symbol" in c for c in cols):
            col = t.columns[[i for i, c in enumerate(cols)
                              if "ticker" in c or "symbol" in c][0]]
            tickers = t[col].astype(str).str.strip().tolist()
            if 95 <= len(tickers) <= 110:
                print(f"  ↳ 当前成分股: {len(tickers)} 只")
                return tickers
    raise RuntimeError("无法从 Wikipedia 解析成分股表，请检查页面结构")


def enrich_with_yfinance(tickers: list[str], batch_pause: float = 0.5) -> pd.DataFrame:
    """
    用 yfinance 批量获取：全市值、行业、上市日期、三个月日均成交额。
    注意：Yahoo 的 marketCap 是公司级市值（含多类股），可近似官方"全市值"。
    """
    import yfinance as yf
    rows = []
    total = len(tickers)
    ref_end = dt.date.today()
    ref_start = ref_end - dt.timedelta(days=95)

    for i, tk in enumerate(tickers, 1):
        if i % 25 == 0 or i == total:
            print(f"  ↳ 拉取基本面 {i}/{total} ...")
        try:
            t = yf.Ticker(tk)
            info = t.info or {}
            mcap = info.get("marketCap")
            if not mcap:
                continue
            hist = t.history(start=ref_start.isoformat(),
                             end=ref_end.isoformat(), auto_adjust=False)
            advt = float((hist["Close"] * hist["Volume"]).mean()) if len(hist) else 0.0
            first_trade = info.get("firstTradeDateEpochUtc")
            first_trade_date = (dt.datetime.utcfromtimestamp(first_trade).date().isoformat()
                                if first_trade else None)
            rows.append({
                "ticker": tk,
                "name": info.get("shortName", ""),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "quote_type": info.get("quoteType", ""),
                "full_market_cap": float(mcap),
                "advt_3m": advt,
                "first_trade_date": first_trade_date,
            })
            time.sleep(batch_pause)
        except Exception as e:
            print(f"    ⚠ {tk}: {e.__class__.__name__}")
            continue
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# 合格性过滤（官方 Security Eligibility Criteria）
# ----------------------------------------------------------------------------

def months_listed(first_trade_iso: str | None, asof: dt.date) -> int:
    """完整日历月数（不含上市当月）——官方 Seasoning 口径"""
    if not first_trade_iso:
        return 999  # 老牌公司缺数据时视为已满足
    f = dt.date.fromisoformat(first_trade_iso)
    months = (asof.year - f.year) * 12 + (asof.month - f.month) - 1
    return max(months, 0)


def apply_eligibility(df: pd.DataFrame, asof: dt.date,
                      constituents: set[str]) -> pd.DataFrame:
    df = df.copy()
    df["company"] = df["ticker"].map(lambda t: SHARE_CLASS_MAP.get(t, t))

    def eligible(row):
        reasons = []
        tk = row["ticker"]
        # 1) 行业：排除金融（带 ICB 修正）
        is_fin = (row["sector"] == "Financial Services")
        if tk in ICB_NON_FINANCIAL_OVERRIDES:
            is_fin = False
        if tk in ICB_FINANCIAL_OVERRIDES:
            is_fin = True
        if is_fin:
            reasons.append("金融行业(ICB)")
        # 2) REIT 排除
        if "REIT" in (row["industry"] or ""):
            reasons.append("REIT")
        # 3) 流动性
        if row["advt_3m"] < ADVT_MIN_USD:
            reasons.append(f"ADVT不足(${row['advt_3m']/1e6:.1f}M<$5M)")
        # 4) Seasoning（已是成分股则豁免）
        if tk not in constituents and months_listed(row["first_trade_date"], asof) < 3:
            reasons.append("上市不满3个完整月")
        return "; ".join(reasons)

    df["ineligible_reasons"] = df.apply(eligible, axis=1)
    df["eligible"] = df["ineligible_reasons"] == ""
    return df


# ----------------------------------------------------------------------------
# 核心规则引擎：官方季度再平衡选股算法
# ----------------------------------------------------------------------------

@dataclass
class PredictionResult:
    asof: str
    schedule: dict
    removals: list = field(default_factory=list)
    additions: list = field(default_factory=list)
    fast_entries: list = field(default_factory=list)
    at_risk: list = field(default_factory=list)        # 排名105-125，接近移除线
    on_the_bubble: list = field(default_factory=list)  # 非成分股，排名接近替补线
    notes: list = field(default_factory=list)


def predict_quarterly_rebalance(df: pd.DataFrame,
                                constituents: set[str],
                                schedule: dict) -> PredictionResult:
    """
    严格按官方 March/June/September Rebalance 规则执行：
      Step 1: 全部合格公司按 Full Market Cap 排名（公司级）
      Step 2: 成分股排名 > 125 → 移除
      Step 3: 排名最高的合格非成分股替补至 100
      Step 4: 全市值 >= 现有成分股第40名 的非成分股 → Fast Entry 加入
    """
    res = PredictionResult(asof=dt.date.today().isoformat(), schedule=schedule)

    el = df[df["eligible"]].copy()
    # 公司级全市值 = 各股份类别市值取最大（Yahoo公司级市值通常已含全部类别）
    comp = (el.groupby("company")
              .agg(full_market_cap=("full_market_cap", "max"),
                   name=("name", "first"),
                   tickers=("ticker", lambda s: "/".join(sorted(s))))
              .reset_index())
    comp = comp.sort_values("full_market_cap", ascending=False).reset_index(drop=True)
    comp["rank"] = comp.index + 1

    const_companies = {SHARE_CLASS_MAP.get(t, t) for t in constituents}
    comp["is_constituent"] = comp["company"].isin(const_companies)

    # --- Step 2: 移除（排名 > 125 的成分股）---
    removed = comp[comp["is_constituent"] & (comp["rank"] > REMOVAL_RANK_CUTOFF)]
    for _, r in removed.sort_values("rank", ascending=False).iterrows():
        res.removals.append({
            "ticker": r["tickers"], "name": r["name"],
            "rank": int(r["rank"]),
            "full_market_cap_B": round(r["full_market_cap"] / 1e9, 1),
            "reason": f"全市值排名 #{int(r['rank'])} > {REMOVAL_RANK_CUTOFF}",
        })

    # --- Step 3: 替补（最高排名的合格非成分股）---
    n_after_removal = int(comp["is_constituent"].sum()) - len(removed)
    n_needed = max(INDEX_TARGET_COUNT - n_after_removal, 0)
    replacements = comp[~comp["is_constituent"]].head(n_needed * 3).head(n_needed)
    for _, r in replacements.iterrows():
        res.additions.append({
            "ticker": r["tickers"], "name": r["name"],
            "rank": int(r["rank"]),
            "full_market_cap_B": round(r["full_market_cap"] / 1e9, 1),
            "reason": "替补：合格非成分股中全市值排名最高",
        })

    # --- Step 4: Fast Entry（全市值进入现有成分股前40名）---
    const_caps = comp[comp["is_constituent"]]["full_market_cap"]
    if len(const_caps) >= FAST_ENTRY_TOP_N:
        threshold = const_caps.nlargest(FAST_ENTRY_TOP_N).iloc[-1]
        added_companies = {a["ticker"] for a in res.additions}
        fe = comp[(~comp["is_constituent"])
                  & (comp["full_market_cap"] >= threshold)
                  & (~comp["tickers"].isin(added_companies))]
        for _, r in fe.iterrows():
            res.fast_entries.append({
                "ticker": r["tickers"], "name": r["name"],
                "rank": int(r["rank"]),
                "full_market_cap_B": round(r["full_market_cap"] / 1e9, 1),
                "reason": (f"Fast Entry：全市值超过现有成分股第{FAST_ENTRY_TOP_N}名"
                           f"(${threshold/1e9:.0f}B)"),
            })

    # --- 观察名单：风险区与候补区 ---
    risk = comp[comp["is_constituent"]
                & comp["rank"].between(REMOVAL_RANK_CUTOFF - 20, REMOVAL_RANK_CUTOFF)]
    for _, r in risk.sort_values("rank", ascending=False).iterrows():
        margin = REMOVAL_RANK_CUTOFF - int(r["rank"])
        res.at_risk.append({
            "ticker": r["tickers"], "name": r["name"], "rank": int(r["rank"]),
            "full_market_cap_B": round(r["full_market_cap"] / 1e9, 1),
            "margin": f"距移除线还有{margin}名",
        })

    bubble = comp[(~comp["is_constituent"])
                  & (comp["rank"] <= REMOVAL_RANK_CUTOFF + 15)].head(15)
    added_set = ({a["ticker"] for a in res.additions}
                 | {f["ticker"] for f in res.fast_entries})
    for _, r in bubble.iterrows():
        if r["tickers"] in added_set:
            continue
        res.on_the_bubble.append({
            "ticker": r["tickers"], "name": r["name"], "rank": int(r["rank"]),
            "full_market_cap_B": round(r["full_market_cap"] / 1e9, 1),
        })

    res.notes.append("全市值采用Yahoo公司级marketCap近似官方Full Market Cap（含未上市股份）")
    res.notes.append("行业过滤基于Yahoo分类近似ICB，边界公司（支付/金融科技）需人工复核")
    res.notes.append("官方参考日为2/5/8/11月最后交易日的数据快照；越接近参考日预测越准")
    return res


# ----------------------------------------------------------------------------
# 报告输出
# ----------------------------------------------------------------------------

def render_report(res: PredictionResult) -> str:
    s = res.schedule
    L = []
    L.append("# 纳斯达克100 成分股调整预测报告")
    L.append(f"\n生成日期: {res.asof}")
    L.append(f"\n## 下一次调整事件: {s['kind']}")
    L.append(f"- 数据参考日: {s['reference_date']}")
    L.append(f"- 预计公告日: {s['announcement_approx']} 前后（生效日前第6个交易日收盘后）")
    L.append(f"- 生效日: {s['effective_date']} 开盘前")

    def section(title, items, cols):
        L.append(f"\n## {title} ({len(items)})")
        if not items:
            L.append("（无）")
            return
        for it in items:
            line = " | ".join(str(it.get(c, "")) for c in cols)
            L.append(f"- {line}")

    section("预测移除", res.removals,
            ["ticker", "name", "rank", "full_market_cap_B", "reason"])
    section("预测加入（替补）", res.additions,
            ["ticker", "name", "rank", "full_market_cap_B", "reason"])
    section("预测加入（Fast Entry）", res.fast_entries,
            ["ticker", "name", "rank", "full_market_cap_B", "reason"])
    section("风险观察区（成分股，排名105-125）", res.at_risk,
            ["ticker", "name", "rank", "full_market_cap_B", "margin"])
    section("候补观察区（非成分股，排名接近替补线）", res.on_the_bubble,
            ["ticker", "name", "rank", "full_market_cap_B"])

    L.append("\n## 方法论说明")
    for n in res.notes:
        L.append(f"- {n}")
    L.append("- 规则依据: Nasdaq官方NDX方法论(2026年5月版): 季度再平衡时排名跌出前125的"
             "成分股被移除并替补；全市值进入成分股前40名的非成分股Fast Entry直接加入")
    L.append("- 本工具仅供研究参考，不构成投资建议")
    return "\n".join(L)


def save_snapshot(res: PredictionResult):
    CACHE_DIR.mkdir(exist_ok=True)
    hist = []
    if SNAPSHOT_FILE.exists():
        hist = json.loads(SNAPSHOT_FILE.read_text())
    hist.append(asdict(res))
    SNAPSHOT_FILE.write_text(json.dumps(hist, ensure_ascii=False, indent=2))
    print(f"  ↳ 快照已保存至 {SNAPSHOT_FILE}（可用于追踪预测随时间的变化）")


# ----------------------------------------------------------------------------
# 演示模式：合成数据验证规则引擎（离线可跑）
# ----------------------------------------------------------------------------

def build_demo_data() -> tuple[pd.DataFrame, set[str]]:
    """
    构造微缩市场验证引擎：
      - C098/C099/C100/C101/C102: 排名跌出125的成分股（应被移除）
      - N001..N030: 普通非成分股（最高排名者替补加入）
      - MEGA: 全市值挤进成分股前40的新股（应 Fast Entry）
      - FINX: 金融股（应被合格性过滤排除）
      - FRESH: 上市不满3个月（应被 Seasoning 排除）
    """
    rows = []
    constituents = set()
    for i in range(1, 96):
        tk = f"C{i:03d}"
        constituents.add(tk)
        rows.append(dict(ticker=tk, name=f"成分股{i}", sector="Technology",
                         industry="Software", quote_type="EQUITY",
                         full_market_cap=(3000 - i * 8) * 1e9,
                         advt_3m=5e8, first_trade_date="2010-01-01"))
    for j, tk in enumerate(["C098", "C099", "C100", "C101", "C102"]):
        constituents.add(tk)
        rows.append(dict(ticker=tk, name=f"衰退成分股{j+1}", sector="Communication Services",
                         industry="Telecom", quote_type="EQUITY",
                         full_market_cap=(8 - j) * 1e9,
                         advt_3m=5e7, first_trade_date="2005-01-01"))
    for k in range(1, 31):
        rows.append(dict(ticker=f"N{k:03d}", name=f"非成分股{k}", sector="Healthcare",
                         industry="Biotech", quote_type="EQUITY",
                         full_market_cap=(60 - k) * 1e9,
                         advt_3m=3e7, first_trade_date="2018-06-01"))
    rows.append(dict(ticker="MEGA", name="AI云巨头", sector="Technology",
                     industry="Infrastructure", quote_type="EQUITY",
                     full_market_cap=900e9, advt_3m=2e9,
                     first_trade_date="2025-03-01"))
    rows.append(dict(ticker="FINX", name="某银行", sector="Financial Services",
                     industry="Banks", quote_type="EQUITY",
                     full_market_cap=500e9, advt_3m=1e9, first_trade_date="2000-01-01"))
    rows.append(dict(ticker="FRESH", name="新股", sector="Technology",
                     industry="Semiconductors", quote_type="EQUITY",
                     full_market_cap=400e9, advt_3m=1e9,
                     first_trade_date=(dt.date.today() - dt.timedelta(days=40)).isoformat()))
    return pd.DataFrame(rows), constituents


# ----------------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="纳斯达克100成分股调整预测器")
    ap.add_argument("--demo", action="store_true", help="用合成数据离线验证规则引擎")
    ap.add_argument("--top", type=int, default=300,
                    help="按市值预筛的非成分股候选池大小(默认300，加速用)")
    ap.add_argument("--out", type=str, default=None, help="报告输出文件(.md)")
    args = ap.parse_args()

    schedule = next_rebalance_schedule()
    print(f"下一次调整: {schedule['kind']}  "
          f"参考日={schedule['reference_date']}  生效日={schedule['effective_date']}\n")

    if args.demo:
        print("演示模式：合成数据验证规则引擎")
        df, constituents = build_demo_data()
    else:
        print("实跑模式：拉取真实数据（约需10-30分钟，取决于候选池大小）")
        universe = fetch_nasdaq_universe()
        constituents = set(fetch_current_constituents())
        all_tickers = sorted(set(universe["ticker"]) | constituents)
        print(f"  ↳ 待评估证券总数: {len(all_tickers)}（将按市值筛至前{args.top}）")
        df = enrich_with_yfinance(all_tickers)
        df = df[df["quote_type"] == "EQUITY"]
        keep = set(df.nlargest(args.top, "full_market_cap")["ticker"]) | constituents
        df = df[df["ticker"].isin(keep)]

    asof = dt.date.today()
    df = apply_eligibility(df, asof, constituents)

    excluded = df[~df["eligible"]]
    if len(excluded):
        print(f"\n被合格性规则排除 {len(excluded)} 只，例如:")
        for _, r in excluded.head(5).iterrows():
            print(f"   {r['ticker']}: {r['ineligible_reasons']}")

    res = predict_quarterly_rebalance(df, constituents, schedule)
    report = render_report(res)
    print("\n" + "=" * 70)
    print(report)

    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"\n报告已写入 {args.out}")
    save_snapshot(res)


if __name__ == "__main__":
    main()
