#!/home/ryan/miniconda3/bin/python
# -*- coding: utf-8 -*-
"""
us_collar_iv.py — 拉某只美股某个到期日的期权链实时 IV / 权利金，并把 collar(领口)
的净成本算具体。

典型用法（MU 财报 6/24，财报后第一个到期 6/26）：
    python us_collar_iv.py --ticker US.MU --expiry 2026-06-26

机制：
  1. 取正股快照 -> 现价 S。
  2. get_option_chain 拿该到期日所有合约代码。
  3. 在现价 ±band% 内筛 strike，分批 get_market_snapshot 取 IV/Greeks/bid/ask/last。
  4. 打印 PUT / CALL 两张表（含 IV、delta、买卖盘、OI）。
  5. collar = 持 100 股/张 + 买 OTM put(下行保险) + 卖 OTM call(收权利金抵成本)。
     对每个 (put_strike, call_strike) 组合算每张合约(×100)的净成本：
        净借记(debit) = 买 put 付出 - 卖 call 收入
     给两种口径：
        mid  : 双边中间价(理论公允) = put_mid - call_mid
        fill : 真实成交(你吃价差)  = put_ask - call_bid   <- 最保守
     net < 0 = 净收钱(credit)；net > 0 = 净付钱(debit)。

财报陷阱提示：到期日就在财报后，put 和 call 的 IV 都会在财报后塌缩(IV crush)，
但 collar 两腿方向相反、IV 影响大部分相消，净敞口远小于单买 put。脚本打印的 IV
是“塌缩前”的当前值，仅用于看当下定价是否便宜/贵，不代表持有到期的盈亏。
"""
import argparse
import sys
from futu import OpenQuoteContext, RET_OK

HOST = "127.0.0.1"
PORT = 11111
SNAP_BATCH = 350  # get_market_snapshot 单次上限 400，留余量


def _f(x):
    """安全转 float，'N/A' / None -> None。"""
    try:
        if x is None or x == "N/A":
            return None
        return float(x)
    except (ValueError, TypeError):
        return None


def fetch(ticker, expiry, band, ctx):
    # 1) 正股现价
    ret, snap = ctx.get_market_snapshot([ticker])
    if ret != RET_OK:
        sys.exit(f"取正股快照失败: {snap}")
    row = snap.iloc[0]
    last = _f(row["last_price"])
    bid = _f(row.get("bid_price"))
    ask = _f(row.get("ask_price"))
    # 现价基准：优先 last，盘前盘后 last 可能滞后于买卖盘，附带展示
    spot = last
    print(f"\n正股 {ticker}  现价(last)={last}  买一={bid}  卖一={ask}")
    print(f"参考现价 S = {spot}\n")

    # 2) 期权链合约
    ret, chain = ctx.get_option_chain(ticker, start=expiry, end=expiry)
    if ret != RET_OK:
        sys.exit(f"取期权链失败: {chain}")
    lo, hi = spot * (1 - band), spot * (1 + band)
    chain = chain[(chain["strike_price"] >= lo) & (chain["strike_price"] <= hi)]
    codes = chain["code"].tolist()
    if not codes:
        sys.exit(f"现价 ±{band:.0%} 内无合约（band 调大试试）")
    print(f"到期 {expiry}: 现价 ±{band:.0%} 内 {len(codes)} 个合约，拉快照取 IV/权利金...")

    # 3) 分批快照
    recs = []
    for i in range(0, len(codes), SNAP_BATCH):
        batch = codes[i:i + SNAP_BATCH]
        ret, s = ctx.get_market_snapshot(batch)
        if ret != RET_OK:
            sys.exit(f"期权快照失败: {s}")
        recs.extend(s.to_dict("records"))

    puts, calls = {}, {}
    for r in recs:
        rec = {
            "strike": _f(r["option_strike_price"]),
            "bid": _f(r["bid_price"]),
            "ask": _f(r["ask_price"]),
            "last": _f(r["last_price"]),
            "iv": _f(r["option_implied_volatility"]),     # 单位 %
            "delta": _f(r["option_delta"]),
            "oi": _f(r["option_open_interest"]),
            "code": r["code"],
        }
        b, a = rec["bid"], rec["ask"]
        rec["mid"] = round((b + a) / 2, 2) if (b and a) else rec["last"]
        (puts if r["option_type"] == "PUT" else calls)[rec["strike"]] = rec
    return spot, puts, calls


def _tbl(title, d, reverse):
    print(f"\n===== {title} =====")
    print(f"{'strike':>9} {'bid':>8} {'ask':>8} {'mid':>8} {'last':>8} "
          f"{'IV%':>7} {'delta':>7} {'OI':>7}")
    for k in sorted(d, reverse=reverse):
        r = d[k]
        print(f"{r['strike']:>9.1f} {r['bid'] or 0:>8.2f} {r['ask'] or 0:>8.2f} "
              f"{r['mid'] or 0:>8.2f} {r['last'] or 0:>8.2f} "
              f"{r['iv'] or 0:>7.1f} {r['delta'] or 0:>7.3f} {int(r['oi'] or 0):>7d}")


def nearest(d, target):
    return min(d, key=lambda k: abs(k - target)) if d else None


def collar(spot, puts, calls, put_strikes, call_strikes, mult=100.0):
    # 默认候选：OTM put 在现价下方 -2/-4/-6/-8/-10%，OTM call 在上方 +2..+10%
    if not put_strikes:
        put_strikes = [nearest(puts, spot * (1 + p)) for p in (-.02, -.04, -.06, -.08, -.10)]
    if not call_strikes:
        call_strikes = [nearest(calls, spot * (1 + c)) for c in (.02, .04, .06, .08, .10)]
    put_strikes = sorted({p for p in put_strikes if p in puts}, reverse=True)
    call_strikes = sorted({c for c in call_strikes if c in calls})

    print("\n\n############ COLLAR 净成本（每张合约 = 100 股） ############")
    print("net = 买put成本 - 卖call收入 ；  >0 净付钱(debit)，<0 净收钱(credit)")
    print("[mid] 双边中间价(公允)   [fill] 实际成交(吃价差: 付put_ask, 收call_bid)\n")

    for ps in put_strikes:
        p = puts[ps]
        floor_pct = (ps / spot - 1) * 100
        print(f"\n--- 买 PUT {ps:.1f}  (下行保底 {floor_pct:+.1f}%, IV {p['iv']:.0f}%, "
              f"bid/ask {p['bid']:.2f}/{p['ask']:.2f}) ---")
        print(f"{'卖CALL':>9} {'封顶%':>7} {'IV%':>6} {'callbid':>8} "
              f"{'net_mid$':>10} {'net_fill$':>10}")
        for cs in call_strikes:
            c = calls[cs]
            cap_pct = (cs / spot - 1) * 100
            net_mid = (p["mid"] - c["mid"]) * mult
            net_fill = (p["ask"] - c["bid"]) * mult
            tag = ""
            if net_fill <= 0:
                tag = "  <= 零成本(实拿credit)"
            elif net_mid <= 0:
                tag = "  <= 公允零成本附近"
            print(f"{cs:>9.1f} {cap_pct:>+6.1f}% {c['iv'] or 0:>6.0f} "
                  f"{c['bid'] or 0:>8.2f} {net_mid:>+10.0f} {net_fill:>+10.0f}{tag}")


def main():
    ap = argparse.ArgumentParser(description="期权链实时 IV/权利金 + collar 净成本")
    ap.add_argument("--ticker", default="US.MU")
    ap.add_argument("--expiry", default="2026-06-26", help="到期日 yyyy-MM-dd")
    ap.add_argument("--band", type=float, default=0.15, help="筛 strike 的现价上下幅度，默认 0.15")
    ap.add_argument("--puts", type=float, nargs="*", help="指定要买的 put 行权价（多个）")
    ap.add_argument("--calls", type=float, nargs="*", help="指定要卖的 call 行权价（多个）")
    args = ap.parse_args()

    ctx = OpenQuoteContext(host=HOST, port=PORT)
    try:
        spot, puts, calls = fetch(args.ticker, args.expiry, args.band, ctx)
        _tbl("PUT 链", puts, reverse=True)
        _tbl("CALL 链", calls, reverse=False)
        collar(spot, puts, calls, args.puts, args.calls)
    finally:
        ctx.close()


if __name__ == "__main__":
    main()
