# BLUEPRINT — `t_us_key_kline.py` 关键K线标注脚本

> **给下一个 worker 的施工蓝图。** 读完本文 + `docs/key_kline_methodology.md`,你应能从零构建、完成、自验。
> 本文是 spec,不是代码。所有阈值是初始默认值,可调但要集中成模块常量。
> 创建于 2026-06-13。

---

## 0. 一句话任务

写一个独立脚本 `t_us_key_kline.py`:给定一只美股代码,自动识别它过去 N 个月的**关键 K 线**,
画出蜡烛图(含量能副图),在每根关键 K 上**标注类型**,并打印一张文本图例。

CWD 约定 = 仓库根 `/home/ryan/tushare_ryan`,沿用 `t_us_*` 分析脚本命名。

---

## 1. 必读背景(否则会误分类)

**先读 `docs/key_kline_methodology.md` 全文。** 核心要点浓缩如下,直接决定检测逻辑:

1. **K 线的"关键"不在形状,在"主语"**(谁/多大钱/什么位置)= 位置(关键位)+ 成交量。
   → 所以每条检测规则都**必须带成交量/位置条件**,不能裸看形态。
2. **本工具服务"优质标的"**(已由人工 watchlist 圈定),角色是**择时 + 设止损**,不是判断方向。
   → **重点检测"趋势延续类"K 线,弱化/不做"底部反转抄底"类。**
3. **要检测的关键 K 线(优先级从高到低):**
   - **财报跳空**(王中之王;且要分"续涨命 vs 高开低走陷阱"双命)
   - **平台突破**(放量突破横盘区间)
   - **初吻**(突破后第一次回踩上升均线、收阳站稳)
   - **Pocket Pivot**(上升途中再放量)
   - (可选)**高潮量**事件,仅标记不解读。
4. **进场敏感、离场迟钝**:本工具只标"进场类"关键 K;离场逻辑(周线跌破20周线)归 `t_us_tech_swing.py`,本工具**不做离场标注**。

**项目约束(ADR):**
- ADR-0001:**yfinance 是唯一行情源**。不要引入第二个 K 线来源。复用现有缓存层(见 §3)。
- 命名/落点:`select.yml` 存 watchlist;输出落 `/home/ryan/DATA/result/`(该目录存在,是独立 git repo)。

---

## 2. 环境现状(2026-06-13 实测)

- Python: `/home/ryan/miniconda3/bin/python`(Python 3.12,miniconda)
- ✅ `yfinance 1.4.1` · `matplotlib 3.10.0` · `pandas 2.2.3` · `numpy 2.2.3`
- ❌ **`mplfinance` 未安装** → **施工第一步**:`/home/ryan/miniconda3/bin/pip install mplfinance`
- ⚠️ `yfinance 1.4.1` 的 `Ticker.get_earnings_dates()` 是否可用 / 返回什么列,**worker 必须先实测**(见 §5.4),不要假设。需要联网取财报日期;无网或返回空时要**优雅降级**(照常画图,只是没有财报标注)。

---

## 3. 复用现有数据层(不要重造)

`t_us_tech_swing.py` 已有一套 yfinance 缓存 + 取数工具,**直接 import 复用**:

```python
from t_us_tech_swing import _fetch_daily, _history, _sma
```

- `_fetch_daily(ticker) -> DataFrame`:复权日线 OHLCV,索引为 `date`(tz-naive),列 `open/high/low/close/volume`;
  当天已取过则读缓存,联网失败回退陈旧缓存(带告警)。缓存目录 `/home/ryan/DATA/DAY_Global/US_yf/`。
- `_history(ticker, period, interval)`:从缓存切片 + 可选周线重采样(本工具用日线即可)。
- `_sma(series, n)`:滚动均值。

> 注意:`import t_us_tech_swing` 会触发其模块级 `_load_watchlist()`(读 select.yml,无副作用风险),可接受。
> 不会连 Futu(Futu 仅在函数内 import)。

---

## 4. High-level Design

```
t_us_key_kline.py
├─ 常量区          所有阈值集中(参考 §5 各规则)
├─ 数据/指标
│   prepare_frame(ticker, period) -> df
│       复用 _fetch_daily;⚠ 指标在【全量历史】上算完再切 period(warmup,见 §9):
│       补列 ma20/ma50/ma150/vol_avg20/atr14/range_high,否则窗口左缘 MA/区间全 NaN
├─ 检测器(每个返回 list[KeyBar],KeyBar = {date,type,price,stop,note,fate?})
│   detect_breakout(df)        → §5.1
│   detect_first_kiss(df, breakouts) → §5.2
│   detect_pocket_pivot(df)    → §5.3
│   detect_earnings_gaps(df, ticker) → §5.4  (含双命分类)
│   detect_volume_climax(df)   → §5.5  (可选)
├─ 汇总
│   collect_key_bars(...) -> 去重/合并同日多标签, 按日期排序
├─ 绘图
│   plot_chart(df, key_bars, out_png)  → §6  (mplfinance, returnfig 自定义标注)
├─ 文本图例
│   print_legend(key_bars)             → §6
└─ main()  OptionParser(沿用 t_us_tech_swing 的 CLI 风格)
```

建议数据结构:
```python
KeyBar = {
  'date': Timestamp, 'type': str,   # 'BREAKOUT'|'FIRST_KISS'|'POCKET_PIVOT'|'EARNINGS_GAP'|'CLIMAX'
  'price': float,                   # 用于标注定位(通常 high 或 low)
  'stop': float | None,             # WHERE(止损位);事件类(CLIMAX)无入场 → None
  'note': str,                      # 简短中文/英文说明
  'fate': str | None,               # 仅 EARNINGS_GAP: 'CONTINUATION'|'TRAP'
}
```

---

## 5. 检测规则(精确到可实现)

> 通用:`i` 为当前 bar 整数下标;`vol_avg20[i]` = 前 20 日成交量均值。所有"放量"= 对比 `vol_avg20`。

### 5.1 平台突破 BREAKOUT
- 横盘区间高点 `range_high = max(high[i-60 : i])`(回看 `CONSOL_DAYS=60`,不含 i);
  **预计算成列** `df['range_high'] = high.rolling(60).max().shift(1)`(在全量历史上算 → 左缘不截断)。
- 触发:`close[i] > range_high` **且** `close[i-1] <= range_high`(首次清越,避免连标)
  **且** `volume[i] >= 1.5 * vol_avg20[i]`(`VOL_MULT=1.5`)。
- **趋势闸门(主语之位置):** `close[i] > ma150[i]`(MA150 为 NaN 即短历史 → 放行)。
  突破必须发生在自身长期均线上方,才算趋势延续而非下跌反弹(方法论 §0 RS 领涨 / §6 看结构)。
- 去重:与上一个已标 BREAKOUT 间隔 < 10 bar 则跳过。
- `price = high[i]`,**`stop = low[i]`**(跌破突破当根 low → 止损),`note = f"放量突破{CONSOL_DAYS}日高 vol×{ratio:.1f}"`。

### 5.2 初吻 FIRST_KISS(依赖 5.1 的结果)
- 对每个 breakout 下标 `b`,向后在 `(b, b+60]` 找**第一个** `k` 满足:
  - 触吻上升均线:`low[k] <= ma20[k]*(1+0.015)` 且 `low[k] >= ma20[k]*(1-0.03)`(贴近 MA20,容差 `KISS_TOL=1.5%`);
  - 均线向上:`ma20[k] > ma20[k-5]`;
  - 趋势未坏:`close[k] > ma50[k]`;
  - 收阳/反包:`close[k] > open[k]`;
  - **缩量(主语之成交量):** `volume[k] <= KISS_VOL_DRY * vol_avg20[k]`(`KISS_VOL_DRY=1.0`)。
    方法论 §3.2:初吻是"缩量站稳"那根;**放量回踩 = 派发,不是健康初吻**,必须带这条量条件(否则"无主语")。
- **每个 breakout 只取第一次**(第二、三次吻不标——见方法论:第一吻是邀请,第四吻是陷阱)。
- 跨多个 breakout 命中同一天 → 按 date 去重(`seen` 集合),避免重复标注。
- `price = low[k]`,**`stop = low[k]`**(跌破初吻 low → 止损),`note = f"突破后首次回踩MA20·缩量站稳 vol×{ratio:.1f}"`。

### 5.3 Pocket Pivot POCKET_PIVOT
- 上涨日:`close[i] > close[i-1]`;
- 量能:`volume[i] > max(down-day volume over prior 10 bars)`,down-day = `close<prev close`;
- 位置:`close[i] > ma50[i]`(只在均线上方算数)。
- 可能较多 → 可设最小间隔 5 bar 去重,或全标但绘图用小标记。
- `price = high[i]`,**`stop = low[i]`**(跌破加油根 low / MA50 → 止损),`note = "Pocket Pivot 加油"`。

### 5.4 财报跳空 EARNINGS_GAP(王中之王 + 双命)⭐
1. 取财报日期:`yf.Ticker(ticker).get_earnings_dates(limit=12)`。
   - **先实测返回结构**(应为 DataFrame,index=财报datetime,列含 `EPS Estimate`/`Reported EPS`/`Surprise(%)` 之类——以实测为准)。
   - 失败/空 → 返回 `[]`,主流程照常(降级)。
2. 对每个财报日期,找**市场反应那根**:第一根 `date >= earnings_date.date()` 的 bar `g`
   (财报多在盘后 → 反应常是次日;少数盘前 → 当日)。
3. 仅保留**向上跳空**:`open[g] > close[g-1] * (1 + 0.02)`(`GAP_PCT=2%`)。
4. **量是跳空的主语(放量 = 真主语在抢筹,方法论 §4):**
   `vol_ratio = volume[g]/vol_avg20[g]`,`vol_ok = vol_ratio >= EARN_VOL_MULT`(`EARN_VOL_MULT=1.5`)。
   **缩量 up-gap 不是"王中之王",拿不到续涨命**——量折进下面的双命分类,不是单独丢弃。
5. **双命分类(核心,先判陷阱再判续涨):**
   - 当日振幅 `rng = high[g]-low[g]`,收盘位置 `pos = (close[g]-low[g])/rng`;`held = close[g]>=close[g-1]`。
   - **高开低走陷阱 TRAP**:`close[g] < close[g-1]`(收阴) **或** `low[g] <= close[g-1]`(盘中填缺口) **或** `pos < 0.33`。
   - **续涨命 CONTINUATION(续涨命)**:`held` **且** `pos >= 0.5` **且** `vol_ok`(守住缺口、收上半区、且放量)。
   - **续涨命(缩量续涨·存疑)**:`held` 且 `pos >= 0.5` 但 **未放量** → 仍归 CONTINUATION,note 标"缩量续涨·存疑"。
   - 其余 → CONTINUATION 但 note 标"中性·需观察"。
6. 方向(若有 `Surprise%`):>0 标 `财报↑(beat)`,<0 标 `财报↓(miss)`;note 末尾附 `vol×{ratio}`。
- `price = high[g]`,**`stop = close[g-1]`**(跌破缺口下沿/前收 → 止损),`fate` 置 CONTINUATION/TRAP。

### 5.5 高潮量 CLIMAX(可选,默认开)
- `volume[i] >= 2.5*vol_avg20[i]` 且 `(high[i]-low[i]) >= 1.5 * ATR近似`(或实体很大)。
- 仅标记为"事件",不判方向。低优先级,绘图用淡色。`stop = None`(无入场)。

---

## 6. 输出规格

### 6.1 图(PNG)
- 用 `mplfinance`:`type='candle'`, `volume=True`, `mav=(20,50)`, 合理 `figratio`/`figsize`, 暗色或默认 style。
- 取 `mpf.plot(..., returnfig=True)` 拿到 `fig, axes`,在主轴上对每个 KeyBar 用 `ax.annotate(...)` 加文字 + 箭头:
  - BREAKOUT → 上方绿色 "突破"
  - FIRST_KISS → 下方蓝色 "初吻"
  - POCKET_PIVOT → 小号 "PP"
  - EARNINGS_GAP → 醒目标 "财报↑/↓" + 命(续涨=金色,陷阱=红色)
  - CLIMAX → 淡灰 "量"
  - 注:mplfinance 主轴 x 用的是**整数位置**(非日期),需把 KeyBar 的日期映射到 df 的 `iloc` 位置再标注。
- 标题:`f"{ticker} 关键K线  {start}~{end}"`。
- 存:`/home/ryan/DATA/result/key_kline_{ticker}_{YYYYMMDD}.png`(若 `--output` 给定则用之)。

### 6.2 文本图例(stdout)
按日期排序,`tabulate` 打印:`日期 | 类型 | 价位 | 止损 | 命 | 说明`(`止损` = KeyBar.stop,事件类显示 `—`)。
末尾一行统计:各类型计数。

---

## 7. CLI(沿用 `t_us_tech_swing.py` 的 OptionParser 风格)

```
python t_us_key_kline.py --ticker NVDA            # 默认 period=1y
python t_us_key_kline.py --ticker NVDA --period 2y
python t_us_key_kline.py --ticker NVDA --output /tmp/nvda.png
python t_us_key_kline.py --ticker NVDA --no-earnings   # 跳过财报取数(离线)
```
- `--ticker`(必填,大写化)· `--period`(默认 `1y`)· `--output`(默认走 result/)· `--no-earnings`(布尔)。

---

## 8. 验收标准(worker 必须自验后才算完成)

**A. 能跑通主路径**
```
/home/ryan/miniconda3/bin/python t_us_key_kline.py --ticker NVDA --period 2y
```
- 退出码 0;在 result/ 生成 PNG;stdout 打印图例。
- 图例中 **至少 1 个 BREAKOUT** 且 **至少 1 个 EARNINGS_GAP**(NVDA 两年内必然有)。
- 每个 EARNINGS_GAP 都带 `fate`(CONTINUATION 或 TRAP),不为空。

**B. 图正确**
- 打开 PNG:蜡烛 + 量能副图 + MA20/MA50;标注文字**对齐到正确日期的 K 线**(随机抽查 2 个标注,核对日期与图例一致)。

**C. 离线降级**
```
python t_us_key_kline.py --ticker NVDA --no-earnings
```
- 不取财报、不报错,照常出图(无财报标注)。
- 断网或 `get_earnings_dates` 抛错时,主流程不崩,只少财报标注 + 一条 warning。

**D. 边界**
- 数据极少的票(或 `--period 3mo` 不足以算 MA50)→ 不崩,给出"数据不足"提示并尽力画。
- 复用校验:确认从 `US_yf` 缓存读取(可加日志或检查无重复 fetch 逻辑)。

**E. 自检清单**
- [ ] `mplfinance` 已装并 import 成功
- [ ] 复用了 `t_us_tech_swing` 的 `_fetch_daily`,未另起炉灶取行情
- [ ] 阈值集中在常量区,未散落
- [ ] 财报双命分类(§5.4)按规则实现且非空,且缩量 up-gap 归"存疑"而非"续涨命"
- [ ] 每条进场规则都带量/位置"主语":突破 MA150 闸门、初吻缩量、财报放量(§5.1/5.2/5.4)
- [ ] 每个进场型 KeyBar 都有 `stop`,图例含 `止损` 列(CLIMAX 为 None/—)
- [ ] 指标在全量历史上算完再切 period(warmup,§9)
- [ ] 标注 x 坐标用 iloc 位置映射(非裸日期)
- [ ] A/B/C/D 四项手动跑过并通过

---

## 9. 已知坑 / 提示
- mplfinance 主图 x 轴是整数位置:`pos = df.index.get_loc(keybar_date)`,annotate 用 `(pos, price)`。
- `get_earnings_dates()` 返回的 index 可能带时区 → 比较前 `tz_localize(None)` 或用 `.date()`。
- 财报"反应那根"对齐:用 `df.index.searchsorted(earnings_date)` 找第一根 ≥ 财报日的 bar。
- 财报日期可能落在非交易日 → 必须用"第一根 ≥ 该日"的 bar,别精确匹配。
- breakout 的 `range_high` 用 `high` 还是 `close` 影响灵敏度;默认用 `high`,可在常量(`RANGE_ON`)切换。
- **指标 warmup(易错):** `prepare_frame` 必须**先在全量历史上算 MA/vol_avg/ATR/range_high,再切 period**。
  若先切再算,窗口左缘前 50/150 根 MA50/MA150 全 NaN、突破区间被截断,会静默吞掉早期信号。
- **趋势/缩量是"主语",不是可选项:** breakout 的 MA150 闸门、初吻的缩量、财报的放量都对应方法论的
  "K 线关键不在形状在主语(位置+成交量)"。改阈值可以,但**别把这几条量/位置条件删成裸看形态**。

---

## 10. 完成后回写
- 在 `docs/key_kline_methodology.md` §7 标记"绘图脚本已落地",并写明实际文件名/用法。
- 若 `get_earnings_dates` 行为与本文假设不符,更新 §5.4 与 §2 的实测结论,供后续参考。

### 落地后修订(2026-06-13,review 驱动)
脚本已实现并按一轮 review 收紧,**本蓝图 §4/§5/§6.2/§9 已同步到实现**。相对初版草案的差异:
- §5.2 初吻新增**缩量**条件(`KISS_VOL_DRY=1.0`);§5.4 财报新增**放量**门槛(`EARN_VOL_MULT=1.5`,缩量跳空→"存疑")
  —— 草案表曾缺量条件,与方法论 §3.2/§4"主语=位置+成交量"不符,现已补齐(硬约束 §1.1)。
- §5.1 突破新增 **MA150 趋势闸门**(此前 `ma150` 算了未用)。
- 所有进场型 KeyBar 新增 **`stop`** 字段 + 图例 `止损` 列 —— 兑现脚本宣称的 WHEN+**WHERE**(方法论 §2.2)。
- `prepare_frame` 改为**全量算指标再切 period**(warmup),并预计算 `range_high` 列。
- 初吻跨 breakout 按 date 去重;`main` 对区间过短(< CONSOL_DAYS)告警。
- 实测 `get_earnings_dates` 返回结构与 §5.4 假设一致(index tz-aware、列含 `Surprise(%)`)。
```
```
