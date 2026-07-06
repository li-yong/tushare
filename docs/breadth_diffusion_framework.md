# 板块广度扩散检测框架（Breadth Diffusion Framework）

> Session日期：2026-07-06
> 用途：检测板块级"风口"启动/衰竭，量化"响叶子扩散速度"
> 配合现有基础设施：skyte/relative-strength（RS percentile pipeline，QQQ基准）、Futu OpenD（行情数据源）

## 核心思想

单只股票信号（RS突破、量能放大）是噪音候选。板块级广度扩散信号才是"风口确立"的确认。四个指标构成领先-同步-滞后序列，同一组指标反向使用即可检测衰竭。

## 输入数据

- 板块成分股列表（ticker list）
- 每只成分股：日线 OHLCV，至少252个交易日历史
- 板块/基准指数价格（用于A/D Line对比）
- 已有 skyte RS percentile 输出（按ticker、按日期）

## 四个核心指标

### 1. NH-NL 扩散速度（New High - New Low Diffusion Rate）

```
NH(t) = count(ticker in universe where close(t) == max(close[t-251:t+1]))
NL(t) = count(ticker in universe where close(t) == min(close[t-251:t+1]))
diff(t) = NH(t) - NL(t)
diff_accel(t) = diff(t) - diff(t-5)   # 5日变化率，测二阶导
```

判定：
- `diff_accel(t)` 连续5-10日为正且加速 → 早期点火
- `diff(t)` 绝对值高但 `diff_accel(t)` 转平/转负 → 广度见顶

### 2. % Above 50MA

```
pct_above_50ma(t) = count(ticker where close(t) > MA50(ticker, t)) / len(universe)
```

判定（Breadth Thrust，Zweig标准）：
- 从 <30% 在 ≤10个交易日内穿越 70% → 高置信度启动信号
- 长期 >80% 后走平 → 后期，边际参与股减少

### 3. A/D Line（板块内涨跌家数累积线）

```
AD(t) = AD(t-1) + (advances(t) - declines(t))
```
其中 advances/declines 为板块内当日收涨/收跌家数。

判定：
- AD Line 与板块指数同步创新高 → 广度健康，真实参与
- AD Line 与指数背离（指数新高，AD不创新高）→ leadership narrowing，早于价格衰竭

### 4. RS百分位板块扩散占比（核心，基于现有skyte管线扩展）

```
rs_breadth(t) = count(ticker where RS_percentile(ticker, t) > 80) / len(universe)
```

判定：
- `rs_breadth(t)` 加速上升，早于NH-NL明显变化 → 最领先信号
- `rs_breadth(t)` 见顶回落，而龙头股RS仍高位 → leadership narrowing 预警，早于指数

## 时序结构（信号使用顺序）

| 阶段 | 主导信号 | 特征 |
|---|---|---|
| 早期点火 | rs_breadth 加速 | 少数强势股跑赢，NH-NL不明显 |
| 确立 | NH-NL加速 + pct_above_50ma 突破50% | breadth thrust |
| 成熟 | AD Line 与指数同步新高 | 可加仓阶段（对应pyramid add原则） |
| 衰竭预警 | AD Line背离 或 rs_breadth见顶回落 | 领先型衰竭 |

## 实现要求（给Claude Code）

1. Python实现，输入：板块成分股ticker list + 历史OHLCV（可用Futu OpenD或已有数据源获取）
2. 输出：四条时间序列（diff_accel, pct_above_50ma, AD line, rs_breadth）+ 复合状态标签（早期/确立/成熟/衰竭预警）
3. 可回测：允许输入历史日期区间，验证历史上breadth thrust出现后N日的板块指数表现
4. 需要过滤机械性扰动（指数再平衡日、成分股变更）造成的虚假跳变
5. 状态机逻辑：明确的阈值判定 + 二阶导数（加速度）计算，不用主观打分
6. 与现有skyte RS percentile输出对接（假设已有 ticker-date-RS_percentile 的CSV/DataFrame）

## 明确排除

- 不做单一综合分数（false precision，已在之前session中明确拒绝伪多维加权评分）
- 不用debt-assuming逻辑（比如"广度已经很宽该回调了"），只用state-measuring（当下广度处于什么状态）

