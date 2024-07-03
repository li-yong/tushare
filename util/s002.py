"""
脚本使用说明：

1. 配置环境：pip install czsc streamlit loguru pandas numpy tqdm -U

2. 运行脚本：streamlit run 策略案例002.py --server.port 8501 --theme.base dark
"""

# 首次运行，需要设置一下 tushare 的 token
# import tushare as ts
# ts.set_token("your_token")

import inspect

import czsc
import numpy as np
import pandas as pd
import streamlit as st
from loguru import logger
from czsc.connectors import ts_connector as ts
from stqdm import stqdm as tqdm

st.set_page_config(layout="wide", page_title="量价因子分析", page_icon="🧍")


def LLM_240511_085(df: pd.DataFrame, **kwargs):
    """
    因子逻辑：该因子通过计算收盘价与开盘价的比值，然后乘以交易量的对数，来捕捉价格变动和交易活跃度之间的关系。
            收盘价与开盘价的比值可以反映当日的市场情绪和价格趋势，而交易量的对数则可以减少交易量数值的波动性，
            使得因子更加稳定。可调整的参数包括计算比值时的价格字段（close, open）以及是否使用对数处理交易量（log(vol)）。

    :param df: 包含开盘价、最高价、最低价、收盘价和交易量的DataFrame。
    :param kwargs: 其他参数

        - tag: 因子标签，str类型，默认为"DEFAULT"。

    :return: 更新后的DataFrame，包含计算得到的量价因子。
    """
    tag = kwargs.get("tag", "DEFAULT")

    # 通过 inspect 模块获取因子名，构建因子列名
    factor_name = inspect.currentframe().f_code.co_name
    factor_col = f"F#{factor_name}#{tag}"

    # 计算收盘价与开盘价的比值
    df["div_close_open"] = np.where(df["open"] != 0, df["close"] / df["open"], np.nan)

    # 计算交易量的对数
    df["log_vol"] = np.log(df["vol"])

    # 计算因子：mul(div(close, open), log(vol))
    df[factor_col] = df["div_close_open"] * df["log_vol"]

    # 处理缺失值
    df[factor_col] = df[factor_col].fillna(0)

    # 删除中间变量
    df.drop(["div_close_open", "log_vol"], axis=1, inplace=True)

    return df


@st.cache_data()
def calculate_factor():
    """计算全A股票日线数据的量价因子"""
    symbols = ts.get_symbols("stock")
    # 获取全A股票日线数据，耗时大约2个小时
    rows = []
    for symbol in tqdm(symbols, desc="计算量价因子", unit="只"):
        try:
            df = ts.get_raw_bars(symbol, freq="日线", sdt="20100101", edt="20240101", raw_bar=True)
            df = pd.DataFrame(df)
            df = df[["symbol", "dt", "open", "close", "high", "low", "vol", "amount"]].copy()
            if len(df) < 300:
                logger.warning(f"{symbol} 日线数据量不足")
                continue
            df = LLM_240511_085(df)
            df["price"] = df["close"]
            rows.append(df)

        except Exception as e:
            logger.exception(f"计算因子失败：{symbol}: {e}")

    dfk = pd.concat(rows, axis=0)

    # 计算后续N根bar的累计收益，用于回测分析
    dfk = czsc.update_nxb(dfk, nseq=(1, 2, 3, 5, 8, 10, 13), bp=False)
    return dfk


@st.experimental_fragment
def show_portfolio(df, **kwargs):
    st.subheader("三、多空组合收益率", divider="rainbow")
    nxb = [x for x in df.columns if x.startswith("n") and x.endswith("b")]
    exclude_cols = ["dt", "symbol", "next_open", "open", "close", "high", "price", "low", "vol", "amount"] + nxb

    factors = [x for x in df.columns if x not in exclude_cols]
    max_dt, min_dt = df["dt"].max(), df["dt"].min()

    with st.form(key="form_pot"):
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
        sdt = pd.to_datetime(col1.date_input("开始日期", value=pd.to_datetime(min_dt), key="pot_sdt"))
        edt = pd.to_datetime(col2.date_input("结束日期", value=pd.to_datetime(max_dt), key="pot_edt"))
        factor = col3.selectbox("选择因子", factors, index=0, key="pot_factor", help="选择因子列")
        hold_periods = col4.number_input(
            "持仓周期",
            value=1,
            min_value=1,
            max_value=100,
            step=1,
            key="pot_hold_periods",
            help="每隔多少个交易日调整一次仓位",
        )
        fee = col5.number_input(
            "单边手续费（单位：BP）",
            value=2,
            min_value=-5,
            max_value=50,
            step=1,
            key="pot_fee",
            help="单边手续费，单位为BP；默认为2BP，即0.02%",
        )
        r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
        factor_dir = r2_c1.selectbox("因子有效性方向", options=["正向", "负向"], index=0, key="pot_dir")
        long = int(r2_c2.number_input("多头品种数量", value=5, min_value=0, key="pot_long", help="多头品种数量"))
        short = int(r2_c3.number_input("空头品种数量", value=5, min_value=0, key="pot_short", help="空头品种数量"))
        digits = int(
            r2_c4.number_input("小数位数", value=6, min_value=0, key="pot_digits", help="持仓权重小数位数，0表示整数")
        )
        # 将 submit button 设置在右手边
        submit = st.columns([5, 1])[1].form_submit_button("开始回测", use_container_width=True)

    if not submit:
        st.warning("请设置多空组合")
        st.stop()

    df = df[(df["dt"] >= sdt) & (df["dt"] <= edt)].copy().reset_index(drop=True)

    df_bh = df.groupby("dt")["n1b"].mean().to_frame("B&H等权")

    df["weight"] = 0
    # 因子正向：每个时间截面上，选择因子值最大的 long 个品种，最小的 short 个品种
    rows = []
    for dt, dfg in df.groupby("dt"):
        dfg = dfg.sort_values(factor, ascending=False if factor_dir == "正向" else True)
        dfg["weight"] = 0.0
        if long > 0:
            dfg.loc[dfg.index[:long], "weight"] = 1 / long
        if short > 0:
            dfg.loc[dfg.index[-short:], "weight"] = -1 / short
        rows.append(dfg)
    df = pd.concat(rows, ignore_index=True)

    dfw = df[["dt", "symbol", "weight", "price", "n1b"]].copy()
    dfw = dfw.dropna(subset=["price"])

    # 根据 hold_periods 调整 weight
    if hold_periods > 1:
        dfw = czsc.adjust_holding_weights(dfw, hold_periods=hold_periods)

    # 展示每个时间截面上的多空组合，dfw1 分3列，第一列是时间，第二列是多头品种，第三列是空头品种
    rows = []
    for dt, dfg in dfw.groupby("dt"):
        longs = dfg[dfg["weight"] > 0]["symbol"].tolist()
        shorts = dfg[dfg["weight"] < 0]["symbol"].tolist()
        rows.append([dt, sorted(longs), sorted(shorts), dfg["symbol"].nunique()])
    dfw1 = pd.DataFrame(rows, columns=["交易日", "多头持仓", "空头持仓", "候选数量"])
    with st.expander("多空组合持仓详情", expanded=False):
        st.dataframe(dfw1, use_container_width=True)

    dfd = czsc.holds_performance(dfw, fee=fee, digits=digits)
    dfd["dt"] = pd.to_datetime(dfd["date"])
    dfd["portfolio"] = dfd["edge_post_fee"]
    dfd.set_index("dt", inplace=True)
    daily = dfd.merge(df_bh, left_index=True, right_index=True, how="left")
    daily = daily[["portfolio", "B&H等权"]].copy().fillna(0)
    daily["超额"] = daily["portfolio"] - daily["B&H等权"]

    czsc.show_daily_return(daily, stat_hold_days=False)

    c1, c2 = st.columns([1, 1])
    with c1:
        st.write("收益相关性")
        czsc.show_correlation(daily, method="pearson", sub_title="")

    with c2:
        st.write("B&H等权下跌时的收益相关性")
        czsc.show_correlation(daily[daily["B&H等权"] < 0].copy(), method="pearson", sub_title="")

    if kwargs.get("show_drawdowns", True):
        st.write("策略最大回撤分析")
        czsc.show_drawdowns(daily, ret_col="portfolio", sub_title="")

    if kwargs.get("show_yearly_stats", True):
        st.write("策略年度绩效指标")
        czsc.show_yearly_stats(daily, ret_col="portfolio", sub_title="")

    if kwargs.get("show_monthly_return", True):
        st.write("策略月度累计收益")
        czsc.show_monthly_return(daily, ret_col="portfolio", sub_title="")


def main():
    df = calculate_factor()
    x_col = "F#LLM_240511_085#DEFAULT"
    y_col = "n1b"

    # 因子截面归一化
    df = czsc.normalize_feature(df, x_col, q=0.01)

    st.subheader("一、IC分析结果概览", divider="rainbow")
    czsc.show_sectional_ic(df, x_col, y_col, method="pearson", show_factor_histgram=False)

    st.subheader("二、因子分层收益率", divider="rainbow")
    czsc.show_factor_layering(df.copy(), x_col, y_col, n=10)

    show_portfolio(df.copy(), factor=x_col)


if __name__ == "__main__":
    main()
