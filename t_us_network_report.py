# coding: utf-8
"""
US Market Network — 阶段汇总报告生成器 (ZH-CN)

把网络结构分析三个阶段的机器可读 JSON 摘要读进来，自动交叉研判，产出 **一份**
中文综合报告 (Markdown)。三阶段脚本各自在 result/ 子目录里写 `*_<date>.json` sidecar：

  阶段1 静态网络    t_us_network_structure.py   → us_network_structure/*.json
  阶段2 动态/抱团    t_us_network_dynamics.py    → us_network_dynamics/*.json
  阶段4 事件研究    t_us_network_event_study.py → us_network_event_study/*.json

本脚本只读这些 JSON（不重算），按 result/ 的"递归 glob 取最新"约定定位文件，
合成为：市场环境 → 抱团现状 → 历史交易价值 → 综合研判与操作含义 → 局限。

定位（沿用 spec §3）：网络是**风险/择时的环境判断**工具，非选股 alpha 引擎；
本报告给的是环境研判与提示，不是买卖信号。

Usage:
  python t_us_network_report.py                  # 读三阶段最新 JSON → 合成报告
  python t_us_network_report.py --refresh        # 先按默认参数重跑三阶段再合成
  python t_us_network_report.py --refresh --universe ndx --groups baskets
"""

import os
import sys
import glob
import json
import logging
import datetime
import subprocess
from optparse import OptionParser

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('netreport')

RESULT_ROOT = '/home/ryan/DATA/result'
OUT_DIR = os.path.join(RESULT_ROOT, 'us_network_report')
PY = sys.executable

STAGE_GLOBS = {
    'p1': 'us_network_structure_*.json',
    'p2': 'us_network_dynamics_*.json',
    'p4': 'us_network_event_study_*.json',
}


def latest_json(pattern):
    """result/ 下递归 glob 取按文件名(含日期)排序最新的一个（沿用跨脚本读取约定）。"""
    hits = glob.glob(os.path.join(RESULT_ROOT, '**', pattern), recursive=True)
    if not hits:
        return None
    return sorted(hits)[-1]


def load_stage(key):
    path = latest_json(STAGE_GLOBS[key])
    if not path:
        return None, None
    try:
        with open(path) as fh:
            return json.load(fh), path
    except Exception as e:
        log.error(f'读取 {path} 失败: {e}')
        return None, path


# ── 大白话转换 ───────────────────────────────────────────────────────────────
def env_word(mc):
    """市场冷热一句话。"""
    if mc is None:
        return '未知'
    if mc < 0.15:
        return '很松散（股票各走各的，没什么共振）'
    if mc < 0.30:
        return '正常'
    if mc < 0.45:
        return '偏挤（越来越多股票一起涨跌，风险在累积）'
    return '非常挤（几乎同涨同跌，危险）'


# 阶段2 内部状态 → 大白话状态 + 该怎么对待
STAGE_CN = {
    '见顶/瓦解 PEAK→UNWIND ⚠': ('过热后开始降温', '最该小心'),
    '见顶 PEAK ⚠': ('冲到高位、不再涨', '最该小心'),
    '致密化 DENSIFYING': ('正在抱紧、还在变热', '可拿但要收紧'),
    '升温 BUILDING': ('在升温', '观察'),
    '瓦解 UNWINDING': ('在降温', '观察'),
    '平静/松散 QUIET': ('平淡', '无所谓'),
}


def heat_word(pct):
    pct = pct or 0
    if pct >= 0.9:
        return '很热'
    if pct >= 0.7:
        return '偏热'
    if pct >= 0.4:
        return '一般'
    return '低'


def trend_word(slope):
    if slope is None:
        return '—'
    if slope > 0.03:
        return '还在升温'
    if slope < -0.03:
        return '在降温'
    return '基本走平'


def overlap_community(members, p1):
    """给阶段2 一个分组的成员，找阶段1 重叠最多的社区，返回 (cid, 枢纽, 重叠数, sector)。"""
    if not p1 or not members:
        return None
    mset = set(members)
    best = None
    for c in p1.get('communities', []):
        top = c.get('members_top') or []
        ov = len(mset & set(top))
        if best is None or ov > best[2]:
            best = (c['id'], (top[0] if top else '?'), ov, c.get('dominant_sector'))
    return best if best and best[2] > 0 else None


# ════════════════════════════════════════════════════════════════════════════
# 报告合成
# ════════════════════════════════════════════════════════════════════════════
THEME_CN = {'SEMI': '半导体', 'SOFTWARE': '软件', 'MAG7': '七巨头',
            'INTERNET': '互联网', 'AI_INFRA': 'AI基建'}


def theme_cn(name):
    return THEME_CN.get(name, name)


def build_report(p1, p1p, p2, p2p, p4, p4p):
    L = []
    A = L.append
    today = datetime.date.today().isoformat()

    # 先把阶段2 的组分好类（最该当心 / 还在变热）
    watch, hot, groups = [], [], (p2.get('groups', []) if p2 else [])
    for g in groups:
        if g['stage'] in ('见顶/瓦解 PEAK→UNWIND ⚠', '见顶 PEAK ⚠'):
            watch.append(g)
        elif g['stage'] == '致密化 DENSIFYING' and (g.get('comp_pct') or 0) >= 0.9:
            hot.append(g)
    mc_now = p2.get('market_mean_corr_now') if p2 else None

    A(f'# 美股抱团结构 · 今日研判（{today}）')
    A('')
    A('> 这份报告只回答一件事：**现在钱抱团在哪、哪些主题过热该当心**。')
    A('> 是用来看大环境、管风险的，**不是买卖信号**——具体买卖仍看你自己的个股止损和基本面。')
    A('')

    # ───────────────────────── 一句话结论 ──────────────────────────
    A('## 一句话结论')
    A('')
    A(f'- **大盘冷热**：{env_word(mc_now)}'
      + ('。钱没有挤在一起，风险是个别主题的事，不用对整个仓位大动。' if (mc_now or 0) < 0.30
         else '。越来越多股票一起动，要留意整体回撤风险。'))
    if watch:
        A(f'- **最该当心**：{("、".join(theme_cn(g["name"]) for g in watch))}'
          '——已经过热、刚开始降温。这种主题历史上接下来一两个月容易跑输大盘。')
    else:
        A('- **最该当心**：暂无主题处在“过热刚降温”状态。')
    if hot:
        A(f'- **还在变热**：{("、".join(theme_cn(g["name"]) for g in hot))}'
          '——钱还在涌入，能拿，但要收紧止损、别追高。')
    # 操作一句话
    if watch:
        op = (f'把 {"、".join(theme_cn(g["name"]) for g in watch)} 的仓位先减一点或收紧止损，'
              '别在这里抄底加仓；')
        op += (f'{"、".join(theme_cn(g["name"]) for g in hot)} 还热的可以拿，但设好止损、不追高。'
               if hot else '其余主题维持原样即可。')
    elif hot:
        op = (f'{"、".join(theme_cn(g["name"]) for g in hot)} 还在变热，能拿但收紧止损、别追高；'
              '其余无需动作。')
    else:
        op = '没有过热或刚降温的主题，按你原来的仓位和止损框架走就行。'
    A(f'- **怎么做**：{op}')
    A('')

    # ───────────────────────── 各主题抱团温度 ──────────────────────────
    A('## 各主题抱团温度')
    A('')
    if not p2:
        A('_阶段2 数据缺失，跳过。_')
        A('')
    else:
        A('| 主题 | 现在状态 | 热度 | 趋势 | 上一次最热 |')
        A('|------|----------|------|------|------------|')
        for g in groups:
            st_cn, _ = STAGE_CN.get(g['stage'], (g['stage'], ''))
            A(f'| {theme_cn(g["name"])} | {st_cn} | {heat_word(g.get("comp_pct"))}'
              f'（{(g.get("comp_pct") or 0):.0%}） | {trend_word(g.get("comp_slope"))} '
              f'| {g.get("peak_date")} |')
        A('')
        A('> 热度 = 跟它自己过去三年比，现在排在多热的位置（100% = 历史最热）。')
        A('')
        # 逐个当心/变热主题，给一句带结构背景的具体话
        for g in watch + hot:
            ov = overlap_community(g.get('members'), p1)
            core = ''
            if ov and any(h['ticker'] == ov[1] for h in (p1.get('hubs_degree', [])[:3] if p1 else [])):
                core = (f'（{theme_cn(g["name"])}也是全场资金最集中的板块，{ov[1]} 是其中最有'
                        f'代表性的一只、类似板块 ETF——{ov[1]} 一弱往往整簇同时弱，适合当风险'
                        '监控；但它只是同步代表，不是先于别人下跌的领先指标）')
            if g in watch:
                A(f'- **{theme_cn(g["name"])}**：已经过热、开始降温{core}。'
                  '**优先减仓或收紧止损**，别在这逢低补。')
            else:
                A(f'- **{theme_cn(g["name"])}**：还在变热、钱还在进{core}。'
                  '可以继续拿，但已经到了“最显眼=最危险”的区域，设好止损、不再追高。')
        if not (watch or hot):
            A('- 目前没有主题过热或刚降温，整体平淡，不用特别动作。')
        A('')

    # ───────────────────────── 这套信号过去准不准 ──────────────────────────
    A('## 这套信号过去准不准（仅供参考）')
    A('')
    if not p4:
        A('_阶段4 数据缺失，跳过。_')
        A('')
    else:
        tabs = p4.get('tables', {})

        def at(sig, H):
            for r in tabs.get(sig, []):
                if r.get('H') == H and 'mean' in r:
                    return r
            return None
        d5 = at('DENSIFY', 5); u21 = at('UNWIND', 21); u63 = at('UNWIND', 63)
        bench = p4.get('benchmark', 'QQQ')
        A(f'拿过去三年的数据，和大盘（{bench}）比，得到两条经验：')
        if d5:
            A(f'- **在一个主题最热时追进去 → 短期容易亏**：之后一周平均比大盘差约 '
              f'{abs(d5["mean"]):.1f}%（{d5["n"]} 次里 {d5["neg"]:.0f}% 都是差的）。'
              '所以“别追最热的”这条最实在。')
        if u21 and u63:
            A(f'- **主题刚降温时别急着做空**：头一个月通常还会反弹（约 {u21["mean"]:+.1f}%），'
              f'要到两三个月后才真正走弱（约 {u63["mean"]:+.1f}%）。降温是慢慢来的。')
        A('')
        A('> ⚠ **重要**：这只是过去三年里少数几轮行情统计出来的，次数太少，'
          '只能当**方向参考**，不能当成稳赚的规则，更不能单凭这个下单。')
        A('')

    # ───────────────────────── 提醒 ──────────────────────────
    A('## 用之前记住三点')
    A('')
    A('1. 这是**看大环境**的，告诉你哪冷哪热；具体买卖要配合个股止损（如 20 周线）和基本面。')
    A('2. 它**看不出谁领涨**——只知道哪些票一起动，不知道谁先动（那需要更细的数据，还没做）。')
    A('3. 看得最清楚（某主题最热、最抱团）的时候，往往也是**最危险**的时候，越热越要小心。')
    A('')

    # 数据脚注
    dates = [d.get('date') for d in (p1, p2, p4) if d]
    if len(set(dates)) > 1:
        A(f'_注：三部分数据日期不一致（{", ".join(sorted(set(dates)))}），是拼接的快照，'
          '解读时留意时间差；建议同日重跑。_')
    else:
        A(f'_数据日期：{dates[0] if dates else "—"}（半导体/软件等主题口径，纳指100）。_')
    return '\n'.join(L)


def maybe_refresh(opts):
    """--refresh: 先按给定参数重跑三阶段(各自写 JSON)。"""
    cmds = [
        [PY, 't_us_network_structure.py', '--universe', opts.universe],
        [PY, 't_us_network_dynamics.py', '--universe', opts.universe,
         '--groups', opts.groups],
        [PY, 't_us_network_event_study.py', '--universe', opts.universe,
         '--groups', opts.groups],
    ]
    if opts.no_plot:                       # cron: 不产 PNG，保持 result/ git 仓干净
        for c in cmds:
            c.append('--no-plot')
    for c in cmds:
        log.info('重跑: ' + ' '.join(c))
        r = subprocess.run(c, cwd=os.path.dirname(os.path.abspath(__file__)))
        if r.returncode != 0:
            log.warning(f'{c[1]} 退出码 {r.returncode}（继续，用已有 JSON）')


def main():
    parser = OptionParser()
    parser.add_option('--refresh', action='store_true', default=False,
                      help='先按下方参数重跑三阶段再合成')
    parser.add_option('--universe', default='ndx', help='重跑时用 (默认 ndx)')
    parser.add_option('--groups', default='baskets', help='重跑时用 (默认 baskets)')
    parser.add_option('--no-plot', dest='no_plot', action='store_true', default=False,
                      help='--refresh 时各阶段不产 PNG (cron 用，保持 result/ 干净)')
    opts, _ = parser.parse_args()

    if opts.refresh:
        maybe_refresh(opts)

    p1, p1p = load_stage('p1')
    p2, p2p = load_stage('p2')
    p4, p4p = load_stage('p4')
    if not any([p1, p2, p4]):
        raise SystemExit('未找到任何阶段 JSON。先运行三阶段脚本，或加 --refresh。')

    txt = build_report(p1, p1p, p2, p2p, p4, p4p)
    os.makedirs(OUT_DIR, exist_ok=True)
    tag = datetime.date.today().isoformat()
    out_md = os.path.join(OUT_DIR, f'us_network_report_{tag}.md')
    with open(out_md, 'w') as fh:
        fh.write(txt)
    print('\n' + txt)
    log.info(f'综合报告: {out_md}')


if __name__ == '__main__':
    main()
