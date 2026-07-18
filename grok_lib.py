# -*- coding: utf-8 -*-
"""
grok_lib.py — 轻量 xAI/Grok 客户端封装,服务于美股情绪/事件层分析。

定位:不做价格预测、不算技术指标(那是 finlib 的活)。
Grok 在本系统里只负责它独一份的能力——通过 server-side 的
web_search + x_search 工具实时检索新闻/web/X(Twitter),产出
"情绪 + 催化剂"的结构化判断,叠加在现有量化信号之上。

接口:xAI Agent Tools API,走 OpenAI 兼容的 `responses` 端点
(旧的 Live Search `search_parameters` 已于 2026 废弃,返回 410)。
依赖:openai>=2.x;key 从项目 .env 的 XAI_API_KEY 读。
"""
from __future__ import annotations

import os
import re
import json
from typing import Any

from constant import (STRAW_BEST_NEWS_CLIMAX, STRAW_REGULATORY,
                      STRAW_CUSTOMER_CONFLICT, STRAW_DOWNSTREAM_EXCESS,
                      STRAW_TYPES_ALL,
                      SPARK_CATALYST_BREAKOUT, SPARK_SUPPLY_DESTRUCTION,
                      SPARK_DEMAND_COMMITMENT, SPARK_SHORTAGE_ADMISSION,
                      SPARK_TYPES_ALL)

XAI_BASE_URL = "https://api.x.ai/v1"
DEFAULT_MODEL = "grok-4.3"          # 最强且最快;支持 web_search/x_search
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

# 计费换算:usage.cost_in_usd_ticks → 美元。按 token 实价反推约 3e-10/tick,
# 即每次带 4 次 search 的扫描 ≈ $0.10-0.12。经验值,精确账单以 console.x.ai 为准。
USD_PER_TICK = 3e-10


def load_xai_key(env_path: str = ENV_PATH) -> str:
    """从 .env 读 XAI_API_KEY(不引入 python-dotenv 依赖)。优先环境变量。"""
    key = os.environ.get("XAI_API_KEY")
    if key:
        return key.strip()
    if os.path.exists(env_path):
        for line in open(env_path, encoding="utf-8"):
            m = re.match(r"\s*XAI_API_KEY\s*=\s*(.+)", line)
            if m:
                return m.group(1).strip().strip('"').strip("'")
    raise RuntimeError(f"XAI_API_KEY not found in env or {env_path}")


def get_client():
    from openai import OpenAI
    return OpenAI(api_key=load_xai_key(), base_url=XAI_BASE_URL)


_SENTIMENT_SCHEMA = {
    "type": "json_schema",
    "name": "stock_sentiment",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "ticker": {"type": "string"},
            "stance": {"type": "string",
                       "enum": ["bullish", "bearish", "neutral", "unclear"]},
            "confidence": {"type": "number",
                           "description": "0.0-1.0,对该判断的把握"},
            "summary": {"type": "string", "description": "一句话核心结论(中文)"},
            "catalysts": {"type": "array", "items": {"type": "string"},
                          "description": "近期已发生或临近的催化剂事件,简短中文"},
            "risks": {"type": "array", "items": {"type": "string"},
                      "description": "主要风险点,简短中文"},
            "key_facts": {"type": "array", "items": {"type": "string"},
                          "description": "3-6 条带事实依据的要点,中文"},
            "as_of": {"type": "string", "description": "信息时间范围说明"},
        },
        "required": ["ticker", "stance", "confidence", "summary",
                     "catalysts", "risks", "key_facts", "as_of"],
    },
}

_SYS_PROMPT = (
    "你是专注美股的资深分析师。基于 web_search / x_search 检索到的实时信息,"
    "对给定股票输出情绪与催化剂判断。confidence 用 0.0-1.0。"
    "只依据检索到的事实,信息不足就在相应字段说明,绝不编造。所有文本用中文。"
)


def _extract_citations(resp) -> list[str]:
    """从 message 的 url_citation annotations 抽取去重后的来源 URL。"""
    urls: list[str] = []
    for item in getattr(resp, "output", []) or []:
        if getattr(item, "type", None) != "message":
            continue
        for part in getattr(item, "content", []) or []:
            for ann in getattr(part, "annotations", []) or []:
                if getattr(ann, "type", None) == "url_citation":
                    u = getattr(ann, "url", None)
                    if u and u not in urls:
                        urls.append(u)
    return urls


def _structured_scan(sys_prompt: str, user_prompt: str, schema: dict,
                     fallback: dict, model: str = DEFAULT_MODEL,
                     x_handles: list[str] | None = None) -> dict:
    """共享底座:带 web_search/x_search 的结构化(JSON-schema)单次调用。
    解析失败返回 fallback(附 raw);永远附 _meta。"""
    client = get_client()
    x_tool: dict[str, Any] = {"type": "x_search"}
    if x_handles:
        x_tool["x_handles"] = x_handles
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tools=[{"type": "web_search"}, x_tool],
        text={"format": schema},
        temperature=0,
    )
    try:
        data = json.loads(resp.output_text)
    except (json.JSONDecodeError, TypeError, AttributeError):
        data = dict(fallback)
        data["raw"] = getattr(resp, "output_text", None)
    u = resp.usage
    ticks = getattr(u, "cost_in_usd_ticks", None)
    data["_meta"] = {
        "model": model,
        "citations": _extract_citations(resp),
        "total_tokens": getattr(u, "total_tokens", None),
        "tools_used": getattr(u, "num_server_side_tools_used", None),
        "cost_ticks": ticks,
        "cost_usd_est": round(ticks * USD_PER_TICK, 4) if ticks else None,
    }
    return data


def sentiment_scan(ticker: str, model: str = DEFAULT_MODEL,
                   days_back: int = 7,
                   x_handles: list[str] | None = None) -> dict:
    """对单只股票做实时情绪/催化剂扫描,返回结构化 dict。
    含 _meta:模型、引用来源 URL、token/工具用量、估算成本。"""
    return _structured_scan(
        _SYS_PROMPT,
        f"分析股票 {ticker} 最近 {days_back} 天的市场情绪与催化剂。",
        _SENTIMENT_SCHEMA,
        {"ticker": ticker, "stance": "unclear", "summary": "解析失败"},
        model=model, x_handles=x_handles,
    )


# ── 见顶新闻扫描 (docs/news_driven_top_detection.md) ─────────────────────────
# 稻草类型字符串来自 constant.py(它们同时是 signal ledger 的 signal_type)。
_TOP_SCHEMA = {
    "type": "json_schema",
    "name": "news_top_detection",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "ticker": {"type": "string"},
            "top_risk_level": {
                "type": "string",
                "enum": ["none", "low", "medium", "high", "confirmed_top"],
                "description": "新闻侧的周期见顶风险(不含价格确认)"},
            "straw_types": {
                "type": "array",
                "items": {"type": "string", "enum": STRAW_TYPES_ALL},
                "description": ("窗口内实际命中的稻草类型。"
                                f"{STRAW_BEST_NEWS_CLIMAX}=出现本周期级别的超预期利好"
                                "(如财报大超+指引上修);"
                                f"{STRAW_REGULATORY}=行业性反垄断/监管立案;"
                                f"{STRAW_CUSTOMER_CONFLICT}=大客户公开抱怨/涨价转嫁/供需公开互撕;"
                                f"{STRAW_DOWNSTREAM_EXCESS}=下游边际买家承认过剩/砍需求"
                                "(攻击需求假设的新闻)。没有就留空,绝不凑数")},
            "subject_migration": {
                "type": "boolean",
                "description": ("新闻主语是否已从公司自己(财报/产品/新高)"
                                "迁移到生态(客户/监管/法院/下游买家)")},
            "last_straw": {
                "type": "string",
                "description": "若命中 downstream_excess:一句话描述该新闻;否则空字符串"},
            "demand_victim": {
                "type": "string",
                "description": ("按'谁的需求假设被击穿'归因(可以不是新闻主语,"
                                "如 Meta 出售过剩算力击穿的是存储/算力供给链);无则空字符串")},
            "confidence": {"type": "number", "description": "0.0-1.0"},
            "summary": {"type": "string",
                        "description": "一句话中文状态测量(GPS,不是方向预测)"},
            "key_facts": {"type": "array", "items": {"type": "string"},
                          "description": "3-6 条带日期的事实要点,中文"},
            "as_of": {"type": "string", "description": "信息时间范围说明"},
        },
        "required": ["ticker", "top_risk_level", "straw_types",
                     "subject_migration", "last_straw", "demand_victim",
                     "confidence", "summary", "key_facts", "as_of"],
    },
}

_TOP_SYS_PROMPT = (
    "你是专注美股周期顶部识别的分析师,执行'新闻主语迁移'方法论:"
    "见顶前新闻主语是公司自己(财报/产品/订单/新高),顶部区新闻主语迁移到生态"
    "(客户/监管/法院/下游买家)。你只做状态测量(GPS),不做方向预测,不建议仓位动作。"
    "四类'稻草'新闻各测一个状态,只在检索到确凿对应事实时才标记,绝不凑数;"
    "归因按'谁的需求假设被击穿',不按新闻主语。"
    "top_risk_level 标准: none=只有公司主语的常规新闻; low=个别生态新闻但无稻草; "
    "medium=命中1类稻草或主语明显迁移; high=多类稻草共振或命中 downstream_excess; "
    "confirmed_top 仅当稻草共振且检索到价格已明显破位的报道。"
    "只依据检索到的事实,信息不足就说明,绝不编造。所有文本用中文。"
)


def top_scan(ticker: str, model: str = DEFAULT_MODEL, days_back: int = 14,
             cutoff: str | None = None,
             x_handles: list[str] | None = None) -> dict:
    """对单只股票做见顶新闻扫描(四类稻草+主语迁移),返回结构化 dict。

    cutoff='YYYY-MM-DD' 时要求模型只使用发表于该日及之前的新闻(prompt 级
    约束,近似 point-in-time——web 检索无法硬截断,回测结果只能当 best-effort)。
    """
    if cutoff:
        window = (f"只考虑发表于 {cutoff}(含)之前、且距该日 {days_back} 天内的新闻,"
                  f"把 {cutoff} 当作'今天';之后发生的一切信息一律当作不存在。")
    else:
        window = f"分析最近 {days_back} 天的新闻。"
    return _structured_scan(
        _TOP_SYS_PROMPT,
        f"检测股票 {ticker} 是否处于周期顶部区。{window}",
        _TOP_SCHEMA,
        {"ticker": ticker, "top_risk_level": "none", "straw_types": [],
         "subject_migration": False, "last_straw": "", "demand_victim": "",
         "confidence": 0.0, "summary": "解析失败", "key_facts": [], "as_of": ""},
        model=model, x_handles=x_handles,
    )


# ── 启动新闻扫描 (docs/news_driven_top_detection.md §6, 见顶扫描的镜像) ────────
# 火种类型字符串来自 constant.py(它们同时是 signal ledger 的 signal_type)。
_LAUNCH_SCHEMA = {
    "type": "json_schema",
    "name": "news_launch_detection",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "ticker": {"type": "string"},
            "launch_level": {
                "type": "string",
                "enum": ["none", "low", "medium", "high", "confirmed_launch"],
                "description": "新闻侧的周期启动信号强度(不含价格确认)"},
            "spark_types": {
                "type": "array",
                "items": {"type": "string", "enum": SPARK_TYPES_ALL},
                "description": ("窗口内实际命中的火种类型。"
                                f"{SPARK_CATALYST_BREAKOUT}=出现本周期级别的催化剂落地"
                                "(如财报大超+指引上修/重磅订单/design win);"
                                f"{SPARK_SUPPLY_DESTRUCTION}=行业性减产/砍资本开支/产能退出/破产;"
                                f"{SPARK_DEMAND_COMMITMENT}=大客户公开签约/长约/生态伙伴公开背书;"
                                f"{SPARK_SHORTAGE_ADMISSION}=下游边际买家承认短缺/接受提价"
                                "(确立需求假设的新闻)。没有就留空,绝不凑数")},
            "subject_birth": {
                "type": "boolean",
                "description": ("新闻主语是否已从行业性阴霾/宏观(过剩/砍单/降价)"
                                "迁移回公司自己(订单/产品/提价/新客户)")},
            "first_spark": {
                "type": "string",
                "description": "若命中 shortage_admission:一句话描述该新闻;否则空字符串"},
            "demand_builder": {
                "type": "string",
                "description": ("按'谁的需求假设正在建立'归因(可以不是新闻主语,"
                                "如下游宣布扩产建立的是上游设备/材料链的需求);无则空字符串")},
            "confidence": {"type": "number", "description": "0.0-1.0"},
            "summary": {"type": "string",
                        "description": "一句话中文状态测量(GPS,不是方向预测)"},
            "key_facts": {"type": "array", "items": {"type": "string"},
                          "description": "3-6 条带日期的事实要点,中文"},
            "as_of": {"type": "string", "description": "信息时间范围说明"},
        },
        "required": ["ticker", "launch_level", "spark_types",
                     "subject_birth", "first_spark", "demand_builder",
                     "confidence", "summary", "key_facts", "as_of"],
    },
}

_LAUNCH_SYS_PROMPT = (
    "你是专注美股周期启动识别的分析师,执行'新闻主语回归'方法论(见顶'主语迁移'的镜像):"
    "底部/启动前新闻主语是行业性阴霾(过剩/砍单/宏观),启动区新闻主语迁移回公司自己"
    "(订单/产品/提价/新客户)。你只做状态测量(GPS),不做方向预测,不建议仓位动作。"
    "四类'火种'新闻各测一个状态,只在检索到确凿对应事实时才标记,绝不凑数;"
    "归因按'谁的需求假设正在建立',不按新闻主语。"
    "launch_level 标准: none=仍是行业阴霾主语的常规新闻; low=个别公司主语新闻但无火种; "
    "medium=命中1类火种或主语明显回归; high=多类火种共振或命中 shortage_admission; "
    "confirmed_launch 仅当火种共振且检索到价格已放量突破的报道。"
    "只依据检索到的事实,信息不足就说明,绝不编造。所有文本用中文。"
)


def launch_scan(ticker: str, model: str = DEFAULT_MODEL, days_back: int = 14,
                cutoff: str | None = None,
                x_handles: list[str] | None = None) -> dict:
    """对单只股票做启动新闻扫描(四类火种+主语回归),返回结构化 dict。

    cutoff 语义与 top_scan 相同(prompt 级近似 point-in-time,回测 best-effort)。
    """
    if cutoff:
        window = (f"只考虑发表于 {cutoff}(含)之前、且距该日 {days_back} 天内的新闻,"
                  f"把 {cutoff} 当作'今天';之后发生的一切信息一律当作不存在。")
    else:
        window = f"分析最近 {days_back} 天的新闻。"
    return _structured_scan(
        _LAUNCH_SYS_PROMPT,
        f"检测股票 {ticker} 是否处于周期启动区。{window}",
        _LAUNCH_SCHEMA,
        {"ticker": ticker, "launch_level": "none", "spark_types": [],
         "subject_birth": False, "first_spark": "", "demand_builder": "",
         "confidence": 0.0, "summary": "解析失败", "key_facts": [], "as_of": ""},
        model=model, x_handles=x_handles,
    )


# ── 无风有涌扫描 (t_us_swell.py: 涨了, 查是不是"没有名字") ─────────────────────
# low_bounce --grok 的镜像: 那边是"跌了查有没有催化剂"(有=真反转), 这边是
# "涨了查是不是没有催化剂"(没有=涌浪, 能量来自远方, 比有新闻的行情更可信)。
_SWELL_SCHEMA = {
    "type": "json_schema",
    "name": "swell_check",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "ticker": {"type": "string"},
            "news_intensity": {
                "type": "string",
                "enum": ["none", "light", "heavy"],
                "description": ("窗口内公司特定新闻的强度: none=检索不到公司"
                                "特定新闻/催化剂(媒体无人问津); light=有零星"
                                "报道/分析师提及但无实质催化剂; heavy=有明确"
                                "催化剂(财报/订单/评级/并购/产品)")},
            "has_catalyst": {"type": "boolean",
                             "description": "是否找到能解释近期涨幅的实质催化剂"},
            "catalysts": {"type": "array", "items": {"type": "string"},
                          "description": "找到的催化剂, 简短中文带日期; 没有留空, 绝不凑数"},
            "summary": {"type": "string",
                        "description": "一句话中文状态测量(GPS, 不是方向预测)"},
            "as_of": {"type": "string", "description": "信息时间范围说明"},
        },
        "required": ["ticker", "news_intensity", "has_catalyst",
                     "catalysts", "summary", "as_of"],
    },
}

_SWELL_SYS_PROMPT = (
    "你是美股新闻检索员。给定一只近期明显跑赢大盘的股票, 你只回答一个问题: "
    "这段涨幅在公开信息里有没有'名字'(能解释它的公司特定新闻/催化剂)。"
    "你只做状态测量, 不做方向预测, 不建议仓位。"
    "宁可报 none 也不要把行业性/大盘性新闻算作该公司的催化剂; "
    "只依据检索到的事实, 绝不编造。所有文本用中文。"
)


def swell_scan(ticker: str, model: str = DEFAULT_MODEL,
               days_back: int = 21) -> dict:
    """查一只强势股近 days_back 天有无公司特定催化剂; 无 = 涌浪候选。"""
    return _structured_scan(
        _SWELL_SYS_PROMPT,
        f"股票 {ticker} 最近 {days_back} 天明显跑赢大盘。检索这段时间内该公司"
        f"的特定新闻与催化剂, 判断这段涨幅是否'有名有姓'。",
        _SWELL_SCHEMA,
        {"ticker": ticker, "news_intensity": "none", "has_catalyst": False,
         "catalysts": [], "summary": "解析失败", "as_of": ""},
        model=model,
    )


# ── 主导叙事扫描 (阵风/季风分类器 t_us_wind_class.py 的叙事端) ─────────────────
# 潮浪风框架: 阵风(单日新闻)只值浪级响应, 季风(持续数季的叙事)才值仓位级。
# 判断"这是第几周的同一场风"需要跨周的叙事身份 — slug 由模型给, 但把历史 slug
# 清单喂回 prompt 强制复用, 匹配问题在生成端解决而不是事后做字符串相似度。
_NARRATIVE_SCHEMA = {
    "type": "json_schema",
    "name": "market_narratives",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "narratives": {
                "type": "array",
                "maxItems": 6,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "slug": {"type": "string",
                                 "description": ("叙事的稳定标识, 小写英文-连字符 "
                                                 "(如 ai-capex-cycle)。若与提供的"
                                                 "历史 slug 是同一场叙事必须复用原 slug")},
                        "title_cn": {"type": "string", "description": "叙事中文短标题"},
                        "direction": {"type": "string",
                                      "enum": ["risk_on", "risk_off", "sector_specific"],
                                      "description": "对美股整体是顺风/逆风/仅板块级"},
                        "sectors": {"type": "array", "items": {"type": "string"},
                                    "description": "受影响板块(英文 GICS 名), 无则空"},
                        "tickers": {"type": "array", "items": {"type": "string"},
                                    "description": "代表性个股, 最多5个, 无则空"},
                        "one_line": {"type": "string",
                                     "description": "一句话中文: 本周该叙事的新进展(带事实)"},
                    },
                    "required": ["slug", "title_cn", "direction",
                                 "sectors", "tickers", "one_line"],
                },
                "description": "本周主导美股的市场叙事, 按影响力降序, 最多6条, 绝不凑数",
            },
            "as_of": {"type": "string", "description": "信息时间范围说明"},
        },
        "required": ["narratives", "as_of"],
    },
}

_NARRATIVE_SYS_PROMPT = (
    "你是美股市场叙事的观察员。你只做状态测量(本周哪些叙事在主导市场), "
    "不做方向预测, 不建议仓位。叙事=一个被反复引用来解释行情的故事"
    "(如 AI资本开支周期/降息路径/关税), 不是单日新闻事件; "
    "只列检索里确有多个来源反复引用的, 绝不凑数。"
    "slug 是叙事的跨周身份: 与提供的历史清单里同一场叙事必须复用原 slug, "
    "确属新叙事才造新 slug。所有文本用中文(slug 除外)。"
)


def narrative_scan(known_slugs: list[tuple[str, str]] | None = None,
                   model: str = DEFAULT_MODEL, days_back: int = 7) -> dict:
    """扫描本周主导叙事(最多6条), 返回结构化 dict。

    known_slugs: [(slug, title_cn), ...] 历史叙事清单, 喂回 prompt 保证
    同一场风跨周用同一个 slug(阵风/季风计数的身份基础)。
    """
    known = ''
    if known_slugs:
        known = ('历史 slug 清单(同一场叙事必须复用): '
                 + '; '.join(f'{s}={t}' for s, t in known_slugs) + '。')
    return _structured_scan(
        _NARRATIVE_SYS_PROMPT,
        f'列出最近 {days_back} 天主导美股的市场叙事(最多6条, 按影响力降序)。{known}',
        _NARRATIVE_SCHEMA,
        {"narratives": [], "as_of": ""},
        model=model,
    )


def ping(model: str = DEFAULT_MODEL) -> str:
    """连通性自检:不触发 search 工具,最省钱。"""
    client = get_client()
    r = client.responses.create(
        model=model, input="reply with exactly: pong",
        max_output_tokens=16)
    return r.output_text


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "ping":
        print("ping ->", ping())
    else:
        tk = sys.argv[1] if len(sys.argv) > 1 else "NVDA"
        out = sentiment_scan(tk)
        print(json.dumps(out, ensure_ascii=False, indent=2))
