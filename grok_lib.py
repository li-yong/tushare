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


def sentiment_scan(ticker: str, model: str = DEFAULT_MODEL,
                   days_back: int = 7,
                   x_handles: list[str] | None = None) -> dict:
    """对单只股票做实时情绪/催化剂扫描,返回结构化 dict。
    含 _meta:模型、引用来源 URL、token/工具用量、估算成本。"""
    client = get_client()
    x_tool: dict[str, Any] = {"type": "x_search"}
    if x_handles:
        x_tool["x_handles"] = x_handles
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": _SYS_PROMPT},
            {"role": "user", "content":
                f"分析股票 {ticker} 最近 {days_back} 天的市场情绪与催化剂。"},
        ],
        tools=[{"type": "web_search"}, x_tool],
        text={"format": _SENTIMENT_SCHEMA},
        temperature=0,
    )
    try:
        data = json.loads(resp.output_text)
    except (json.JSONDecodeError, TypeError, AttributeError):
        data = {"ticker": ticker, "stance": "unclear",
                "summary": "解析失败", "raw": getattr(resp, "output_text", None)}
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
