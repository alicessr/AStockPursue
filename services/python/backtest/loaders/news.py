"""Financial news aggregation module.

Now powered by AggregateNewsFetcher — fans out to 10 sources in parallel:
  - East Money (stock news + global 7x24)
  - CLS Telegraph (real-time)
  - CNINFO (A-share announcements)
  - Sina Finance, Xueqiu, Futu, THS (via akshare)
  - GNews (Google News RSS)
  - NewsAPI (optional API key)
  - DuckDuckGo web search (fallback)

Backward-compatible: NewsFetcher is an alias for AggregateNewsFetcher.
All existing call sites continue to work unchanged.

Usage:
    from backtest.loaders.news import NewsFetcher
    fetcher = NewsFetcher()
    news = fetcher.search_news("A股 新能源", max_results=10)
    news = fetcher.fetch_stock_news("000001", max_results=20)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from backtest.loaders.news_sources.aggregate import AggregateNewsFetcher

logger = logging.getLogger(__name__)


class NewsFetcher(AggregateNewsFetcher):
    """Backward-compatible alias for AggregateNewsFetcher.

    Delegates all news fetching to the multi-source aggregate engine.
    The economic calendar is still the template-based fallback.
    """

    # search_news, fetch_market_news, fetch_stock_news, fetch_sector_news
    # are all inherited from AggregateNewsFetcher.

    def get_economic_calendar(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get upcoming economic events.

        Primary: Jin10 (金十) MCP — Streamable HTTP at mcp.jin10.com/mcp
        (Bearer token via env JIN10_MCP_TOKEN; list_calendar returns the
        current natural week, which covers the days<=7 use case).
        Fallback: template-based events (former behaviour).
        """
        events = self._fetch_jin10_calendar()
        if events:
            return events
        today = datetime.now()
        events: List[Dict[str, Any]] = []
        current = today
        days_added = 0
        while days_added < days:
            if current.weekday() < 5:
                daily_events = _get_daily_template(current)
                events.extend(daily_events)
                days_added += 1
            current += timedelta(days=1)
        return events[:days * 3]

    @staticmethod
    def _fetch_jin10_calendar() -> List[Dict[str, Any]]:
        """Call Jin10 MCP list_calendar via Streamable HTTP (JSON-RPC over HTTP).

        Protocol: initialize (capture Mcp-Session-Id) → notifications/initialized
        → tools/call list_calendar → parse SSE 'data:' lines.
        """
        import os
        import json
        import urllib.request

        token = os.environ.get("JIN10_MCP_TOKEN", "").strip()
        if not token:
            return []

        url = "https://mcp.jin10.com/mcp"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {token}",
            "MCP-Protocol-Version": "2025-03-26",
        }
        session_id = None

        def post(payload: dict) -> tuple[Any, dict]:
            nonlocal session_id
            req_headers = dict(headers)
            if session_id:
                req_headers["Mcp-Session-Id"] = session_id
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers=req_headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    resp_headers = dict(resp.headers)
                    if "Mcp-Session-Id" in resp_headers:
                        session_id = resp_headers["Mcp-Session-Id"]
                    return body, resp_headers
            except Exception as exc:
                logger.warning("Jin10 MCP request failed: %s", exc)
                return "", {}

        try:
            # 1) initialize → session id
            body, _ = post({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "astockpursue", "version": "1.0"},
                },
            })
            if not body:
                return []
            # 2) notifications/initialized
            post({"jsonrpc": "2.0", "method": "notifications/initialized"})
            # 3) tools/call list_calendar
            body, _ = post({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "list_calendar", "arguments": {}},
            })
        except Exception as exc:
            logger.warning("Jin10 calendar failed: %s", exc)
            return []

        # SSE response: events carry 'data: {...}' lines
        try:
            data_line = None
            for line in body.splitlines():
                if line.startswith("data:"):
                    data_line = line[5:].strip()
                    break
            if not data_line:
                return []
            result = json.loads(data_line)
            text = result.get("result", {}).get("content", [{}])[0].get("text", "")
            payload = json.loads(text) if isinstance(text, str) else text
            rows = payload.get("data") or payload.get("result", {}).get("data") or []
            events = []
            for row in rows:
                title = str(row.get("title", "")).strip()
                pub = str(row.get("pub_time", "")).strip()
                if not title:
                    continue
                star = int(row.get("star") or 0)
                country = ""
                for kw in ("美国", "中国", "欧元区", "日本", "英国", "德国", "法国", "加拿大", "澳大利亚"):
                    if kw in title:
                        country = kw
                        break
                events.append({
                    "date": pub[:10],
                    "time": pub[11:16],
                    "country": country,
                    "event": title,
                    "event_en": "",
                    "importance": "high" if star >= 4 else ("medium" if star == 3 else "low"),
                    "consensus": row.get("consensus"),
                    "previous": row.get("previous"),
                    "actual": row.get("actual"),
                    "affect": row.get("affect_txt", ""),
                })
            return events
        except Exception as exc:
            logger.warning("Jin10 calendar parse failed: %s", exc)
            return []


def _get_daily_template(date: datetime) -> List[Dict[str, Any]]:
    """Return template economic events for a given date."""
    weekday = date.weekday()
    events: List[Dict[str, Any]] = []
    if weekday == 3:
        events.append({
            "date": date.strftime("%Y-%m-%d"), "time": "20:30", "country": "US",
            "event": "初请失业金人数", "event_en": "Initial Jobless Claims", "importance": "high",
        })
    if weekday == 2:
        events.append({
            "date": date.strftime("%Y-%m-%d"), "time": "22:30", "country": "US",
            "event": "EIA原油库存", "event_en": "EIA Crude Oil Inventories", "importance": "medium",
        })
    if date.day >= 28:
        events.append({
            "date": date.strftime("%Y-%m-%d"), "time": "09:30", "country": "CN",
            "event": "中国官方PMI (预计)", "event_en": "China Official PMI (expected)", "importance": "high",
        })
    if weekday == 4 and date.day <= 7:
        events.append({
            "date": date.strftime("%Y-%m-%d"), "time": "20:30", "country": "US",
            "event": "非农就业数据 (预计)", "event_en": "Non-Farm Payrolls (expected)", "importance": "high",
        })
    if weekday == 2 and 15 <= date.day <= 22:
        events.append({
            "date": date.strftime("%Y-%m-%d"), "time": "02:00", "country": "US",
            "event": "FOMC会议纪要 (预计窗口)", "event_en": "FOMC Minutes (expected window)", "importance": "high",
        })
    return events


def _deduplicate(results: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Remove duplicate news by URL."""
    seen = set()
    unique = []
    for r in results:
        url = r.get("url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(r)
    return unique
