# -*- coding: utf-8 -*-
"""XA 肯泰罗+缠论+闸门 SignalEngine — AStockPursue 选股策略挂载 (2026-08-07 v2).

架构: XA 选股逻辑 (肯泰罗入场过滤 + 缠论中枢 + 三层闸门) 封装为 AStockPursue
SignalEngine, serve 启动时 set_strategy() 挂载 → GenerateSignals 返回真选股权重.

三层闸门 (对应 watcher precheck 的入场判定):
  1. 肯泰罗入场过滤 — kentaurus_entry_filter: 红背景+事件触发, 否则拒
  2. 缠论中枢 — run_chan_analysis(source=duckdb): 有中枢数据才算有效候选
  3. 权重归一 — 通过前两层的按分数排序取 TopN

数据源: XA tdx2db (tdx.db, 容器挂载 /data/tdx/tdx.db).
XA 源码路径: ASTOCKPURSUE_XA_SRC (容器 compose 已挂 D:/XA Qlib Claw/src).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── XA 源码挂载 (由 compose 环境变量注入) ──
_XA_SRC = os.environ.get("ASTOCKPURSUE_XA_SRC", "")
if _XA_SRC and _XA_SRC not in sys.path:
    sys.path.insert(0, _XA_SRC)

_HAVE_XA = bool(_XA_SRC)

if _HAVE_XA:
    from claw.research.kentaurus import compute_kentaurus, kentaurus_entry_filter, kentaurus_market_score
    try:
        from claw.research.chan_bridge import run_chan_analysis
        _CHAN_OK = True
    except Exception as _ce:
        print(f"  [chan] 缠论库不可用, 降级跳过: {str(_ce)[:50]}")
        _CHAN_OK = False
else:
    # 无 XA 源码时回退轻量实现 (仅保证服务可用, 非生产)
    def compute_kentaurus(df, symbol="", name=""):
        return _compute_local(df, symbol)

    def kentaurus_entry_filter(k_result):
        return (k_result.get("accepted", False), 0.5, "local_fallback")

    def run_chan_analysis(*a, **k):
        return {"zhongshu": None, "available": False}
    _CHAN_OK = False


def _compute_local(df: pd.DataFrame, symbol: str = "") -> dict:
    """轻量回退: MA 趋势 + 动量 + 量能 (无 XA 源码时仅保底)."""
    if df is None or len(df) < 20:
        return {"accepted": False, "score": 0, "signals": {}, "reason": "data_insufficient",
                "background": "green", "kline_type": "normal"}
    c = df["close"]
    ma5, ma20 = c.rolling(5).mean(), c.rolling(20).mean()
    vol = df["volume"].rolling(5).mean()
    score = 0
    signals = {}
    if c.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-1] > ma20.iloc[-1]:
        score += 40
        signals["trend_up"] = True
    mom5 = (c.iloc[-1] / c.iloc[-5] - 1) if len(c) > 5 else 0
    if mom5 > 0.03:
        score += 30
        signals["momentum"] = True
    if len(vol) > 5 and vol.iloc[-1] > vol.iloc[-6:-1].mean() * 1.2:
        score += 20
        signals["volume_expand"] = True
    return {"accepted": score >= 40, "score": min(100, score), "signals": signals,
            "background": "red" if score >= 40 else "green", "kline_type": "normal",
            "kdj_j": 50.0, "volume_ratio": 1.0}


class SignalEngine:
    """XA 三层闸门信号引擎 — 肯泰罗过滤 + 缠论中枢 + 权重排序。"""

    def __init__(self, top_n: int = None, min_score: int = None):
        self.top_n = top_n or int(os.environ.get("ASTOCKPURSUE_TOP_N", "10"))
        self.min_score = min_score or int(os.environ.get("ASTOCKPURSUE_MIN_SCORE", "40"))
        self._chan_cache = {}

    def _chan_ok(self, code: str) -> bool:
        """缠论闸门: 中枢数据可得才算有效候选 (duckdb 源). 库不可用/异常放行(保守)."""
        if not _CHAN_OK:
            return True  # 缠论库不可用 → 不误杀
        if code in self._chan_cache:
            return self._chan_cache[code]
        ok = True
        try:
            sym = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
            r = run_chan_analysis(sym, source="duckdb", timeframes=["D1"], use_cache=True)
            ok = bool(r and r.get("zhongshu"))
        except Exception:
            ok = True  # 数据缺失不误杀
        self._chan_cache[code] = ok
        return ok

    def generate(self, data_map: dict) -> dict:
        """data_map: {symbol: DataFrame} → {symbol: Series}。

        三层闸门:
          1. compute_kentaurus 计算 → kentaurus_entry_filter 判定允许建仓
          2. 缠论中枢 (D1) 数据可得
          3. 分数排序取 TopN, 权重 = position_factor * score/100
        """
        results = []
        for code, df in data_map.items():
            try:
                if df is None or len(df) < 40:
                    continue  # 数据不足跳过 (防 indexer out-of-bounds)
                k = compute_kentaurus(df, symbol=code)
                allow, factor, reason = kentaurus_entry_filter(k)
                if not allow:
                    continue
                # 分数: kentaurus_market_score 严格评分 (无则用 60 保底)
                try:
                    ms = kentaurus_market_score(k)
                    score = int(ms.get("score", 60))
                except Exception:
                    score = 60
                if score < self.min_score:
                    continue
                if not self._chan_ok(code):
                    continue
                # 权重 = 仓位系数 × 分数归一
                w = round(float(factor) * (score / 100.0), 4)
                results.append((code, w, score, reason))
            except Exception as e:
                print(f"  [gate] {code} 失败: {str(e)[:60]}")
                continue

        if not results:
            return {}
        ranked = sorted(results, key=lambda r: -r[1])[: self.top_n]
        signals = {}
        for code, w, score, reason in ranked:
            signals[code] = pd.Series([w])
        return signals


if __name__ == "__main__":
    n = 60
    idx = pd.date_range("2026-06-01", periods=n, freq="D")
    fake = pd.DataFrame({
        "open": np.linspace(10, 13, n), "high": np.linspace(10.5, 13.5, n),
        "low": np.linspace(9.5, 12.5, n), "close": np.linspace(10, 13, n),
        "volume": np.linspace(1000, 2000, n),
    }, index=idx)
    eng = SignalEngine()
    out = eng.generate({"TEST": fake})
    print("自测:", {k: float(v.iloc[-1]) for k, v in out.items()})
