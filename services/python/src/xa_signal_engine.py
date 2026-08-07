# -*- coding: utf-8 -*-
"""XA 肯泰罗 SignalEngine — AStockPursue 选股策略挂载 (2026-08-07).

架构: XA 的肯泰罗 Plus 引擎 (技术面选股) 封装为 AStockPursue SignalEngine,
serve 启动时 set_strategy() 挂载 → GenerateSignals 返回真实选股权重而非等权.

流程:
  GenerateSignals(bars) → data_map{code: DataFrame} → 每只跑肯泰罗评分
  → 信号 = 肯泰罗分数归一化 (0~100 → 0~1) → weights (取最后bar)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── 挂载 XA 肯泰罗引擎 (容器外 XA 源码可经 ASTOCKPURSUE_XA_SRC 指定) ──
_XA_SRC = os.environ.get("ASTOCKPURSUE_XA_SRC", "")
if _XA_SRC and _XA_SRC not in sys.path:
    sys.path.insert(0, _XA_SRC)

if os.environ.get("ASTOCKPURSUE_XA_SRC"):
    from claw.research.kentaurus import compute_kentaurus, kentaurus_market_score
else:
    # 容器内未挂 XA: 用同目录的轻量实现 (见下方 _compute_kentaurus_local)
    def compute_kentaurus(df, symbol="", name=""):
        return _compute_kentaurus_local(df, symbol, name)

    def kentaurus_market_score(k_result):
        accepted = k_result.get("accepted", False)
        return {"accepted": accepted, "score": k_result.get("score", 0),
                "signals": k_result.get("signals", []), "reason": k_result.get("reason", "")}


def _compute_kentaurus_local(df: pd.DataFrame, symbol: str = "", name: str = "") -> dict:
    """轻量肯泰罗 — 容器内无 XA 时的回退实现 (MA 趋势 + 动量 + 量能).

    注意: 这是简化版; 完整版需挂载 XA 源码 (ASTOCKPURSUE_XA_SRC 指向 D:/XA Qlib Claw/src).
    """
    if df is None or len(df) < 20:
        return {"accepted": False, "score": 0, "signals": [], "reason": "data_insufficient",
                "background": "none", "kline_type": "normal"}
    c = df["close"]
    ma5, ma20 = c.rolling(5).mean(), c.rolling(20).mean()
    vol = df["volume"].rolling(5).mean()
    score = 0
    signals = []
    # 趋势: 价 > MA20 且 MA5 > MA20
    if c.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-1] > ma20.iloc[-1]:
        score += 40
        signals.append("trend_up")
    # 动量: 5日涨幅
    mom5 = (c.iloc[-1] / c.iloc[-5] - 1) if len(c) > 5 else 0
    if mom5 > 0.03:
        score += 30
        signals.append("momentum")
    # 量能: 放量
    if len(vol) > 5 and vol.iloc[-1] > vol.iloc[-6:-1].mean() * 1.2:
        score += 20
        signals.append("volume_expand")
    return {"accepted": score >= 40, "score": min(100, score), "signals": signals,
            "reason": "local_fallback", "background": "red" if score >= 40 else "none",
            "kline_type": "normal"}


class SignalEngine:
    """XA 肯泰罗信号引擎 — 对每只票跑肯泰罗, 信号 = 分数归一化。"""

    def __init__(self, top_n: int = None, min_score: int = None):
        self.top_n = top_n or int(os.environ.get("ASTOCKPURSUE_TOP_N", "10"))
        self.min_score = min_score or int(os.environ.get("ASTOCKPURSUE_MIN_SCORE", "40"))

    def generate(self, data_map: dict) -> dict:
        """data_map: {symbol: DataFrame(open/high/low/close/volume)} → {symbol: Series}。"""
        scores = {}
        for code, df in data_map.items():
            try:
                k = compute_kentaurus(df, symbol=code)
                verdict = kentaurus_market_score(k) if "accepted" not in k else k
                score = int(verdict.get("score", 0))
                accepted = bool(verdict.get("accepted", False))
                if accepted and score >= self.min_score:
                    scores[code] = score
            except Exception as e:
                print(f"  [kentaurus] {code} 失败: {str(e)[:50]}")
                continue

        if not scores:
            return {}
        # 分数 → 0~1 权重 (保持区分度: score/100), 取 TopN
        ranked = sorted(scores.items(), key=lambda kv: -kv[1])[: self.top_n]
        signals = {}
        for code, s in ranked:
            signals[code] = pd.Series([float(s) / 100.0])
        return signals


if __name__ == "__main__":
    # 自测
    n = 60
    idx = pd.date_range("2026-06-01", periods=n, freq="D")
    fake = pd.DataFrame({
        "open": np.linspace(10, 13, n), "high": np.linspace(10.5, 13.5, n),
        "low": np.linspace(9.5, 12.5, n), "close": np.linspace(10, 13, n),
        "volume": np.linspace(1000, 2000, n),
    }, index=idx)
    eng = SignalEngine()
    out = eng.generate({"TEST": fake})
    print("自测输出:", {k: float(v.iloc[-1]) for k, v in out.items()})
