#!/usr/bin/env python
"""
必盈API黄金版 数据桥接层 v1.1
用量: 1分钟300次 · 沪深A股/指数/基金/京市/科创板全覆盖

已确认可用的端点 (gold tier):
  hsrl/ssjy              — 基础实时行情
  hsstock/real/time      — 实时行情+PE/PB
  hsstock/real/five      — 五档买卖盘口 (价+量)
  hsstock/history/transaction — 逐笔成交统计 (日级)
  hsstock/indicators     — 技术指标全量 (28438条/股)
  hsstock/instrument     — 股票基本信息
  hscp/gsjj              — 公司简介
  hscp/jdlr              — 季度利润
  hscp/yjyg              — 业绩预告 (58条历史)
  hscp/jdxj              — 季度现金流
  hscp/cwzb              — 财务指标 (4季度)
  hscp/sdgd              — 十大股东
  hscp/sdgd              — 十大股东
  hscp/ltgd              — 流通股东
  hscp/jjcg              — 基金持股
  hsindex/list           — 指数列表
  hslt/list           — 股票列表 (5204条)
  hslt/new/primarylist/sectorslist — 分类列表

工程加固 (2026-07-22 biying-hardening, 不改数据语义):
  P0 实时行情 30s TTL 手工缓存（替换 lru_cache 僵尸缓存）
  P1 全局限流器（黄金版 300 次/分滑动窗口）+ 429/403 指数退避 + 日配额文件
  P2 按官方更新时间表的磁盘缓存（TTL 表见 _DISK_CACHE_TTL）

集成到 data_broker.py 路由表:
  P1.1 必盈API — 股东结构/基金持股/财务指标/五档盘口/逐笔统计
  P1.2 mootdx   — K线/盘口/分钟数据/财务F10 (主力)
  P2   腾讯API   — 批量报价 (快速快照)
"""

import os
import json
import time
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque
from typing import Optional, Dict, List, Any

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.biyingapi.com"

# ── AStockPursue 移植适配（原 XA 版本依赖 claw.config）──
# Key 来源优先级: 环境变量 BIYING_API_KEY → XA biyingapi.json → 本地 config/secrets
_XA_SECRETS = Path(os.environ.get("XA_PROJECT_ROOT", r"D:/XA Qlib Claw")) / "config" / "secrets"
CONFIG_DIR = Path(os.environ.get("BIYING_CONFIG_DIR", str(_XA_SECRETS)))
CONFIG_FILE = CONFIG_DIR / "biyingapi.json"

# 缓存/配额根: 环境变量 BIYING_CACHE_ROOT → 用户目录 .astockpursue/cache/biying
CACHE_ROOT = Path(os.environ.get(
    "BIYING_CACHE_ROOT",
    str(Path.home() / ".astockpursue" / "cache" / "biying"),
))

# ============================================================
#  工程加固常量（2026-07-22）
# ============================================================

# licence 档位：黄金版 300 次/分（账户级令牌桶，滑动窗口实现）
RATE_LIMIT_PER_MIN = 300

_rate_lock = threading.RLock()

# 缓存/配额同根：state_dir/cache/biying/（目录约定照 dead-code sweep 刀4b；
# 配额文件落根下 quota_YYYYMMDD.json，对齐 iFinD 的 CACHE_DIR/quota_*.json 位置）

# P2 磁盘缓存 TTL 表（键=url_path 前缀，最长前缀命中；依据《必盈数据API文档》更新节奏）
_DISK_CACHE_TTL = {
    # 财报/股东/公司详情类（hscp/*、financial/*，每日 03:30 更新）→ 20h
    "hscp/": 20 * 3600,
    "hsstock/financial/": 20 * 3600,
    "bj/financial/": 20 * 3600,
    # 板块树/板块成分（hszg/*，拆分TTL）
    #   hszg/list 每日16:20更新 → 8h
    #   hszg/gg / hszg/zg 每周六更新 → 24h
    "hszg/list": 8 * 3600,
    "hszg/gg": 24 * 3600,
    "hszg/zg": 24 * 3600,
    # 股票列表/新股/概念板块列表（每日 16:20 更新）→ 8h
    "hslt/list": 8 * 3600,
    "hslt/new": 8 * 3600,
    "hslt/sectorslist": 8 * 3600,
    "hslt/primarylist": 8 * 3600,
    "hsindex/list": 8 * 3600,
    "fd/list": 8 * 3600,
    "bj/list": 8 * 3600,
    "kc/list": 8 * 3600,
    # 涨跌停价（每日 0 点更新）/ 行情指标 indicators（每日 20:00 完）→ 8h
    "hsstock/stopprice/": 8 * 3600,
    "hsstock/indicators": 8 * 3600,
    # 资金流向 transactions（每日 21:30 更新）→ 4h
    "hsstock/history/transaction": 4 * 3600,
    # 实时/盘口/逐笔/K线/股池类不在表内 → 不缓存（保持现拉）
}

_MISS = object()  # 磁盘缓存未命中哨兵（payload 本身可能为 None）

# ============================================================
#  API Key Management
# ============================================================

def _load_key() -> str:
    """Load API key from config file, fallback to env var."""
    if CONFIG_FILE.exists():
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return cfg.get("licence", "")
    return os.environ.get("BIYINGAPI_KEY", "")


def save_key(licence: str):
    """Save API key to config."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps({
        "licence": licence,
        "tier": "gold",
        "updated": datetime.now().isoformat()
    }, indent=2, ensure_ascii=False), encoding="utf-8")


def get_licence() -> str:
    """Lazy-load API key (loaded at first use, not import time)."""
    return _load_key()


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})


# ============================================================
#  Endpoint Registry
# ============================================================

ENDPOINTS = {
    # ── 沪深股票列表 ──
    "stock_list":     "hslt/list/{KEY}",
    "new_stocks":     "hslt/new/{KEY}",
    "sector_list":    "hslt/sectorslist/{KEY}",
    "primary_list":   "hslt/primarylist/{KEY}",
    "sector_detail":  "hslt/sectors/{code}/{KEY}",

    # ── 涨跌股池 (date=YYYY-MM-DD) ──
    "zt_pool":        "hslt/ztgc/{date}/{KEY}",
    "dt_pool":        "hslt/dtgc/{date}/{KEY}",
    "strong_pool":    "hslt/qsgc/{date}/{KEY}",
    "newhigh_pool":   "hslt/cxgc/{date}/{KEY}",
    "limitup_break":  "hslt/zbgc/{date}/{KEY}",

    # ── 指数/行业/概念 ──
    "sector_tree":    "hszg/list/{KEY}",
    "sector_stocks":  "hszg/gg/{code}/{KEY}",
    "stock_sectors":  "hszg/zg/{code}/{KEY}",

    # ── 实时行情 ──
    "realtime":       "hsrl/ssjy/{code}/{KEY}",
    "realtime_multi": "hsrl/ssjy_more/{KEY}",
    "zbjy":           "hsrl/zbjy/{code}/{KEY}",

    # ── hsstock (code=000001.SZ格式) ──
    "real":           "hsstock/real/time/{code}/{KEY}",
    "five_level":     "hsstock/real/five/{code}/{KEY}",
    "latest":         "hsstock/latest/{code}/{tf}/{adj}/{KEY}",
    "history":        "hsstock/history/{code}/{tf}/{adj}/{KEY}",
    "history_ma":     "hsstock/history/ma/{code}/{tf}/{adj}/{KEY}",
    "history_macd":   "hsstock/history/macd/{code}/{tf}/{adj}/{KEY}",
    "history_kdj":    "hsstock/history/kdj/{code}/{tf}/{adj}/{KEY}",
    "history_boll":   "hsstock/history/boll/{code}/{tf}/{adj}/{KEY}",
    "stopprice":      "hsstock/stopprice/history/{code}/{KEY}",
    "indicators":     "hsstock/indicators/{code}/{KEY}",
    "instrument":     "hsstock/instrument/{code}/{KEY}",
    "transactions":   "hsstock/history/transaction/{code}/{KEY}",

    # ── 财务报表 (code=000001.SZ格式) ──
    "fin_balance":         "hsstock/financial/balance/{code}/{KEY}",
    "fin_income":          "hsstock/financial/income/{code}/{KEY}",
    "fin_cashflow":        "hsstock/financial/cashflow/{code}/{KEY}",
    "fin_pershare":        "hsstock/financial/pershareindex/{code}/{KEY}",
    "fin_capital":         "hsstock/financial/capital/{code}/{KEY}",
    "fin_topholder":       "hsstock/financial/topholder/{code}/{KEY}",
    "fin_flowholder":      "hsstock/financial/flowholder/{code}/{KEY}",
    "fin_holders_count":   "hsstock/financial/hm/{code}/{KEY}",

    # ── hscp 上市公司详情 ──
    "company_info":   "hscp/gsjj/{code}/{KEY}",
    "listing_trend":  "hscp/sszs/{code}/{KEY}",
    "executives":     "hscp/ljgg/{code}/{KEY}",
    "board_members":  "hscp/ljds/{code}/{KEY}",
    "supervisors":    "hscp/ljjj/{code}/{KEY}",
    "dividend_hist":  "hscp/jnfh/{code}/{KEY}",
    "rights_issue":   "hscp/jnzf/{code}/{KEY}",
    "share_unlock":   "hscp/jjxs/{code}/{KEY}",
    "quarter_profit": "hscp/jdlr/{code}/{KEY}",
    "quarter_cash":   "hscp/jdxj/{code}/{KEY}",
    "forecast":       "hscp/yjyg/{code}/{KEY}",
    "cwzb":           "hscp/cwzb/{code}/{KEY}",
    "top_holders":    "hscp/sdgd/{code}/{KEY}",
    "float_holders":  "hscp/ltgd/{code}/{KEY}",
    "holder_trend":   "hscp/gdbh/{code}/{KEY}",
    "fund_holders":   "hscp/jjcg/{code}/{KEY}",

    # ── 指数 (code=000001.SH格式) ──
    "index_list":     "hsindex/list/{KEY}",
    "index_real":     "hsindex/real/time/{code}/{KEY}",
    "index_latest":   "hsindex/latest/{code}/{tf}/{KEY}",
    "index_history":  "hsindex/history/{code}/{tf}/{KEY}",
    "index_ma":       "hsindex/history/ma/{code}/{tf}/{KEY}",
    "index_macd":     "hsindex/history/macd/{code}/{tf}/{KEY}",
    "index_kdj":      "hsindex/history/kdj/{code}/{tf}/{KEY}",
    "index_boll":     "hsindex/history/boll/{code}/{tf}/{KEY}",

    # ── 基金 ──
    "fund_list":      "fd/list/all/{KEY}",
    "fund_etf_list":  "fd/list/etf/{KEY}",
    "fund_real":      "fd/real/time/{code}/{KEY}",

    # ── 北京交易所 (code=920547.BJ格式) ──
    "bj_stock_list":     "bj/list/all/{KEY}",
    "bj_index_list":     "bj/list/index/{KEY}",
    "bj_realtime":       "bj/stock/real/time/{code}/{KEY}",
    "bj_five_level":     "bj/stock/real/five/{code}/{KEY}",
    "bj_index_real":     "bj/index/real/time/{code}/{KEY}",
    "bj_history":        "bj/history/{code}/{tf}/{adj}/{KEY}",
    "bj_fin_balance":    "bj/financial/balance/{code}/{KEY}",
    "bj_fin_income":     "bj/financial/income/{code}/{KEY}",
    "bj_fin_cashflow":   "bj/financial/cashflow/{code}/{KEY}",
    "bj_fin_pershare":   "bj/financial/pershareindex/{code}/{KEY}",
    "bj_fin_capital":    "bj/financial/capital/{code}/{KEY}",
    "bj_fin_topholder":  "bj/financial/topholder/{code}/{KEY}",
    "bj_fin_flowholder": "bj/financial/flowholder/{code}/{KEY}",
    "bj_fin_hm":         "bj/financial/hm/{code}/{KEY}",

    # ── 科创板 (code=688001.KC格式, gold待验证) ──
    "kc_stock_list":  "kc/list/all/{KEY}",
    "kc_realtime":    "kc/real/time/{code}/{KEY}",
    "kc_five_level":  "kc/real/five/{code}/{KEY}",
}


# ============================================================
#  P1 配额计数（对齐 iFinD：quota_YYYYMMDD.json，{"count": N}）
# ============================================================

def _today() -> str:
    return datetime.now().strftime("%Y%m%d")


def _quota_file() -> Path:
    return CACHE_ROOT / f"quota_{_today()}.json"


def quota_used() -> int:
    """今日已发起网络请求数（读配额文件）。"""
    f = _quota_file()
    if not f.exists():
        return 0
    try:
        return int(json.loads(f.read_text(encoding="utf-8")).get("count", 0))
    except Exception:
        return 0


def _quota_inc(n: int = 1) -> None:
    with _rate_lock:
        try:
            CACHE_ROOT.mkdir(parents=True, exist_ok=True)
            _quota_file().write_text(
                json.dumps({"count": quota_used() + n}, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass  # 配额计数失败静默跳过，不阻断主链


def quota_remaining() -> int:
    """当前分钟窗口剩余额度（黄金版 300 次/分滑动窗口）。"""
    with _rate_lock:
        now = time.monotonic()
        in_window = sum(1 for t in _rate_window if now - t < 60.0)
        return max(0, RATE_LIMIT_PER_MIN - in_window)


# ============================================================
#  P1 全局限流器（滑动窗口 300 次/分，必要时 sleep 等待）
# ============================================================

_rate_window: deque = deque()  # 近 60s 网络请求时间戳（monotonic）


def _throttle() -> None:
    """每次真实网络请求前调用：修剪窗口 → 超限速则等待 → 计数+配额落盘。"""
    with _rate_lock:
        now = time.monotonic()
        while _rate_window and now - _rate_window[0] >= 60.0:
            _rate_window.popleft()
        if len(_rate_window) >= RATE_LIMIT_PER_MIN:
            wait = 60.0 - (now - _rate_window[0])
            if wait > 0:
                logger.info(f"必盈API 限流等待 {wait:.1f}s（{RATE_LIMIT_PER_MIN} 次/分）")
                time.sleep(wait)
                now = time.monotonic()
                while _rate_window and now - _rate_window[0] >= 60.0:
                    _rate_window.popleft()
        _rate_window.append(time.monotonic())
    _quota_inc(1)


# ============================================================
#  P2 磁盘缓存（按更新时间表的 TTL；写失败静默跳过）
# ============================================================

def _endpoint_ttl(url_path: str) -> int:
    """查 TTL 表（最长前缀命中），表外端点返回 0 = 不缓存。"""
    best_ttl, best_len = 0, 0
    for prefix, ttl in _DISK_CACHE_TTL.items():
        if url_path.startswith(prefix) and len(prefix) > best_len:
            best_ttl, best_len = ttl, len(prefix)
    return best_ttl


def _disk_cache_path(url: str, url_path: str) -> Path:
    """缓存键 = md5(url)（url 含 licence，天然按密钥隔离）；按端点类（首段目录）分组。"""
    group = url_path.split("/")[0]
    return CACHE_ROOT / group / f"{hashlib.md5(url.encode('utf-8')).hexdigest()}.json"


def _disk_cache_read(url: str, url_path: str, ttl: int) -> Any:
    try:
        p = _disk_cache_path(url, url_path)
        if not p.exists():
            return _MISS
        ent = json.loads(p.read_text(encoding="utf-8"))
        if time.time() - float(ent.get("ts", 0)) > ttl:
            return _MISS
        return ent.get("payload")
    except Exception:
        return _MISS


def _disk_cache_write(url: str, url_path: str, payload: Any) -> None:
    try:
        p = _disk_cache_path(url, url_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"ts": time.time(), "payload": payload}, ensure_ascii=False),
                     encoding="utf-8")
    except Exception:
        pass  # 写缓存失败静默跳过，不阻断主链


# ============================================================
#  Core Client
# ============================================================

class BiyingClient:
    """必盈API 黄金版客户端（1分钟300次限流）"""

    def __init__(self, licence: str = ""):
        self.licence = licence or get_licence()
        if not self.licence:
            raise ValueError("必盈API密钥未配置。请设置 BIYINGAPI_KEY 环境变量或调用 save_key()")

    def _get(self, endpoint: str, **kwargs) -> Any:
        """通用 GET 请求，自动替换模板变量；磁盘缓存命中零网络，否则限流计数后发起。"""
        params = {k: v for k, v in kwargs.items() if not k.startswith('_')}
        params['KEY'] = self.licence
        url_path = endpoint.format(**params) if params else endpoint.format(KEY=self.licence)
        url = f"{BASE_URL}/{url_path}"
        # Append query params
        query = kwargs.get('_query', '')
        if query:
            url += query
        # P2 磁盘缓存：TTL 表内端点命中则直接返回（零网络、不计限流/配额）
        ttl = _endpoint_ttl(url_path)
        if ttl > 0:
            cached = _disk_cache_read(url, url_path, ttl)
            if cached is not _MISS:
                return cached
        # P1 限流 + 429 退避重试（最多 2 次：0.5s / 2s）; 403 立即失败不重试
        for attempt in range(3):
            _throttle()
            try:
                r = SESSION.get(url, timeout=15)
            except requests.Timeout:
                logger.warning(f"必盈API timeout: {endpoint}")
                return None
            except Exception as e:
                logger.warning(f"必盈API error: {e}")
                return None
            if r.status_code == 200:
                try:
                    payload = r.json()
                except Exception as e:
                    logger.warning(f"必盈API error: {e}")
                    return None
                if ttl > 0:
                    _disk_cache_write(url, url_path, payload)
                return payload
            if r.status_code == 403:
                logger.warning(f"必盈API 403: {r.text[:80]}")
                return None
            if r.status_code == 429 and attempt < 2:
                time.sleep(0.5 if attempt == 0 else 2.0)
                continue
            logger.warning(f"必盈API {r.status_code}: {r.text[:60]}")
            return None

    # ── 实时行情 ──────────────

    def realtime(self, code: str) -> Optional[Dict]:
        """单股实时行情 (hsstock/real/time — 含PE/PB/pb_ratio/总市值/流通市值/换手率)"""
        return self._get(ENDPOINTS["real"], code=self._clean(code))

    def realtime_basic(self, code: str) -> Optional[Dict]:
        """基础实时行情 (hsrl/ssjy — 轻量版)"""
        return self._get(ENDPOINTS["realtime"], code=self._clean(code))

    def five_level(self, code: str) -> Optional[Dict]:
        """五档买卖盘口: ps=[卖价], pb=[买价], vs=[卖量], vb=[买量]"""
        return self._get(ENDPOINTS["five_level"], code=self._clean(code))

    def transactions(self, code: str) -> Optional[List[Dict]]:
        """资金流向数据（逐笔日统计）"""
        return self._get(ENDPOINTS["transactions"], code=self._clean(code))

    def batch_realtime(self, codes: List[str]) -> Dict[str, Optional[Dict]]:
        """批量实时行情（逐个请求，注意限流）"""
        return {code: self.realtime(code) for code in codes}

    def realtime_multi(self, codes: List[str]) -> Optional[List[Dict]]:
        """多股实时行情 (1次HTTP拉≤20只)"""
        qs = "?stock_codes=" + ",".join(codes[:20])
        return self._get(ENDPOINTS["realtime_multi"], _query=qs)

    def zbjy(self, code: str) -> Optional[List[Dict]]:
        """当天逐笔交易"""
        return self._get(ENDPOINTS["zbjy"], code=self._clean(code))

    # ── 分时K线 (code=000001.SZ, tf=d/w/m, adj=n/f/b/fr/br) ──

    def kline(self, code: str, tf: str = "d", adj: str = "f",
              st: str = "", et: str = "", lt: int = 0) -> Optional[List[Dict]]:
        """历史分时K线。adj 复权方式透传参数：n=不复权 f=前复权 b=后复权
        fr=等比前复权 br=等比后复权（fr/br 实际可用，直接透传）"""
        q = self._kline_query(tf, adj, st, et, lt)
        return self._get(ENDPOINTS["history"], code=self._sfx(code), tf=tf, adj=adj, _query=q)

    def kline_ma(self, code: str, tf: str = "d", adj: str = "f",
                 st: str = "", et: str = "", lt: int = 0) -> Optional[List[Dict]]:
        q = self._kline_query(tf, adj, st, et, lt)
        return self._get(ENDPOINTS["history_ma"], code=self._sfx(code), tf=tf, adj=adj, _query=q)

    def kline_macd(self, code: str, tf: str = "d", adj: str = "f",
                   st: str = "", et: str = "", lt: int = 0) -> Optional[List[Dict]]:
        q = self._kline_query(tf, adj, st, et, lt)
        return self._get(ENDPOINTS["history_macd"], code=self._sfx(code), tf=tf, adj=adj, _query=q)

    def kline_kdj(self, code: str, tf: str = "d", adj: str = "f",
                  st: str = "", et: str = "", lt: int = 0) -> Optional[List[Dict]]:
        q = self._kline_query(tf, adj, st, et, lt)
        return self._get(ENDPOINTS["history_kdj"], code=self._sfx(code), tf=tf, adj=adj, _query=q)

    def kline_boll(self, code: str, tf: str = "d", adj: str = "f",
                   st: str = "", et: str = "", lt: int = 0) -> Optional[List[Dict]]:
        q = self._kline_query(tf, adj, st, et, lt)
        return self._get(ENDPOINTS["history_boll"], code=self._sfx(code), tf=tf, adj=adj, _query=q)

    def latest_kline(self, code: str, tf: str = "d", adj: str = "n",
                     lt: int = 10) -> Optional[List[Dict]]:
        """最新分时K线"""
        return self._get(ENDPOINTS["latest"], code=self._sfx(code), tf=tf, adj=adj,
                         _query=f"?lt={lt}")

    def stopprice_history(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        """历史涨跌停价格"""
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["stopprice"], code=self._sfx(code), _query=q)

    def latest(self, code: str) -> Optional[List[Dict]]:
        """最新分时 (简写)"""
        # This redirects to latest_kline
        return self.latest_kline(code)

    def history(self, code: str, tf: str = "d", adj: str = "f") -> Optional[List[Dict]]:
        """历史分时 (简写)"""
        return self.kline(code, tf, adj)

    # ── 财务/股东 ──────────────

    def financials(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["cwzb"], code=self._clean(code))

    def top_holders(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["top_holders"], code=self._clean(code))

    def float_holders(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["float_holders"], code=self._clean(code))

    def fund_holders(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["fund_holders"], code=self._clean(code))

    def holders_full(self, code: str) -> Dict[str, Any]:
        return {
            "code": code,
            "top10": self.top_holders(code),
            "float": self.float_holders(code),
            "funds": self.fund_holders(code),
            "fetched_at": datetime.now().isoformat(),
        }

    # ── 三表 (code=000001.SZ, st/et=YYYYMMDD) ──

    def balance_sheet(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["fin_balance"], code=self._sfx(code), _query=q)

    def income_statement(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["fin_income"], code=self._sfx(code), _query=q)

    def cashflow_statement(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["fin_cashflow"], code=self._sfx(code), _query=q)

    def pershare_index(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["fin_pershare"], code=self._sfx(code), _query=q)

    def capital_structure(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["fin_capital"], code=self._sfx(code), _query=q)

    def holders_count(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["fin_holders_count"], code=self._sfx(code), _query=q)

    # ── 技术/基本 ──────────────

    def indicators(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["indicators"], code=self._sfx(code))

    def instrument(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["instrument"], code=self._sfx(code))

    def company_info(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["company_info"], code=self._clean(code))

    # ── 列表 ──────────────

    def stock_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["stock_list"])

    def sector_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["sector_list"])

    def index_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["index_list"])

    def new_stocks(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["new_stocks"])

    # ── 涨跌股池 ──
    # 2026-08-01: 池类端点默认日期回退到最近交易日（周末/节假日无数据，服务端返回404）
    @staticmethod
    def _pool_date(date_str: str) -> str:
        if date_str:
            return date_str
        # AStockPursue port: weekend rollback (XA used trading_calendar.is_trading_day;
        # holiday edge cases fall back to 404-tolerant pool callers)
        d = datetime.now()
        while d.weekday() >= 5:  # Sat=5, Sun=6
            d = d - timedelta(days=1)
        return d.strftime("%Y-%m-%d")

    def zt_pool(self, date_str: str = "") -> Optional[List[Dict]]:
        d = self._pool_date(date_str)
        return self._get(ENDPOINTS["zt_pool"], date=d)

    def dt_pool(self, date_str: str = "") -> Optional[List[Dict]]:
        d = self._pool_date(date_str)
        return self._get(ENDPOINTS["dt_pool"], date=d)

    def strong_pool(self, date_str: str = "") -> Optional[List[Dict]]:
        d = self._pool_date(date_str)
        return self._get(ENDPOINTS["strong_pool"], date=d)

    def newhigh_pool(self, date_str: str = "") -> Optional[List[Dict]]:
        d = self._pool_date(date_str)
        return self._get(ENDPOINTS["newhigh_pool"], date=d)

    def limitup_break(self, date_str: str = "") -> Optional[List[Dict]]:
        d = self._pool_date(date_str)
        return self._get(ENDPOINTS["limitup_break"], date=d)

    # ── 指数 ──

    def index_realtime(self, code: str = "000001.SH") -> Optional[Dict]:
        return self._get(ENDPOINTS["index_real"], code=code)

    def index_kline(self, code: str = "000001.SH", tf: str = "d",
                    st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["index_history"], code=code, tf=tf, _query=q)

    # ── hscp 公司详情 ──

    def quarter_profit(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["quarter_profit"], code=self._clean(code))

    def forecast(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["forecast"], code=self._clean(code))

    def board_members(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["board_members"], code=self._clean(code))

    def share_unlock(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["share_unlock"], code=self._clean(code))

    def dividend_history(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["dividend_hist"], code=self._clean(code))

    def rights_issue(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["rights_issue"], code=self._clean(code))

    def listing_trend(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["listing_trend"], code=self._clean(code))

    def executives(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["executives"], code=self._clean(code))

    def supervisors(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["supervisors"], code=self._clean(code))

    def holder_trend(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["holder_trend"], code=self._clean(code))

    def quarter_cash(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["quarter_cash"], code=self._clean(code))

    # ── hszg ──

    def sector_tree(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["sector_tree"])

    def sector_stocks(self, sector_code: str) -> Optional[List[Dict]]:
        """根据指数/行业/概念代码找相关股票。
        
        sector_code 支持两种格式:
          1. sector_tree 代码 (sw/gn/chgn开头) — 直接调用
          2. 中文名称 (如"证券") — 自动从 sector_tree 查找对应代码
        """
        # 尝试直接调用
        r = self._get(ENDPOINTS["sector_stocks"], code=sector_code)
        if r:
            return r
        # sector_list 代码(如 101476.BKZS) hszg/gg 不认 → 查 sector_tree 映射
        tree = self.sector_tree()
        if tree:
            for t in tree:
                if sector_code in t.get("code","") or sector_code in t.get("name",""):
                    return self._get(ENDPOINTS["sector_stocks"], code=t["code"])
        return None

    def stock_sectors(self, code: str) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["stock_sectors"], code=self._clean(code))

    # ── 基金 ──

    def fund_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["fund_list"])

    def fund_etf_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["fund_etf_list"])

    def fund_realtime(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["fund_real"], code=self._clean(code))

    # ── 北京交易所 ──

    def bj_stock_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["bj_stock_list"])

    def bj_index_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["bj_index_list"])

    def bj_realtime(self, code: str) -> Optional[Dict]:
        # 北交所 API 需要纯数字代码 (不加 .BJ 后缀)
        return self._get(ENDPOINTS["bj_realtime"], code=self._clean(code))

    def bj_five_level(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["bj_five_level"], code=self._sfx(code))

    def bj_index_real(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["bj_index_real"], code=code)

    def bj_kline(self, code: str, tf: str = "d", adj: str = "n",
                 st: str = "", et: str = "", lt: int = 0) -> Optional[List[Dict]]:
        q = self._kline_query(tf, adj, st, et, lt)
        return self._get(ENDPOINTS["bj_history"], code=self._sfx(code), tf=tf, adj=adj, _query=q)

    def bj_balance(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["bj_fin_balance"], code=self._sfx(code), _query=q)

    def bj_income(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["bj_fin_income"], code=self._sfx(code), _query=q)

    def bj_cashflow(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["bj_fin_cashflow"], code=self._sfx(code), _query=q)

    def bj_pershare(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["bj_fin_pershare"], code=self._sfx(code), _query=q)

    def bj_capital(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["bj_fin_capital"], code=self._sfx(code), _query=q)

    def bj_topholder(self, code: str, st: str = "", et: str = "") -> Optional[List[Dict]]:
        q = f"?st={st}&et={et}" if st else ""
        return self._get(ENDPOINTS["bj_fin_topholder"], code=self._sfx(code), _query=q)

    # ── 科创板 ──

    def kc_stock_list(self) -> Optional[List[Dict]]:
        return self._get(ENDPOINTS["kc_stock_list"])

    def kc_realtime(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["kc_realtime"], code=self._sfx(code))

    def kc_five_level(self, code: str) -> Optional[Dict]:
        return self._get(ENDPOINTS["kc_five_level"], code=self._sfx(code))

    # ── 工具 ──────────────

    def health(self) -> Dict[str, Any]:
        """轻探活：stock_list 轻端点（P2 磁盘缓存 8h，命中零网络）。
        返回 {ok, latency_ms, detail}，与 providers base 契约对齐。"""
        t0 = time.monotonic()
        try:
            r = self.stock_list()
            latency = int((time.monotonic() - t0) * 1000)
            if r:
                return {"ok": True, "latency_ms": latency,
                        "detail": f"stock_list {len(r)} 条 (gold 300/min)"}
            return {"ok": False, "latency_ms": latency, "detail": "stock_list 返回空"}
        except Exception as e:
            return {"ok": False, "latency_ms": int((time.monotonic() - t0) * 1000),
                    "detail": str(e)[:120]}

    @staticmethod
    def _clean(code: str) -> str:
        """纯数字代码: sh600938|sz000970|600938.SH -> 600938"""
        c = code.strip().upper().split('.')[0]
        for p in ('SH', 'SZ', 'BJ'):
            if c.startswith(p):
                c = c[2:]
        return c

    @staticmethod
    def _sfx(code: str) -> str:
        """带交易所后缀: 600938->600938.SH, 000970->000970.SZ, 430017.BJ保持"""
        c = code.strip().upper()
        if '.' in c:
            return c
        if c.startswith(('60', '68')):
            return f"{c}.SH"
        elif c.startswith(('00', '30', '002', '003')):
            return f"{c}.SZ"
        elif len(c) == 6 and c.startswith(('43', '8', '9')):  # 京市: 43xxxx/8xxxxx/9xxxxx (v2.3: 补43)
            return f"{c}.BJ"
        return c

    @staticmethod
    def _kline_query(tf: str, adj: str, st: str, et: str, lt: int) -> str:
        parts = []
        if st:
            parts.append(f"st={st}")
        if et:
            parts.append(f"et={et}")
        if lt:
            parts.append(f"lt={lt}")
        return f"?{'&'.join(parts)}" if parts else ""


# ============================================================
#  Convenience Functions (for collector.py / auto_trader_a.py)
# ============================================================

_client: Optional[BiyingClient] = None


def get_client() -> BiyingClient:
    """全局单例"""
    global _client
    if _client is None:
        _client = BiyingClient()
    return _client


# ============================================================
#  P0 实时行情 TTL 手工缓存（替换 lru_cache 僵尸缓存）
# ============================================================

_RT_CACHE_TTL = 30   # 秒
_RT_CACHE_MAX = 256  # 超过容量时淘汰最旧一半
_rt_cache: Dict[str, tuple] = {}  # code -> (epoch_ts, payload)


def _fetch_realtime(code: str) -> Optional[Dict]:
    # ETF 路由: 51/15/56/58 → fund_real
    if code.startswith(("51", "15", "56", "58")):
        return get_client().fund_realtime(code)
    # 北交所路由: 92/8/9 开头纯数字 → bj_realtime (不加.BJ后缀)
    if code.startswith(("92", "8", "9")) and len(code) == 6:
        return get_client().bj_realtime(code)
    return get_client().realtime(code)


def get_realtime(code: str, use_cache: bool = True) -> Optional[Dict]:
    """缓存版实时行情（TTL 30s 内不重复请求；use_cache=False 强制现拉）"""
    if use_cache:
        ent = _rt_cache.get(code)
        if ent is not None and time.time() - ent[0] < _RT_CACHE_TTL:
            return ent[1]
    payload = _fetch_realtime(code)
    if use_cache and payload is not None:
        if len(_rt_cache) >= _RT_CACHE_MAX:
            oldest = sorted(_rt_cache, key=lambda c: _rt_cache[c][0])[: _RT_CACHE_MAX // 2]
            for c in oldest:
                _rt_cache.pop(c, None)
        _rt_cache[code] = (time.time(), payload)
    return payload


def get_holders(code: str) -> Dict[str, Any]:
    """股东结构快照"""
    return get_client().holders_full(code)


def get_financials(code: str) -> Optional[List[Dict]]:
    """财务指标"""
    return get_client().financials(code)


def get_indicators(code: str) -> Optional[List[Dict]]:
    """技术指标"""
    return get_client().indicators(code)


def health_check() -> Dict[str, Any]:
    """数据底座健康检查入口"""
    return get_client().health()


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import sys

    if not get_licence() and not (len(sys.argv) > 2 and sys.argv[1] == "--save-key"):
        print("❌ 必盈API密钥未配置")
        print("   python biyingapi_bridge.py --save-key <KEY>")
        sys.exit(1)

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "--save-key" and len(sys.argv) > 2:
            save_key(sys.argv[2])
            print("✅ 密钥已保存")
            sys.exit(0)

    client = BiyingClient()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "--health":
            h = client.health()
            print(f"必盈API健康: {'✅' if h['ok'] else '❌'}  延迟: {h['latency_ms']}ms  {h['detail']}")
            sys.exit(0)

        elif cmd == "--realtime" and len(sys.argv) > 2:
            import json
            data = client.realtime(sys.argv[2])
            print(json.dumps(data, ensure_ascii=False, indent=2))

        elif cmd == "--five" and len(sys.argv) > 2:
            import json
            data = client.five_level(sys.argv[2])
            print(json.dumps(data, ensure_ascii=False, indent=2))

        elif cmd == "--tx" and len(sys.argv) > 2:
            import json
            data = client.transactions(sys.argv[2])
            print(json.dumps(data[:3], ensure_ascii=False, indent=2))

        elif cmd == "--holders" and len(sys.argv) > 2:
            import json
            data = client.holders_full(sys.argv[2])
            print(json.dumps(data, ensure_ascii=False, indent=2))

        elif cmd == "--financials" and len(sys.argv) > 2:
            import json
            data = client.financials(sys.argv[2])
            print(json.dumps(data, ensure_ascii=False, indent=2))

        elif cmd == "--forecast" and len(sys.argv) > 2:
            import json
            data = client.forecast(sys.argv[2])
            print(json.dumps(data[:3], ensure_ascii=False, indent=2))

        else:
            print("用法:")
            print("  --save-key <KEY>    保存密钥")
            print("  --health            健康检查")
            print("  --realtime <code>   实时行情")
            print("  --five <code>       五档盘口")
            print("  --tx <code>         逐笔统计")
            print("  --holders <code>    股东结构")
            print("  --financials <code> 财务指标")
            print("  --forecast <code>   业绩预告")
    else:
        h = client.health()
        print(f"必盈API黄金版 数据桥接层 v1.1")
        print(f"  状态: {'✅ 在线' if h['ok'] else '❌ 离线'} ({h['latency_ms']}ms, {h['detail']})")
        print(f"  限速: 1分钟300次")
