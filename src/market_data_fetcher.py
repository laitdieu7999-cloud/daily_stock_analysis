"""
市场数据获取模块 - 通过 Jin10 MCP 获取金银/IC 实时数据，akshare 获取历史K线
"""
import subprocess
import json
import time
import select
import os
import logging
import re
import calendar
import shutil
import pandas as pd
import requests
from dataclasses import dataclass
from typing import List, Optional, Union
from datetime import datetime

logger = logging.getLogger(__name__)

JIN10_CALENDAR_BASE_URL = "https://e0430d16720e4211b5e072c26205c890.z3c.jin10.com"
JIN10_CALENDAR_HEADERS = {
    "x-app-id": "sKKYe29sFuJaeOCJ",
    "x-version": "2.0",
}
JIN10_VIP_WATCH_BASE_URL = "https://0a8650194d064704ac11ec524a07f49e.z3c.jin10.com"
JIN10_VIP_WATCH_HEADERS = {
    "x-app-id": "arU9WZF7TC9m7nWn",
    "x-version": "1.0",
    "origin": "https://www.jin10.com",
    "referer": "https://www.jin10.com/vip_watch/index.html#/desktop",
}
NASDAQ_GOLDEN_DRAGON_OVERVIEW_URL = "https://indexes.nasdaq.com/Index/Overview/HXC"


@dataclass
class GoldSilverQuote:
    """金银报价数据"""
    name: str           # 品种名称
    code: str           # 品种代码
    price: float        # 最新价
    change_pct: float   # 涨跌幅(%)
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    time: str = ""      # 报价时间


@dataclass
class ICFutureData:
    """IC期货数据"""
    spot_price: float          # 中证500现货价格
    futures_price: float       # IC主力合约价格
    basis: float               # 基差（现货-期货）
    annualized_basis_pct: float # 年化贴水收益率(%)
    contract_code: str = ""    # 合约代码
    days_to_expiry: int = 0    # 距到期天数


@dataclass
class ICTermStructureData:
    """IC期限结构实时快照"""
    near_symbol: str
    near_price: float
    near_days: int
    next_symbol: str
    next_price: float
    next_days: int
    m1_m2_annualized_pct: float
    q1_symbol: str = ""
    q1_price: float = 0.0
    q1_days: int = 0
    q2_symbol: str = ""
    q2_price: float = 0.0
    q2_days: int = 0
    q1_q2_annualized_pct: Optional[float] = None
    front_end_gap_pct: Optional[float] = None


@dataclass
class ICContractSnapshot:
    """IC 单个合约快照"""
    symbol: str
    price: float
    expiry_date: str
    days_to_expiry: int
    term_gap_days: int
    basis: float
    annualized_basis_pct: float
    is_main: bool = False


@dataclass
class ICMarketSnapshotData:
    """IC 现货 + 多合约快照"""
    spot_price: float
    main_contract_code: str
    fetched_at: str
    contracts: List[ICContractSnapshot]
    option_proxy: Optional["ETFOptionProxyData"] = None


@dataclass
class ETFOptionProxyData:
    """500ETF 期权代理快照"""
    board_timestamp: str
    expiry_ym: str
    expiry_style: str
    qvix_latest: float
    qvix_prev: float
    qvix_jump_pct: float
    qvix_zscore: float
    atm_strike: float
    atm_call_trade_code: str
    atm_call_price: float
    atm_put_trade_code: str
    atm_put_price: float
    otm_put_trade_code: str
    otm_put_strike: float
    otm_put_price: float
    put_skew_ratio: float
    atm_put_last_price: Optional[float] = None
    atm_put_bid1: Optional[float] = None
    atm_put_ask1: Optional[float] = None
    atm_put_quote_time: str = ""
    atm_put_days_to_expiry: Optional[int] = None
    atm_put_price_source: str = "latest"
    otm_put_last_price: Optional[float] = None
    otm_put_bid1: Optional[float] = None
    otm_put_ask1: Optional[float] = None
    otm_put_quote_time: str = ""
    otm_put_days_to_expiry: Optional[int] = None
    otm_put_price_source: str = "latest"
    atm_put_call_volume_ratio: Optional[float] = None
    atm_put_volume: Optional[float] = None
    atm_call_volume: Optional[float] = None
    expiry_days_to_expiry: Optional[int] = None
    roll_window_shifted: bool = False
    source: str = "akshare_public"


class MarketDataFetcher:
    """通过 Jin10 MCP 获取市场数据

    MCP 会话管理参考 src/search_service.py 中 Jin10SearchProvider 的实现，
    使用 npx mcp-remote 作为 stdio 桥接与远程 MCP 服务通信。
    """

    def __init__(self, jin10_api_key: str, jin10_x_token: str = ""):
        self._api_key = (jin10_api_key or "").strip()
        self._x_token = (jin10_x_token or "").strip()
        self._proc = None
        self._initialized = False
        self._mcp_remote_cmd = self._resolve_mcp_remote_command()

    @staticmethod
    def _resolve_mcp_remote_command() -> Optional[List[str]]:
        npx_path = shutil.which("npx")
        if not npx_path:
            logger.warning("[MarketData] 未找到 npx，Jin10 MCP 功能将自动降级为不可用")
            return None
        return [npx_path, "mcp-remote"]

    def _cleanup_proc(self):
        """安全清理旧 MCP 子进程"""
        if self._proc is not None:
            try:
                self._proc.terminate()
            except OSError:
                pass
            try:
                self._proc.wait(timeout=3)
            except Exception:
                try:
                    self._proc.kill()
                except OSError:
                    pass
            self._proc = None
        self._initialized = False

    def _ensure_initialized(self) -> bool:
        """确保 MCP 会话已初始化"""
        if not self._api_key:
            logger.debug("[MarketData] 未配置 JIN10_API_KEY，跳过 MCP 初始化")
            return False

        if not self._mcp_remote_cmd:
            return False

        if self._initialized and self._proc and self._proc.poll() is None:
            return True

        self._cleanup_proc()

        self._proc = subprocess.Popen(
            [
                *self._mcp_remote_cmd,
                'https://mcp.jin10.com/mcp',
                '--header', f'Authorization: Bearer {self._api_key}'
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        # 等待 mcp-remote 启动完成
        time.sleep(3)

        # 检查进程是否在启动后立即退出
        if self._proc.poll() is not None:
            stderr = self._proc.stderr.read().decode(errors='replace') if self._proc.stderr else ''
            logger.error(f"[MarketData] mcp-remote 启动失败: {stderr[:200]}")
            self._proc = None
            return False

        # 1) 发送 initialize 请求
        self._send({
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "dsa-market", "version": "1.0"},
            },
            "id": 1,
        })
        time.sleep(2)
        self._read(5)

        # 2) 发送 initialized 通知
        self._send({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        })
        time.sleep(1)
        self._initialized = True
        logger.debug("[MarketData] MCP 会话初始化完成")
        return True

    def _send(self, msg):
        """向 mcp-remote 子进程 stdin 写入一行 JSON"""
        data = (json.dumps(msg) + "\n").encode("utf-8")
        assert self._proc and self._proc.stdin
        self._proc.stdin.write(data)
        self._proc.stdin.flush()

    def _read(self, timeout=10):
        """从 mcp-remote 子进程 stdout 读取可用数据（非阻塞轮询）"""
        assert self._proc and self._proc.stdout
        output = b""
        start = time.time()
        while time.time() - start < timeout:
            ready, _, _ = select.select([self._proc.stdout], [], [], 0.3)
            if ready:
                chunk = os.read(self._proc.stdout.fileno(), 65536)
                if chunk:
                    output += chunk
                else:
                    break
        return output.decode(errors="replace")

    def _call_tool(self, tool_name, arguments=None, retries=2):
        """调用 MCP 工具（带重试）"""
        if not self._api_key:
            return None

        for attempt in range(retries + 1):
            try:
                if attempt > 0:
                    # 重试时重新初始化连接
                    self._cleanup_proc()
                    time.sleep(1)
                if not self._ensure_initialized():
                    return None
                msg = {
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": arguments or {},
                    },
                    "id": hash(f"{tool_name}{attempt}") % 10000,
                }
                self._send(msg)
                time.sleep(2)
                output = self._read(timeout=8)

                # 解析 SSE 格式的 JSON-RPC 响应
                for line in output.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("{"):
                        try:
                            data = json.loads(line)
                            if "result" in data:
                                content = data["result"].get("content", [])
                                if content and content[0].get("type") == "text":
                                    return json.loads(content[0]["text"])
                        except (json.JSONDecodeError, KeyError, IndexError):
                            pass
            except Exception as e:
                logger.warning(f"[MarketData] 调用 {tool_name} 失败 (attempt {attempt+1}/{retries+1}): {e}")
        return None

    def get_quote(self, code: str) -> Optional[GoldSilverQuote]:
        """获取品种报价（每次调用新建连接确保可靠性）"""
        try:
            if not self._api_key:
                return None
            # 每次调用都重新初始化连接，避免 MCP 会话复用问题
            self._cleanup_proc()
            result = self._call_tool("get_quote", {"code": code})
            if result and result.get("status") == 200:
                d = result["data"]
                return GoldSilverQuote(
                    name=d.get("name", code),
                    code=d.get("code", code),
                    price=float(d.get("close", 0)),
                    change_pct=float(d.get("ups_percent", 0)),
                    open=float(d.get("open", 0)),
                    high=float(d.get("high", 0)),
                    low=float(d.get("low", 0)),
                    time=d.get("time", ""),
                )
        except Exception as e:
            logger.warning(f"[MarketData] 获取 {code} 报价失败: {e}")
        return None

    def list_flash(self, limit: int = 50) -> list[dict]:
        """获取金十快讯列表。"""
        try:
            if not self._api_key:
                return []

            self._cleanup_proc()
            result = self._call_tool("list_flash", {})
            if not result:
                return []

            if isinstance(result, list):
                return [item for item in result[:limit] if isinstance(item, dict)]

            items = result.get("data", {}).get("items", []) if isinstance(result, dict) else []
            return [item for item in items[:limit] if isinstance(item, dict)]
        except Exception as e:
            logger.warning(f"[MarketData] 获取金十快讯失败: {e}")
            return []

    def _calendar_get(self, path: str, params: Optional[dict] = None) -> Optional[dict]:
        """访问 Jin10 日历开放接口。"""
        try:
            response = requests.get(
                f"{JIN10_CALENDAR_BASE_URL}{path}",
                params=params or {},
                headers=JIN10_CALENDAR_HEADERS,
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"[MarketData] 获取 Jin10 日历接口 {path} 失败: {e}")
            return None

    def _vip_watch_get(self, path: str, params: Optional[dict] = None) -> Optional[dict]:
        """访问 Jin10 会员盯盘接口。"""
        if not self._x_token:
            return None

        headers = dict(JIN10_VIP_WATCH_HEADERS)
        headers["x-token"] = self._x_token
        try:
            response = requests.get(
                f"{JIN10_VIP_WATCH_BASE_URL}{path}",
                params=params or {},
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"[MarketData] 获取 Jin10 会员接口 {path} 失败: {e}")
            return None

    def list_calendar(self, date: Optional[str] = None, category: str = "cj", limit: int = 50) -> list[dict]:
        """获取指定日期的财经日历数据。"""
        target_date = date or datetime.now().strftime("%Y-%m-%d")
        result = self._calendar_get("/get/data", {"date": target_date, "category": category})
        if not result or result.get("status") != 200:
            return []
        data = result.get("data", [])
        return [item for item in data[:limit] if isinstance(item, dict)]

    def get_calendar_interpretation(self, data_id: Union[int, str]) -> Optional[dict]:
        """获取单条财经数据的详细解读。"""
        result = self._calendar_get("/web/data/jiedu", {"id": data_id})
        if not result or result.get("status") != 200:
            return None
        data = result.get("data")
        return data if isinstance(data, dict) else None

    def list_calendar_indicators(self) -> list[dict]:
        """获取财经指标元数据列表（含 tags/affect）。"""
        result = self._calendar_get("/web/indicator_list")
        if not result or result.get("status") != 200:
            return []
        data = result.get("data", [])
        return [item for item in data if isinstance(item, dict)]

    def list_vip_watch_events(self, limit: int = 20, **params) -> list[dict]:
        """获取会员盯盘事件流。"""
        result = self._vip_watch_get("/api/vip-watch/events", params=params)
        if not result or result.get("status") != 200:
            return []
        data = result.get("data", [])
        return [item for item in data[:limit] if isinstance(item, dict)]

    def get_vip_watch_indicator_resonance(self, code: str) -> Optional[dict]:
        """获取会员盯盘指标共振信息。"""
        result = self._vip_watch_get("/api/vip-watch/indicator/resonance", {"code": code})
        if not result or result.get("status") != 200:
            return None
        data = result.get("data")
        return data if isinstance(data, dict) else None

    def list_vip_watch_products(self) -> list[dict]:
        """获取会员盯盘神器可用品种列表。"""
        result = self._vip_watch_get("/api/vip-watch")
        if not result or result.get("status") != 200:
            return []
        data = result.get("data", [])
        return [item for item in data if isinstance(item, dict)]

    def get_nasdaq_golden_dragon_snapshot(self) -> Optional[dict]:
        """抓取 Nasdaq 官方 HXC 页面，用作隔夜中概情绪参考。"""
        try:
            response = requests.get(
                NASDAQ_GOLDEN_DRAGON_OVERVIEW_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            response.raise_for_status()
            html = response.text

            def _extract(pattern: str) -> Optional[str]:
                match = re.search(pattern, html, re.S)
                if not match:
                    return None
                value = (match.group(1) or "").strip()
                return value or None

            last_text = _extract(r"Last</td>\s*<td[^>]*>\s*([\-0-9.,]+)") or _extract(
                r"Last\s*</span>\s*<span>\s*([\-0-9.,]+)"
            )
            change_text = _extract(r"Net Change</td>\s*<td[^>]*>\s*([\-0-9.,]+)") or _extract(
                r"Net Change\s*</span>\s*<span>\s*([\-0-9.,]+)"
            )
            pct_text = (
                _extract(r'Change"\s*>\s*([\-0-9.,]+%)')
                or _extract(r"Change\s*</span>\s*<span>\s*([\-0-9.,]+%)")
                or _extract(r"([\-0-9.,]+%)")
            )
            prev_close_text = _extract(r"Previous Close\s*</span>\s*<span>\s*([0-9.,]+)")
            high_text = _extract(r"Today’s High\s*</span>\s*<span>\s*([0-9.,]+)")
            low_text = _extract(r"Today.?s Low\s*</span>\s*<span>\s*([0-9.,]+)")

            if not last_text or not pct_text:
                return None

            def _to_float(text: Optional[str]) -> Optional[float]:
                if not text:
                    return None
                try:
                    return float(text.replace(",", "").replace("%", ""))
                except Exception:
                    return None

            return {
                "code": "HXC",
                "name": "纳斯达克中国金龙指数",
                "last": _to_float(last_text),
                "change": _to_float(change_text),
                "change_pct": _to_float(pct_text),
                "prev_close": _to_float(prev_close_text),
                "high": _to_float(high_text),
                "low": _to_float(low_text),
                "source": "nasdaq_official",
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        except Exception as e:
            logger.warning(f"[MarketData] 获取纳斯达克中国金龙指数失败: {e}")
            return None

    def get_gold_silver_quotes(self) -> dict:
        """获取黄金和白银报价"""
        quotes = {}
        for code in ["XAUUSD", "XAGUSD"]:
            q = self.get_quote(code)
            if q:
                quotes[code] = q
        return quotes

    @staticmethod
    def _third_friday(year: int, month: int) -> datetime:
        first_weekday, _ = calendar.monthrange(year, month)
        first_friday = 1 + (4 - first_weekday) % 7
        return datetime(year, month, first_friday + 14)

    @classmethod
    def _extract_ic_expiry(cls, contract_code: str) -> Optional[datetime]:
        code = (contract_code or "").upper().strip()
        if not code.startswith("IC"):
            return None
        digits = code[2:]
        if len(digits) == 4 and digits.isdigit():
            year = 2000 + int(digits[:2])
            month = int(digits[2:])
            return cls._third_friday(year, month)
        return None

    @staticmethod
    def _annualized_calendar_spread(
        near_close: float,
        far_close: float,
        near_days: int,
        far_days: int,
    ) -> float:
        tenor_days = max(float(far_days) - float(near_days), 1.0)
        return float((float(far_close) - float(near_close)) / float(near_close)) / (tenor_days / 365.0) * 100

    @classmethod
    def _load_ic_realtime_contracts(cls):
        try:
            import akshare as ak

            realtime = ak.futures_zh_realtime()
            if realtime is None or len(realtime) == 0:
                return []

            symbol_col = None
            for candidate in ("symbol", "合约代码", "合约", "代码"):
                if candidate in realtime.columns:
                    symbol_col = candidate
                    break

            price_col = None
            for candidate in ("trade", "最新价", "最新", "close"):
                if candidate in realtime.columns:
                    price_col = candidate
                    break

            if not symbol_col or not price_col:
                return []

            rows = []
            seen: set[str] = set()
            for _, row in realtime.iterrows():
                code = str(row.get(symbol_col, "")).upper().strip()
                if not code.startswith("IC") or code in seen:
                    continue
                expiry = cls._extract_ic_expiry(code)
                if expiry is None:
                    continue
                try:
                    price = float(row.get(price_col))
                except Exception:
                    continue
                if price <= 0:
                    continue
                rows.append(
                    {
                        "symbol": code,
                        "trade": price,
                        "expiry": expiry,
                    }
                )
                seen.add(code)
            rows.sort(key=lambda item: item["expiry"])
            return rows
        except Exception as exc:
            logger.debug("[MarketData] futures_zh_realtime failed: %s", exc)
            return []

    @classmethod
    def _iter_ic_candidate_symbols(cls, start_contract: str, months_ahead: int = 6) -> list[str]:
        code = (start_contract or "").upper().strip()
        expiry = cls._extract_ic_expiry(code)
        if not code.startswith("IC") or expiry is None:
            return []
        year = expiry.year
        month = expiry.month
        candidates: list[str] = []
        for step in range(months_ahead + 1):
            total_month = month + step
            new_year = year + (total_month - 1) // 12
            new_month = (total_month - 1) % 12 + 1
            candidates.append(f"IC{str(new_year)[-2:]}{new_month:02d}")
        return candidates

    @classmethod
    def _load_ic_contracts_via_sina_hq(cls, symbols: list[str]) -> list[dict]:
        if not symbols:
            return []
        try:
            subscribe_list = ",".join(f"nf_{code}" for code in symbols)
            headers = {
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            }
            resp = requests.get(
                f"https://hq.sinajs.cn/rn={int(time.time() * 1000)}&list={subscribe_list}",
                headers=headers,
                timeout=8,
            )
            resp.raise_for_status()

            rows = []
            for raw_line in resp.text.split(";"):
                line = raw_line.strip()
                if not line or "=" not in line:
                    continue
                left, right = line.split("=", 1)
                code = left.rsplit("_", 1)[-1].upper().strip()
                expiry = cls._extract_ic_expiry(code)
                if expiry is None:
                    continue
                payload = right.strip().strip('"')
                parts = payload.split(",")
                if len(parts) < 4:
                    continue
                price = cls._coerce_float(parts[3])
                if price is None or price <= 0:
                    continue
                rows.append(
                    {
                        "symbol": code,
                        "trade": float(price),
                        "expiry": expiry,
                    }
                )
            rows.sort(key=lambda item: item["expiry"])
            return rows
        except Exception as exc:
            logger.debug("[MarketData] sina hq fallback failed: %s", exc)
            return []

    @classmethod
    def _load_ic_contracts_via_spot_fallback(cls, start_contract: str) -> list[dict]:
        candidates = cls._iter_ic_candidate_symbols(start_contract)
        if not candidates:
            return []
        direct_rows = cls._load_ic_contracts_via_sina_hq(candidates)
        if direct_rows:
            return direct_rows
        try:
            import akshare as ak

            rows = []
            for code in candidates:
                expiry = cls._extract_ic_expiry(code)
                if expiry is None:
                    continue
                try:
                    df = ak.futures_zh_spot(symbol=code, market="FF", adjust="0")
                except Exception:
                    continue
                if df is None or len(df) == 0:
                    continue
                price = None
                for column in ("current_price", "最新价", "最新", "close"):
                    if column in df.columns:
                        try:
                            price = float(df.iloc[0][column])
                            break
                        except Exception:
                            price = None
                if price is None or price <= 0:
                    continue
                rows.append(
                    {
                        "symbol": code,
                        "trade": price,
                        "expiry": expiry,
                    }
                )
            rows.sort(key=lambda item: item["expiry"])
            return rows
        except Exception as exc:
            logger.debug("[MarketData] futures_zh_spot fallback failed: %s", exc)
            return []

    @staticmethod
    def _load_csi500_spot_price() -> Optional[float]:
        try:
            import akshare as ak

            df = ak.stock_zh_index_spot_sina()
            if df is not None and not df.empty:
                match = df[df["代码"] == "sh000905"]
                if not match.empty:
                    return float(match.iloc[0]["最新价"])
        except Exception as exc:
            logger.debug("[MarketData] stock_zh_index_spot_sina failed: %s", exc)

        try:
            import akshare as ak

            hist = ak.stock_zh_index_daily(symbol="sh000905")
            if hist is not None and not hist.empty:
                return float(hist.iloc[-1]["close"])
        except Exception as exc:
            logger.debug("[MarketData] stock_zh_index_daily fallback failed: %s", exc)
        return None

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(str(value).replace(",", "").replace("%", ""))
        except Exception:
            return None

    @staticmethod
    def _parse_500etf_option_trade_code(trade_code: str) -> Optional[dict]:
        code = str(trade_code or "").strip().upper()
        match = re.match(r"510500([CP])(\d{4})([AM])(\d+)$", code)
        if not match:
            return None
        side, ym, style, strike_text = match.groups()
        try:
            strike = float(int(strike_text) / 1000.0)
        except Exception:
            return None
        return {
            "side": side,
            "ym": ym,
            "style": style,
            "strike": strike,
        }

    @staticmethod
    def _extract_option_spot_metrics(payload_df: pd.DataFrame) -> dict:
        if payload_df is None or payload_df.empty:
            return {}
        mapping = dict(zip(payload_df["字段"].astype(str), payload_df["值"]))
        return {
            "latest": MarketDataFetcher._coerce_float(mapping.get("最新价")),
            "volume": MarketDataFetcher._coerce_float(mapping.get("成交量")),
            "time": str(mapping.get("行情时间") or ""),
            "bid1": MarketDataFetcher._coerce_float(mapping.get("申买价一") or mapping.get("买价")),
            "ask1": MarketDataFetcher._coerce_float(mapping.get("申卖价一") or mapping.get("卖价")),
            "bid_size1": MarketDataFetcher._coerce_float(mapping.get("申买量一") or mapping.get("买量")),
            "ask_size1": MarketDataFetcher._coerce_float(mapping.get("申卖量一") or mapping.get("卖量")),
        }

    @staticmethod
    def _parse_option_expiry_ymd(value: object) -> Optional[datetime]:
        text = str(value or "").strip()
        if len(text) != 8 or not text.isdigit():
            return None
        try:
            return datetime.strptime(text, "%Y%m%d")
        except Exception:
            return None

    @staticmethod
    def _select_option_entry_price(metrics: dict, fallback: Optional[float]) -> tuple[Optional[float], str]:
        ask1 = MarketDataFetcher._coerce_float(metrics.get("ask1"))
        if ask1 not in (None, 0):
            return float(ask1), "ask1"
        latest = MarketDataFetcher._coerce_float(metrics.get("latest"))
        if latest not in (None, 0):
            return float(latest), "latest_fallback"
        if fallback not in (None, 0):
            return float(fallback), "board_current_fallback"
        return None, "missing"

    def get_500etf_option_proxy(self) -> Optional[ETFOptionProxyData]:
        """获取 500ETF 期权公开代理快照，用作 IC 第一层预警候选。"""
        try:
            import akshare as ak

            min_qvix = ak.index_option_500etf_min_qvix()
            if min_qvix is None or min_qvix.empty or "qvix" not in min_qvix.columns:
                return None
            qvix_series = pd.to_numeric(min_qvix["qvix"], errors="coerce").dropna().astype(float)
            if qvix_series.empty:
                return None
            qvix_latest = float(qvix_series.iloc[-1])
            qvix_prev = float(qvix_series.iloc[-2]) if len(qvix_series) >= 2 else qvix_latest
            qvix_jump_pct = ((qvix_latest / qvix_prev) - 1.0) * 100 if qvix_prev else 0.0
            qvix_window = qvix_series.tail(min(60, len(qvix_series)))
            qvix_mean = float(qvix_window.mean())
            qvix_std = float(qvix_window.std(ddof=0)) if len(qvix_window) >= 2 else 0.0
            qvix_zscore = (qvix_latest - qvix_mean) / qvix_std if qvix_std > 0 else 0.0

            board = ak.option_finance_board(symbol="南方中证500ETF期权")
            if board is None or board.empty:
                return None
            board = board.copy()
            board["trade_code"] = board["合约交易代码"].astype(str)
            board["current_price"] = pd.to_numeric(board["当前价"], errors="coerce")
            board["board_timestamp"] = board["日期"].astype(str)
            parsed = board["trade_code"].map(self._parse_500etf_option_trade_code)
            board["side"] = parsed.map(lambda item: item.get("side") if isinstance(item, dict) else None)
            board["ym"] = parsed.map(lambda item: item.get("ym") if isinstance(item, dict) else None)
            board["style"] = parsed.map(lambda item: item.get("style") if isinstance(item, dict) else None)
            board["strike"] = parsed.map(lambda item: item.get("strike") if isinstance(item, dict) else None)
            board = board.dropna(subset=["current_price", "side", "ym", "style", "strike"])
            if board.empty:
                return None

            group_stats = []
            for (ym, style), group in board.groupby(["ym", "style"], sort=True):
                calls = group[group["side"] == "C"]["strike"].nunique()
                puts = group[group["side"] == "P"]["strike"].nunique()
                pair_count = min(calls, puts)
                if pair_count <= 0:
                    continue
                group_stats.append(
                    {
                        "ym": ym,
                        "style": style,
                        "pair_count": pair_count,
                    }
                )
            if not group_stats:
                return None

            meta = ak.option_current_day_sse()
            code_mapping: dict[str, str] = {}
            expiry_mapping: dict[str, datetime] = {}
            group_expiry_mapping: dict[tuple[str, str], datetime] = {}
            if meta is not None and not meta.empty:
                meta = meta.copy()
                meta = meta[meta["标的券名称及代码"].astype(str) == "500ETF(510500)"]
                code_mapping = {
                    str(row["合约交易代码"]): str(row["合约编码"])
                    for _, row in meta.iterrows()
                }
                expiry_mapping = {
                    str(row["合约交易代码"]): parsed
                    for _, row in meta.iterrows()
                    for parsed in [self._parse_option_expiry_ymd(row.get("到期日"))]
                    if parsed is not None
                }
                for _, row in meta.iterrows():
                    trade_code = str(row.get("合约交易代码") or "")
                    parsed = self._parse_option_expiry_ymd(row.get("到期日"))
                    option_meta = self._parse_500etf_option_trade_code(trade_code)
                    if not trade_code or parsed is None or not option_meta:
                        continue
                    key = (str(option_meta["ym"]), str(option_meta["style"]))
                    existing = group_expiry_mapping.get(key)
                    if existing is None or parsed < existing:
                        group_expiry_mapping[key] = parsed

            now_date = datetime.now().date()
            raw_chosen = sorted(
                group_stats,
                key=lambda item: (item["ym"], 0 if item["style"] == "M" else 1, -item["pair_count"]),
            )[0]
            eligible_group_stats = []
            for item in group_stats:
                expiry_dt = group_expiry_mapping.get((str(item["ym"]), str(item["style"])))
                if expiry_dt is None or (expiry_dt.date() - now_date).days > 3:
                    eligible_group_stats.append(item)
            chosen = sorted(
                eligible_group_stats or group_stats,
                key=lambda item: (item["ym"], 0 if item["style"] == "M" else 1, -item["pair_count"]),
            )[0]
            roll_window_shifted = (
                str(chosen["ym"]) != str(raw_chosen["ym"])
                or str(chosen["style"]) != str(raw_chosen["style"])
            )
            scoped = board[(board["ym"] == chosen["ym"]) & (board["style"] == chosen["style"])].copy()
            if scoped.empty:
                return None

            calls = scoped[scoped["side"] == "C"][["strike", "current_price", "trade_code"]].rename(
                columns={"current_price": "call_price", "trade_code": "call_trade_code"}
            )
            puts = scoped[scoped["side"] == "P"][["strike", "current_price", "trade_code"]].rename(
                columns={"current_price": "put_price", "trade_code": "put_trade_code"}
            )
            paired = calls.merge(puts, on="strike", how="inner")
            if paired.empty:
                return None
            paired["parity_gap"] = (paired["call_price"] - paired["put_price"]).abs()
            atm_row = paired.sort_values(["parity_gap", "strike"]).iloc[0]
            atm_strike = float(atm_row["strike"])
            atm_call_trade_code = str(atm_row["call_trade_code"])
            atm_put_trade_code = str(atm_row["put_trade_code"])
            atm_call_price = float(atm_row["call_price"])
            atm_put_price = float(atm_row["put_price"])

            puts_below = paired[paired["strike"] <= atm_strike * 0.95].sort_values("strike")
            if puts_below.empty:
                puts_below = paired[paired["strike"] < atm_strike].sort_values("strike")
            if puts_below.empty:
                return None
            otm_row = puts_below.iloc[-1]
            otm_put_trade_code = str(otm_row["put_trade_code"])
            otm_put_strike = float(otm_row["strike"])
            otm_put_price = float(otm_row["put_price"])
            put_skew_ratio = (otm_put_price / atm_put_price) if atm_put_price > 0 else 0.0

            atm_put_volume = None
            atm_call_volume = None
            atm_put_metrics: dict = {}
            otm_put_metrics: dict = {}
            put_code = code_mapping.get(atm_put_trade_code)
            call_code = code_mapping.get(atm_call_trade_code)
            otm_put_code = code_mapping.get(otm_put_trade_code)
            if put_code:
                atm_put_metrics = self._extract_option_spot_metrics(ak.option_sse_spot_price_sina(symbol=put_code))
                if atm_put_metrics:
                    atm_put_volume = atm_put_metrics.get("volume")
            if call_code:
                call_metrics = self._extract_option_spot_metrics(ak.option_sse_spot_price_sina(symbol=call_code))
                if call_metrics:
                    atm_call_volume = call_metrics.get("volume")
            if otm_put_code:
                otm_put_metrics = self._extract_option_spot_metrics(ak.option_sse_spot_price_sina(symbol=otm_put_code))
            atm_put_call_volume_ratio = None
            if atm_put_volume is not None and atm_call_volume not in (None, 0):
                atm_put_call_volume_ratio = float(atm_put_volume) / float(atm_call_volume)

            selected_atm_put_price, atm_put_price_source = self._select_option_entry_price(atm_put_metrics, atm_put_price)
            selected_otm_put_price, otm_put_price_source = self._select_option_entry_price(otm_put_metrics, otm_put_price)

            atm_put_expiry = expiry_mapping.get(atm_put_trade_code)
            otm_put_expiry = expiry_mapping.get(otm_put_trade_code)
            atm_put_days_to_expiry = (
                max((atm_put_expiry.date() - now_date).days, 1) if atm_put_expiry is not None else None
            )
            otm_put_days_to_expiry = (
                max((otm_put_expiry.date() - now_date).days, 1) if otm_put_expiry is not None else None
            )
            expiry_days_to_expiry = otm_put_days_to_expiry or atm_put_days_to_expiry

            board_timestamp = str(scoped["board_timestamp"].iloc[0])
            return ETFOptionProxyData(
                board_timestamp=board_timestamp,
                expiry_ym=str(chosen["ym"]),
                expiry_style=str(chosen["style"]),
                qvix_latest=round(qvix_latest, 2),
                qvix_prev=round(qvix_prev, 2),
                qvix_jump_pct=round(qvix_jump_pct, 2),
                qvix_zscore=round(qvix_zscore, 2),
                atm_strike=round(atm_strike, 3),
                atm_call_trade_code=atm_call_trade_code,
                atm_call_price=round(atm_call_price, 4),
                atm_put_trade_code=atm_put_trade_code,
                atm_put_price=round(selected_atm_put_price if selected_atm_put_price is not None else atm_put_price, 4),
                atm_put_last_price=round(atm_put_metrics.get("latest"), 4) if atm_put_metrics.get("latest") not in (None, "") else None,
                atm_put_bid1=round(atm_put_metrics.get("bid1"), 4) if atm_put_metrics.get("bid1") not in (None, "") else None,
                atm_put_ask1=round(atm_put_metrics.get("ask1"), 4) if atm_put_metrics.get("ask1") not in (None, "") else None,
                atm_put_quote_time=str(atm_put_metrics.get("time") or ""),
                atm_put_days_to_expiry=atm_put_days_to_expiry,
                atm_put_price_source=atm_put_price_source,
                otm_put_trade_code=otm_put_trade_code,
                otm_put_strike=round(otm_put_strike, 3),
                otm_put_price=round(selected_otm_put_price if selected_otm_put_price is not None else otm_put_price, 4),
                otm_put_last_price=round(otm_put_metrics.get("latest"), 4) if otm_put_metrics.get("latest") not in (None, "") else None,
                otm_put_bid1=round(otm_put_metrics.get("bid1"), 4) if otm_put_metrics.get("bid1") not in (None, "") else None,
                otm_put_ask1=round(otm_put_metrics.get("ask1"), 4) if otm_put_metrics.get("ask1") not in (None, "") else None,
                otm_put_quote_time=str(otm_put_metrics.get("time") or ""),
                otm_put_days_to_expiry=otm_put_days_to_expiry,
                otm_put_price_source=otm_put_price_source,
                put_skew_ratio=round(put_skew_ratio, 3),
                atm_put_call_volume_ratio=round(atm_put_call_volume_ratio, 2) if atm_put_call_volume_ratio is not None else None,
                atm_put_volume=round(atm_put_volume, 2) if atm_put_volume is not None else None,
                atm_call_volume=round(atm_call_volume, 2) if atm_call_volume is not None else None,
                expiry_days_to_expiry=expiry_days_to_expiry,
                roll_window_shifted=roll_window_shifted,
            )
        except Exception as exc:
            logger.warning("[MarketData] 获取 500ETF 期权代理快照失败: %s", exc)
            return None

    def get_ic_basis(self) -> Optional[ICFutureData]:
        """获取 IC 期货基差数据"""
        try:
            import akshare as ak

            spot_price = self._load_csi500_spot_price()
            if spot_price is None:
                logger.warning("[MarketData] 无法获取中证500现货价格")
                return None

            futures_price = None
            contract_code = ""
            realtime_contracts = self._load_ic_realtime_contracts()

            try:
                main_contracts = ak.match_main_contract(symbol="cffex")
                if isinstance(main_contracts, str):
                    for code in main_contracts.split(","):
                        code = code.strip().upper()
                        if code.startswith("IC"):
                            contract_code = code
                            break
            except Exception as exc:
                logger.debug("[MarketData] 匹配 CFFEX 主力合约失败: %s", exc)

            if contract_code:
                for item in realtime_contracts:
                    if item["symbol"] == contract_code:
                        futures_price = float(item["trade"])
                        break

            if futures_price is None and realtime_contracts:
                futures_price = float(realtime_contracts[0]["trade"])
                if not contract_code:
                    contract_code = str(realtime_contracts[0]["symbol"])

            if futures_price is None:
                df = ak.futures_main_sina(symbol="IC0")
                if df is not None and len(df) > 0:
                    latest = df.iloc[-1]
                    futures_price = float(latest.get("收盘价", latest.get("close")))
                    if not contract_code:
                        contract_code = str(latest.get("合约名称") or latest.get("symbol") or "IC主力")

            if futures_price is not None:
                contract_code = contract_code or "IC主力"

                basis = spot_price - futures_price
                expiry = self._extract_ic_expiry(contract_code)
                if expiry is not None:
                    days = max((expiry.date() - datetime.now().date()).days, 1)
                else:
                    days = 30
                annualized = (basis / futures_price) / (days / 365) * 100

                return ICFutureData(
                    spot_price=spot_price,
                    futures_price=futures_price,
                    basis=round(basis, 2),
                    annualized_basis_pct=round(annualized, 2),
                    contract_code=contract_code,
                    days_to_expiry=days,
                )
        except Exception as e:
            logger.warning(f"[MarketData] 获取 IC 基差失败: {e}")
        return None

    def get_ic_term_structure(self) -> Optional[ICTermStructureData]:
        """获取 IC 近月/次月与远季锚的实时期限结构快照。"""
        try:
            contracts = self._load_ic_realtime_contracts()
            if len(contracts) < 2:
                near_contract = ""
                try:
                    import akshare as ak

                    main_contracts = ak.match_main_contract(symbol="cffex")
                    if isinstance(main_contracts, str):
                        for code in main_contracts.split(","):
                            code = code.strip().upper()
                            if code.startswith("IC"):
                                near_contract = code
                                break
                except Exception as exc:
                    logger.debug("[MarketData] 匹配 CFFEX 主力合约失败(term): %s", exc)
                if near_contract:
                    fallback_contracts = self._load_ic_contracts_via_spot_fallback(near_contract)
                    if len(fallback_contracts) >= 2:
                        contracts = fallback_contracts
            if len(contracts) < 2:
                return None

            now_date = datetime.now().date()
            near = contracts[0]
            next_month = contracts[1]
            near_days = max((near["expiry"].date() - now_date).days, 1)
            next_days = max((next_month["expiry"].date() - now_date).days, near_days + 1)
            m1_m2_annualized_pct = self._annualized_calendar_spread(
                near["trade"],
                next_month["trade"],
                near_days,
                next_days,
            )

            quarter_months = {3, 6, 9, 12}
            quarter_contracts = [
                item for item in contracts
                if item["expiry"].month in quarter_months
            ]
            q1 = quarter_contracts[0] if len(quarter_contracts) >= 1 else None
            q2 = quarter_contracts[1] if len(quarter_contracts) >= 2 else None

            q1_q2_annualized_pct = None
            front_end_gap_pct = None
            q1_symbol = ""
            q1_price = 0.0
            q1_days = 0
            q2_symbol = ""
            q2_price = 0.0
            q2_days = 0
            if q1 is not None:
                q1_symbol = str(q1["symbol"])
                q1_price = float(q1["trade"])
                q1_days = max((q1["expiry"].date() - now_date).days, 1)
            if q2 is not None:
                q2_symbol = str(q2["symbol"])
                q2_price = float(q2["trade"])
                q2_days = max((q2["expiry"].date() - now_date).days, q1_days + 1 if q1_days else 1)
            if q1 is not None and q2 is not None:
                q1_q2_annualized_pct = self._annualized_calendar_spread(
                    q1["trade"],
                    q2["trade"],
                    q1_days,
                    q2_days,
                )
                front_end_gap_pct = m1_m2_annualized_pct - q1_q2_annualized_pct

            return ICTermStructureData(
                near_symbol=str(near["symbol"]),
                near_price=float(near["trade"]),
                near_days=near_days,
                next_symbol=str(next_month["symbol"]),
                next_price=float(next_month["trade"]),
                next_days=next_days,
                m1_m2_annualized_pct=round(m1_m2_annualized_pct, 2),
                q1_symbol=q1_symbol,
                q1_price=round(q1_price, 2) if q1_symbol else 0.0,
                q1_days=q1_days,
                q2_symbol=q2_symbol,
                q2_price=round(q2_price, 2) if q2_symbol else 0.0,
                q2_days=q2_days,
                q1_q2_annualized_pct=round(q1_q2_annualized_pct, 2) if q1_q2_annualized_pct is not None else None,
                front_end_gap_pct=round(front_end_gap_pct, 2) if front_end_gap_pct is not None else None,
            )
        except Exception as exc:
            logger.warning("[MarketData] 获取 IC 期限结构失败: %s", exc)
            return None

    def get_ic_market_snapshot(self) -> Optional[ICMarketSnapshotData]:
        """获取 IC 现货与多合约期限结构快照。"""
        try:
            spot_price = self._load_csi500_spot_price()
            if spot_price is None or spot_price <= 0:
                logger.warning("[MarketData] 无法获取中证500现货价格(snapshot)")
                return None

            contracts = self._load_ic_realtime_contracts()
            if not contracts:
                near_contract = ""
                try:
                    import akshare as ak

                    main_contracts = ak.match_main_contract(symbol="cffex")
                    if isinstance(main_contracts, str):
                        for code in main_contracts.split(","):
                            code = code.strip().upper()
                            if code.startswith("IC"):
                                near_contract = code
                                break
                except Exception as exc:
                    logger.debug("[MarketData] 匹配 CFFEX 主力合约失败(snapshot): %s", exc)
                if near_contract:
                    contracts = self._load_ic_contracts_via_spot_fallback(near_contract)

            if not contracts:
                logger.warning("[MarketData] 无法获取 IC 实时合约行情(snapshot)")
                return None

            main_contract_code = ""
            try:
                import akshare as ak

                main_contracts = ak.match_main_contract(symbol="cffex")
                if isinstance(main_contracts, str):
                    for code in main_contracts.split(","):
                        code = code.strip().upper()
                        if code.startswith("IC"):
                            main_contract_code = code
                            break
            except Exception as exc:
                logger.debug("[MarketData] 匹配 CFFEX 主力合约失败(snapshot-main): %s", exc)

            now_date = datetime.now().date()
            base_days = max((contracts[0]["expiry"].date() - now_date).days, 1)
            snapshots: List[ICContractSnapshot] = []

            for item in contracts:
                expiry_date = item["expiry"].date()
                days_to_expiry = max((expiry_date - now_date).days, 1)
                price = float(item["trade"])
                basis = spot_price - price
                annualized = (basis / price) / (days_to_expiry / 365) * 100 if price > 0 else 0.0
                snapshots.append(
                    ICContractSnapshot(
                        symbol=str(item["symbol"]),
                        price=round(price, 2),
                        expiry_date=expiry_date.isoformat(),
                        days_to_expiry=days_to_expiry,
                        term_gap_days=max(days_to_expiry - base_days, 0),
                        basis=round(basis, 2),
                        annualized_basis_pct=round(annualized, 2),
                        is_main=str(item["symbol"]) == main_contract_code,
                    )
                )

            if not main_contract_code:
                main_contract_code = snapshots[0].symbol
                snapshots[0].is_main = True

            option_proxy = None
            try:
                option_proxy = self.get_500etf_option_proxy()
            except Exception as exc:
                logger.debug("[MarketData] 500ETF 期权代理并入 IC 快照失败: %s", exc)

            return ICMarketSnapshotData(
                spot_price=round(float(spot_price), 2),
                main_contract_code=main_contract_code,
                fetched_at=datetime.now().isoformat(),
                contracts=snapshots,
                option_proxy=option_proxy,
            )
        except Exception as exc:
            logger.warning("[MarketData] 获取 IC 行情快照失败: %s", exc)
            return None

    def get_historical_kline(self, symbol: str, source: str = "futures_sina") -> Optional[pd.DataFrame]:
        """获取品种历史日K线数据

        Args:
            symbol: 品种代码
                - 期货: "AU0"(黄金), "AG0"(白银), "IC0"(中证500期货)
                - 指数: "000905"(中证500指数)
            source: 数据源
                - "futures_sina": 新浪期货主力连续合约
                - "index_zh_a": 东方财富A股指数

        Returns:
            标准化后的 DataFrame，列名: date/open/high/low/close/volume
            失败返回 None
        """
        try:
            import akshare as ak

            if source == "futures_sina":
                df = ak.futures_main_sina(symbol=symbol)
                if df is None or len(df) == 0:
                    return None
                # 中文列名 -> 英文列名映射
                col_map = {
                    '日期': 'date', '开盘价': 'open', '最高价': 'high',
                    '最低价': 'low', '收盘价': 'close', '成交量': 'volume',
                }
                df = df.rename(columns=col_map)
                df = df[[c for c in ['date', 'open', 'high', 'low', 'close', 'volume'] if c in df.columns]]

            elif source == "index_zh_a":
                df = None
                normalized_symbol = symbol
                if symbol.isdigit():
                    normalized_symbol = f"sh{symbol}"
                try:
                    df = ak.stock_zh_index_daily(symbol=normalized_symbol)
                except Exception:
                    df = None

                if df is None or len(df) == 0:
                    df = ak.index_zh_a_hist(symbol=symbol, period="daily")
                    if df is None or len(df) == 0:
                        return None
                    col_map = {
                        '日期': 'date', '开盘': 'open', '最高': 'high',
                        '最低': 'low', '收盘': 'close', '成交量': 'volume',
                    }
                    df = df.rename(columns=col_map)
                    df = df[[c for c in ['date', 'open', 'high', 'low', 'close', 'volume'] if c in df.columns]]
                else:
                    df = df[[c for c in ['date', 'open', 'high', 'low', 'close', 'volume'] if c in df.columns]]

            else:
                logger.warning(f"[MarketData] 未知数据源: {source}")
                return None

            # 确保数值列为 float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df = df.dropna(subset=['close']).reset_index(drop=True)
            logger.info(f"[MarketData] 获取 {symbol} 历史K线成功: {len(df)} 条")
            return df

        except Exception as e:
            logger.warning(f"[MarketData] 获取 {symbol} 历史K线失败: {e}")
            return None

    def close(self):
        """关闭 MCP 会话"""
        self._cleanup_proc()
