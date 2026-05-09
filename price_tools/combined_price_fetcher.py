"""
Polymarket Up/Down + Binance BTC 历史秒级数据抓取脚本
=====================================================

功能：
    抓取 Polymarket 上过去 N 个"Bitcoin Up or Down"五分钟市场的 Up/Down
    秒级价格历史，同时抓取相同时间窗口的 Binance BTC 秒级价格，按秒配对存储。

输出 JSON 结构：
    {
      "Bitcoin Up or Down - May 3, 5:00PM-5:05PM ET": {
        "condition_id": "...",
        "token_ids": {"Up": "...", "Down": "..."},
        "window_et": {"start": "...", "end": "..."},
        "data": [
          {
            "timestamp": 1714762800,
            "datetime_et": "2026-05-03 17:00:00 EDT",
            "up_price": 0.55,
            "down_price": 0.45,
            "btc_price": 97234.56
          },
          ...
        ]
      },
      ...
    }

用法：
    # 抓取最近 288 个 BTC 5min 市场（≈过去 24 小时）
    python combined_price_fetcher.py --markets 288

    # 抓取最近 12 个市场（≈过去 1 小时）
    python combined_price_fetcher.py --markets 12

    # 抓取其他币种
    python combined_price_fetcher.py --markets 100 --asset ethereum

依赖：
    pip install requests
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

ET_ZONE = ZoneInfo("America/New_York")
UTC_ZONE = ZoneInfo("UTC")

PAGE_SIZE = 500
MAX_OFFSET = 5000

SYMBOL_GUESS = {
    "bitcoin": "BTCUSDT",
    "btc": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "ether": "ETHUSDT",
    "eth": "ETHUSDT",
    "solana": "SOLUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "dogecoin": "DOGEUSDT",
    "doge": "DOGEUSDT",
}

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# 解析市场名，如 "Bitcoin Up or Down - May 3, 5:00PM-5:05PM ET"
POSITION_RE = re.compile(
    r"""
    ^\s*
    (?P<asset>[A-Za-z]+)\s+Up\s+or\s+Down\s*-\s*
    (?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})\s*,\s*
    (?P<start>\d{1,2}(?::\d{2})?\s*[AP]M)\s*-\s*
    (?P<end>\d{1,2}(?::\d{2})?\s*[AP]M)\s*ET
    \s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "polymarket-combined-fetcher/2.0"})
    return s


# ═══════════════════════════════════════════════════════════════════════════════
# 市场名解析
# ═══════════════════════════════════════════════════════════════════════════════

def parse_market_window(question: str, year: int) -> Optional[tuple[datetime, datetime]]:
    """从市场名解析时间窗口（ET 时间）"""
    m = POSITION_RE.match(question)
    if not m:
        return None

    month = MONTHS.get(m.group("month").lower())
    if not month:
        return None
    day = int(m.group("day"))

    def parse_clock(s: str) -> tuple[int, int]:
        s = s.strip().upper().replace(" ", "")
        if ":" in s:
            hh, rest = s.split(":")
            mm = rest[:-2]
            ampm = rest[-2:]
        else:
            hh = s[:-2]
            mm = "0"
            ampm = s[-2:]
        h = int(hh) % 12
        if ampm == "PM":
            h += 12
        return h, int(mm)

    sh, sm = parse_clock(m.group("start"))
    eh, em = parse_clock(m.group("end"))

    start_et = datetime(year, month, day, sh, sm, tzinfo=ET_ZONE)
    end_et = datetime(year, month, day, eh, em, tzinfo=ET_ZONE)

    if end_et <= start_et:
        end_et += timedelta(days=1)

    return start_et, end_et


# ═══════════════════════════════════════════════════════════════════════════════
# 历史市场发现
# ═══════════════════════════════════════════════════════════════════════════════

def discover_historical_markets(
    session: requests.Session,
    asset: str,
    count: int,
) -> list[dict]:
    """
    发现过去 N 个已结算的 5 分钟市场（按结束时间倒序）。

    用 end_date_max 滚动窗口翻页拉取已 closed 的市场，过滤：
      - 标题为 "<asset> Up or Down - <date>, <HH:MM>AM/PM-<HH:MM>AM/PM ET"
      - 5 分钟窗口（end - start == 5min）
    """
    from datetime import timezone as _tz

    search_key = f"{asset} up or down"
    print(f"[i] 搜索历史市场: {search_key} (目标 {count} 个)")

    found: list[dict] = []
    seen_cids: set[str] = set()
    page_size = 500
    now_year = datetime.now(ET_ZONE).year
    end_max_dt = datetime.now(_tz.utc)
    iter_count = 0

    while len(found) < count and iter_count < 50:
        iter_count += 1
        end_max_iso = end_max_dt.isoformat()
        try:
            resp = session.get(
                f"{GAMMA_API}/markets",
                params={
                    "limit": page_size,
                    "order": "endDate",
                    "ascending": "false",
                    "closed": "true",
                    "end_date_max": end_max_iso,
                },
                timeout=20,
            )
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            print(f"  [!] Gamma 拉取失败: {e}")
            break

        if not page:
            break

        new_in_batch = 0
        oldest_end_in_page: Optional[datetime] = None

        for m in page:
            end_iso = m.get("endDate", "")
            if end_iso:
                try:
                    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                    if oldest_end_in_page is None or end_dt < oldest_end_in_page:
                        oldest_end_in_page = end_dt
                except ValueError:
                    pass

            q = m.get("question", "")
            if search_key not in q.lower():
                continue
            cid = m.get("conditionId", "")
            if not cid or cid in seen_cids:
                continue

            window = parse_market_window(q, now_year)
            if not window:
                continue
            start_et, end_et = window
            duration = (end_et - start_et).total_seconds()
            if abs(duration - 300) > 1:
                continue

            seen_cids.add(cid)
            found.append({
                "question": q,
                "condition_id": cid,
                "start_et": start_et,
                "end_et": end_et,
                "end_date_iso": end_iso,
            })
            new_in_batch += 1

            if len(found) >= count:
                break

        print(f"  iter {iter_count} | 扫描到 {oldest_end_in_page.isoformat() if oldest_end_in_page else '?'} | "
              f"本批 +{new_in_batch} | 累计 {len(found)}/{count}")

        if len(page) < page_size or oldest_end_in_page is None:
            break

        # 下一轮从这一页最早的 endDate 继续往前
        end_max_dt = oldest_end_in_page - timedelta(seconds=1)
        time.sleep(0.15)

    return found[:count]


def fetch_token_ids(
    session: requests.Session, condition_id: str
) -> tuple[str, str]:
    """从 CLOB API 获取 (up_token_id, down_token_id)"""
    try:
        r = session.get(f"{CLOB_API}/markets/{condition_id}", timeout=10)
        if r.status_code != 200:
            return "", ""
        data = r.json()
    except Exception:
        return "", ""

    up_token = ""
    down_token = ""
    for tk in data.get("tokens", []):
        outcome = (tk.get("outcome") or "").strip().lower()
        tid = tk.get("token_id", "")
        if outcome == "up":
            up_token = tid
        elif outcome == "down":
            down_token = tid

    return up_token, down_token


# ═══════════════════════════════════════════════════════════════════════════════
# Polymarket 秒级历史价格
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_trade_tape(
    session: requests.Session, condition_id: str, outcome: str
) -> list[tuple[int, float]]:
    """
    拉取整个市场（全部用户）该 outcome 侧的成交记录，按时间升序返回。
    """
    all_trades: list[dict] = []
    offset = 0
    while offset <= MAX_OFFSET:
        try:
            r = session.get(
                f"{DATA_API}/trades",
                params={
                    "market": condition_id,
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "takerOnly": "false",
                },
                timeout=30,
            )
            if r.status_code != 200:
                break
            page = r.json()
            if not page:
                break
            all_trades.extend(x for x in page if isinstance(x, dict))
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            time.sleep(0.1)
        except Exception:
            break

    # 取每秒最后一笔成交价
    by_ts: dict[int, float] = {}
    for t in all_trades:
        if t.get("outcome", "").lower() != outcome.lower():
            continue
        ts = int(t.get("timestamp", 0) or 0)
        try:
            p = float(t.get("price", 0) or 0)
        except (ValueError, TypeError):
            continue
        if ts and p > 0:
            by_ts[ts] = p
    return sorted(by_ts.items())


def fetch_clob_price_history(
    session: requests.Session, token_id: str,
    start_ts: int, end_ts: int,
) -> list[tuple[int, float]]:
    """从 CLOB prices-history 获取指定窗口的价格点（兜底用）"""
    if not token_id:
        return []

    try:
        r = session.get(
            f"{CLOB_API}/prices-history",
            params={
                "market": token_id,
                "startTs": start_ts,
                "endTs": end_ts,
                "fidelity": 1,
            },
            timeout=20,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        history = data.get("history", data) if isinstance(data, dict) else data
        if not isinstance(history, list):
            return []
        out: list[tuple[int, float]] = []
        for pt in history:
            t = pt.get("t")
            p = pt.get("p")
            if t is not None and p is not None:
                try:
                    out.append((int(t), float(p)))
                except (ValueError, TypeError):
                    continue
        out.sort()
        return out
    except Exception:
        return []


def build_per_second_prices(
    tape: list[tuple[int, float]],
    clob_history: list[tuple[int, float]],
    start_ts: int, end_ts: int,
) -> dict[int, float]:
    """
    把成交 tape 和 CLOB 历史合并成"每秒一个价格"的字典。

    优先用 tape（真实成交价），缺失的秒用最近的 tape 或 CLOB 点向前填充。
    """
    merged: dict[int, float] = {}
    for ts, p in clob_history:
        merged[ts] = p
    for ts, p in tape:
        merged[ts] = p

    if not merged:
        return {}

    sorted_pts = sorted(merged.items())
    result: dict[int, float] = {}
    last_price: Optional[float] = None
    idx = 0

    # 先找到 start_ts 之前的最近一个价格作为初始值
    for ts, p in sorted_pts:
        if ts <= start_ts:
            last_price = p
        else:
            break

    for sec in range(start_ts, end_ts + 1):
        while idx < len(sorted_pts) and sorted_pts[idx][0] <= sec:
            last_price = sorted_pts[idx][1]
            idx += 1
        if last_price is not None:
            result[sec] = last_price

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Binance 秒级价格
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_binance_klines(
    session: requests.Session,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[list[Any]]:
    """分页抓取 Binance 1s K 线"""
    all_rows: list[list[Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        params = {
            "symbol": symbol,
            "interval": "1s",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        }
        try:
            r = session.get(BINANCE_KLINES_URL, params=params, timeout=15)
            r.raise_for_status()
            batch = r.json()
        except Exception:
            break
        if not batch:
            break
        all_rows.extend(batch)
        last_close = batch[-1][6]
        next_cursor = last_close + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < 1000:
            break
        time.sleep(0.1)
    return all_rows


def build_btc_per_second(
    klines: list[list[Any]],
    start_ts: int, end_ts: int,
) -> dict[int, float]:
    """从 1s K 线构造 ts -> close_price 字典，缺失秒向前填充"""
    by_ts: dict[int, float] = {}
    for row in klines:
        open_ms = int(row[0])
        sec = open_ms // 1000
        try:
            close = float(row[4])
        except (ValueError, TypeError):
            continue
        by_ts[sec] = close

    if not by_ts:
        return {}

    sorted_secs = sorted(by_ts.keys())
    result: dict[int, float] = {}
    last_price: Optional[float] = None
    idx = 0

    for ts, p in sorted(by_ts.items()):
        if ts <= start_ts:
            last_price = p
        else:
            break

    for sec in range(start_ts, end_ts + 1):
        while idx < len(sorted_secs) and sorted_secs[idx] <= sec:
            last_price = by_ts[sorted_secs[idx]]
            idx += 1
        if last_price is not None:
            result[sec] = last_price

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 单个市场处理
# ═══════════════════════════════════════════════════════════════════════════════

def process_market(
    session: requests.Session,
    market: dict,
    binance_symbol: str,
) -> Optional[dict]:
    """处理单个市场，返回带 data 列表的结果对象"""
    cid = market["condition_id"]
    start_et: datetime = market["start_et"]
    end_et: datetime = market["end_et"]

    start_ts = int(start_et.timestamp())
    end_ts = int(end_et.timestamp())

    up_token, down_token = fetch_token_ids(session, cid)
    if not up_token or not down_token:
        return None

    # Polymarket: tape + CLOB history
    up_tape = fetch_market_trade_tape(session, cid, "Up")
    down_tape = fetch_market_trade_tape(session, cid, "Down")
    up_clob = fetch_clob_price_history(session, up_token, start_ts - 60, end_ts + 60)
    down_clob = fetch_clob_price_history(session, down_token, start_ts - 60, end_ts + 60)

    up_secs = build_per_second_prices(up_tape, up_clob, start_ts, end_ts)
    down_secs = build_per_second_prices(down_tape, down_clob, start_ts, end_ts)

    # Binance
    klines = fetch_binance_klines(
        session, binance_symbol, start_ts * 1000, end_ts * 1000
    )
    btc_secs = build_btc_per_second(klines, start_ts, end_ts)

    # 配对组装
    data_points: list[dict] = []
    for sec in range(start_ts, end_ts + 1):
        et_dt = datetime.fromtimestamp(sec, tz=ET_ZONE)
        up_p = up_secs.get(sec)
        down_p = down_secs.get(sec)
        btc_p = btc_secs.get(sec)
        data_points.append({
            "timestamp": sec,
            "datetime_et": et_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "up_price": round(up_p, 4) if up_p is not None else None,
            "down_price": round(down_p, 4) if down_p is not None else None,
            "btc_price": round(btc_p, 2) if btc_p is not None else None,
        })

    return {
        "condition_id": cid,
        "token_ids": {"Up": up_token, "Down": down_token},
        "window_et": {
            "start": start_et.isoformat(),
            "end": end_et.isoformat(),
        },
        "data": data_points,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════════════

def _save_json(data: dict, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polymarket Up/Down + Binance BTC 历史秒级数据抓取",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--asset", default="bitcoin",
        help="资产名称（默认 bitcoin，可选 ethereum/solana/dogecoin）",
    )
    parser.add_argument(
        "--markets", type=int, default=288,
        help="抓取过去多少个 5 分钟市场（默认 288 ≈ 24 小时）",
    )
    parser.add_argument(
        "--output", default="",
        help="输出 JSON 文件路径（默认自动生成）",
    )
    args = parser.parse_args()

    asset = args.asset.lower()
    binance_symbol = SYMBOL_GUESS.get(asset)
    if not binance_symbol:
        print(f"[!] 未知资产 {asset}，已知: {list(SYMBOL_GUESS.keys())}")
        return 1

    session = make_session()

    print("=" * 60)
    print("  Polymarket Up/Down + Binance 历史数据抓取")
    print("=" * 60)
    print(f"  资产       : {asset} ({binance_symbol})")
    print(f"  目标市场数 : {args.markets}")

    # 1. 发现历史市场
    markets = discover_historical_markets(session, asset, args.markets)
    if not markets:
        print("[!] 未找到匹配的历史市场")
        return 1

    print(f"\n[i] 找到 {len(markets)} 个市场")
    print(f"[i] 时间范围: {markets[-1]['question']}  →  {markets[0]['question']}")

    # 2. 输出路径
    if args.output:
        output_path = Path(args.output)
    else:
        now = datetime.now(ET_ZONE)
        tag = now.strftime("%Y%m%d_%H%M")
        output_path = Path(f"historical_data_{asset}_{args.markets}_{tag}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 3. 处理每个市场
    result: dict[str, Any] = {}
    failed = 0
    for i, m in enumerate(markets, 1):
        q = m["question"]
        print(f"\n[{i}/{len(markets)}] {q}")
        try:
            payload = process_market(session, m, binance_symbol)
        except Exception as e:
            print(f"  [!] 处理失败: {e}")
            failed += 1
            continue
        if payload is None:
            print(f"  [!] 跳过：token_id 缺失")
            failed += 1
            continue
        result[q] = payload
        n = len(payload["data"])
        n_up = sum(1 for d in payload["data"] if d["up_price"] is not None)
        n_btc = sum(1 for d in payload["data"] if d["btc_price"] is not None)
        print(f"  ✓ {n} 个数据点 (up 覆盖 {n_up}, btc 覆盖 {n_btc})")

        # 每 10 个市场保存一次
        if i % 10 == 0:
            _save_json(result, output_path)

    _save_json(result, output_path)

    print(f"\n{'=' * 60}")
    print(f"[✓] 完成")
    print(f"[✓] 成功 {len(result)} 个，失败 {failed} 个")
    print(f"[✓] 输出: {output_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
