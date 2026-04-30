"""
Binance 秒级价格抓取脚本
=========================

功能：
    抓取 Binance 现货某交易对在指定 ET 时间窗口内的秒级 K 线数据，
    输出一个 JSON 数据文件和一个 HTML 价格曲线图。

时区：
    输入时间默认按 ET (America/New_York) 解释，会自动处理 EDT/EST
    夏令时切换，再转换为 UTC 毫秒时间戳调用 Binance API。

依赖：
    pip install requests
    （时区使用标准库 zoneinfo，需要 Python 3.9+）

用法：
    # 直接给 Polymarket 的市场名（推荐）
    python binance_price_fetcher.py --position "Bitcoin Up or Down - April 30, 2:45AM-2:50AM ET"

    # 或者显式指定起止时间（ET）
    python binance_price_fetcher.py \\
        --symbol BTCUSDT \\
        --start "2026-04-27 00:00:00" \\
        --end   "2026-04-27 00:05:00"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_AGG_TRADES_URL = "https://api.binance.com/api/v3/aggTrades"
ET_ZONE = ZoneInfo("America/New_York")
UTC_ZONE = ZoneInfo("UTC")

# 各种 Polymarket "Up or Down" 市场对应的 Binance 现货交易对
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

# ═══════════════════════════════════════════════════════════════════════════════
# Polymarket 市场名解析
# ═══════════════════════════════════════════════════════════════════════════════

POSITION_RE = re.compile(
    r"""
    ^\s*
    (?P<asset>[A-Za-z]+)\s+Up\s+or\s+Down\s*-\s*
    (?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})\s*,\s*
    (?P<start>\d{1,2}:\d{2}\s*[AP]M)\s*-\s*
    (?P<end>\d{1,2}:\d{2}\s*[AP]M)\s*ET
    \s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)


def parse_position(position: str, year: int) -> tuple[str, datetime, datetime]:
    """
    解析类似 "Bitcoin Up or Down - April 27, 12:00AM-12:05AM ET" 的市场名。

    Returns:
        (symbol, start_et, end_et) — 时间为带时区的 ET datetime
    """
    m = POSITION_RE.match(position)
    if not m:
        raise ValueError(
            f"无法解析 position 字符串：{position!r}\n"
            f"期望格式：'<Asset> Up or Down - <Month> <Day>, <HH:MM>AM/PM-<HH:MM>AM/PM ET'"
        )

    asset = m.group("asset").lower()
    symbol = SYMBOL_GUESS.get(asset)
    if not symbol:
        raise ValueError(
            f"未知资产 {asset!r}，请用 --symbol 显式指定 Binance 交易对。"
            f"已知映射：{SYMBOL_GUESS}"
        )

    month = MONTHS.get(m.group("month").lower())
    if not month:
        raise ValueError(f"未知月份：{m.group('month')!r}")
    day = int(m.group("day"))

    def parse_clock(s: str) -> tuple[int, int]:
        s = s.strip().upper().replace(" ", "")
        hh, rest = s.split(":")
        mm = rest[:-2]
        ampm = rest[-2:]
        h = int(hh) % 12
        if ampm == "PM":
            h += 12
        return h, int(mm)

    sh, sm = parse_clock(m.group("start"))
    eh, em = parse_clock(m.group("end"))

    start_et = datetime(year, month, day, sh, sm, tzinfo=ET_ZONE)
    end_et = datetime(year, month, day, eh, em, tzinfo=ET_ZONE)

    # 处理跨午夜的情况（极少见，但 12:00AM-12:05AM 实际是当天）
    if end_et <= start_et:
        end_et += timedelta(days=1)

    return symbol, start_et, end_et


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "binance-price-fetcher/1.0"})
    return s


# ═══════════════════════════════════════════════════════════════════════════════
# 抓取
# ═══════════════════════════════════════════════════════════════════════════════

def to_ms(dt: datetime) -> int:
    return int(dt.astimezone(UTC_ZONE).timestamp() * 1000)


def fetch_klines(
    session: requests.Session,
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1s",
) -> list[list[Any]]:
    """
    分页抓取 Binance K 线。Binance 每次最多返回 1000 条，
    1s 间隔下 1000 条 ≈ 16.67 分钟。
    """
    all_rows: list[list[Any]] = []
    cursor = start_ms

    while cursor < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        }
        r = session.get(BINANCE_KLINES_URL, params=params, timeout=15)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break

        all_rows.extend(batch)

        # 下一页从最后一条 close_time + 1 开始
        last_close = batch[-1][6]
        next_cursor = last_close + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor

        # 不到 limit 说明已经到末尾
        if len(batch) < 1000:
            break

        time.sleep(0.1)

    return all_rows


# ═══════════════════════════════════════════════════════════════════════════════
# 输出
# ═══════════════════════════════════════════════════════════════════════════════

def kline_to_record(row: list[Any]) -> dict[str, Any]:
    open_ms = int(row[0])
    close_ms = int(row[6])
    open_dt_utc = datetime.fromtimestamp(open_ms / 1000, tz=UTC_ZONE)
    open_dt_et = open_dt_utc.astimezone(ET_ZONE)
    return {
        "open_time_ms": open_ms,
        "open_time_et": open_dt_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "open_time_utc": open_dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
        "close_time_ms": close_ms,
        "quote_volume": float(row[7]),
        "trades": int(row[8]),
        "taker_buy_base_volume": float(row[9]),
        "taker_buy_quote_volume": float(row[10]),
    }


def write_json(payload: dict[str, Any], path: Path) -> None:
    with path.open("w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         margin: 24px; background: #fafafa; color: #222; }}
  h1 {{ font-size: 18px; margin: 0 0 4px; }}
  .meta {{ color: #666; font-size: 13px; margin-bottom: 16px; }}
  .meta span {{ margin-right: 18px; }}
  .stats {{ display: flex; gap: 24px; margin: 12px 0 16px;
            font-size: 13px; flex-wrap: wrap; }}
  .stats div b {{ color: #111; }}
  .up {{ color: #16a34a; }} .down {{ color: #dc2626; }}
  #chart-wrap {{ background: #fff; border: 1px solid #e5e5e5;
                 border-radius: 8px; padding: 16px; height: 520px; }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="meta">
  <span><b>Symbol:</b> {symbol}</span>
  <span><b>Interval:</b> {interval}</span>
  <span><b>ET:</b> {et_start} → {et_end}</span>
  <span><b>UTC:</b> {utc_start} → {utc_end}</span>
  <span><b>Points:</b> {count}</span>
</div>
<div class="stats">
  <div>Open: <b>{open_price:.4f}</b></div>
  <div>Close: <b>{close_price:.4f}</b></div>
  <div>High: <b>{high_price:.4f}</b></div>
  <div>Low: <b>{low_price:.4f}</b></div>
  <div>Change: <b class="{change_class}">{change:+.4f} ({change_pct:+.4f}%)</b></div>
</div>
<div id="chart-wrap"><canvas id="priceChart"></canvas></div>
<script>
const DATA = {data_json};
const ctx = document.getElementById('priceChart').getContext('2d');
const OPEN_PRICE = {open_price};
const openLinePlugin = {{
  id: 'openLine',
  afterDatasetsDraw(chart) {{
    const {{ ctx, chartArea, scales }} = chart;
    if (!chartArea) return;
    const y = scales.y.getPixelForValue(OPEN_PRICE);
    if (y < chartArea.top || y > chartArea.bottom) return;
    ctx.save();
    ctx.strokeStyle = '#888';
    ctx.lineWidth = 1;
    ctx.setLineDash([6, 4]);
    ctx.beginPath();
    ctx.moveTo(chartArea.left, y);
    ctx.lineTo(chartArea.right, y);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#555';
    ctx.font = '11px -apple-system, sans-serif';
    ctx.textBaseline = 'bottom';
    ctx.fillText('open ' + OPEN_PRICE.toFixed(4), chartArea.left + 6, y - 2);
    ctx.restore();
  }}
}};

new Chart(ctx, {{
  type: 'line',
  data: {{
    datasets: [{{
      label: '{symbol} close',
      data: DATA,
      borderColor: '#2563eb',
      backgroundColor: 'rgba(37,99,235,0.08)',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0,
      fill: true,
    }}]
  }},
  plugins: [openLinePlugin],
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          title: (items) => new Date(items[0].parsed.x).toLocaleString('en-US',
            {{ timeZone: 'America/New_York', hour12: false }}) + ' ET',
          label: (item) => item.parsed.y.toFixed(4) +
            ' (Δ ' + (item.parsed.y - OPEN_PRICE >= 0 ? '+' : '') +
            (item.parsed.y - OPEN_PRICE).toFixed(4) + ')',
        }}
      }}
    }},
    scales: {{
      x: {{
        type: 'time',
        time: {{ tooltipFormat: 'HH:mm:ss', displayFormats: {{ second: 'HH:mm:ss', minute: 'HH:mm' }} }},
        adapters: {{ date: {{ zone: 'America/New_York' }} }},
        ticks: {{ source: 'auto', maxRotation: 0, autoSkip: true }},
        title: {{ display: true, text: 'Time (ET)' }},
      }},
      y: {{
        title: {{ display: true, text: 'Price (USDT)' }},
        ticks: {{ callback: (v) => v.toFixed(2) }},
      }}
    }}
  }}
}});
</script>
</body>
</html>
"""


def write_html(payload: dict[str, Any], path: Path) -> None:
    records = payload["records"]
    closes = [r["close"] for r in records]
    highs = [r["high"] for r in records]
    lows = [r["low"] for r in records]
    open_p, close_p = closes[0], closes[-1]
    change = close_p - open_p
    change_pct = (close_p / open_p - 1) * 100 if open_p else 0.0

    chart_data = [{"x": r["open_time_ms"], "y": r["close"]} for r in records]

    html = HTML_TEMPLATE.format(
        title=f'{payload["symbol"]} {payload["interval"]} '
              f'{payload["window_et"]["start"][:19]} → {payload["window_et"]["end"][11:19]} ET',
        symbol=payload["symbol"],
        interval=payload["interval"],
        et_start=payload["window_et"]["start"],
        et_end=payload["window_et"]["end"],
        utc_start=payload["window_utc"]["start"],
        utc_end=payload["window_utc"]["end"],
        count=payload["count"],
        open_price=open_p,
        close_price=close_p,
        high_price=max(highs),
        low_price=min(lows),
        change=change,
        change_pct=change_pct,
        change_class="up" if change >= 0 else "down",
        data_json=json.dumps(chart_data),
    )
    path.write_text(html, encoding="utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_local_dt(s: str) -> datetime:
    """解析 '2026-04-27 00:00:00' 这种字符串为 ET 时间。"""
    fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=ET_ZONE)
        except ValueError:
            continue
    raise ValueError(f"无法解析时间：{s!r}")


def main() -> int:
    p = argparse.ArgumentParser(
        description="抓取 Binance 现货秒级 K 线（输入时间按 ET 解释）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--position",
        help='Polymarket 市场名，如 "Bitcoin Up or Down - April 27, 12:00AM-12:05AM ET"',
    )
    p.add_argument("--symbol", help="Binance 交易对，如 BTCUSDT。不给会从 --position 推断。")
    p.add_argument("--start", help="起始时间（ET），如 '2026-04-27 00:00:00'")
    p.add_argument("--end", help="结束时间（ET），如 '2026-04-27 00:05:00'")
    p.add_argument("--year", type=int, default=datetime.now(ET_ZONE).year,
                   help="解析 --position 时使用的年份（默认今年）")
    p.add_argument("--interval", default="1s",
                   help="K 线间隔，1s/1m/5m 等（默认 1s）")
    p.add_argument("--output-dir", default="./crypto_price", help="输出目录")
    p.add_argument("--prefix", help="文件名前缀，默认根据 symbol+时间生成")
    args = p.parse_args()

    # 解析时间窗口
    if args.position:
        symbol_guess, start_et, end_et = parse_position(args.position, args.year)
        symbol = args.symbol or symbol_guess
    else:
        if not (args.start and args.end and args.symbol):
            p.error("必须给 --position，或同时给 --symbol --start --end")
        symbol = args.symbol
        start_et = parse_local_dt(args.start)
        end_et = parse_local_dt(args.end)

    start_ms = to_ms(start_et)
    end_ms = to_ms(end_et)

    print(f"[i] symbol   : {symbol}")
    print(f"[i] interval : {args.interval}")
    print(f"[i] window ET: {start_et.isoformat()}  →  {end_et.isoformat()}")
    print(f"[i] window UT: {start_et.astimezone(UTC_ZONE).isoformat()}  →  {end_et.astimezone(UTC_ZONE).isoformat()}")
    print(f"[i] window ms: {start_ms}  →  {end_ms}  ({(end_ms - start_ms) / 1000:.0f}s)")

    # 抓取
    session = make_session()
    print(f"[i] fetching klines...")
    raw = fetch_klines(session, symbol, start_ms, end_ms, args.interval)
    records = [kline_to_record(r) for r in raw]
    print(f"[i] got {len(records)} klines")

    if not records:
        print("[!] Binance 返回空数据，请检查交易对、时间窗口、interval 是否被支持。",
              file=sys.stderr)
        return 1

    # 简单统计
    closes = [r["close"] for r in records]
    print(f"[i] price range: {min(r['low'] for r in records):.4f} ~ {max(r['high'] for r in records):.4f}")
    print(f"[i] close first/last: {closes[0]:.4f} → {closes[-1]:.4f}  "
          f"(Δ {closes[-1] - closes[0]:+.4f}, {(closes[-1] / closes[0] - 1) * 100:+.4f}%)")

    # 输出
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.prefix:
        prefix = args.prefix
    else:
        tag = start_et.strftime("%Y%m%d_%H%M") + "-" + end_et.strftime("%H%M") + "ET"
        prefix = f"{symbol}_{args.interval}_{tag}"

    json_path = out_dir / f"{prefix}.json"
    html_path = out_dir / f"{prefix}.html"

    payload = {
        "symbol": symbol,
        "interval": args.interval,
        "window_et": {
            "start": start_et.isoformat(),
            "end": end_et.isoformat(),
        },
        "window_utc": {
            "start": start_et.astimezone(UTC_ZONE).isoformat(),
            "end": end_et.astimezone(UTC_ZONE).isoformat(),
        },
        "count": len(records),
        "records": records,
    }
    write_json(payload, json_path)
    write_html(payload, html_path)

    print(f"[✓] wrote {json_path}")
    print(f"[✓] wrote {html_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
