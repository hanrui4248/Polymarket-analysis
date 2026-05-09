"""
Polymarket L2 订单簿数据下载器
用法: python download_poly_data.py --date 2026-04-10 --coin btc --timeframe 5m
"""

import argparse
import pandas as pd
from datetime import datetime, timedelta, timezone

TIMEFRAME_MINUTES = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

BASE_URL = "https://s.wangshuox.com/poly_l2"
STORAGE_OPTIONS = {"User-Agent": "Mozilla/5.0"}


def download_day(date_str: str, coin: str = "btc", timeframe: str = "5m"):
    minutes = TIMEFRAME_MINUTES.get(timeframe)
    if minutes is None:
        raise ValueError(f"不支持的 timeframe: {timeframe}，可选: {list(TIMEFRAME_MINUTES.keys())}")

    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    total = int(1440 / minutes)

    print(f"下载 {coin}-updown-{timeframe} | 日期: {date_str} | 共 {total} 个窗口")
    print("-" * 50)

    dfs = []
    success = 0
    failed = 0
    current = start

    while current < end:
        ts = int(current.timestamp())
        url = f"{BASE_URL}/{coin}-updown-{timeframe}-{ts}.parquet"
        time_label = current.strftime("%H:%M")
        try:
            df = pd.read_parquet(url, storage_options=STORAGE_OPTIONS)
            dfs.append(df)
            success += 1
            print(f"  ✓ {time_label}  ({len(df)} rows)")
        except Exception as e:
            failed += 1
            print(f"  ✗ {time_label}  ({e})")
        current += timedelta(minutes=minutes)

    print("-" * 50)
    print(f"完成: {success} 成功, {failed} 失败")

    if not dfs:
        print("没有下载到任何数据")
        return

    result = pd.concat(dfs, ignore_index=True)
    out_file = f"data/{coin}-{timeframe}-{date_str}.parquet"
    result.to_parquet(out_file)
    size_mb = round(__import__("os").path.getsize(out_file) / 1024 / 1024, 1)
    print(f"已保存: {out_file} ({len(result)} rows, {size_mb} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket L2 数据下载器")
    parser.add_argument("--date", required=True, help="日期，格式 YYYY-MM-DD")
    parser.add_argument("--coin", default="btc", help="币种 (默认: btc)")
    parser.add_argument("--timeframe", default="5m", help="时间周期: 5m/15m/1h/4h/1d (默认: 5m)")
    args = parser.parse_args()

    download_day(args.date, args.coin, args.timeframe)
