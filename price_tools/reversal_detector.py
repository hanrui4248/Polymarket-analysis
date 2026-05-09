"""
涨跌反转检测器
=============

扫描指定目录下由 binance_price_fetcher.py 生成的 JSON 价格文件，
检测哪些 5 分钟市场发生了涨跌方向反转（价格穿越开盘基准价）。

反转定义:
    以 records[0]["close"] 为开盘基准价，每次价格从 Up 侧穿越到 Down 侧
    （或反之）即记为一次反转。可检测 1 次、2 次乃至 n 次反转。

用法:
    # 扫描目录，显示所有市场
    python price_tools/reversal_detector.py data/test_log

    # 只输出发生过反转的场次
    python price_tools/reversal_detector.py data/test_log --only-reversals

    # 只看最后 60 秒内发生的反转
    python price_tools/reversal_detector.py data/test_log --tail 60

    # 输出 JSON 格式
    python price_tools/reversal_detector.py data/test_log --json
"""

from __future__ import annotations

import argparse
import json
import sys
from glob import glob
from pathlib import Path
from typing import Any


def load_market(path: Path) -> dict[str, Any] | None:
    try:
        with path.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _direction(price: float, open_price: float) -> str:
    return "Up" if price > open_price else "Down"


def analyze_market(
    data: dict[str, Any],
    window_sec: int = 300,
    tail_sec: int | None = None,
) -> dict[str, Any]:
    records = data["records"]
    start_ms = records[0]["open_time_ms"]
    end_ms = start_ms + window_sec * 1000
    open_price = records[0]["open"]

    tail_start_ms = end_ms - tail_sec * 1000 if tail_sec else start_ms

    cur_dir: str | None = None
    crossings: list[dict[str, Any]] = []
    close_price = open_price

    for r in records:
        t = r["open_time_ms"]
        if t >= end_ms:
            break
        close_price = r["close"]
        if close_price == open_price:
            continue
        d = _direction(close_price, open_price)
        if cur_dir is not None and d != cur_dir:
            elapsed_sec = (t - start_ms) / 1000
            in_tail = t >= tail_start_ms
            crossings.append({
                "time_et": r["open_time_et"],
                "elapsed_sec": elapsed_sec,
                "from_dir": cur_dir,
                "to_dir": d,
                "price": close_price,
                "delta": close_price - open_price,
                "in_tail": in_tail,
            })
        cur_dir = d

    tail_crossings = [c for c in crossings if c["in_tail"]]
    dir_final = _direction(close_price, open_price) if close_price != open_price else "Flat"

    return {
        "window_et": data.get("window_et", {}),
        "symbol": data.get("symbol", ""),
        "open": open_price,
        "close": close_price,
        "delta_final": close_price - open_price,
        "dir_final": dir_final,
        "total_crossings": len(crossings),
        "tail_crossings": len(tail_crossings),
        "crossings": crossings,
        "tail_crossing_details": tail_crossings,
        "reversed": len(tail_crossings) > 0,
    }


def fmt_delta(v: float) -> str:
    return f"{v:+.2f}"


def print_table(results: list[tuple[str, dict]], only_reversals: bool, tail_sec: int | None) -> None:
    tail_label = f"尾部{tail_sec}s" if tail_sec else "全程"
    header = (
        f"{'市场':<35} {'开盘基准':>10} {'收盘价':>10} {'涨跌':>10} "
        f"{'最终方向':>8} {'全程穿越':>8} {tail_label + '穿越':>10}"
    )
    sep = "=" * len(header)

    print(sep)
    print(header)
    print(sep)

    reversal_count = 0
    for name, r in results:
        if only_reversals and not r["reversed"]:
            continue
        if r["reversed"]:
            reversal_count += 1
        tail_n = r["tail_crossings"]
        marker = f"← {tail_n}次反转" if tail_n > 0 else ""
        print(
            f"{name:<35} {r['open']:>10.2f} {r['close']:>10.2f} "
            f"{fmt_delta(r['delta_final']):>10} "
            f"{r['dir_final']:>8} {r['total_crossings']:>8} "
            f"{tail_n:>10} {marker}"
        )

    print(sep)
    total = len(results)
    print(f"\n共 {total} 个市场, 其中 {reversal_count} 个发生过穿越反转")

    if reversal_count:
        print()
        idx = 0
        for name, r in results:
            if not r["reversed"]:
                continue
            idx += 1
            start = r["window_et"].get("start", "")
            end = r["window_et"].get("end", "")
            print(f"  {idx}. {name}  (全程 {r['total_crossings']} 次穿越)")
            if start and end:
                print(f"     ET: {start} → {end}")
            print(f"     基准价 {r['open']:.2f}  →  收盘 {r['close']:.2f} ({r['dir_final']}, Δ{fmt_delta(r['delta_final'])})")
            print(f"     穿越明细:")
            for c in r["crossings"]:
                tail_tag = " [尾部]" if c["in_tail"] else ""
                print(
                    f"       t={c['elapsed_sec']:>5.0f}s  {c['from_dir']:>4}→{c['to_dir']:<4} "
                    f"price={c['price']:.2f} (Δ{fmt_delta(c['delta'])}){tail_tag}"
                )
            print()


def main() -> None:
    p = argparse.ArgumentParser(description="检测 BTC 5 分钟市场涨跌反转（穿越开盘基准价）")
    p.add_argument("dir", help="包含 binance_price_fetcher JSON 文件的目录")
    p.add_argument(
        "--window", type=int, default=300,
        help="市场窗口总秒数 (默认 300 = 5 分钟)",
    )
    p.add_argument(
        "--tail", type=int, default=60,
        help="只关注最后 N 秒内的反转 (默认 60s = 最后一分钟)",
    )
    p.add_argument("--only-reversals", action="store_true", help="只输出反转的场次")
    p.add_argument("--json", action="store_true", dest="json_out", help="输出 JSON 格式")
    args = p.parse_args()

    data_dir = Path(args.dir)
    if not data_dir.is_dir():
        print(f"[!] 目录不存在: {data_dir}", file=sys.stderr)
        sys.exit(1)

    json_files = sorted(data_dir.glob("*.json"))
    if not json_files:
        print(f"[!] 目录中没有 JSON 文件: {data_dir}", file=sys.stderr)
        sys.exit(1)

    results: list[tuple[str, dict]] = []
    for fp in json_files:
        data = load_market(fp)
        if data is None or "records" not in data or len(data["records"]) < 2:
            print(f"[!] 跳过无效文件: {fp.name}", file=sys.stderr)
            continue
        name = fp.stem.replace("BTCUSDT_1s_", "")
        r = analyze_market(data, window_sec=args.window, tail_sec=args.tail)
        results.append((name, r))

    if args.json_out:
        out = []
        for name, r in results:
            if args.only_reversals and not r["reversed"]:
                continue
            out.append({"file": name, **r})
        json.dump(out, sys.stdout, indent=2, ensure_ascii=False)
        print()
    else:
        tail_label = f"tail={args.tail}s" if args.tail else "全程"
        print(f"扫描 {data_dir}/, 共 {len(results)} 个市场, {tail_label}\n")
        print_table(results, args.only_reversals, args.tail)


if __name__ == "__main__":
    main()
