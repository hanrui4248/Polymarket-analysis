"""批量并行抓取所有5分钟窗口的Binance价格数据"""
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
OUT_DIR = "data/test_log"

def fmt_time(dt):
    h = dt.hour % 12
    if h == 0:
        h = 12
    ampm = "AM" if dt.hour < 12 else "PM"
    return f"{h}:{dt.minute:02d}{ampm}"

def generate_positions():
    start = datetime(2026, 5, 6, 22, 0, tzinfo=ET)
    end = datetime(2026, 5, 7, 11, 0, tzinfo=ET)
    positions = []
    t = start
    while t < end:
        t2 = t + timedelta(minutes=5)
        month = t.strftime("%B")
        pos = f"Bitcoin Up or Down - {month} {t.day}, {fmt_time(t)}-{fmt_time(t2)} ET"
        positions.append(pos)
        t = t2
    return positions

def fetch_one(pos):
    try:
        r = subprocess.run(
            [sys.executable, "price_tools/binance_price_fetcher.py",
             "--position", pos, "--output-dir", OUT_DIR],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                if line.startswith("[✓]"):
                    return (pos, "OK", line)
            return (pos, "OK", r.stdout.splitlines()[-1] if r.stdout.strip() else "")
        else:
            return (pos, "FAIL", r.stderr.strip().splitlines()[-1] if r.stderr.strip() else r.stdout.strip())
    except subprocess.TimeoutExpired:
        return (pos, "TIMEOUT", "")
    except Exception as e:
        return (pos, "ERROR", str(e))

def main():
    positions = generate_positions()
    print(f"[*] 共 {len(positions)} 个市场，开始并行抓取 (4 workers)...")

    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_one, p): p for p in positions}
        for i, fut in enumerate(as_completed(futures), 1):
            pos, status, msg = fut.result()
            short = pos.split(" - ")[1]
            if status == "OK":
                ok += 1
                print(f"  [{i:3d}/156] ✓ {short}")
            else:
                fail += 1
                print(f"  [{i:3d}/156] ✗ {short} — {status}: {msg}")

    print(f"\n[*] 完成: {ok} 成功, {fail} 失败")

if __name__ == "__main__":
    main()
