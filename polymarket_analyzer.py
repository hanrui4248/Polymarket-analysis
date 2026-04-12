"""
Polymarket 交易数据爬取 & 分析脚本
地址: 0x86233274B645FeD316C6eD72352b158aF53a3C3b

运行方式:
    pip install requests
    python polymarket_analyzer.py
"""

import requests
import json
from collections import defaultdict

WALLET = "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a"
BASE_URL = "https://data-api.polymarket.com"

MAX_API_OFFSET = 10000  # data-api.polymarket.com 的 offset 上限


def fetch_activity_by_type(wallet: str, activity_type: str, max_records: int = 1000) -> list[dict]:
    """
    分页抓取指定类型的活动记录，最多 max_records 条。
    使用 DESC 排序，优先获取最近的记录；受 API offset 上限(10000)约束。
    """
    all_records = []
    offset = 0
    limit = 500  # API 单页最大值

    print(f"📡 正在抓取 {activity_type} 记录（最多 {max_records} 条，取最近数据）...")

    while len(all_records) < max_records:
        if offset > MAX_API_OFFSET:
            print(f"  ⚠️  已达 API offset 上限({MAX_API_OFFSET})，停止分页。")
            break

        url = f"{BASE_URL}/activity"
        params = {
            "user": wallet,
            "limit": limit,
            "offset": offset,
            "type": activity_type,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",  # 最近的记录优先
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        all_records.extend(data)
        print(f"  已获取 {len(all_records)} 条记录 (offset={offset})...")

        if len(data) < limit:
            break  # 最后一页
        offset += limit

    return all_records[:max_records]


def analyze(records: list[dict]):
    """统计交易数据"""
    total = len(records)
    buys = [r for r in records if r.get("side") == "BUY"]
    sells = [r for r in records if r.get("side") == "SELL"]

    # usdcSize: 每笔交易花费/获得的 USDC
    total_spent   = sum(r.get("usdcSize", 0) for r in buys)
    total_received = sum(r.get("usdcSize", 0) for r in sells)

    # 还需要加上 REDEEM（市场结算时赎回的钱）
    print("\n⚠️  注意: 纯 TRADE 类型不含市场结算(REDEEM)收入，以下 PnL 仅为买卖差。")
    print("     若需精确 PnL，建议同时抓取 REDEEM 类型记录（见脚本末尾扩展）。\n")

    net_pnl = total_received - total_spent  # 正=盈利, 负=亏损

    print("=" * 55)
    print(f"  钱包地址   : {WALLET}")
    print(f"  总交易笔数 : {total:,} 笔")
    print(f"  买入次数   : {len(buys):,} 次  (共花费  ${total_spent:,.2f} USDC)")
    print(f"  卖出次数   : {len(sells):,} 次  (共获得  ${total_received:,.2f} USDC)")
    print("-" * 55)
    if net_pnl >= 0:
        print(f"  买卖差 PnL : +${net_pnl:,.2f} USDC  ✅ 盈利")
    else:
        print(f"  买卖差 PnL : -${abs(net_pnl):,.2f} USDC  ❌ 亏损")
    print("=" * 55)

    # 按市场拆分
    by_market = defaultdict(lambda: {"buys": 0, "sells": 0, "spent": 0.0, "received": 0.0})
    for r in records:
        title = r.get("title", "Unknown")[:60]
        side  = r.get("side", "")
        usdc  = r.get("usdcSize", 0)
        if side == "BUY":
            by_market[title]["buys"] += 1
            by_market[title]["spent"] += usdc
        elif side == "SELL":
            by_market[title]["sells"] += 1
            by_market[title]["received"] += usdc

    print("\n📊 各市场明细 (按净亏损排序):")
    print(f"  {'市场':<62} {'买':>4} {'卖':>4} {'净PnL':>10}")
    print("  " + "-" * 85)
    market_pnls = []
    for title, d in by_market.items():
        pnl = d["received"] - d["spent"]
        market_pnls.append((title, d["buys"], d["sells"], pnl))
    market_pnls.sort(key=lambda x: x[3])  # 亏损最多的排前面

    for title, b, s, pnl in market_pnls:
        sign = "+" if pnl >= 0 else ""
        print(f"  {title:<62} {b:>4} {s:>4} {sign}{pnl:>9.2f}")

    # 保存原始数据
    with open("polymarket_trades_raw.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\n💾 原始数据已保存至 polymarket_trades_raw.json ({total} 条)")


def fetch_redeems(wallet: str, max_records: int = 1000) -> float:
    """
    抓取赎回收入，与 TRADE 使用相同的 max_records 上限，保证口径一致。
    两者都取最近的 max_records 条记录。
    """
    redeem_records = fetch_activity_by_type(wallet, "REDEEM", max_records)
    return sum(r.get("usdcSize", 0) for r in redeem_records)


if __name__ == "__main__":
    MAX_RECORDS = 1000  # 统一控制 TRADE 和 REDEEM 的抓取上限，保证口径一致

    records = fetch_activity_by_type(WALLET, "TRADE", MAX_RECORDS)

    if not records:
        print("❌ 未找到任何交易记录，请检查钱包地址。")
    else:
        analyze(records)

        # 精确 PnL（含结算赎回），使用相同的 MAX_RECORDS 保证口径一致
        print("\n🔄 正在补充抓取市场结算(REDEEM)收入...")
        redeemed = fetch_redeems(WALLET, MAX_RECORDS)
        total_spent = sum(r.get("usdcSize", 0) for r in records if r.get("side") == "BUY")
        total_received = sum(r.get("usdcSize", 0) for r in records if r.get("side") == "SELL")
        precise_pnl = total_received + redeemed - total_spent
        print(f"  市场结算赎回总额 : ${redeemed:,.2f} USDC")
        print(f"\n{'='*55}")
        if precise_pnl >= 0:
            print(f"  精确总 PnL : +${precise_pnl:,.2f} USDC  ✅ 盈利")
        else:
            print(f"  精确总 PnL : -${abs(precise_pnl):,.2f} USDC  ❌ 亏损")
        print(f"{'='*55}")
        print(f"\n⚠️  以上数据基于最近 {MAX_RECORDS} 条 TRADE 和 {MAX_RECORDS} 条 REDEEM 记录，口径一致。")
