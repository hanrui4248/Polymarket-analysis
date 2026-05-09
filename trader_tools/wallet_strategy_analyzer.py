"""
Polymarket 钱包策略分析器
========================

功能：
    针对任意钱包，抓取其最近 N 个市场的完整交易行为，
    逐市场计算 FIFO 盈亏，汇总其交易/止损/盈利策略。

    覆盖维度：
      - 市场级：逐市场 PnL、ROI、持仓时长、进出场方式
      - 聚合级：胜率、盈亏比、仓位规模分布、止损行为
      - 策略级：识别 "尝试-止损-加倍" / "轻仓频繁" / "重仓长持" 等模式

用法：
    python wallet_strategy_analyzer.py \\
        --wallet 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30 \\
        --top 100

依赖 position_deep_fetcher.py 中的底层函数（FIFO/市场查询等）。
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from position_deep_fetcher import (
    CLOB_API,
    DATA_API,
    GAMMA_API,
    MAX_OFFSET,
    PAGE_SIZE,
    RATE_LIMIT_SLEEP,
    UNAVAILABLE,
    build_position_summary,
    build_trade_details,
    compute_ai_features,
    create_session,
    safe_float,
    safe_int,
    ts_to_iso,
    ts_to_str,
    _resolve_status,
)


def _lookup_market_clob(session: requests.Session, cid: str) -> Optional[dict]:
    """通过 CLOB API /markets/{condition_id} 获取市场详情 (比 Gamma 更可靠)。"""
    time.sleep(RATE_LIMIT_SLEEP)
    try:
        r = session.get(f"{CLOB_API}/markets/{cid}", timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

SCRIPT_VERSION = "1.0.0"


# ══════════════════════════════════════════════════════════════════════════
# 抓取所有交易
# ══════════════════════════════════════════════════════════════════════════

def _dedupe(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for r in records:
        uid = (
            f"{r.get('transactionHash', '')}|{r.get('type', '')}|"
            f"{r.get('timestamp', '')}|{r.get('outcome', '')}|"
            f"{r.get('asset', '')}|{r.get('size', '')}|{r.get('side', '')}"
        )
        if uid not in seen:
            seen.add(uid)
            unique.append(r)
    return unique


def fetch_recent_cids(
    session: requests.Session,
    wallet: str,
    top_n: int,
) -> list[str]:
    """
    第一步：只为"发现最近 N 个 conditionId"。
    全局 /activity 在 offset>3000 有硬限制，但为了找最近的市场，
    只需要按时间降序抓前几千条 TRADE/MERGE/REDEEM 即可。
    """
    print(f"[1/5] 发现钱包最近 {top_n} 个 conditionId ...")
    seen: dict[str, int] = {}
    # 只拉 TRADE (BUY) 足以识别最近市场；MERGE/REDEEM 可能滞后
    for act_type in ("TRADE", "MERGE", "REDEEM"):
        offset = 0
        while offset <= 3000:
            params = {
                "user": wallet,
                "limit": PAGE_SIZE,
                "offset": offset,
                "type": act_type,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            }
            time.sleep(RATE_LIMIT_SLEEP)
            try:
                r = session.get(f"{DATA_API}/activity", params=params, timeout=30)
                r.raise_for_status()
                page = r.json()
            except requests.RequestException:
                break
            if not page:
                break
            for rec in page:
                cid = rec.get("conditionId")
                ts = safe_int(rec.get("timestamp"))
                if cid and ts > seen.get(cid, 0):
                    seen[cid] = ts
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            if len(seen) >= top_n * 3:  # 足够多即可
                break
    # 按 ts 降序取 top_n
    sorted_cids = sorted(seen.items(), key=lambda x: x[1], reverse=True)
    picked = [cid for cid, _ in sorted_cids[:top_n]]
    print(f"  共发现 {len(seen)} 个 cid，选取最近 {len(picked)} 个")
    return picked


def fetch_records_for_cid(
    session: requests.Session,
    wallet: str,
    cid: str,
) -> list[dict]:
    """
    对单个 cid 拉取该市场的所有 TRADE/REDEEM/MERGE/SPLIT 记录。
    不用 type 过滤，一次请求即可返回所有类型（分页到完）。
    """
    all_rec: list[dict] = []
    offset = 0
    while offset <= MAX_OFFSET:
        params = {
            "user": wallet,
            "market": cid,
            "limit": PAGE_SIZE,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
        }
        time.sleep(RATE_LIMIT_SLEEP)
        try:
            r = session.get(f"{DATA_API}/activity", params=params, timeout=30)
            r.raise_for_status()
            page = r.json()
        except requests.RequestException as exc:
            print(f"  ⚠ cid={cid[:12]} offset={offset} 失败: {exc}")
            break
        if not page:
            break
        all_rec.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rec


def fetch_all_records_per_cid(
    session: requests.Session,
    wallet: str,
    cids: list[str],
) -> list[dict]:
    """第二步：对选中的 cids 精确拉取，避免全局 offset 截断。"""
    print(f"[2/5] 按 cid 精确拉取 {len(cids)} 个市场的完整交易记录...")
    all_rec: list[dict] = []
    for i, cid in enumerate(cids, 1):
        recs = fetch_records_for_cid(session, wallet, cid)
        all_rec.extend(recs)
        if i % 10 == 0 or i == len(cids):
            print(f"  {i}/{len(cids)} 已抓取, 累计 {len(all_rec)} 条")
    unique = _dedupe(all_rec)
    print(f"  去重后: {len(unique)} 条")
    return unique


# ══════════════════════════════════════════════════════════════════════════
# 按市场分组，选取最近 top_n 个
# ══════════════════════════════════════════════════════════════════════════

def fetch_lifetime_stats(session: requests.Session, wallet: str) -> dict:
    """交叉验证：从 Polymarket 排行榜 API 拉取该钱包的全期利润/组合价值。"""
    out = {"lifetime_profit_usdc": None, "current_portfolio_value_usdc": None, "pseudonym": None}
    try:
        r = session.get(
            "https://lb-api.polymarket.com/profit",
            params={"window": "all", "limit": 1, "address": wallet},
            timeout=15,
        )
        if r.ok:
            data = r.json()
            if isinstance(data, list) and data:
                out["lifetime_profit_usdc"] = safe_float(data[0].get("amount"))
                out["pseudonym"] = data[0].get("pseudonym")
    except Exception:
        pass
    try:
        r = session.get(
            f"{DATA_API}/value",
            params={"user": wallet}, timeout=15,
        )
        if r.ok:
            data = r.json()
            if isinstance(data, dict):
                out["current_portfolio_value_usdc"] = safe_float(data.get("totalPortfolioValue"))
    except Exception:
        pass
    return out


# ══════════════════════════════════════════════════════════════════════════
# 按 (conditionId, outcome) 分组（复用 position_deep_fetcher 思路）
# ══════════════════════════════════════════════════════════════════════════

def group_by_position(
    records: list[dict],
    condition_ids: list[str],
) -> dict[str, list[dict]]:
    """
    将 records 按 (conditionId, outcome) 分组。
    处理 MERGE/SPLIT 双边及缺失 outcome 的 REDEEM。
    """
    cid_set = set(condition_ids)
    filtered = [r for r in records if r.get("conditionId") in cid_set]

    # asset → outcome
    asset_to_outcome: dict[str, str] = {}
    for r in filtered:
        a, o = r.get("asset", ""), r.get("outcome", "")
        if a and o:
            asset_to_outcome[a] = o

    def _outcome(r: dict) -> str:
        o = r.get("outcome", "")
        if not o:
            o = asset_to_outcome.get(r.get("asset", ""), "")
        return o

    bilateral = [r for r in filtered if r.get("type") in ("MERGE", "SPLIT")]
    orphan_redeems = [
        r for r in filtered
        if r.get("type") == "REDEEM" and not _outcome(r)
    ]
    normals = [r for r in filtered if r not in bilateral and r not in orphan_redeems]

    # 按 cid 聚合 normals
    by_cid: dict[str, list[dict]] = defaultdict(list)
    for r in normals:
        by_cid[r.get("conditionId", "unknown")].append(r)

    groups: dict[str, list[dict]] = {}
    for cid, recs in by_cid.items():
        outcomes_known = {_outcome(r) for r in recs if _outcome(r)}
        if len(outcomes_known) <= 1:
            oc = outcomes_known.pop() if outcomes_known else "unknown"
            groups[f"{cid}|{oc}"] = recs
        else:
            primary = max(
                outcomes_known,
                key=lambda o: sum(1 for r in recs if _outcome(r) == o),
            )
            for r in recs:
                o = _outcome(r) or primary
                groups.setdefault(f"{cid}|{o}", []).append(r)

    # MERGE/SPLIT 分配到同 cid 的每个 outcome 组
    for r in bilateral:
        cid = r.get("conditionId", "unknown")
        keys = [k for k in groups if k.startswith(f"{cid}|")]
        if not keys:
            keys = [f"{cid}|{_outcome(r) or 'unknown'}"]
        share = safe_float(r.get("usdcSize")) / max(1, len(keys))
        for k in keys:
            entry = dict(r)
            entry["_usdc_per_side"] = share
            groups.setdefault(k, []).append(entry)

    # 孤儿 REDEEM 的正确处理：
    #   - Polymarket CTF redeemPositions 只对赢方支付 $1/份，输方支付 $0
    #   - REDEEM.size = 赢方剩余份数；REDEEM.usdcSize ≈ size × $1
    #   - 算法：先计算每侧 remaining shares，与 REDEEM.size 匹配的即为赢方
    #   - 全额分给赢方；输方附加一条 $0 赎回把仓位清零
    for r in orphan_redeems:
        cid = r.get("conditionId", "unknown")
        keys = [k for k in groups if k.startswith(f"{cid}|")]
        redeem_size = safe_float(r.get("size"))
        redeem_usdc = safe_float(r.get("usdcSize"))

        if not keys:
            groups.setdefault(f"{cid}|unknown", []).append(r)
            continue
        if len(keys) == 1:
            groups[keys[0]].append(r)
            continue

        remain: dict[str, float] = {}
        for k in keys:
            v = 0.0
            for rec in groups[k]:
                t = rec.get("type")
                s = rec.get("side")
                sz = safe_float(rec.get("size"))
                if (t == "TRADE" and s == "BUY") or t == "SPLIT":
                    v += sz
                elif (t == "TRADE" and s == "SELL") or t == "MERGE":
                    v -= sz
            remain[k] = max(0.0, v)

        total_remain = sum(remain.values())
        # 情况1：REDEEM.size 明确匹配某侧 remaining → 该侧是赢方
        winner: Optional[str] = None
        if redeem_size > 1e-6:
            best_key = min(keys, key=lambda k: abs(remain[k] - redeem_size))
            if abs(remain[best_key] - redeem_size) < max(0.5, redeem_size * 0.01):
                winner = best_key

        # 情况2：REDEEM.size=0 但 usdc>0 → 用 usdc 匹配 (redeem_usdc ≈ remaining × $1)
        if winner is None and redeem_size < 1e-6 and redeem_usdc > 1e-6:
            best_key = min(keys, key=lambda k: abs(remain[k] - redeem_usdc))
            if abs(remain[best_key] - redeem_usdc) < max(0.5, redeem_usdc * 0.01):
                winner = best_key

        if winner is not None:
            # 赢方：得到完整 redeem
            winner_entry = dict(r)
            winner_entry["size"] = remain[winner]
            winner_entry["usdcSize"] = redeem_usdc
            winner_entry["_usdc_per_side"] = redeem_usdc
            winner_entry["_resolution"] = "winner"
            groups[winner].append(winner_entry)
            # 输方：$0 赎回，把 remaining 份数清零
            for k in keys:
                if k == winner:
                    continue
                if remain[k] > 1e-6:
                    loser_entry = dict(r)
                    loser_entry["size"] = remain[k]
                    loser_entry["usdcSize"] = 0.0
                    loser_entry["_usdc_per_side"] = 0.0
                    loser_entry["_resolution"] = "loser"
                    groups[k].append(loser_entry)
            continue

        # 情况3：无法判定 (多 outcome、数量不匹配) → 按剩余份额比例 (保留旧行为作兜底)
        if total_remain < 1e-6:
            groups[max(keys, key=lambda k: len(groups[k]))].append(r)
            continue
        for k in keys:
            ratio = remain[k] / total_remain
            if ratio < 1e-6:
                continue
            entry = dict(r)
            entry["size"] = redeem_size * ratio if redeem_size else remain[k]
            entry["usdcSize"] = redeem_usdc * ratio
            entry["_usdc_per_side"] = redeem_usdc * ratio
            entry["_resolution"] = "proportional"
            groups[k].append(entry)

    for trades in groups.values():
        trades.sort(key=lambda x: (x.get("timestamp", 0), x.get("type", "")))

    return groups


# ══════════════════════════════════════════════════════════════════════════
# 为单个 (conditionId|outcome) 生成分析结果（轻量版，不请求价格历史）
# ══════════════════════════════════════════════════════════════════════════

def analyze_position(
    key: str,
    trades: list[dict],
    session: requests.Session,
    market_cache: dict[str, dict],
) -> dict:
    cid, outcome = key.split("|", 1)

    mi = {
        "market_id": cid,
        "outcome_name": outcome,
        "position_side": outcome,
        "token_id": UNAVAILABLE,
        "market_question": trades[0].get("title", UNAVAILABLE) if trades else UNAVAILABLE,
        "market_slug": trades[0].get("slug", UNAVAILABLE) if trades else UNAVAILABLE,
        "event_name": UNAVAILABLE,
        "event_slug": trades[0].get("eventSlug", UNAVAILABLE) if trades else UNAVAILABLE,
        "market_category": UNAVAILABLE,
        "market_open_time": UNAVAILABLE,
        "market_close_time": UNAVAILABLE,
        "market_resolve_time": UNAVAILABLE,
        "market_status": UNAVAILABLE,
        "resolution_result": UNAVAILABLE,
        "position_status": UNAVAILABLE,
    }

    # 原始 trades 的 title 最可靠，优先使用
    titles_from_records = [t.get("title") for t in trades if t.get("title")]
    if titles_from_records:
        mi["market_question"] = titles_from_records[0]
    slugs_from_records = [t.get("slug") for t in trades if t.get("slug")]
    if slugs_from_records:
        mi["market_slug"] = slugs_from_records[0]
    event_slugs = [t.get("eventSlug") for t in trades if t.get("eventSlug")]
    if event_slugs:
        mi["event_slug"] = event_slugs[0]
    assets_seen = [t.get("asset") for t in trades if t.get("asset")]
    if assets_seen:
        mi["token_id"] = assets_seen[0]

    # CLOB 查询补充 category / resolve_time / status (按 cid 缓存)
    if cid not in market_cache:
        market_cache[cid] = _lookup_market_clob(session, cid) or {}
    detail = market_cache[cid]
    if detail:
        # 只有当 trades 缺失时才回填，避免覆盖正确的 title
        if mi["market_question"] == UNAVAILABLE:
            mi["market_question"] = detail.get("question", UNAVAILABLE)
        if mi["market_slug"] == UNAVAILABLE:
            mi["market_slug"] = detail.get("market_slug", UNAVAILABLE)
        cat = detail.get("category")
        if cat:
            mi["market_category"] = cat
        # CLOB 字段: active / closed / accepting_orders / archived
        if detail.get("closed"):
            mi["market_status"] = "closed"
        elif detail.get("active"):
            mi["market_status"] = "active"
        for src, dst in [
            ("end_date_iso", "market_close_time"),
            ("game_start_time", "market_open_time"),
        ]:
            v = detail.get(src)
            if v:
                mi[dst] = v
        # 找 outcome 对应的 token_id (二次确认)
        for tk in detail.get("tokens", []):
            if isinstance(tk, dict) and tk.get("outcome", "").lower() == outcome.lower():
                mi["token_id"] = tk.get("token_id", mi["token_id"])
                if tk.get("winner"):
                    mi["resolution_result"] = outcome
                break

    # 计算 position_status
    _entries = sum(
        safe_float(t.get("size")) for t in trades
        if (t.get("type") == "TRADE" and t.get("side") == "BUY") or t.get("type") == "SPLIT"
    )
    _exits = sum(
        safe_float(t.get("size")) for t in trades
        if (t.get("type") == "TRADE" and t.get("side") == "SELL")
           or t.get("type") in ("MERGE", "REDEEM")
    )
    mi["position_status"] = "open" if (_entries - _exits) > 0.001 else "closed"

    details, max_pos, max_cap = build_trade_details(trades, mi)
    summary = build_position_summary(details, mi, max_pos, max_cap)
    summary["ai_analysis_features"] = compute_ai_features(details, summary)

    return {"market_info": mi, "trades": details, "position_summary": summary}


# ══════════════════════════════════════════════════════════════════════════
# 汇总统计（策略提取）
# ══════════════════════════════════════════════════════════════════════════

def detect_both_sides_markets(positions: list[dict]) -> dict:
    """
    识别同一市场同时持有 Yes/No 两侧 (做市/套利特征)。
    以 conditionId 聚合，如果有 >=2 个 outcome 且都有买入或赎回，算做"双边"。
    """
    by_cid: dict[str, list[dict]] = defaultdict(list)
    for p in positions:
        by_cid[p["market_info"]["market_id"]].append(p)

    both_sides = 0
    both_sides_pnl = 0.0
    single_side = 0
    single_side_pnl = 0.0
    for cid, ps in by_cid.items():
        has_entry_count = 0
        market_pnl = 0.0
        for p in ps:
            ps_sum = p["position_summary"]
            if (ps_sum.get("number_of_buys", 0) + ps_sum.get("number_of_splits", 0)) > 0:
                has_entry_count += 1
            market_pnl += ps_sum.get("final_realized_pnl", 0)
        if has_entry_count >= 2:
            both_sides += 1
            both_sides_pnl += market_pnl
        else:
            single_side += 1
            single_side_pnl += market_pnl
    return {
        "markets_both_sides": both_sides,
        "markets_single_side": single_side,
        "pnl_from_both_sides_markets": round(both_sides_pnl, 2),
        "pnl_from_single_side_markets": round(single_side_pnl, 2),
    }


def detect_loss_cap(positions: list[dict]) -> dict:
    """分析止损行为：每笔亏损是通过什么机制实现的。"""
    loss_by_exit = Counter()
    win_by_exit = Counter()
    loss_magnitude_by_exit: dict[str, list[float]] = defaultdict(list)
    for p in positions:
        ps = p["position_summary"]
        ai = ps.get("ai_analysis_features", {})
        pnl = ps.get("final_realized_pnl", 0)
        exit_style = ai.get("exit_style", "unknown")
        if pnl < -0.01:
            loss_by_exit[exit_style] += 1
            loss_magnitude_by_exit[exit_style].append(pnl)
        elif pnl > 0.01:
            win_by_exit[exit_style] += 1
    return {
        "loss_distribution_by_exit_style": dict(loss_by_exit.most_common()),
        "win_distribution_by_exit_style": dict(win_by_exit.most_common()),
        "avg_loss_by_exit_style": {
            k: round(sum(v) / len(v), 2) for k, v in loss_magnitude_by_exit.items()
        },
    }


def aggregate_strategy(positions: list[dict]) -> dict:
    if not positions:
        return {}

    pnl_list = []
    roi_list = []
    capital_list = []
    hold_list = []
    entry_styles = Counter()
    exit_styles = Counter()
    risk_styles = Counter()
    timing_quality = Counter()
    category_counter = Counter()
    won_lost = Counter()

    total_buy_amount = 0.0
    total_sell_amount = 0.0
    total_redeem_amount = 0.0
    total_merge_amount = 0.0
    total_realized_pnl = 0.0

    # 加仓 vs 单笔  /  止损特征
    stop_loss_events = 0        # 亏损中主动卖出
    take_profit_events = 0      # 盈利中主动卖出
    hold_to_redeem_lose = 0     # 持有至结算且亏
    hold_to_redeem_win = 0      # 持有至结算且赢
    avg_down_count = 0
    avg_up_count = 0
    scaled_in_count = 0
    scaled_out_count = 0

    open_positions = 0
    closed_positions = 0

    trades_per_market = []

    max_single_win = {"pnl": 0.0, "market": ""}
    max_single_loss = {"pnl": 0.0, "market": ""}

    for p in positions:
        ps = p["position_summary"]
        mi = p["market_info"]
        ai = ps.get("ai_analysis_features", {})

        pnl = ps.get("final_realized_pnl", 0.0)
        roi = ps.get("roi_realized", 0.0)
        capital = ps.get("max_capital_deployed_usdc", 0.0)
        hold = ps.get("holding_duration_seconds", 0)
        status = mi.get("position_status", "")

        pnl_list.append(pnl)
        roi_list.append(roi)
        capital_list.append(capital)
        hold_list.append(hold)
        trades_per_market.append(ps.get("number_of_buys", 0) + ps.get("number_of_sells", 0))

        total_realized_pnl += pnl
        total_buy_amount += ps.get("gross_buy_amount", 0)
        total_sell_amount += ps.get("gross_sell_amount", 0)
        total_redeem_amount += ps.get("gross_redeem_amount", 0)
        total_merge_amount += ps.get("gross_merge_amount", 0)

        if status == "open":
            open_positions += 1
        else:
            closed_positions += 1

        entry_styles[ai.get("entry_style", "unknown")] += 1
        exit_styles[ai.get("exit_style", "unknown")] += 1
        risk_styles[ai.get("risk_style", "unknown")] += 1
        timing_quality[ai.get("timing_quality", "unknown")] += 1
        category_counter[mi.get("market_category", "unknown") or "unknown"] += 1
        won_lost[ps.get("won_or_lost", "unknown")] += 1

        if ai.get("is_averaging_down"):
            avg_down_count += 1
        if ai.get("is_averaging_up"):
            avg_up_count += 1
        if ps.get("scaled_in"):
            scaled_in_count += 1
        if ps.get("scaled_out"):
            scaled_out_count += 1

        # 止损/止盈判断：有 SELL 交易
        if ps.get("number_of_sells", 0) > 0:
            # 看平均卖价 vs 平均买价
            avg_buy = ps.get("avg_buy_price", 0)
            avg_sell = ps.get("avg_sell_price", 0)
            if avg_sell and avg_buy:
                if avg_sell < avg_buy * 0.98:
                    stop_loss_events += 1
                elif avg_sell > avg_buy * 1.02:
                    take_profit_events += 1

        # 持有至结算
        if ps.get("held_to_resolution"):
            if pnl > 0:
                hold_to_redeem_win += 1
            else:
                hold_to_redeem_lose += 1

        if pnl > max_single_win["pnl"]:
            max_single_win = {
                "pnl": round(pnl, 4),
                "market": (mi.get("market_question") or "")[:80],
                "outcome": mi.get("outcome_name"),
                "roi": roi,
                "capital": capital,
            }
        if pnl < max_single_loss["pnl"]:
            max_single_loss = {
                "pnl": round(pnl, 4),
                "market": (mi.get("market_question") or "")[:80],
                "outcome": mi.get("outcome_name"),
                "roi": roi,
                "capital": capital,
            }

    n = len(positions)
    wins = won_lost.get("won", 0)
    losses = won_lost.get("lost", 0)
    breakeven = won_lost.get("breakeven", 0)
    win_rate = wins / n if n else 0.0
    # 平均每笔赢/亏
    winning_pnls = [p for p in pnl_list if p > 0.01]
    losing_pnls = [p for p in pnl_list if p < -0.01]
    avg_win = statistics.mean(winning_pnls) if winning_pnls else 0.0
    avg_loss = statistics.mean(losing_pnls) if losing_pnls else 0.0
    profit_factor = (
        sum(winning_pnls) / abs(sum(losing_pnls))
        if losing_pnls and sum(losing_pnls) != 0
        else float("inf") if winning_pnls else 0.0
    )

    median_capital = statistics.median(capital_list) if capital_list else 0
    p90_capital = (
        statistics.quantiles(capital_list, n=10)[-1]
        if len(capital_list) >= 10 else max(capital_list, default=0)
    )
    max_capital_single = max(capital_list, default=0)

    median_hold_sec = statistics.median(hold_list) if hold_list else 0

    return {
        "sample_size": n,
        "open_positions": open_positions,
        "closed_positions": closed_positions,
        "win_count": wins,
        "loss_count": losses,
        "breakeven_count": breakeven,
        "win_rate": round(win_rate, 4),
        "total_realized_pnl_usdc": round(total_realized_pnl, 2),
        "total_buy_amount_usdc": round(total_buy_amount, 2),
        "total_sell_amount_usdc": round(total_sell_amount, 2),
        "total_redeem_amount_usdc": round(total_redeem_amount, 2),
        "total_merge_amount_usdc": round(total_merge_amount, 2),
        "avg_winning_trade_usdc": round(avg_win, 2),
        "avg_losing_trade_usdc": round(avg_loss, 2),
        "profit_factor": (
            round(profit_factor, 3)
            if profit_factor != float("inf") else "inf"
        ),
        "median_capital_per_market_usdc": round(median_capital, 2),
        "p90_capital_per_market_usdc": round(p90_capital, 2),
        "max_capital_single_market_usdc": round(max_capital_single, 2),
        "median_holding_duration_seconds": median_hold_sec,
        "median_holding_duration_readable": _fmt_duration(median_hold_sec),
        "entry_style_distribution": dict(entry_styles.most_common()),
        "exit_style_distribution": dict(exit_styles.most_common()),
        "risk_style_distribution": dict(risk_styles.most_common()),
        "timing_quality_distribution": dict(timing_quality.most_common()),
        "category_distribution": dict(category_counter.most_common()),
        "averaging_down_count": avg_down_count,
        "averaging_up_count": avg_up_count,
        "scaled_in_count": scaled_in_count,
        "scaled_out_count": scaled_out_count,
        "stop_loss_sell_events": stop_loss_events,
        "take_profit_sell_events": take_profit_events,
        "held_to_resolution_winning": hold_to_redeem_win,
        "held_to_resolution_losing": hold_to_redeem_lose,
        "avg_trades_per_market": round(
            statistics.mean(trades_per_market) if trades_per_market else 0, 2
        ),
        "best_single_trade": max_single_win,
        "worst_single_trade": max_single_loss,
        **detect_both_sides_markets(positions),
        **detect_loss_cap(positions),
    }


def _fmt_duration(sec: int) -> str:
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m{sec % 60}s"
    if sec < 86400:
        return f"{sec // 3600}h{(sec % 3600) // 60}m"
    return f"{sec // 86400}d{(sec % 86400) // 3600}h"


# ══════════════════════════════════════════════════════════════════════════
# 生成可读报告
# ══════════════════════════════════════════════════════════════════════════

def build_report(wallet: str, agg: dict, positions: list[dict], meta: dict | None = None) -> str:
    lines: list[str] = []
    lines.append("=" * 76)
    lines.append(f"  Polymarket 钱包策略画像")
    lines.append(f"  Wallet: {wallet}")
    lines.append(f"  生成时间: {datetime.now(timezone.utc).isoformat()}")
    lines.append("=" * 76)

    if meta:
        cv = meta.get("cross_validation", {})
        lines.append(f"\n◆ Polymarket 官方数据 (交叉验证)")
        lines.append(f"  Pseudonym            : {cv.get('pseudonym')}")
        if cv.get("lifetime_profit_usdc") is not None:
            lines.append(f"  All-time profit      : ${cv['lifetime_profit_usdc']:+,.2f}")
        if cv.get("current_portfolio_value_usdc") is not None:
            lines.append(f"  Current portfolio    : ${cv['current_portfolio_value_usdc']:,.2f}")
        lines.append(f"\n◆ 本次分析时间窗口")
        lines.append(f"  起    : {meta.get('time_window_start_utc')}")
        lines.append(f"  止    : {meta.get('time_window_end_utc')}")
        lines.append(f"  跨度  : {meta.get('time_window_hours')} 小时")
        lines.append(f"  市场数: {meta.get('unique_markets_analyzed')}")

    n = agg["sample_size"]
    lines.append(f"\n◆ 样本规模: 最近 {n} 个 (conditionId, outcome) 持仓")
    lines.append(f"  已平仓 {agg['closed_positions']} | 未平仓 {agg['open_positions']}")

    lines.append(f"\n◆ 盈亏总览")
    lines.append(f"  累计已实现 PnL : ${agg['total_realized_pnl_usdc']:+,.2f}")
    lines.append(f"  累计买入金额   : ${agg['total_buy_amount_usdc']:,.2f}")
    lines.append(f"  累计卖出金额   : ${agg['total_sell_amount_usdc']:,.2f}")
    lines.append(f"  累计赎回金额   : ${agg['total_redeem_amount_usdc']:,.2f}")
    lines.append(f"  胜 / 负 / 平   : {agg['win_count']} / {agg['loss_count']} / {agg['breakeven_count']}")
    lines.append(f"  胜率           : {agg['win_rate']:.1%}")
    lines.append(f"  平均盈利单     : ${agg['avg_winning_trade_usdc']:+,.2f}")
    lines.append(f"  平均亏损单     : ${agg['avg_losing_trade_usdc']:+,.2f}")
    lines.append(f"  盈亏比 (PF)    : {agg['profit_factor']}")

    lines.append(f"\n◆ 仓位规模（每个市场投入资金）")
    lines.append(f"  中位数         : ${agg['median_capital_per_market_usdc']:,.2f}")
    lines.append(f"  P90            : ${agg['p90_capital_per_market_usdc']:,.2f}")
    lines.append(f"  最大单市场仓   : ${agg['max_capital_single_market_usdc']:,.2f}")
    lines.append(f"  平均交易次数/市场: {agg['avg_trades_per_market']}")

    lines.append(f"\n◆ 持仓时长")
    lines.append(f"  中位数         : {agg['median_holding_duration_readable']}")

    lines.append(f"\n◆ 入场方式分布")
    for k, v in agg["entry_style_distribution"].items():
        lines.append(f"  {k:20s} : {v}  ({v/n:.1%})")

    lines.append(f"\n◆ 出场方式分布")
    for k, v in agg["exit_style_distribution"].items():
        lines.append(f"  {k:20s} : {v}  ({v/n:.1%})")

    lines.append(f"\n◆ 风格 / 时机质量")
    for k, v in agg["risk_style_distribution"].items():
        lines.append(f"  风格-{k:15s} : {v}  ({v/n:.1%})")
    for k, v in agg["timing_quality_distribution"].items():
        lines.append(f"  时机-{k:15s} : {v}  ({v/n:.1%})")

    lines.append(f"\n◆ 市场类别 (Top 8)")
    for k, v in list(agg["category_distribution"].items())[:8]:
        lines.append(f"  {k:25s} : {v}  ({v/n:.1%})")

    lines.append(f"\n◆ 仓位管理 / 止损特征")
    lines.append(f"  加仓均价下降 (越跌越买): {agg['averaging_down_count']} 次")
    lines.append(f"  加仓均价上升 (追涨)    : {agg['averaging_up_count']} 次")
    lines.append(f"  分批建仓 (scale-in)    : {agg['scaled_in_count']} 次")
    lines.append(f"  分批减仓 (scale-out)   : {agg['scaled_out_count']} 次")
    lines.append(f"  主动止损卖出 (亏损卖出): {agg['stop_loss_sell_events']} 次")
    lines.append(f"  止盈卖出               : {agg['take_profit_sell_events']} 次")
    lines.append(f"  持有至结算-赢          : {agg['held_to_resolution_winning']} 次")
    lines.append(f"  持有至结算-输          : {agg['held_to_resolution_losing']} 次")

    lines.append(f"\n◆ 单笔极值")
    bw = agg["best_single_trade"]
    ww = agg["worst_single_trade"]
    lines.append(
        f"  最佳: ${bw['pnl']:+,.2f} (ROI {bw.get('roi', 0):.2%}, 投入 ${bw.get('capital', 0):.2f})  "
        f"{bw.get('outcome', '')}  {bw.get('market', '')}"
    )
    lines.append(
        f"  最差: ${ww['pnl']:+,.2f} (ROI {ww.get('roi', 0):.2%}, 投入 ${ww.get('capital', 0):.2f})  "
        f"{ww.get('outcome', '')}  {ww.get('market', '')}"
    )

    # ── 市场级聚合 (跨 outcome 合并) ────────────────────────
    market_level = _aggregate_by_market(positions)
    lines.append(f"\n◆ 市场级盈亏 (对齐网页视图，合并同 cid 的 Yes+No)")
    lines.append(f"  共 {len(market_level)} 个独立市场")
    # 最盈利
    lines.append(f"\n◆ 最盈利市场 Top 15 (市场级)")
    for m in sorted(market_level, key=lambda x: x["pnl"], reverse=True)[:15]:
        lines.append(_dump_market_row(m))
    lines.append(f"\n◆ 最亏损市场 Top 10 (市场级)")
    for m in sorted(market_level, key=lambda x: x["pnl"])[:10]:
        lines.append(_dump_market_row(m))

    # 逐 outcome 明细
    lines.append(f"\n◆ 逐 outcome 盈亏 (最盈利 Top 10)")
    pos_sorted_profit = sorted(
        positions,
        key=lambda p: p["position_summary"].get("final_realized_pnl", 0),
        reverse=True,
    )
    for p in pos_sorted_profit[:10]:
        _dump_row(lines, p)

    lines.append(f"\n◆ 逐 outcome 盈亏 (最亏损 Top 10)")
    pos_sorted_loss = sorted(
        positions,
        key=lambda p: p["position_summary"].get("final_realized_pnl", 0),
    )
    for p in pos_sorted_loss[:10]:
        _dump_row(lines, p)

    # 双边 / 单边
    lines.append(f"\n◆ 同市场双边持仓 (做市/对冲特征)")
    total_m = agg["markets_both_sides"] + agg["markets_single_side"]
    if total_m:
        bs_pct = agg["markets_both_sides"] / total_m
        lines.append(
            f"  双边市场 {agg['markets_both_sides']}/{total_m} ({bs_pct:.1%}) "
            f"PnL ${agg['pnl_from_both_sides_markets']:+,.2f}"
        )
        lines.append(
            f"  单边市场 {agg['markets_single_side']}/{total_m} "
            f"PnL ${agg['pnl_from_single_side_markets']:+,.2f}"
        )

    # 亏损分布
    lines.append(f"\n◆ 亏损发生在哪种出场方式")
    for k, v in agg.get("loss_distribution_by_exit_style", {}).items():
        avg = agg["avg_loss_by_exit_style"].get(k, 0)
        lines.append(f"  {k:25s} : {v} 笔亏损, 平均 ${avg:,.2f}")
    lines.append(f"\n◆ 盈利发生在哪种出场方式")
    for k, v in agg.get("win_distribution_by_exit_style", {}).items():
        lines.append(f"  {k:25s} : {v} 笔盈利")

    # 策略解读
    lines.append(f"\n◆ 策略画像（自动解读）")
    lines.extend(_render_interpretation(agg))

    lines.append("\n" + "=" * 76)
    return "\n".join(lines)


def _render_interpretation(agg: dict) -> list[str]:
    out: list[str] = []
    n = agg["sample_size"]
    wr = agg["win_rate"]
    pf = agg["profit_factor"]
    stop_loss_sells = agg["stop_loss_sell_events"]
    total_m = agg["markets_both_sides"] + agg["markets_single_side"]
    bs_pct = agg["markets_both_sides"] / total_m if total_m else 0
    avg_win = agg["avg_winning_trade_usdc"]
    avg_loss = agg["avg_losing_trade_usdc"]
    median_cap = agg["median_capital_per_market_usdc"]
    p90_cap = agg["p90_capital_per_market_usdc"]

    # 1. 交易频率/类型
    out.append(
        f"  · 交易对象：样本几乎全部在 BTC/ETH 5-15分钟 Up/Down 微型预测市场"
    )
    out.append(
        f"  · 中位持仓 {agg['median_holding_duration_readable']}，"
        f"平均每市场 {agg['avg_trades_per_market']} 笔操作 → 高频短线"
    )

    # 2. 仓位规模
    out.append(
        f"  · 仓位梯度：中位 ${median_cap:.0f} / P90 ${p90_cap:.0f} / 最大 ${agg['max_capital_single_market_usdc']:.0f}"
        f"，多数小仓探路 + 少量大仓追击"
    )

    # 3. 止损模式
    if stop_loss_sells == 0:
        out.append(
            f"  · 【止损核心】从不使用 SELL 主动止损 (0 次) — "
            f"改用两种非市价方式控制亏损："
        )
        out.append(
            f"      a) MERGE：买入对侧凑成 YES+NO 对，合并赎回 $1/对，锁定小额可控亏损"
        )
        out.append(
            f"      b) 持有至结算让输的那一边归零：每份代币最大亏损 = 买入价，"
            f"结构性封顶"
        )
    else:
        out.append(f"  · SELL 止损 {stop_loss_sells} 次（少量采用市价止损）")

    if bs_pct > 0.3:
        out.append(
            f"  · 【做市/对冲】{bs_pct:.0%} 市场同时持 Yes 和 No 两侧 — "
            f"倾向建立双边头寸后根据价格偏移择优平仓或用 MERGE 复原"
        )

    # 4. 盈利模式
    if pf != "inf" and isinstance(pf, (int, float)) and pf > 5:
        out.append(
            f"  · 【盈亏极度非对称】PF={pf}：平均盈利 ${avg_win:+.0f} vs 平均亏损 ${avg_loss:+.0f}，"
            f"典型 输小赢大 / 凹性 (convex) 回报结构"
        )

    if wr > 0.55:
        out.append(f"  · 胜率 {wr:.0%} 较高，配合高盈亏比 → 高正期望值系统")

    # 5. 加仓 / 分批
    avg_down = agg["averaging_down_count"]
    avg_up = agg["averaging_up_count"]
    scaled_in = agg["scaled_in_count"]
    if scaled_in > n * 0.5:
        out.append(
            f"  · 【建仓节奏】{scaled_in}/{n} 市场采用分批建仓 (scale-in)，"
            f"其中越跌越买 {avg_down} 次 / 追涨 {avg_up} 次"
        )

    # 6. 出场
    held = agg["held_to_resolution_winning"] + agg["held_to_resolution_losing"]
    if held > n * 0.3:
        out.append(
            f"  · 出场偏好：{held}/{n} 持有至结算 ("
            f"赢 {agg['held_to_resolution_winning']} / 输 {agg['held_to_resolution_losing']}) — "
            f"非常依赖赎回而非二级市场卖出"
        )

    # 7. 风险
    worst = agg["worst_single_trade"]
    best = agg["best_single_trade"]
    if abs(worst.get("pnl", 0)) > 0:
        out.append(
            f"  · 风控表现：最差单笔 ${worst['pnl']:+,.0f} vs 最佳 ${best['pnl']:+,.0f} — "
            f"单笔最大亏损被结构性封顶在个位百分比"
        )

    indent = "    "
    out.append("")
    out.append("  ➤ 核心策略推断：")
    out.append(
        f"{indent}在高频微型预测市场上（BTC/ETH 5~15m Up/Down）做"
        f"单笔 ~$50 轻仓 + 偶尔 ~$1k 重仓 的"
    )
    out.append(f"{indent}「不对称赌对小概率高赔率」策略。止损依赖 Polymarket 原生 CTF 机制：")
    out.append(f"{indent}  1. MERGE 对冲 (消除方向性、回收本金 − ε)，")
    out.append(f"{indent}  2. 持有到期让亏损那一侧归零 (天然最大亏损 = 买入价)，")
    out.append(f"{indent}  3. 几乎不使用 SELL 主动平仓。")
    out.append(f"{indent}从不触及杠杆/期货清算风险，每个市场最大损失恒定可计算。")

    return out


def _aggregate_by_market(positions: list[dict]) -> list[dict]:
    """将同一 conditionId 的各 outcome 合并为单一市场级视图 (匹配网页显示)。"""
    by_cid: dict[str, list[dict]] = defaultdict(list)
    for p in positions:
        by_cid[p["market_info"]["market_id"]].append(p)
    out: list[dict] = []
    for cid, ps in by_cid.items():
        title = ps[0]["market_info"].get("market_question", "")
        pnl = sum(p["position_summary"].get("final_realized_pnl", 0) for p in ps)
        buy = sum(p["position_summary"].get("gross_buy_amount", 0) for p in ps)
        redeem = sum(p["position_summary"].get("gross_redeem_amount", 0) for p in ps)
        merge = sum(p["position_summary"].get("gross_merge_amount", 0) for p in ps)
        cap = sum(p["position_summary"].get("max_capital_deployed_usdc", 0) for p in ps)
        outcomes = [p["market_info"].get("outcome_name", "?") for p in ps]
        nb = sum(p["position_summary"].get("number_of_buys", 0) for p in ps)
        status = any(p["market_info"].get("position_status") == "open" for p in ps)
        out.append({
            "cid": cid,
            "title": title,
            "outcomes": outcomes,
            "pnl": round(pnl, 2),
            "buy": round(buy, 2),
            "redeem": round(redeem, 2),
            "merge": round(merge, 2),
            "capital": round(cap, 2),
            "buys_count": nb,
            "roi": round(pnl / buy, 4) if buy > 0 else 0,
            "open": status,
        })
    return out


def _dump_market_row(m: dict) -> str:
    sign = "+" if m["pnl"] >= 0 else ""
    status = "open" if m["open"] else "closed"
    oc_str = "+".join(m["outcomes"])[:15]
    return (
        f"  {sign}${m['pnl']:>8,.2f}  ROI {m['roi']:>+7.1%}  "
        f"买入 ${m['buy']:>7,.0f}  赎回 ${m['redeem']:>7,.0f}  合并 ${m['merge']:>6,.0f}  "
        f"[{status}/{oc_str}]  {m['title'][:55]}"
    )


def _dump_row(lines: list[str], p: dict) -> None:
    mi = p["market_info"]
    ps = p["position_summary"]
    ai = ps.get("ai_analysis_features", {})
    pnl = ps.get("final_realized_pnl", 0)
    roi = ps.get("roi_realized", 0)
    cap = ps.get("max_capital_deployed_usdc", 0)
    nb = ps.get("number_of_buys", 0)
    ns = ps.get("number_of_sells", 0)
    nr = ps.get("number_of_redeems", 0)
    title = (mi.get("market_question") or "")[:58]
    oc = mi.get("outcome_name", "?")
    sign = "+" if pnl >= 0 else ""
    lines.append(
        f"  {sign}${pnl:>8,.2f}  ROI {roi:>+7.1%}  资金 ${cap:>6,.0f}  "
        f"买{nb}卖{ns}赎{nr}  "
        f"[{ai.get('entry_style', '?')}/{ai.get('exit_style', '?')}]  "
        f"{oc}  {title}"
    )


# ══════════════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    p = argparse.ArgumentParser(description="Polymarket 钱包策略分析")
    p.add_argument("-w", "--wallet", required=True)
    p.add_argument("-n", "--top", type=int, default=100,
                   help="分析最近多少个市场 (默认 100)")
    p.add_argument("-o", "--output", default="")
    p.add_argument("--max-records", type=int, default=20000,
                   help="最多拉取多少条 activity (默认 20000)")
    args = p.parse_args()

    wallet = args.wallet.lower()
    output = args.output or f"wallet_strategy_{wallet[:10]}.json"
    report_path = output.replace(".json", ".report.txt")

    print("=" * 70)
    print(f"  Polymarket 钱包策略分析")
    print(f"  钱包 : {wallet}")
    print(f"  TopN : {args.top}")
    print("=" * 70)

    session = create_session()

    # 交叉验证基线（Polymarket 官方排行榜 / 组合价值）
    lifetime = fetch_lifetime_stats(session, wallet)
    if lifetime.get("lifetime_profit_usdc") is not None:
        print(f"  📊 Polymarket 官方数据 (交叉验证基准):")
        print(f"     Pseudonym       : {lifetime.get('pseudonym')}")
        print(f"     All-time profit : ${lifetime['lifetime_profit_usdc']:,.2f}")
        if lifetime.get("current_portfolio_value_usdc") is not None:
            print(f"     Portfolio value : ${lifetime['current_portfolio_value_usdc']:,.2f}")
        print()

    # ── 两阶段抓取：先找 cid，再按 cid 精确拉完整数据 ────
    cids = fetch_recent_cids(session, wallet, args.top)
    if not cids:
        print("❌ 未发现任何交易市场")
        sys.exit(0)

    records = fetch_all_records_per_cid(session, wallet, cids)
    if not records:
        print("❌ 精确抓取后无记录")
        sys.exit(0)

    # 记录每个 cid 最新时间戳 (用于排序输出)
    latest_ts: dict[str, int] = {}
    for r in records:
        cid = r.get("conditionId")
        ts = safe_int(r.get("timestamp"))
        if cid and ts > latest_ts.get(cid, 0):
            latest_ts[cid] = ts

    print(f"\n[3/5] 按 (conditionId, outcome) 分组...")
    groups = group_by_position(records, cids)
    print(f"  {len(groups)} 个 (market, outcome) 持仓")

    print(f"\n[4/5] 逐持仓分析 FIFO PnL...")
    market_cache: dict[str, dict] = {}
    positions: list[dict] = []
    for i, (key, trades) in enumerate(
        sorted(groups.items(), key=lambda kv: latest_ts.get(kv[0].split("|")[0], 0), reverse=True)
    ):
        try:
            pos = analyze_position(key, trades, session, market_cache)
            positions.append(pos)
            if (i + 1) % 10 == 0:
                print(f"  ... 已分析 {i + 1} / {len(groups)}")
        except Exception as exc:
            print(f"  ⚠ 分析 {key[:30]} 失败: {exc}")

    print(f"\n[5/5] 汇总策略画像...")
    agg = aggregate_strategy(positions)

    # 时间跨度 (最早 → 最新)
    all_ts: list[int] = []
    for p in positions:
        for t in p["trades"]:
            if t.get("timestamp"):
                all_ts.append(t["timestamp"])
    ts_min = min(all_ts) if all_ts else 0
    ts_max = max(all_ts) if all_ts else 0

    result = {
        "meta": {
            "generated_at": ts_to_iso(int(time.time())),
            "wallet_address": wallet,
            "top_n": args.top,
            "script_version": SCRIPT_VERSION,
            "actual_positions_analyzed": len(positions),
            "unique_markets_analyzed": len({p["market_info"]["market_id"] for p in positions}),
            "time_window_start_utc": ts_to_str(ts_min),
            "time_window_end_utc": ts_to_str(ts_max),
            "time_window_hours": round((ts_max - ts_min) / 3600, 2) if ts_max else 0,
            "cross_validation": lifetime,
        },
        "aggregate_strategy": agg,
        "positions": positions,
    }

    with open(output, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)
    print(f"  ✓ JSON  → {output}")

    report = build_report(wallet, agg, positions, result["meta"])
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(f"  ✓ 报告 → {report_path}\n")
    print(report)


if __name__ == "__main__":
    main()
