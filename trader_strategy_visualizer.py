#!/usr/bin/env python3
"""
Polymarket 交易员策略可视化 & 解码器 (Trader Strategy Visualizer & Decoder)

输入: position_analysis_*.json (由 polymarket 分析脚本产出)
输出: 一个交互式 HTML dashboard, 把这个钱包在单一 market 上的所有交易
      拆解成 "能看懂策略" 的多面板视图。

用法:
    python trader_strategy_visualizer.py INPUT.json [-o OUTPUT.html] [--no-open]

设计目标:
    让一个完全没有上下文的读者, 在 30 秒内看懂:
        1. 这名交易员赚/亏了多少, 用了多少本金, 持续多久
        2. 这是 "单边方向性押注" 还是 "双边对冲/做市"
        3. 他是在建仓还是在去仓? 是 scale-in 还是 rapid-fire?
        4. 执行上他是吃单 (cross spread) 还是挂单? 滑点多大?
        5. 他靠什么赚钱 —— 方向、时机、spread、还是配对套利?

依赖:
    pip install pandas numpy plotly
"""
from __future__ import annotations

import argparse
import json
import sys
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# 颜色 / 样式常量
# ---------------------------------------------------------------------------
SIDE_COLOR = {
    "Up": "#16a34a",      # 绿
    "Down": "#dc2626",    # 红
}
ACTION_STYLE = {
    # action_type -> (symbol, color_override_or_None, label)
    "buy":    ("triangle-up", None,        "Buy"),
    "sell":   ("triangle-down", None,      "Sell"),
    "merge":  ("diamond",     "#8b5cf6",   "Merge (配对兑现)"),
    "redeem": ("star",        "#f59e0b",   "Redeem (结算)"),
    "split":  ("cross",       "#06b6d4",   "Split"),
}

PLOT_BG = "#0f172a"
PAPER_BG = "#0f172a"
FG = "#e2e8f0"
GRID = "#1e293b"


# ---------------------------------------------------------------------------
# 数据加载 & 规整
# ---------------------------------------------------------------------------
def _to_float(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def load_positions(path: Path) -> tuple[dict, list[pd.DataFrame]]:
    """返回 (meta, [每个 position 的 trades DataFrame])."""
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    meta = {
        **raw.get("meta", {}),
        "positions_summary": [p.get("position_summary", {}) for p in raw["positions"]],
        "market_info":       [p.get("market_info", {})       for p in raw["positions"]],
    }

    dfs: list[pd.DataFrame] = []
    for p in raw["positions"]:
        side = p["market_info"]["position_side"]
        rows = []
        for t in p["trades"]:
            ctx = t.get("market_context") or {}
            rows.append({
                "side": side,
                "timestamp": int(t["timestamp"]),
                "datetime": pd.to_datetime(int(t["timestamp"]), unit="s", utc=True),
                "sequence": int(t["sequence_in_position"]),
                "action": t["action_type"],
                "action_label": t.get("trade_side_label", t["action_type"]).strip(),
                "shares": _to_float(t.get("shares")),
                "price": _to_float(t.get("price")),
                "amount_usdc": _to_float(t.get("amount_usdc")),
                "remaining": _to_float(t.get("remaining_position_after_trade")),
                "avg_entry": _to_float(t.get("avg_entry_price_after_trade")),
                "realized_this": _to_float(t.get("realized_pnl_from_this_trade")),
                "cum_pnl": _to_float(t.get("cumulative_realized_pnl")),
                "is_opening": bool(t.get("is_opening_trade")),
                "is_add":     bool(t.get("is_add_to_position")),
                "is_reduce":  bool(t.get("is_reduce_trade")),
                "is_close":   bool(t.get("is_close_trade")),
                "mkt_price_before": _to_float(ctx.get("market_price_before_trade")),
                "mkt_price_after":  _to_float(ctx.get("market_price_after_trade")),
                "tx_hash": t.get("tx_hash"),
            })
        df = pd.DataFrame(rows).sort_values(["timestamp", "sequence"]).reset_index(drop=True)
        # 每笔之后的 "持仓美元成本基础": remaining * avg_entry
        df["cost_basis_value"] = df["remaining"] * df["avg_entry"]
        dfs.append(df)
    return meta, dfs


# ---------------------------------------------------------------------------
# 策略自动解读 (从数据推断)
# ---------------------------------------------------------------------------
@dataclass
class StrategyVerdict:
    headline: str
    bullets: list[str]


def _simultaneous_peak_capital(dfs: list[pd.DataFrame]) -> float:
    """跨 leg 合并 cost_basis_value 时间序列, 取同一瞬间两边之和的峰值."""
    if not dfs:
        return 0.0
    pieces = []
    for df in dfs:
        pieces.append(df[["datetime", "cost_basis_value"]].rename(
            columns={"cost_basis_value": df["side"].iloc[0]}))
    merged = pieces[0]
    for other in pieces[1:]:
        merged = pd.merge(merged, other, on="datetime", how="outer")
    merged = merged.sort_values("datetime").ffill().fillna(0.0)
    total = merged.drop(columns="datetime").sum(axis=1)
    return float(total.max()) if len(total) else 0.0


def interpret_strategy(meta: dict, dfs: list[pd.DataFrame]) -> StrategyVerdict:
    summaries = meta["positions_summary"]
    sides = [mi["position_side"] for mi in meta["market_info"]]

    total_pnl = sum(_to_float(s.get("final_realized_pnl")) for s in summaries)
    # 真实同时峰值 (优先), 回退到各腿峰值之和
    total_deployed = _simultaneous_peak_capital(dfs)
    if total_deployed == 0:
        total_deployed = sum(_to_float(s.get("max_capital_deployed_usdc")) for s in summaries)
    num_trades = sum(_to_float(s.get("ai_analysis_features", {}).get("total_trade_count", 0)) for s in summaries)
    # 同一 merge on-chain event 在两边各记一次 -> 取 max 作为真实事件数
    merges = max((int(s.get("number_of_merges", 0)) for s in summaries), default=0)
    redeems = sum(int(s.get("number_of_redeems", 0)) for s in summaries)

    all_trades = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if not all_trades.empty:
        duration_s = int(all_trades["timestamp"].max() - all_trades["timestamp"].min())
    else:
        duration_s = 0

    both_sides = set(sides) >= {"Up", "Down"}

    # 头条
    sign = "盈" if total_pnl > 0 else ("亏" if total_pnl < 0 else "平")
    headline = (
        f"{sign} ${total_pnl:,.2f} / ROI {(total_pnl / total_deployed * 100 if total_deployed else 0):+.1f}%"
        f" · {int(num_trades)} 笔 / {duration_s}s"
        f" · {'双边 (Up+Down 同时持仓)' if both_sides else '单边方向性押注'}"
    )

    bullets: list[str] = []

    # 双边 + 大量 merge => 配对做市/套利
    if both_sides and merges >= 5:
        bullets.append(
            f"🧩 **双边配对套利 / 做市剥头皮**: 同时在 Up 和 Down 两边累积头寸, "
            f"共发起 {merges} 次 merge (1 Up + 1 Down = $1 配对兑现). "
            f"这意味着他不是在赌方向, 而是在吃 Up 价 + Down 价 < $1 的瞬时错价."
        )

    # 交易节奏
    features = [s.get("ai_analysis_features", {}) for s in summaries]
    avg_gaps = [f.get("average_time_between_buys_seconds") for f in features if f.get("average_time_between_buys_seconds") is not None]
    if avg_gaps:
        bullets.append(
            f"⚡ **节奏**: 平均每 {min(avg_gaps):.2f}–{max(avg_gaps):.2f} 秒一单 ({', '.join(set(f.get('buy_time_distribution','?') for f in features))}). "
            f"基本可以判断是 bot 或高度自动化脚本, 不是人手操作."
        )

    # 对比两边的绩效
    pos_df = pd.DataFrame([
        {"side": sides[i], **summaries[i], **summaries[i].get("ai_analysis_features", {})}
        for i in range(len(summaries))
    ])
    if len(pos_df) == 2:
        pnl_by_side = pos_df.set_index("side")["final_realized_pnl"].apply(_to_float)
        winner = pnl_by_side.idxmax()
        loser = pnl_by_side.idxmin()
        bullets.append(
            f"🎯 **盈亏不对称**: {winner} 腿 {pnl_by_side[winner]:+,.2f} vs {loser} 腿 {pnl_by_side[loser]:+,.2f}. "
            f"赢的一边均价 {_to_float(pos_df.set_index('side').loc[winner, 'avg_buy_price']):.3f}, "
            f"输的一边均价 {_to_float(pos_df.set_index('side').loc[loser, 'avg_buy_price']):.3f} "
            f"—— 盈亏主要取决于 **哪一边抓到了更便宜的入场价**."
        )

    # Redeem 提示 (结算)
    if redeems > 0:
        bullets.append(
            f"🏁 **持有到结算**: 有 {redeems} 次 redeem (市场最终裁定, 胜方 $1 / 股), "
            f"说明最后留了一小段裸多头寸等待结果, 不是纯中性策略."
        )

    # 最大资金占用 vs 盈亏
    if total_deployed > 0:
        turnover_ratio = num_trades / max(1, int(total_deployed / 10))
        bullets.append(
            f"💰 **资本效率**: 最大同时占用资金 ${total_deployed:,.0f}, 单笔成交均值 "
            f"${sum(_to_float(s.get('ai_analysis_features',{}).get('avg_trade_size_usdc')) for s in summaries) / max(1,len(summaries)):.1f}. "
            f"换手次数 / 资金规模比值 ≈ {turnover_ratio:.1f}, 属于 **高频小额** 作风."
        )

    return StrategyVerdict(headline=headline, bullets=bullets)


# ---------------------------------------------------------------------------
# 构图辅助
# ---------------------------------------------------------------------------
def _action_marker(action: str) -> tuple[str, str | None]:
    sym, color, _ = ACTION_STYLE.get(action, ("circle", None, action))
    return sym, color


def _price_timeline(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """把两边的 market_price_before_trade 对齐到同一时间轴上, 便于算 Up+Down 价差."""
    frames = []
    for df in dfs:
        frames.append(df[["datetime", "side", "mkt_price_before"]].copy())
    mp = pd.concat(frames, ignore_index=True)
    mp = mp.dropna(subset=["mkt_price_before"])
    mp = mp.sort_values("datetime")
    pivoted = mp.pivot_table(
        index="datetime", columns="side", values="mkt_price_before", aggfunc="last"
    ).sort_index().ffill()
    for s in ("Up", "Down"):
        if s not in pivoted.columns:
            pivoted[s] = np.nan
    pivoted["sum"] = pivoted["Up"] + pivoted["Down"]
    return pivoted.reset_index()


# ---------------------------------------------------------------------------
# 主图: 时间轴 4 行
# ---------------------------------------------------------------------------
def build_timeseries_figure(dfs: list[pd.DataFrame]) -> go.Figure:
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.035,
        row_heights=[0.35, 0.20, 0.18, 0.27],
        subplot_titles=(
            "① 成交价 & 市场价 (trade marker size ∝ USDC, diamond = merge, star = redeem)",
            "② 持仓规模 (Up 向上, Down 向下, 面积 = shares)",
            "③ 占用资金 ($, cost basis = remaining × avg entry)",
            "④ 累计已实现盈亏 ($)",
        ),
    )

    # ---- Row 1: 市场价线 + 成交散点 ----
    price_tl = _price_timeline(dfs)
    for side in ("Up", "Down"):
        if side in price_tl.columns and price_tl[side].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=price_tl["datetime"], y=price_tl[side],
                    mode="lines", name=f"{side} mid",
                    line=dict(color=SIDE_COLOR[side], width=1.2, dash="dot"),
                    hovertemplate=f"<b>{side} mid</b><br>%{{x|%H:%M:%S}}<br>price=%{{y:.3f}}<extra></extra>",
                    opacity=0.55,
                ),
                row=1, col=1,
            )
    # Up + Down 之和 (<1 = 可 merge 套利, >1 = split 套利)
    if "sum" in price_tl.columns and price_tl["sum"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=price_tl["datetime"], y=price_tl["sum"],
                mode="lines", name="Up+Down sum",
                line=dict(color="#f59e0b", width=1.5),
                hovertemplate="<b>Up+Down</b><br>%{x|%H:%M:%S}<br>sum=%{y:.3f}<extra></extra>",
            ),
            row=1, col=1,
        )
        # 参考线: y=1.0
        fig.add_hline(
            y=1.0, line=dict(color="#64748b", width=1, dash="dash"),
            annotation_text="sum = 1.00 (无套利)", annotation_position="top right",
            row=1, col=1,
        )

    # 每笔成交 (按 action 类型分组, 便于 legend 点选)
    combined = pd.concat(dfs, ignore_index=True)
    for (side, action), grp in combined.groupby(["side", "action"]):
        sym, color_override = _action_marker(action)
        color = color_override or SIDE_COLOR.get(side, "#94a3b8")
        # 散点尺寸: sqrt(USDC) 更均衡
        size_raw = grp["amount_usdc"].fillna(0).clip(lower=0)
        sizes = 4 + np.sqrt(size_raw) * 1.6
        # merge 的 "price" 是 0, 用 avg_entry 反而更有意义; redeem 用 1.0 (winner payout)
        if action == "merge":
            y_values = grp["avg_entry"]
            price_hint = "(merge: y = avg entry)"
        elif action == "redeem":
            y_values = pd.Series([1.0] * len(grp), index=grp.index)
            price_hint = "(redeem: y = 1.00 payout)"
        else:
            y_values = grp["price"]
            price_hint = ""
        fig.add_trace(
            go.Scatter(
                x=grp["datetime"], y=y_values,
                mode="markers",
                name=f"{side} · {ACTION_STYLE.get(action,(None,None,action))[2]}",
                marker=dict(
                    symbol=sym, color=color, size=sizes,
                    line=dict(width=0.6, color="#0f172a"),
                    opacity=0.85,
                ),
                customdata=np.stack([
                    grp["shares"].fillna(0),
                    grp["amount_usdc"].fillna(0),
                    grp["price"].fillna(0),
                    grp["cum_pnl"].fillna(0),
                    grp["sequence"],
                ], axis=-1),
                hovertemplate=(
                    f"<b>{side} · %{{text}}</b> {price_hint}<br>"
                    "%{x|%H:%M:%S}<br>"
                    "price=%{customdata[2]:.3f}<br>"
                    "shares=%{customdata[0]:.2f}<br>"
                    "USDC=$%{customdata[1]:,.2f}<br>"
                    "cum PnL=$%{customdata[3]:,.2f}<br>"
                    "seq=#%{customdata[4]}<extra></extra>"
                ),
                text=grp["action_label"],
                legendgroup=f"{side}-{action}",
            ),
            row=1, col=1,
        )

    # ---- Row 2: 持仓规模 (镜像) ----
    for df in dfs:
        side = df["side"].iloc[0]
        sign = 1 if side == "Up" else -1
        fig.add_trace(
            go.Scatter(
                x=df["datetime"],
                y=df["remaining"] * sign,
                mode="lines",
                name=f"{side} shares held",
                line=dict(color=SIDE_COLOR[side], width=1.2),
                fill="tozeroy",
                fillcolor=_rgba(SIDE_COLOR[side], 0.25),
                hovertemplate=f"<b>{side} shares</b><br>%{{x|%H:%M:%S}}<br>%{{customdata:.2f}} shares<extra></extra>",
                customdata=df["remaining"],
                legendgroup=f"shares-{side}",
            ),
            row=2, col=1,
        )
    fig.add_hline(y=0, line=dict(color=GRID, width=1), row=2, col=1)

    # ---- Row 3: 占用资金 ----
    for df in dfs:
        side = df["side"].iloc[0]
        fig.add_trace(
            go.Scatter(
                x=df["datetime"], y=df["cost_basis_value"],
                mode="lines",
                name=f"{side} capital",
                line=dict(color=SIDE_COLOR[side], width=1.2),
                fill="tozeroy",
                fillcolor=_rgba(SIDE_COLOR[side], 0.18),
                hovertemplate=f"<b>{side} capital</b><br>%{{x|%H:%M:%S}}<br>$%{{y:,.2f}}<extra></extra>",
                legendgroup=f"cap-{side}",
            ),
            row=3, col=1,
        )

    # ---- Row 4: 累计 PnL ----
    for df in dfs:
        side = df["side"].iloc[0]
        fig.add_trace(
            go.Scatter(
                x=df["datetime"], y=df["cum_pnl"],
                mode="lines",
                name=f"{side} cum PnL",
                line=dict(color=SIDE_COLOR[side], width=1.6),
                hovertemplate=f"<b>{side} cum PnL</b><br>%{{x|%H:%M:%S}}<br>$%{{y:,.2f}}<extra></extra>",
                legendgroup=f"pnl-{side}",
            ),
            row=4, col=1,
        )
    # 合成 net PnL (按时间对齐)
    net = _net_pnl_curve(dfs)
    if not net.empty:
        fig.add_trace(
            go.Scatter(
                x=net["datetime"], y=net["net"],
                mode="lines", name="Net PnL",
                line=dict(color="#38bdf8", width=2.2),
                hovertemplate="<b>Net PnL</b><br>%{x|%H:%M:%S}<br>$%{y:,.2f}<extra></extra>",
            ),
            row=4, col=1,
        )
    fig.add_hline(y=0, line=dict(color=GRID, width=1), row=4, col=1)

    # 轴/布局
    fig.update_yaxes(title_text="price", range=[-0.02, 1.25], row=1, col=1,
                     gridcolor=GRID, zeroline=False)
    fig.update_yaxes(title_text="shares (±)", row=2, col=1, gridcolor=GRID, zeroline=False)
    fig.update_yaxes(title_text="$ at risk", row=3, col=1, gridcolor=GRID, zeroline=False)
    fig.update_yaxes(title_text="$ PnL", row=4, col=1, gridcolor=GRID, zeroline=False)
    fig.update_xaxes(gridcolor=GRID, zeroline=False)

    fig.update_layout(
        height=1050,
        template="plotly_dark",
        paper_bgcolor=PAPER_BG, plot_bgcolor=PLOT_BG, font=dict(color=FG),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, bgcolor="rgba(0,0,0,0)",
            font=dict(size=10),
        ),
        margin=dict(l=70, r=30, t=80, b=40),
        hovermode="closest",
    )
    return fig


def _net_pnl_curve(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """按时间合并两边的 cum_pnl, 前向填充后相加得到净 PnL 曲线."""
    if not dfs:
        return pd.DataFrame()
    pieces = []
    for df in dfs:
        pieces.append(df[["datetime", "cum_pnl"]].rename(columns={"cum_pnl": df["side"].iloc[0]}))
    # outer merge + ffill
    merged = pieces[0]
    for other in pieces[1:]:
        merged = pd.merge(merged, other, on="datetime", how="outer")
    merged = merged.sort_values("datetime").ffill().fillna(0.0)
    merged["net"] = merged.drop(columns="datetime").sum(axis=1)
    return merged


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[:2], 16), int(h[2:4], 16), int(h[4:], 16)
    return f"rgba({r},{g},{b},{alpha})"


# ---------------------------------------------------------------------------
# 策略指纹 2x2
# ---------------------------------------------------------------------------
def build_fingerprint_figure(dfs: list[pd.DataFrame]) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "A. 执行成本: 成交价 vs 市场中间价 (偏离对角线 = 吃单滑点)",
            "B. 交易节奏: 相邻交易间隔 (秒) 分布",
            "C. 单笔规模分布 (USDC, 按方向分)",
            "D. Merge 事件: 每次 merge 了多少股 (同一次 merge 两边同步)",
        ),
        horizontal_spacing=0.1, vertical_spacing=0.15,
    )

    combined = pd.concat(dfs, ignore_index=True)

    # A. 执行散点
    for side, grp in combined.groupby("side"):
        mask = grp["action"] == "buy"
        sub = grp[mask]
        fig.add_trace(
            go.Scatter(
                x=sub["mkt_price_before"], y=sub["price"],
                mode="markers",
                name=f"{side} buys",
                marker=dict(color=SIDE_COLOR[side], size=5, opacity=0.6,
                            line=dict(width=0.3, color="#0f172a")),
                hovertemplate=f"<b>{side} buy</b><br>mid=%{{x:.3f}}<br>fill=%{{y:.3f}}<extra></extra>",
                legendgroup=f"exec-{side}", showlegend=True,
            ),
            row=1, col=1,
        )
    fig.add_trace(
        go.Scatter(
            x=[0, 1], y=[0, 1], mode="lines",
            line=dict(color="#64748b", dash="dash"),
            name="y=x (零滑点)", showlegend=False,
        ),
        row=1, col=1,
    )

    # B. 交易间隔分布
    for side, grp in combined.groupby("side"):
        grp = grp[grp["action"] == "buy"].sort_values("timestamp")
        gaps = grp["timestamp"].diff().dropna()
        gaps = gaps[gaps >= 0]
        if len(gaps) == 0:
            continue
        fig.add_trace(
            go.Histogram(
                x=gaps, name=f"{side} Δt",
                marker_color=SIDE_COLOR[side],
                opacity=0.65, nbinsx=40,
                legendgroup=f"gap-{side}",
            ),
            row=1, col=2,
        )
    fig.update_layout(barmode="overlay")

    # C. 单笔规模分布 (log-ish: 用 box + scatter overlay)
    for side, grp in combined.groupby("side"):
        buys = grp[grp["action"] == "buy"]
        if buys.empty:
            continue
        fig.add_trace(
            go.Violin(
                y=buys["amount_usdc"], x=[side] * len(buys),
                name=f"{side} $ size",
                line_color=SIDE_COLOR[side],
                fillcolor=_rgba(SIDE_COLOR[side], 0.3),
                box_visible=True, meanline_visible=True,
                points="outliers",
                legendgroup=f"size-{side}",
            ),
            row=2, col=1,
        )

    # D. Merge 事件柱状图 (合并两边的 merge, 用 x=时间, y=shares)
    merges = combined[combined["action"] == "merge"].copy()
    if not merges.empty:
        # 同一 timestamp 两边都记一次 -> 只取一边避免重复 (用最大 shares)
        by_ts = merges.groupby("timestamp").agg(
            shares=("shares", "max"),
            datetime=("datetime", "first"),
        ).reset_index()
        fig.add_trace(
            go.Bar(
                x=by_ts["datetime"], y=by_ts["shares"],
                name="Merge shares",
                marker_color="#8b5cf6",
                hovertemplate="<b>Merge</b><br>%{x|%H:%M:%S}<br>%{y:.2f} shares<extra></extra>",
            ),
            row=2, col=2,
        )

    # 轴
    fig.update_xaxes(title_text="market mid price", row=1, col=1, range=[0, 1], gridcolor=GRID)
    fig.update_yaxes(title_text="fill price",       row=1, col=1, range=[0, 1], gridcolor=GRID)

    fig.update_xaxes(title_text="seconds between buys", row=1, col=2, gridcolor=GRID)
    fig.update_yaxes(title_text="count",                row=1, col=2, gridcolor=GRID)

    fig.update_xaxes(title_text="side", row=2, col=1, gridcolor=GRID)
    fig.update_yaxes(title_text="USDC per trade", row=2, col=1, gridcolor=GRID)

    fig.update_xaxes(title_text="time", row=2, col=2, gridcolor=GRID)
    fig.update_yaxes(title_text="shares merged", row=2, col=2, gridcolor=GRID)

    fig.update_layout(
        height=800,
        template="plotly_dark",
        paper_bgcolor=PAPER_BG, plot_bgcolor=PLOT_BG, font=dict(color=FG),
        showlegend=True,
        legend=dict(orientation="h", y=-0.12, x=0, font=dict(size=10)),
        margin=dict(l=60, r=30, t=70, b=80),
    )
    return fig


# ---------------------------------------------------------------------------
# HTML 组装
# ---------------------------------------------------------------------------
def build_kpi_cards(meta: dict, dfs: list[pd.DataFrame]) -> str:
    summaries = meta["positions_summary"]
    sides = [mi["position_side"] for mi in meta["market_info"]]

    total_pnl = sum(_to_float(s.get("final_realized_pnl")) for s in summaries)
    total_deployed = _simultaneous_peak_capital(dfs) or \
        sum(_to_float(s.get("max_capital_deployed_usdc")) for s in summaries)
    total_trades = sum(int(s.get("ai_analysis_features", {}).get("total_trade_count", 0)) for s in summaries)
    merges = max((int(s.get("number_of_merges", 0)) for s in summaries), default=0)
    redeems = sum(int(s.get("number_of_redeems", 0)) for s in summaries)
    holding_s = max((int(s.get("holding_duration_seconds") or 0)) for s in summaries) if summaries else 0
    avg_gap = np.mean([
        _to_float(s.get("ai_analysis_features", {}).get("average_time_between_buys_seconds"))
        for s in summaries
        if s.get("ai_analysis_features", {}).get("average_time_between_buys_seconds") is not None
    ]) if summaries else float("nan")

    roi = (total_pnl / total_deployed * 100) if total_deployed else 0

    def card(title: str, value: str, sub: str = "", color: str = FG) -> str:
        return (
            f'<div class="kpi"><div class="kpi-title">{title}</div>'
            f'<div class="kpi-value" style="color:{color}">{value}</div>'
            f'<div class="kpi-sub">{sub}</div></div>'
        )

    pnl_color = "#22c55e" if total_pnl > 0 else ("#ef4444" if total_pnl < 0 else FG)
    cards = [
        card("Net PnL (已实现)", f"${total_pnl:,.2f}", f"ROI {roi:+.2f}% (对峰值占用)", pnl_color),
        card("峰值占用资金", f"${total_deployed:,.0f}", "同一瞬间两腿之和"),
        card("交易笔数", f"{total_trades:,}", f"平均 {avg_gap:.2f}s/单" if not np.isnan(avg_gap) else ""),
        card("持续时长", f"{holding_s}s", f"≈ {holding_s/60:.1f} 分钟"),
        card("Merge 次数", f"{merges}", "双边配对兑现"),
        card("Redeem 次数", f"{redeems}", "持有到结算"),
    ]

    # per-side mini cards
    side_cards = []
    for i, s in enumerate(summaries):
        side = sides[i]
        pnl = _to_float(s.get("final_realized_pnl"))
        avg_buy = _to_float(s.get("avg_buy_price"))
        max_shares = _to_float(s.get("max_position_size_shares"))
        features = s.get("ai_analysis_features", {})
        pnl_color_s = "#22c55e" if pnl > 0 else ("#ef4444" if pnl < 0 else FG)
        body = (
            f"<b style='color:{SIDE_COLOR.get(side, FG)}'>{side} 腿</b><br>"
            f"PnL: <span style='color:{pnl_color_s}'>${pnl:+,.2f}</span><br>"
            f"avg buy: {avg_buy:.3f} · max: {max_shares:,.0f} shares<br>"
            f"<span class='tag'>{features.get('entry_style','?')}</span> "
            f"<span class='tag'>{features.get('risk_style','?')}</span> "
            f"<span class='tag'>{features.get('timing_quality','?')} timing</span>"
        )
        side_cards.append(f'<div class="side-card">{body}</div>')

    return (
        '<div class="kpi-row">' + "".join(cards) + '</div>'
        '<div class="side-row">' + "".join(side_cards) + '</div>'
    )


def render_html(meta: dict, dfs: list[pd.DataFrame], out_path: Path) -> None:
    ts_fig = build_timeseries_figure(dfs)
    fp_fig = build_fingerprint_figure(dfs)
    verdict = interpret_strategy(meta, dfs)

    kpi_html = build_kpi_cards(meta, dfs)
    bullets_html = "\n".join(f"<li>{b}</li>" for b in verdict.bullets)

    wallet = meta.get("wallet_address", "?")
    market = meta.get("position_query", "?")

    html = f"""<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>Polymarket 交易员策略解码 · {market}</title>
<style>
    :root {{
        --bg: {PLOT_BG};
        --fg: {FG};
        --muted: #94a3b8;
        --card: #1e293b;
        --accent: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
        margin: 0; padding: 24px 32px 48px;
        font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'PingFang SC', sans-serif;
        background: var(--bg); color: var(--fg);
    }}
    h1 {{ font-size: 22px; margin: 0 0 4px; font-weight: 600; }}
    h2 {{ font-size: 16px; margin: 32px 0 12px; color: var(--accent); font-weight: 600; }}
    .meta {{ color: var(--muted); font-size: 12px; margin-bottom: 16px; }}
    .meta code {{ background: var(--card); padding: 1px 6px; border-radius: 3px; }}
    .headline {{
        font-size: 18px; padding: 10px 14px; margin: 12px 0 20px;
        background: linear-gradient(90deg, #1e293b, #0f172a);
        border-left: 3px solid var(--accent); border-radius: 4px;
    }}
    .kpi-row {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 12px; }}
    .kpi {{ background: var(--card); padding: 10px 14px; border-radius: 6px; }}
    .kpi-title {{ font-size: 11px; color: var(--muted); margin-bottom: 4px; letter-spacing: 0.04em; text-transform: uppercase; }}
    .kpi-value {{ font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; }}
    .kpi-sub   {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}
    .side-row  {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin-bottom: 8px; }}
    .side-card {{ background: var(--card); padding: 10px 14px; border-radius: 6px; font-size: 13px; line-height: 1.6; }}
    .tag {{
        display: inline-block; padding: 1px 6px; border-radius: 3px;
        background: #334155; color: var(--fg); font-size: 11px; margin-right: 4px;
    }}
    .bullets {{ background: var(--card); padding: 12px 20px; border-radius: 6px; }}
    .bullets li {{ margin: 6px 0; line-height: 1.55; }}
    .bullets strong {{ color: var(--accent); }}
    code {{ color: #cbd5e1; }}
    .hint {{ color: var(--muted); font-size: 12px; margin-top: -6px; margin-bottom: 8px; }}
</style>
</head>
<body>
<h1>Polymarket 交易员策略解码</h1>
<div class="meta">
  Wallet <code>{wallet}</code> &nbsp;·&nbsp; Market <code>{market}</code> &nbsp;·&nbsp;
  生成于 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
</div>

<div class="headline">{verdict.headline}</div>

<h2>关键指标 (Key Numbers)</h2>
{kpi_html}

<h2>自动策略解读 (Auto-decoded Strategy)</h2>
<ul class="bullets">
{bullets_html}
</ul>
<div class="hint">
  下方图表支持框选缩放、图例点选隐藏、hover 查看每笔细节。
  主时间轴 4 行共享 X 轴 —— 在价格图里框选一段时间, 其他 3 行会同步缩放, 便于对准某个 merge / 拉升瞬间看整体行为。
</div>

<h2>① 时间轴主视图 (Time-series)</h2>
{ts_fig.to_html(full_html=False, include_plotlyjs='cdn', div_id='ts')}

<h2>② 策略指纹 (Strategy Fingerprint)</h2>
{fp_fig.to_html(full_html=False, include_plotlyjs=False, div_id='fp')}

<h2>如何读懂这张图</h2>
<ul class="bullets">
<li><strong>行①</strong>: 每个三角 = 一次买入, 大小 ∝ USDC 金额; 钻石 = merge (把 1 Up + 1 Down 配对兑成 $1); 星 = redeem (市场结算时赢家按 $1/股 拿钱). 橙色细线是 Up 价 + Down 价之和, 低于 1 = merge 可获利, 高于 1 = split 可获利.</li>
<li><strong>行②</strong>: Up 向上, Down 向下 —— 两边镜像看. 如果 Up 面积 ≈ Down 面积, 说明他在做 <b>delta 中性</b>; 明显一边更大 = 有方向性偏向.</li>
<li><strong>行③</strong>: 任何时刻占用的总美元成本, 用于评估资金效率与风险暴露.</li>
<li><strong>行④</strong>: 两边的已实现盈亏 + 净值曲线 (蓝). 看净值在 merge / redeem 点的跳变最能说明 "钱是从哪一步赚出来的".</li>
<li><strong>A (执行成本)</strong>: 点远离对角线 = 吃单滑点大, 也就是为了抢速度在付 spread. 高频 bot 通常会有明显偏离.</li>
<li><strong>B (交易节奏)</strong>: 间隔集中在 <1s = 机器人; 多峰分布 = 可能有触发条件 (比如价格变动触发).</li>
<li><strong>C (规模分布)</strong>: 是 "均匀小单" 还是 "偶尔有大单"? 大单的位置通常对应关键时刻.</li>
<li><strong>D (Merge 分布)</strong>: merge 集中在哪几个时点? 与行①的 sum<1 瞬间是否对齐? 能验证 "套利型策略" 的假设.</li>
</ul>

</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Polymarket 交易员策略可视化")
    ap.add_argument("json_path", type=Path, help="position_analysis_*.json 文件路径")
    ap.add_argument("-o", "--output", type=Path, default=None, help="输出 HTML 路径 (默认: 输入文件同目录同名 _strategy.html)")
    ap.add_argument("--no-open", action="store_true", help="生成后不自动在浏览器打开")
    args = ap.parse_args()

    if not args.json_path.exists():
        print(f"❌ 文件不存在: {args.json_path}", file=sys.stderr)
        return 1

    meta, dfs = load_positions(args.json_path)
    out_path = args.output or args.json_path.with_name(args.json_path.stem + "_strategy.html")

    render_html(meta, dfs, out_path)

    verdict = interpret_strategy(meta, dfs)
    print(f"\n✅ 已生成: {out_path}")
    print(f"   {verdict.headline}\n")
    for b in verdict.bullets:
        # 控制台版本去掉 markdown
        plain = b.replace("**", "").replace("`", "")
        print(f"   • {plain}")

    if not args.no_open:
        try:
            webbrowser.open(out_path.resolve().as_uri())
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
