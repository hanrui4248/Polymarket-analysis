#!/usr/bin/env bash
#
# Polymarket 持仓深度分析 — 快速启动脚本
#
# 用法：
#   ./run.sh                          ← 使用下方默认配置运行
#   ./run.sh "市场名称关键词"          ← 覆盖持仓名称
#   ./run.sh "市场名称" 0xABC...      ← 覆盖持仓名称 + 钱包地址

# ──────────────── 默认配置（按需修改） ────────────────
WALLET="0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82"
POSITION="Bitcoin Up or Down - April 26, 9:45AM-9:50AM ET"
START_DATE=""
OUTPUT=""  # 留空则自动生成文件名
# ──────────────────────────────────────────────────────

# 命令行参数覆盖
[ -n "$1" ] && POSITION="$1"
[ -n "$2" ] && WALLET="$2"
[ -n "$3" ] && START_DATE="$3"

cd "$(dirname "$0")"

python position_deep_fetcher.py \
    --wallet "$WALLET" \
    --position "$POSITION" 
    # --start-date "$START_DATE" \
    # ${OUTPUT:+--output "$OUTPUT"}
