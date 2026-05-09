"""
从 parquet 文件中导出前 N 个市场窗口的数据为 CSV
用法: python export_sample.py data/btc-5m-2026-04-10.parquet -n 3
"""

import argparse
import os
import fastparquet


def export_sample(parquet_path: str, n: int):
    pf = fastparquet.ParquetFile(parquet_path)

    print("读取 market_slug 列...")
    slugs_df = pf.to_pandas(columns=["market_slug"])
    all_slugs = slugs_df["market_slug"].unique()
    selected = set(all_slugs[:n])

    print(f"总市场数: {len(all_slugs)}，筛选前 {n} 个...")
    filters = [("market_slug", "in", selected)]
    sample = pf.to_pandas(filters=filters)

    base = os.path.splitext(parquet_path)[0]
    out_file = f"{base}-sample-{n}markets.csv"
    sample.to_csv(out_file, index=False)

    size_mb = round(os.path.getsize(out_file) / 1024 / 1024, 1)
    print(f"导出市场: {list(selected)}")
    print(f"行数: {len(sample)} / {len(slugs_df)}")
    print(f"已保存: {out_file} ({size_mb} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导出前 N 个市场窗口为 CSV 小样本")
    parser.add_argument("parquet_file", help="parquet 文件路径")
    parser.add_argument("-n", type=int, default=3, help="导出前几个市场 (默认: 3)")
    args = parser.parse_args()

    export_sample(args.parquet_file, args.n)
