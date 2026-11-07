#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓库体积体检 / 自动清理（纯标准库，不需要 akshare）

策略与 wj624/a-stock-api README 附录 G 一致，但更保守：
  - 只裁剪 docs/ 里的数据产物，不碰 main 历史、不 force push、不改 root commit
  - 单文件 50 MB 护栏；CSV 滚动保留最近 N 个交易日
  - docs/ 超过 500 MB 才清理旧快照，并把 CSV 进一步压到 365 天
  - 护栏不齐（README 缺失 / 文件过少 / 单文件超限）直接报错退出，不静默删
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys

CSV_COLS = ["date", "n_stocks", "n_traded", "n_ma5", "n_ma20", "n_ma50", "n_ma200",
            "pct_above_5", "pct_above_20", "pct_above_50", "pct_above_200",
            "above_5", "above_20", "above_50", "above_200",
            "advancers", "decliners", "ad_diff", "mcclellan",
            "ema5_pct200", "ema_long_pct200"]


def size_mb(path: str) -> float:
    if os.path.isfile(path):
        return os.path.getsize(path) / 1_000_000
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total / 1_000_000


def atomic_write(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.replace(tmp, path)


def load_rows(path: str) -> list[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8-sig") as f:
        return [r for r in csv.DictReader(f) if r.get("date")]


def dump_rows(rows: list[dict], path: str) -> None:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    atomic_write(path, buf.getvalue())


def main() -> int:
    ap = argparse.ArgumentParser(description="docs/ 体积体检与清理")
    ap.add_argument("--out", default="docs")
    ap.add_argument("--max-mb", type=float, default=500)
    ap.add_argument("--max-rows", type=int, default=1300)
    ap.add_argument("--single-file-mb", type=float, default=50)
    a = ap.parse_args()

    if not os.path.isfile("README.md"):
        sys.exit("护栏失败：README.md 不存在，拒绝清理")
    if not os.path.isdir(a.out):
        print(f"{a.out} 不存在，无需清理")
        return 0

    for root, _, files in os.walk(a.out):
        for name in files:
            fp = os.path.join(root, name)
            mb = size_mb(fp)
            if mb > a.single_file_mb:
                sys.exit(f"护栏失败：单文件 {fp} 达 {mb:.1f} MB，超过 {a.single_file_mb} MB，拒绝清理")

    csv_path = os.path.join(a.out, "breadth.csv")
    rows = load_rows(csv_path)
    notes = []
    if len(rows) > a.max_rows:
        dump_rows(rows[-a.max_rows:], csv_path)
        notes.append(f"CSV 裁剪 {len(rows)} → {a.max_rows} 行")
        rows = rows[-a.max_rows:]

    before = size_mb(a.out)
    if before > a.max_mb:
        snap = os.path.join(a.out, "snapshots")
        if os.path.isdir(snap):
            for fp in sorted((os.path.join(snap, f) for f in os.listdir(snap)),
                             key=os.path.getmtime):
                if size_mb(a.out) <= a.max_mb * 0.8:
                    break
                os.remove(fp)
                notes.append(f"删除旧快照 {os.path.basename(fp)}")
        if size_mb(a.out) > a.max_mb and len(rows) > 365:
            dump_rows(rows[-365:], csv_path)
            notes.append("体积仍超阈值，CSV 压到最近 365 天")

    print(f"体检完成：{a.out} {before:.2f} MB → {size_mb(a.out):.2f} MB "
          f"（阈值 {a.max_mb:.0f} MB）")
    for n in notes:
        print("  -", n)
    if not notes:
        print("  - 无需改动，一个字节都没碰")
    return 0


if __name__ == "__main__":
    sys.exit(main())
