#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股市场宽度（TradingView INDEX:S5TH 同类）—— 云端版，完全脱离本地行情客户端

主数据源（算均线宽度必须用逐股历史 + 复权，快照类 API 无法替代）:
  AKShare -> 东方财富
    stock_zh_a_spot_em()                 全市场代码清单主源（push2，境外偶发 WAF）
    stock_info_a_code_name()             代码清单备用源（约 8s，纯在市清单；主源挂了自动切）
    codes.txt                            仓库内带日期的兜底快照（三级链路最后一环）
    stock_zh_a_hist(adjust="hfq")       逐只后复权日线（push2his；后复权序列连续，无需 gbbq）
    index_zh_a_hist()                   沪深300 / 上证综指 / 创业板指日线，用于同图对比

辅助数据源（a-stock-api / Cloudflare Worker，云出口友好，仅做轻量补充，绝不当主源）:
    /api/market-breadth   当日涨跌家数 / 涨停 / 连板快照，用于幂等判断 + 结果页校验卡片

数据窗口: 近 5 年（约 1300 交易日）。每次运行重拉约 6.5 年历史重算，
因此不依赖历史 CSV 累积；CSV 滚动保留 1300 个交易日。
注意：历史宽度是用「当前股票池」回算的，早期存在幸存者偏差，见 README 第十一章。

存储治理（2~3 年无人维护）:
  - 不落盘逐股原始行情，只留聚合结果（无状态）
  - CSV 按日期合并去重、滚动保留 1300 个交易日；单文件 50 MB 护栏
  - docs/ 超过 500 MB 自动裁剪旧数据；有效样本低于下限拒绝覆盖已有结果
  - 依赖锁版本 + 上界；代码池三级链路 + 日期标记；补充源失败不阻塞主流程
  - 新上市 / 退市股票由每日在线快照自动纳入或剔除；代码池超过 7 天没刷新就禁止幂等跳过

用法:
    python breadth.py                                   # 全市场，约 10~20 分钟
    python breadth.py --limit 200 --codes-file codes.txt --out test-out --force --min-valid 100
    python breadth.py --out docs --force                 # 全量刷新
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import io
import json
import os
import socket
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

import numpy as np

# 页面时间统一显示北京时间：GitHub Actions runner 是 UTC，直接用 datetime.now()
# 会让「生成于」在本地（北京）与云端（UTC）含义不一致，进而误判数据是否最新。
BJ_TZ = timezone(timedelta(hours=8))

try:
    import akshare as ak
except ImportError:
    sys.exit("缺少依赖，请先执行: pip install -r requirements.txt")

# --- 可调旋钮（改这里之前先读 README 第九章） ---------------------------------
MA_WINDOWS = (5, 20, 50, 200)
OUT_DAYS = 1300              # 图表最多展示的交易日（5 年 ≈ 1250）
MAX_CSV_ROWS = 1300          # CSV 滚动窗口（约 5 年）
LOOKBACK_DAYS = 2400         # 自然日，约 6.5 年，覆盖 5 年 + 200 日均线预热
CALENDAR_BARS = 1500         # 参与计算的交易日上限
MAX_WORKERS = 10             # 境外 runner 逐只请求慢
RETRY = 2
SOCKET_TIMEOUT = 12          # akshare 内部 requests 未设超时，境外会挂死
MIN_VALID = 1000             # 有效样本下限，低于此值拒绝覆盖
MAX_DIR_MB = 500
SINGLE_FILE_MB = 50
MIN_BARS = 51               # 进入计算所需最少日线（MA50 需 50 根 + 1）；改回 205 = 恢复「上市满 200 日才纳入」旧口径
POOL_MAX_AGE_DAYS = 7       # codes.txt 超过该自然日数未刷新就禁止幂等跳过（覆盖春节/国庆长假）
PCT_WINDOW = 250            # 默认：etf-lsi 归一化分位的滚动窗口（交易日）
PCT_WINDOW_5Y = MAX_CSV_ROWS  # 可选：图 2 可切换的 5 年全序列滚动窗口（交易日）
SPREAD_BIG = 25             # |Spread| 达到该值算「大劈叉」

CODES_SNAPSHOT = "codes.txt"
AUX_API = "https://a-stock-api.wj4020410.workers.dev"
INDEXES = {
    "hs300": ("000300", "沪深300", "#e0972f"),
    "sse":   ("000001", "上证综指", "#2f6fd0"),
    "cyb":   ("399006", "创业板指", "#8a63d2"),
}

CSV_COLS = ["date", "n_stocks", "n_traded", "n_ma5", "n_ma20", "n_ma50", "n_ma200",
            "pct_above_5", "pct_above_20", "pct_above_50", "pct_above_200",
            "above_5", "above_20", "above_50", "above_200",
            "advancers", "decliners", "ad_diff", "mcclellan",
            "ema5_pct200", "ema_long_pct200"]


# ----------------------------------------------------------------------------
# 小工具
# ----------------------------------------------------------------------------
def atomic_write(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.replace(tmp, path)


def dir_size_mb(path: str) -> float:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total / 1_000_000


def d2i(d) -> int:
    if hasattr(d, "year"):
        return d.year * 10000 + d.month * 100 + d.day
    return int(str(d).replace("-", "")[:8])


# ----------------------------------------------------------------------------
# 股票池：主源在线快照 → 备用源纯在市清单 → 仓库内带日期的 codes.txt
# 新上市 / 退市股票靠「每次运行重新拉清单」自动纳入或剔除。
# codes.txt 第一行写 # updated / source / count，read_codes_file 会跳过注释行。
# ----------------------------------------------------------------------------
def is_a_stock(code: str) -> bool:
    c = str(code).zfill(6)
    return c.startswith(("600", "601", "603", "605", "688", "689",
                         "000", "001", "002", "003", "300", "301",
                         "43", "83", "87", "92"))


def read_codes_file(path: str) -> list[str]:
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as f:
        return sorted({ln.strip().zfill(6) for ln in f
                       if ln.strip() and ln.strip()[:1].isdigit() and is_a_stock(ln.strip())})


def read_codes_meta(path: str) -> dict:
    """读 codes.txt 头部注释，用于判断代码池是否陈旧。"""
    meta: dict = {"updated": "", "source": "", "count": 0, "stale_days": None}
    if not os.path.isfile(path):
        return meta
    with open(path, encoding="utf-8") as f:
        first = f.readline().strip()
    if first.startswith("#"):
        for part in first.lstrip("# ").split("|"):
            k, _, v = part.partition(":")
            k, v = k.strip(), v.strip()
            if k == "count" and v.isdigit():
                meta[k] = int(v)
            elif k in ("updated", "source"):
                meta[k] = v
    if meta["updated"]:
        try:
            d = datetime.strptime(meta["updated"], "%Y-%m-%d")
            meta["stale_days"] = max(0, (datetime.now() - d).days)
        except ValueError:
            pass
    return meta


def write_codes_snapshot(codes: list[str], source: str) -> dict:
    stamp = datetime.now().strftime("%Y-%m-%d")
    header = f"# updated: {stamp} | source: {source} | count: {len(codes)}\n"
    atomic_write(CODES_SNAPSHOT, header + "\n".join(codes) + "\n")
    return {"updated": stamp, "source": source, "count": len(codes), "stale_days": 0}


def get_codes(limit: int, codes_file: str | None) -> tuple[list[str], dict]:
    if codes_file:
        codes = read_codes_file(codes_file)
        if not codes:
            sys.exit(f"自定义股票池为空或不存在: {codes_file}")
        print(f"[1/6] 自定义股票池 {len(codes)} 只（{codes_file}）", flush=True)
        meta = {"updated": datetime.now().strftime("%Y-%m-%d"), "source": "custom",
                "count": len(codes), "stale_days": 0}
        return (codes[:limit] if limit else codes), meta

    print("[1/6] 拉取全市场 A 股代码清单（主源 push2，约 60~90 秒）...", flush=True)
    # 主源：完整快照，含新股 / 退市整理股
    try:
        df = ak.stock_zh_a_spot_em()
        codes = sorted({str(c).zfill(6) for c in df["代码"] if is_a_stock(c)})
        if len(codes) < 3000:
            raise RuntimeError(f"主源快照只有 {len(codes)} 只，疑似上游异常")
        meta = write_codes_snapshot(codes, "online")
        print(f"[1/6] 主源快照 {len(df)} 行 → A 股正股 {len(codes)} 只；已刷新 {CODES_SNAPSHOT}", flush=True)
        return (codes[:limit] if limit else codes), meta
    except Exception as e:
        print(f"  [pool] 主源失败（{type(e).__name__}: {str(e)[:80]}），切备用源 ...", flush=True)

    # 备用源：走不同端点、约 8 秒、纯在市清单（不含退市 / 长期停牌），照样能纳入新股
    try:
        df = ak.stock_info_a_code_name()
        codes = sorted({str(c).zfill(6) for c in df["code"] if is_a_stock(c)})
        if len(codes) < 5000:
            raise RuntimeError(f"备用源只有 {len(codes)} 只，疑似上游异常")
        meta = write_codes_snapshot(codes, "backup")
        print(f"[1/6] 备用源 stock_info_a_code_name → {len(codes)} 只（纯在市）；已刷新 {CODES_SNAPSHOT}", flush=True)
        return (codes[:limit] if limit else codes), meta
    except Exception as e:
        print(f"  [pool] 备用源失败（{type(e).__name__}: {str(e)[:80]}），回退 {CODES_SNAPSHOT}", flush=True)

    # 兜底：仓库内快照。不刷新日期，让陈旧度可见，并由幂等保护强制持续重试
    codes = read_codes_file(CODES_SNAPSHOT)
    if not codes:
        sys.exit("主源 / 备用源 / codes.txt 三级链路全部不可用，本次不覆盖已有结果")
    meta = read_codes_meta(CODES_SNAPSHOT)
    meta["source"] = "fallback:" + (meta["source"] or "unknown")
    meta["count"] = len(codes)
    print(f"[1/6] ⚠️ 兜底 {CODES_SNAPSHOT}：{len(codes)} 只，快照日期 "
          f"{meta['updated'] or '未知'}，已 {meta['stale_days']} 天未刷新", flush=True)
    return (codes[:limit] if limit else codes), meta


# ----------------------------------------------------------------------------
# 辅助源（可选）+ 指数日线
# ----------------------------------------------------------------------------
def fetch_aux(timeout: int = 12) -> dict | None:
    try:
        req = urllib.request.Request(AUX_API + "/api/market-breadth",
                                     headers={"User-Agent": "market-breadth-ci"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.load(r)
        if not d.get("total"):
            return None
        return {k: d.get(k) for k in ("date", "up", "down", "flat", "total",
                                      "limit_up", "limit_down", "max_streak", "is_today")}
    except Exception as e:
        print(f"  [aux] 补充源不可用（{type(e).__name__}），跳过", flush=True)
        return None


def fetch_indices(start: str, end: str) -> dict:
    out: dict = {}
    for key, (sym, name, _) in INDEXES.items():
        for attempt in range(2):
            try:
                df = ak.index_zh_a_hist(symbol=sym, period="daily",
                                        start_date=start, end_date=end)
                if df is None or len(df) < 200:
                    raise RuntimeError("返回行数过少")
                m = {d2i(d): float(c) for d, c in zip(df["日期"], df["收盘"]) if c == c}
                out[key] = m
                print(f"  指数 {name}({sym})：{len(m)} 根日线", flush=True)
                break
            except Exception as e:
                if attempt:
                    print(f"  指数 {name} 取数失败（{type(e).__name__}），该曲线留空", flush=True)
                time.sleep(1)
    return out


def align_index(calendar: list[int], mapping: dict | None) -> list | None:
    if not mapping:
        return None
    out, last = [], None
    for d in calendar:
        v = mapping.get(d, last)
        if v is not None:
            last = v
        out.append(None if v is None else round(float(v), 2))
    return out


# ----------------------------------------------------------------------------
# 逐股历史
# ----------------------------------------------------------------------------
def fetch_one(code: str, start: str, end: str):
    for attempt in range(RETRY):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start, end_date=end, adjust="hfq")
            if df is None or len(df) < MIN_BARS or "收盘" not in df.columns:
                return code, None
            # 只留两列：5 年窗口下 5900 只全字段会吃掉近 1 GB 内存
            return code, df[["日期", "收盘"]].copy()
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return code, None


def fetch_all(codes: list[str], start: str, end: str) -> dict:
    out: dict = {}
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(fetch_one, c, start, end) for c in codes]
        for i, fu in enumerate(cf.as_completed(futs), 1):
            code, df = fu.result()
            if df is not None:
                out[code] = df
            if i % 500 == 0 or i == len(codes):
                print(f"  历史日线 {i}/{len(codes)}，成功 {len(out)}", flush=True)
    return out


# ----------------------------------------------------------------------------
# 计算
# ----------------------------------------------------------------------------
def rolling_minmax_pct(values: list, window: int = PCT_WINDOW) -> list:
    """etf-lsi 口径的归一化分位：每个点取「自己 + 之前 window-1 个交易日」的
    极值做 min-max 映射到 0-100。滚动窗口保证 5 年曲线近端不会被远端极值压平，
    且不随图表缩放窗口变化（口径不漂移）。"""
    out: list = []
    for i, v in enumerate(values):
        if v is None:
            out.append(None)
            continue
        seg = [x for x in values[max(0, i - window + 1):i + 1] if x is not None]
        if not seg:
            out.append(None)
            continue
        mn, mx = min(seg), max(seg)
        out.append(50.0 if mx == mn else round((v - mn) / (mx - mn) * 100.0, 2))
    return out


def rolling_mean(x: np.ndarray, n: int) -> np.ndarray:
    c = np.concatenate(([0.0], np.cumsum(x)))
    out = np.full(len(x), np.nan)
    if len(x) >= n:
        out[n - 1:] = (c[n:] - c[:-n]) / n
    return out


def ema(x: np.ndarray, n: int) -> np.ndarray:
    a = 2.0 / (n + 1.0)
    out = np.empty(len(x))
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def compute(data: dict, idx_maps: dict) -> dict:
    freq: Counter = Counter()
    for df in data.values():
        freq.update(d2i(d) for d in df["日期"])
    threshold = max(20, int(len(data) * 0.30))
    calendar = sorted(d for d, c in freq.items() if c >= threshold)[-CALENDAR_BARS:]
    T = len(calendar)
    ci = {d: i for i, d in enumerate(calendar)}
    print(f"[4/6] 交易日历 {T} 天：{calendar[0]} ~ {calendar[-1]}", flush=True)

    N = len(data)
    valid = np.zeros((N, T), dtype=bool)
    ready = {n: np.zeros((N, T), dtype=bool) for n in MA_WINDOWS}
    above = {n: np.zeros((N, T), dtype=bool) for n in MA_WINDOWS}
    is_up = np.zeros((N, T), dtype=bool)
    is_down = np.zeros((N, T), dtype=bool)
    kept = 0

    for row, (_, df) in enumerate(data.items()):
        ds = np.array([d2i(d) for d in df["日期"]], dtype=np.int64)
        cl = df["收盘"].astype(float).to_numpy()
        pos = np.array([ci.get(int(d), -1) for d in ds], dtype=int)
        sel = (pos >= 0) & (cl > 0)
        cl, pos = cl[sel], pos[sel]
        if len(cl) < MIN_BARS:
            continue
        valid[row, pos] = True
        for n in MA_WINDOWS:
            ma = rolling_mean(cl, n)
            ok = np.isfinite(ma)
            ready[n][row, pos] = ok
            above[n][row, pos] = ok & (cl > ma)
        chg = np.diff(cl, prepend=cl[0])
        is_up[row, pos] = chg > 0
        is_down[row, pos] = chg < 0
        kept += 1

    all_rows = []
    for t in range(T):
        d = calendar[t]
        rec = {
            "date": f"{d // 10000:04d}-{d // 100 % 100:02d}-{d % 100:02d}",
            "n_traded": int(valid[:, t].sum()),
            "advancers": int((valid[:, t] & is_up[:, t]).sum()),
            "decliners": int((valid[:, t] & is_down[:, t]).sum()),
        }
        for w in MA_WINDOWS:
            n = int((valid[:, t] & ready[w][:, t]).sum())
            hits = int((valid[:, t] & above[w][:, t]).sum())
            rec[f"n_ma{w}"] = n
            rec[f"above_{w}"] = hits
            rec[f"pct_above_{w}"] = round(100.0 * hits / n, 2) if n else None
        rec["n_stocks"] = rec["n_ma200"]
        all_rows.append(rec)

    used = [t for t, r in enumerate(all_rows) if r["pct_above_200"] is not None][-OUT_DAYS:]
    rows = [all_rows[t] for t in used]
    for r in rows:
        r["ad_diff"] = r["advancers"] - r["decliners"]
    p200 = np.array([float(r["pct_above_200"]) for r in rows])
    net = np.array([float(r["ad_diff"]) for r in rows])
    e5 = ema(p200, 5)
    elong = ema(p200, 200 if len(rows) >= 200 else max(5, len(rows) // 3))
    mc = ema(net, 19) - ema(net, 39)
    for i, r in enumerate(rows):
        r["ema5_pct200"] = round(float(e5[i]), 2)
        r["ema_long_pct200"] = round(float(elong[i]), 2)
        r["mcclellan"] = round(float(mc[i]), 1)

    indices = {}
    for key in INDEXES:
        s = align_index(calendar, idx_maps.get(key))
        if s:
            indices[key] = [s[t] for t in used]

    p200_raw = [float(r["pct_above_200"]) if r["pct_above_200"] is not None else None
                for r in rows]
    return {"rows": rows, "n_valid": kept, "n_fetched": len(data),
            "calendar_days": T, "used_days": len(rows),
            "indices": indices,
            "p200_pct": rolling_minmax_pct(p200_raw),
            "index_pct": {k: rolling_minmax_pct(v) for k, v in indices.items()},
            "p200_pct_5y": rolling_minmax_pct(p200_raw, window=PCT_WINDOW_5Y),
            "index_pct_5y": {k: rolling_minmax_pct(v, window=PCT_WINDOW_5Y)
                             for k, v in indices.items()},
            "p200_values": p200_raw,
            "index_names": {k: v[1] for k, v in INDEXES.items()},
            "index_colors": {k: v[2] for k, v in INDEXES.items()},
            "latest": rows[-1]["date"] if rows else "",
            "generated_at": datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M")}


# ----------------------------------------------------------------------------
# 存储治理
# ----------------------------------------------------------------------------
def load_existing_rows(path: str) -> dict:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8-sig") as f:
            return {r["date"]: r for r in csv.DictReader(f) if r.get("date")}
    except Exception:
        return {}


def merge_rows(existing: dict, new: list, max_rows: int) -> list:
    merged = dict(existing)
    for r in new:
        merged[r["date"]] = r
    return [merged[k] for k in sorted(merged)][-max_rows:]


def housekeeping(out_dir: str, max_mb: float, max_rows: int) -> list[str]:
    notes: list[str] = []
    for root, _, files in os.walk(out_dir):
        for name in files:
            fp = os.path.join(root, name)
            mb = os.path.getsize(fp) / 1_000_000
            if mb > SINGLE_FILE_MB:
                raise RuntimeError(f"单文件超过 {SINGLE_FILE_MB} MB 护栏: {fp} ({mb:.1f} MB)")
    csv_path = os.path.join(out_dir, "breadth.csv")
    rows = load_existing_rows(csv_path)
    if len(rows) > max_rows:
        write_csv([rows[k] for k in sorted(rows)][-max_rows:], csv_path)
        notes.append(f"CSV 裁剪到最近 {max_rows} 个交易日（原 {len(rows)} 行）")
    if dir_size_mb(out_dir) > max_mb:
        snap = os.path.join(out_dir, "snapshots")
        if os.path.isdir(snap):
            for fp in sorted((os.path.join(snap, f) for f in os.listdir(snap)), key=os.path.getmtime):
                if dir_size_mb(out_dir) <= max_mb * 0.8:
                    break
                os.remove(fp)
                notes.append(f"清理旧快照 {os.path.basename(fp)}")
        rows = load_existing_rows(csv_path)
        if dir_size_mb(out_dir) > max_mb and len(rows) > 365:
            write_csv([rows[k] for k in sorted(rows)][-365:], csv_path)
            notes.append("体积超阈值，CSV 进一步裁剪到 365 天")
    notes.append(f"{out_dir}/ 当前 {dir_size_mb(out_dir):.2f} MB（阈值 {max_mb:.0f} MB）")
    return notes


def write_csv(rows: list, path: str) -> None:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    atomic_write(path, buf.getvalue())


# ----------------------------------------------------------------------------
# HTML（纯字符串模板 + 唯一占位符替换，避免 f-string 大括号转义事故）
# ----------------------------------------------------------------------------
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="A股宽度">
<meta name="theme-color" content="#f7f8fa">
<title>A 股市场宽度 · S5TH 云端版</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
:root{--bg:#f7f8fa;--card:#fff;--line:#e6e9ef;--txt:#1f2733;--muted:#6b7688}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font:14px/1.6 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:28px 20px 60px}
h1{font-size:25px;margin:0 0 6px}h2{font-size:17px;margin:30px 0 10px}
.headrow{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:6px}
.headrow h1{margin:0}
.datebadge{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:8px 14px;white-space:nowrap}
.dbtop{font-size:12px;color:var(--muted)}
.dbtop b{display:inline-block;margin-left:5px;font-size:17px;color:var(--txt);letter-spacing:.3px;vertical-align:-1px}
.dbweek{font-size:11px;color:var(--muted);margin-left:6px}
.dbsub{font-size:11px;color:var(--muted);margin-top:2px}
.freshtag{display:inline-block;margin-left:9px;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:600;background:#eef7f0;color:#235c3a;border:1px solid #cfe6d6;vertical-align:2px}
.freshtag.stale{background:#fff4e8;border-color:#f0cfa4;color:#8a5218}
.lead{color:var(--muted);margin:0 0 16px;font-size:13px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.lab{color:var(--muted);font-size:12px}.val{font-size:28px;font-weight:650;line-height:1.25}.val span{font-size:14px;margin-left:2px}
.sub{color:var(--muted);font-size:12px}
.aux{background:#eef4ff;border:1px solid #cfe0f7;color:#284b7a;padding:9px 13px;border-radius:8px;font-size:13px;margin-bottom:10px}
.pool{background:#eef7f0;border:1px solid #cfe6d6;color:#235c3a;padding:9px 13px;border-radius:8px;font-size:13px;margin-bottom:10px}
.pool.warn{background:#fff4e8;border-color:#f0cfa4;color:#8a5218}
.pctband{background:linear-gradient(90deg,#fff 0%,#f6f9fd 100%);border:1px solid var(--line);border-left:4px solid #2f6fd0;border-radius:10px;padding:13px 18px;margin:14px 0 6px}
.pbtitle{font-size:12px;color:var(--muted);margin-bottom:5px}
.pbrow{display:flex;align-items:center;gap:22px;flex-wrap:wrap}
.pbbig{font-size:38px;font-weight:700;line-height:1;color:#2f6fd0}
.pbbig span{font-size:16px;margin-left:2px;font-weight:600}
.pbdesc{font-size:13px;line-height:1.7}
.pbdesc span{color:var(--muted);font-size:12px}
.pbbar{height:7px;background:#e9edf3;border-radius:4px;margin-top:11px;overflow:hidden}
.pbbar i{display:block;height:100%;background:linear-gradient(90deg,#1f9d6b 0%,#e0972f 50%,#d64545 100%)}
.spread-note{color:var(--muted);font-size:12px;margin:-6px 0 12px}
.ctl{display:flex;flex-wrap:wrap;align-items:center;gap:7px;margin:10px 0 14px;font-size:12px;color:var(--muted)}
.ctl b{color:var(--txt);font-weight:600;margin-left:6px}
.ctl button{background:#fff;border:1px solid #d8dde6;color:#1f2733;border-radius:6px;padding:5px 11px;cursor:pointer;font-size:12px}
.ctl button:hover{border-color:#2f6fd0;color:#2f6fd0}
.ctl button.on{background:#2f6fd0;border-color:#2f6fd0;color:#fff}
.hint{margin-left:auto;font-size:11px;color:#8a93a3}
.ctl2{margin:-4px 0 10px}
.pcthint{font-size:11px;color:#8a93a3;margin-left:2px}
.panel{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;margin-bottom:16px}
#main{height:420px}#idx_sse,#idx_hs300,#idx_cyb{height:300px}
h3{font-size:15px;margin:18px 0 6px;color:#1f2733}
.idxwrap{margin-bottom:6px}
.guid{color:#6b7688;font-size:12.5px;margin:0 0 8px;line-height:1.7;background:#f3f6fb;border:1px solid #e3e9f2;border-left:3px solid #2f6fd0;border-radius:8px;padding:8px 12px}
.guidebox{background:#fff;border:1px solid var(--line);border-left:4px solid #e0972f;border-radius:10px;padding:13px 18px;margin:14px 0 6px}
.gtitle{font-size:13px;font-weight:600;color:#1f2733;margin-bottom:6px}
.gbody p{margin:5px 0;font-size:12.5px;line-height:1.75;color:#3a4250}
.gbody b{color:#1f2733}
.idxcards{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:12px 0}
.idxcard{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:13px 15px}
.ilab{color:var(--muted);font-size:12px}.ival{font-size:26px;font-weight:650;line-height:1.2;margin-top:3px}.ival span{font-size:13px;margin-left:2px}
.isub{color:var(--muted);font-size:11.5px;margin-top:2px}
.ibar{height:6px;background:#e9edf3;border-radius:4px;margin-top:9px;overflow:hidden}.ibar i{display:block;height:100%;background:linear-gradient(90deg,#1f9d6b 0%,#e0972f 50%,#d64545 100%)}
.verdict{background:#fff;border:1px solid var(--line);border-left:5px solid #8a93a3;border-radius:10px;padding:14px 18px;margin:14px 0}
.verdict.v-green{border-left-color:#1f9d6b}.verdict.v-amber{border-left-color:#e0972f}.verdict.v-red{border-left-color:#d64545}.verdict.v-neutral{border-left-color:#8a93a3}
.vtag{display:inline-block;font-size:13px;font-weight:700;padding:3px 12px;border-radius:20px;margin-bottom:8px}
.v-green .vtag{background:#e9f7f0;color:#1f7a4d}.v-amber .vtag{background:#fdf0dc;color:#9a6212}.v-red .vtag{background:#fdeaea;color:#b23b3b}.v-neutral .vtag{background:#eef0f4;color:#5a6473}
.vhead{font-size:14px;font-weight:600;color:#1f2733;margin-bottom:3px}
.vsub{font-size:13px;color:#3a4250;margin-bottom:6px}
.vbody{font-size:12.5px;line-height:1.8;color:#3a4250}
.vmeta{font-size:11.5px;color:#8a93a3;margin-top:8px;border-top:1px dashed #e3e7ee;padding-top:7px}
select.ctlsel{border:1px solid #d8dde6;border-radius:6px;padding:5px 8px;background:#fff;color:#1f2733;font-size:12px;cursor:pointer}
table{width:100%;border-collapse:collapse;background:var(--card);font-size:12px}
th,td{padding:7px 9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
th{background:#f1f3f7;color:var(--muted);font-weight:600}th:first-child,td:first-child{text-align:left}
.src{color:var(--muted);font-size:12px;margin-top:20px;border-top:1px solid var(--line);padding-top:12px}
code{background:#eef1f6;padding:1px 5px;border-radius:4px;font-size:12px}
@media(max-width:760px){.cards{grid-template-columns:repeat(2,1fr)}.wrap{padding:18px 12px 40px}.hint,.pcthint{display:none}#main{height:360px}#idx_sse,#idx_hs300,#idx_cyb{height:260px}.pbbig{font-size:30px}.headrow{align-items:flex-start}.datebadge{white-space:normal}.dbtop b{font-size:15px}.idxcards{grid-template-columns:repeat(1,1fr)}}
</style></head><body><div class="wrap">
<div class="headrow">
  <h1>A 股市场宽度 · S5TH 云端版</h1>
  <div class="datebadge" id="dateBadge">
    <div class="dbtop">数据日期 <b>__LATEST__</b><span class="dbweek" id="dateWeek"></span><span class="freshtag" id="freshTag">—</span></div>
    <div class="dbsub">更新于 __GEN__（北京时间）· 每交易日 16:30 / 18:00 自动刷新</div>
  </div>
</div>
<p class="lead">等权口径：A 股中「后复权收盘价站上 N 日均线」的占比。近 5 年历史，由 GitHub Actions 每交易日自动计算；主图只保留中长期 MA50 / MA200，指数另开一图做归一化背离对比。</p>
<div class="cards">__CARDS__</div>
__PCTBAND__
__IDXCARD__
__GUIDE__
__VERDICT__
__POOL__
__AUX__
<h2>宽度曲线（中长期）</h2>
<div class="ctl">
  <b>区间</b>
  <button data-days="125">半年</button><button data-days="250">1 年</button>
  <button data-days="750">3 年</button><button data-days="1300" class="on">5 年</button>
  <b>图框</b><button id="sizeBtn">放大图框</button>
  <b>纵轴</b><button id="yBtn">纵轴自适应</button>
  <span class="hint">四图联动 · 鼠标滚轮缩放 · 拖动滑块平移 · 图例可勾选显隐</span>
</div>
<div class="panel"><div id="main"></div></div>
__IDXSECT__
<h2>最近 15 个交易日</h2>
<table><thead><tr><th>日期</th><th>样本数</th><th>&gt;MA5</th><th>&gt;MA20</th><th>&gt;MA50</th><th>&gt;MA200</th><th>上涨</th><th>下跌</th></tr></thead><tbody>__TRS__</tbody></table>
<div class="src">
主数据源：AKShare → 东方财富 <code>stock_zh_a_spot_em</code>（代码清单主源）→ <code>stock_info_a_code_name</code>（备用源）→ <code>codes.txt</code>（带日期兜底）+ <code>stock_zh_a_hist(adjust="hfq")</code>（逐只后复权日线）+ <code>index_zh_a_hist</code>（沪深300 / 上证综指 / 创业板指）。辅助校验：<code>a-stock-api /api/market-breadth</code> 当日快照（不参与均线计算）。<br>
数据截止 <b>__LATEST__</b>；成功取数 <b>__NFETCH__</b> 只、计入计算 <b>__NVALID__</b> 只、本次序列 <b>__NDAYS__</b> 个交易日。等权、含北交所/科创创业、ST 未单独剔除。股票池由每日在线清单自动纳入新股、剔除退市股；上市满 __MINBARS__ 根日线进入 MA5/20/50 宽度，满 200 根进入 MA200 宽度；停牌日不计入分母。<br>
归一化分位参照 <code>wj624/etf-lsi</code>：每个点取自身及之前 N 个交易日的极值做 min-max 映射到 0-100，N 默认 __PCTWIN5Y__（约 5 年全序列滚动，图 2 默认），可切换为 __PCTWINDOW__（250 日滚动）。两种都是「当日及之前」的滚动窗口，不使用未来极值，也不按图表缩放窗口重算；5 年周期看长期历史位置，但近端波动会被压平。Spread = MA200 占比分位 − 基准指数分位（−100 ~ +100），正红负绿、±__SPREADBIG__ 为大劈叉；这是形状对比，不是收益率。头部百分位是「当前 MA200 占比在近 5 年序列中的 percentile rank」，与滚动归一化分位是两个不同指标。<br>
⚠️ 5 年历史是用「当前股票池」回算的，早期存在幸存者偏差（当时未上市 / 现已退市的股票不在池内），越往前偏差越大，不可表述为「当时的真实全市场宽度」。<br>
CSV 滚动保留 __MAXROWS__ 个交易日；docs/ 超过 __MAXMB__ MB 自动裁剪。GitHub Actions 生成于 __GEN__（北京时间）；仅为公开数据统计结果，不构成投资建议。
</div>
</div>
<script>
var DATA = __PAYLOAD__;
var COLORS = {'50':'#e0972f','200':'#d64545'};
var IDX_KEYS = Object.keys(DATA.indices || {});
var rawMode = false, autoY = false, bigFrame = false;
var curStart = 0, curEnd = 100, pctWin = '5y';    // 默认 5 年全序列滚动窗口；'250' = 250 日滚动
var chart = null;
var idxCharts = [];        // [{key, el, chart}]
var curSpreadMap = {};     // key -> 当前 Spread 序列（供 tooltip）

function fmt(v, suffix) {
  return (v == null || isNaN(v)) ? '—' : (+v).toFixed(2) + (suffix || '');
}
function bounds(startPct, endPct) {
  var n = DATA.dates.length;
  var x = Math.max(0, Math.floor(startPct / 100 * (n - 1)));
  var y = Math.min(n - 1, Math.ceil(endPct / 100 * (n - 1)));
  if (y <= x) { y = Math.min(n - 1, x + 1); }
  return [x, y];
}
// 两套分位都由 Python 端用同一个 rolling_minmax_pct() 算好烤进 payload，
// 前端只切换「取哪一套」，绝不在浏览器里重算（防止口径漂移 / 前视偏差 / 缩放后重算）。
function curP200Pct(){ return pctWin === '5y' ? (DATA.p200Pct5y || []) : (DATA.p200Pct || []); }
function curIdxPct(k){
  var m = pctWin === '5y' ? (DATA.indexPct5y || {}) : (DATA.indexPct || {});
  return m[k] || [];
}
function pctWinLabel(){
  return pctWin === '5y'
    ? '滚动 ' + DATA.pctWindow5y + ' 个交易日（约 5 年）'
    : '滚动 ' + DATA.pctWindow + ' 个交易日';
}
function spreadArrayOf(k) {
  var a = curP200Pct(), b = curIdxPct(k);
  return a.map(function (v, i) {
    return (v == null || b[i] == null) ? null : +(v - b[i]).toFixed(2);
  });
}
// 默认（归一化视图）：沪深300 显示；MA200 占比分位 + 上证 / 创业板默认关闭。
// 原始走势视图（rawMode）下 MA200 占比默认显示（dft 见下方 rawMode 判断）。
// 用户手动勾选后以图表实例的真实 selected 为准（不依赖 legendselectchanged 事件，
// 因此无论点击图例还是脚本 dispatchAction 都能保留）。切换「分位 / 基准 100」
// 时 series 名称会变，这里按指数 key 在两套命名之间做映射，避免选择被重置。
function makeIdxLegend(chart, key) {
  var cur = {};
  try {
    if (chart) {
      var op = chart.getOption();
      if (op && op.legend && op.legend[0] && op.legend[0].selected) {
        cur = op.legend[0].selected || {};
      }
    }
  } catch (e) { cur = {}; }
  function pick(a, b, dft) {
    if (a in cur) return cur[a];
    if (b in cur) return cur[b];
    return dft;
  }
  var suffix = rawMode ? '（基准 100）' : ' 分位';
  var s = {};
  s[DATA.indexNames[key] + suffix] =
    pick(DATA.indexNames[key] + ' 分位', DATA.indexNames[key] + '（基准 100）', true);
  s[rawMode ? 'MA200 占比' : 'MA200 占比分位'] =
    pick('MA200 占比分位', 'MA200 占比', rawMode ? true : false);
  return s;
}
function originalIndexSeries(key, startPct, endPct) {
  var r = bounds(startPct, endPct), x = r[0], y = r[1];
  var arr = DATA.indices[key], base = null;
  for (var j = x; j <= y; j++) { if (arr[j] != null) { base = arr[j]; break; } }
  return {id:'idx_'+key, name: DATA.indexNames[key]+'（基准 100）', type:'line', yAxisIndex:1,
    data: arr.map(function(v){ return (v==null||!base)?null:+((v/base)*100).toFixed(2); }),
    showSymbol:false, smooth:true, connectNulls:true,
    lineStyle:{width:1.9,color:DATA.indexColors[key]}, itemStyle:{color:DATA.indexColors[key]}};
}
function pctSeriesFor(key) {
  var out = [{id:'p200pct', name:'MA200 占比分位', type:'line', yAxisIndex:0, z:6,
    data: curP200Pct(), showSymbol:false, smooth:true, connectNulls:true,
    lineStyle:{width:2.8,color:'#d64545'}, itemStyle:{color:'#d64545'}}];
  out.push({id:'idx_'+key, name: DATA.indexNames[key]+' 分位', type:'line', yAxisIndex:0, z:5,
    data: curIdxPct(key), showSymbol:false, smooth:true, connectNulls:true,
    lineStyle:{width:1.9,color:DATA.indexColors[key]}, itemStyle:{color:DATA.indexColors[key]}});
  return out;
}
function spreadSeriesFor(key, spread) {
  return [
    {name:'Spread 背景', type:'bar', yAxisIndex:1, z:1, data:spread, barWidth:'100%', silent:true,
     itemStyle:{color:function(p){
       if (p.value == null) return 'rgba(0,0,0,0)';
       return p.value >= 0 ? 'rgba(232,69,60,0.20)' : 'rgba(31,159,107,0.20)';
     }}},
    {name:'Spread', type:'line', yAxisIndex:1, z:2, data:spread, showSymbol:false,
     lineStyle:{width:1,color:'rgba(107,118,136,0.55)'}, itemStyle:{color:'#6b7688'},
     markLine:{silent:true, symbol:'none', label:{show:false},
       lineStyle:{type:'dashed',width:1,color:'#aab2c0'},
       data:[{yAxis:DATA.spreadBig},{yAxis:-DATA.spreadBig},{yAxis:0,lineStyle:{color:'#c9ced8',width:1}}]}}
  ];
}
function rawSeriesFor(key, startPct, endPct) {
  var s = [{id:'p200raw', name:'MA200 占比', type:'line', yAxisIndex:0, z:6,
    data: DATA.p200Values, showSymbol:false, smooth:true, connectNulls:true,
    lineStyle:{width:2.8,color:'#d64545'}, itemStyle:{color:'#d64545'}}];
  return s.concat([originalIndexSeries(key, startPct, endPct)]);
}
function idxTooltipFor(key) {
  return function(ps) {
    if (!ps || !ps.length) return '';
    var i = ps[0].dataIndex, out = ['<b>'+DATA.dates[i]+'</b>'];
    out.push('<span style="color:#8a93a3">归一化窗口：'+pctWinLabel()+'</span>');
    out.push('MA200 占比：'+fmt(DATA.p200Values[i],'%')+'　分位：'+fmt(curP200Pct()[i]));
    out.push(DATA.indexNames[key]+'：'+fmt(DATA.indices[key][i])+'　分位：'+fmt(curIdxPct(key)[i]));
    var sp = (curSpreadMap[key] || [])[i];
    if (sp != null) {
      var col = sp >= 0 ? '#E8453C' : '#1FB37A';
      var tag = Math.abs(sp) >= DATA.spreadBig ? ' · 大劈叉' : '';
      out.push('<span style="color:'+col+'">Spread（vs '+DATA.indexNames[key]+'）：'+(sp>0?'+':'')+sp.toFixed(1)+tag+'</span>');
    }
    return out.join('<br>');
  };
}

function mainChart() {
  var el = document.getElementById('main');
  if (typeof echarts === 'undefined') { el.innerHTML='<p style="color:#6b7688">图表库未加载（离线），请查看下方数据表。</p>'; return; }
  chart = echarts.init(el);
  var names = {'50':'站上 50 日线','200':'站上 200 日线（S5TH 同类）'};
  var series = ['50','200'].map(function(k){
    return {name:names[k],type:'line',data:DATA.series[k],smooth:true,showSymbol:false,
      lineStyle:{width:k==='200'?2.8:1.8,color:COLORS[k]},itemStyle:{color:COLORS[k]}};
  });
  series.push({name:'MA200 占比长周期 EMA',type:'line',data:DATA.emaLong,smooth:true,
    showSymbol:false,lineStyle:{width:1.3,type:'dashed',color:'#8a93a3'},itemStyle:{color:'#8a93a3'}});
  chart.setOption({
    animation:false, tooltip:{trigger:'axis'},
    legend:{top:0,textStyle:{color:'#1f2733'},selected:{'站上 50 日线':false}},
    grid:{left:52,right:24,top:46,bottom:64},
    xAxis:{type:'category',data:DATA.dates,axisLabel:{color:'#6b7688'},axisLine:{lineStyle:{color:'#e6e9ef'}}},
    yAxis:{type:'value',name:'宽度 %',min:0,max:100,axisLabel:{formatter:'{value}%',color:'#6b7688'},splitLine:{lineStyle:{color:'#eef0f4'}}},
    dataZoom:[
      {type:'inside',xAxisIndex:0,zoomOnMouseWheel:true,moveOnMouseMove:true,moveOnMouseWheel:false},
      {type:'slider',xAxisIndex:0,bottom:12,height:20,borderColor:'#e6e9ef',fillerColor:'rgba(47,111,208,0.12)'}
    ],
    series:series
  });
  chart.on('dataZoom', function(p){
    var d = p.batch ? p.batch[0] : p;
    if (d && d.start != null) { curStart=d.start; curEnd=d.end; }
    if (rawMode && idxCharts.length) refreshAll();
  });
}

function buildIdxChart(cfg) {
  var el = document.getElementById(cfg.el);
  if (typeof echarts === 'undefined') { el.innerHTML='<p style="color:#6b7688">图表库未加载（离线），请查看下方数据表。</p>'; return; }
  var chart = echarts.init(el);
  var spread = spreadArrayOf(cfg.key);
  curSpreadMap[cfg.key] = spread;
  chart.setOption({
    animation:false, tooltip:{trigger:'axis',formatter:idxTooltipFor(cfg.key)},
    legend:{top:0,textStyle:{color:'#1f2733'},selected:makeIdxLegend(chart, cfg.key)},
    grid:{left:52,right:58,top:44,bottom:56},
    xAxis:{type:'category',data:DATA.dates,axisLabel:{color:'#6b7688'},axisLine:{lineStyle:{color:'#e6e9ef'}}},
    yAxis:[
      {type:'value',name:'分位 0-100',min:0,max:100,axisLabel:{color:'#6b7688'},splitLine:{lineStyle:{color:'#eef0f4'}}},
      {type:'value',name:'Spread',min:-100,max:100,position:'right',axisLabel:{color:'#9aa3b2'},splitLine:{show:false}}
    ],
    dataZoom:[
      {type:'inside',xAxisIndex:0,zoomOnMouseWheel:true,moveOnMouseMove:true,moveOnMouseWheel:false},
      {type:'slider',xAxisIndex:0,bottom:10,height:18,borderColor:'#e6e9ef',fillerColor:'rgba(47,111,208,0.12)'}
    ],
    series: pctSeriesFor(cfg.key).concat(spreadSeriesFor(cfg.key, spread))
  });
  cfg.chart = chart;
}

function refreshIdxChart(cfg) {
  var chart = cfg.chart; if (!chart) return;
  var sel = makeIdxLegend(chart, cfg.key);
  if (rawMode) {
    var spread = [];
    curSpreadMap[cfg.key] = spread;
    chart.setOption({legend:{selected:sel}, yAxis:[{},{name:'指数（基准 100）',min:'dataMin',max:'dataMax'}]});
    chart.setOption({series: rawSeriesFor(cfg.key, curStart, curEnd)}, {replaceMerge:['series']});
  } else {
    var spread = spreadArrayOf(cfg.key);
    curSpreadMap[cfg.key] = spread;
    chart.setOption({legend:{selected:sel}, yAxis:[{},{name:'Spread',min:-100,max:100}]});
    chart.setOption({series: pctSeriesFor(cfg.key).concat(spreadSeriesFor(cfg.key, spread))}, {replaceMerge:['series']});
  }
}
function refreshAll() { idxCharts.forEach(refreshIdxChart); }

function setRange(days) {
  var n = DATA.dates.length;
  var pct = Math.max(0, (n - days) / (n - 1) * 100);
  if (chart) chart.dispatchAction({type:'dataZoom', start:pct, end:100});
  curStart = pct; curEnd = 100;
  if (rawMode) refreshAll();
  document.querySelectorAll('.ctl button[data-days]').forEach(function (b) { b.classList.remove('on'); });
  var btn = document.querySelector('.ctl button[data-days="' + days + '"]');
  if (btn) btn.classList.add('on');
}

// 标题旁的新鲜度标签：用北京时间判断数据日期是否已是「应有」的最近交易日。
// 口径：北京 18:30（18:00 兜底 cron + 余量）之前期待上一工作日数据，之后期待当日数据；
// 周六日自动跳过。法定节假日 / 临时休市纯前端无法识别，故只给温和提示，
// 最终一律以徽章上显示的数据日期为准（tag.title 里也写明了这个边界）。
function pad2(n){ return n < 10 ? '0'+n : ''+n; }
function ymd(d){ return d.getFullYear()+'-'+pad2(d.getMonth()+1)+'-'+pad2(d.getDate()); }
function bjNow(){ return new Date(Date.now() + (480 + new Date().getTimezoneOffset())*60000); }
function markFreshness(){
  var tag = document.getElementById('freshTag');
  if (!tag) return;
  tag.className = 'freshtag';
  tag.title = '判断口径：北京时间 18:30 前期待上一工作日数据，之后期待当日数据，已跳过周末。法定节假日 / 临时休市不在判断范围内，请以数据日期为准。';
  var ds = DATA.dates || [];
  if (!ds.length) { tag.textContent = '无数据'; tag.className = 'freshtag stale'; return; }
  var latest = ds[ds.length-1], p = latest.split('-');
  var ld = new Date(+p[0], +p[1]-1, +p[2]);
  var wk = document.getElementById('dateWeek');
  if (wk && !isNaN(ld.getTime())) wk.textContent = ['周日','周一','周二','周三','周四','周五','周六'][ld.getDay()];
  var now = bjNow();
  var exp = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if (now.getHours()*60 + now.getMinutes() < 18*60+30) exp.setDate(exp.getDate()-1);
  while (exp.getDay()===0 || exp.getDay()===6) exp.setDate(exp.getDate()-1);
  var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  var gap = Math.max(0, Math.round((today - ld)/86400000));
  // YYYY-MM-DD 字典序即时间序：latest 不早于应有交易日就算最新（含数据比预期更新的情形）
  if (latest >= ymd(exp)) { tag.textContent = '最新'; }
  else { tag.textContent = '距数据日 ' + gap + ' 天'; tag.className = 'freshtag stale'; }
}

function wireControls() {
  document.querySelectorAll('.ctl button[data-days]').forEach(function (b) {
    b.onclick = function () { setRange(+this.getAttribute('data-days')); };
  });
  document.querySelectorAll('.ctl2 button[data-pct]').forEach(function (b) {
    b.onclick = function () {
      pctWin = b.getAttribute('data-pct');
      document.querySelectorAll('.ctl2 button[data-pct]').forEach(function (x) {
        x.classList.toggle('on', x === b);
      });
      document.getElementById('pctHint').textContent = pctWin === '5y'
        ? '5 年全序列滚动窗口：看长期历史位置，近端波动会被压平'
        : '250 日滚动窗口：看中短期相对位置，对近期变化更敏感';
      document.getElementById('pctWinDesc').textContent = pctWinLabel();
      refreshAll();   // 重算 Spread + 重建三张曲线；图例勾选由 makeIdxLegend() 保留
    };
  });
  document.getElementById('axisBtn').onclick = function () {
    rawMode = !rawMode;
    this.textContent = rawMode ? '显示归一化分位' : '显示原始走势';
    this.classList.toggle('on', rawMode);
    refreshAll();
  };
  document.getElementById('sizeBtn').onclick = function () {
    bigFrame = !bigFrame;
    document.getElementById('main').style.height = bigFrame ? '680px' : '420px';
    idxCharts.forEach(function (c) { var el = document.getElementById(c.el); if (el) el.style.height = bigFrame ? '460px' : '300px'; });
    this.textContent = bigFrame ? '还原图框' : '放大图框';
    this.classList.toggle('on', bigFrame);
    if (chart) chart.resize();
    idxCharts.forEach(function (c) { if (c.chart) c.chart.resize(); });
  };
  document.getElementById('yBtn').onclick = function () {
    autoY = !autoY;
    this.textContent = autoY ? '纵轴固定 0-100' : '纵轴自适应';
    this.classList.toggle('on', autoY);
    if (chart) chart.setOption({yAxis:{min: autoY ? 'dataMin' : 0, max: autoY ? 'dataMax' : 100}});
  };
  window.addEventListener('resize', function () {
    if (chart) chart.resize();
    idxCharts.forEach(function (c) { if (c.chart) c.chart.resize(); });
  });
}

var IDX_CONFIGS = (DATA.indices ? Object.keys(DATA.indices) : []).map(function (k) {
  return {key: k, el: 'idx_' + k};
});
mainChart();
IDX_CONFIGS.forEach(function (c) { buildIdxChart(c); idxCharts.push(c); });
if (typeof echarts !== 'undefined') {
  var g = [chart].concat(idxCharts.map(function (c) { return c.chart; })).filter(Boolean);
  if (g.length > 1) echarts.connect(g);
}
wireControls();
markFreshness();
</script>
</body></html>"""


def build_verdict(b: float, idx: dict) -> tuple:
    """基于 MA200 占比百分位 b 与三大指数百分位 idx，给出综合研判。
    返回 (标签, 等级, 一句话摘要, 详细论证)。等级: green/amber/red/neutral。
    阈值: LOW=20, HIGH=80；剪刀差收敛(<25)与出手规则见页内「用法说明」。"""
    iv = list(idx.values())
    if not iv:
        return ("数据不足", "neutral", "指数数据缺失，无法研判。", "")
    i_low_all = all(v < 20 for v in iv)
    i_high_all = all(v > 80 for v in iv)
    i_mid_all = all(v >= 20 for v in iv)   # 全部脱离 5 年低位（含高位）
    b_low = b < 20
    b_high = b > 80
    b_mid = (not b_low) and (not b_high)
    if b_low and i_low_all:
        return ("极度超卖 · 历史低位区", "green",
                "4 个百分位全部 < 20%：宽度与三大指数同步处在近 5 年底部。",
                "这是历史上赔率最高的「黄金坑」区间（如 2018 年底、2020 年疫情坑、2024 年春节前）。"
                "但「低位」不等于「立刻反转」——下行趋势仍可能延续（便宜可以更便宜）。"
                "策略：分批左侧布局，并以「宽度回升 + 剪刀差收敛」作右侧加仓确认，不一把梭。")
    if b_low and i_mid_all:
        return ("指数抢跑 · 宽度滞后", "amber",
                "三大指数已脱离 5 年低位（>20%），但 MA200 占比仍处底部（<20%）。",
                "典型「指数领涨、宽度没跟上」——权重股拉抬指数，多数个股仍在 200 日线下方，"
                "内部分化大、持续性存疑（剪刀差为负、宽度落后）。此时不宜追高；"
                "若宽度始终跟不上，指数易回落补跌。耐心等宽度修复。")
    if b_low:
        return ("分化磨底 · 结构撕裂", "amber",
                "MA200 占比仍在底部，但三大指数百分位分化（有的已脱离低位、有的仍低）。",
                "市场处于过渡 / 震荡磨底阶段，主线不清晰。控制仓位、观望为主，"
                "等待宽度与指数共振走强后再介入。")
    if b_high and i_high_all:
        return ("全面过热 · 风险积聚", "red",
                "宽度与三大指数百分位全部 > 80%，近 5 年高位区。",
                "市场情绪与仓位偏拥挤，均值回归风险上升。此时不是入场时机，"
                "反而应警惕回调、考虑分批止盈 / 降仓。")
    if b_high:
        return ("宽度偏热 · 指数分化", "amber",
                "MA200 占比已过热（>80%），但指数未同步到高位。",
                "个股普涨但指数相对落后，可能处于阶段末端的 exhaustion（涨不动）。"
                "不宜新开重仓，注意波动放大与兑现压力。")
    if i_low_all:
        return ("宽度领先 · 底部领先信号", "green",
                "MA200 占比已脱离底部（>20%），但三大指数仍处 5 年低位（<20%）。",
                "宽度（个股参与度）先于指数回暖，是底部常见的「宽度领涨」领先信号——"
                "内部分歧转好往往先于价格反转。可积极关注、分批左侧建仓。")
    if b_mid and i_high_all:
        return ("普涨 · 接近过热", "amber",
                "宽度与指数均脱离低位、且指数已近 5 年高位（>80%）。",
                "趋势仍健康但赔率下降，只宜在回踩关键支撑时低吸，不再追高；"
                "密切留意宽度是否率先见顶（顶部常由宽度恶化预告）。")
    return ("常态参与 · 趋势进行中", "neutral",
            "宽度与指数均处于 5 年中间区间（20%–80%），无极端信号。",
            "市场处于正常运行的趋势 / 震荡中。策略：顺势而为、买回调不追涨；"
            "真正的「便宜入手」机会要等百分位重新跌回低位，且剪刀差收敛"
            "（|宽度−指数相对位置| < 25，即无大劈叉）后再分批考虑。")


def write_html(res: dict, aux: dict | None, pool: dict, path: str) -> None:
    rows = res["rows"][-OUT_DAYS:]
    dates = [r["date"] for r in rows]
    payload = json.dumps({
        "dates": dates,
        "series": {str(n): [float(r[f"pct_above_{n}"]) for r in rows] for n in MA_WINDOWS},
        "ema5": [float(r.get("ema5_pct200") or 0) for r in rows],
        "emaLong": [float(r.get("ema_long_pct200") or 0) for r in rows],
        "indices": res.get("indices", {}),
        "p200Pct": res.get("p200_pct", []),
        "indexPct": res.get("index_pct", {}),
        "p200Pct5y": res.get("p200_pct_5y", []),
        "indexPct5y": res.get("index_pct_5y", {}),
        "p200Values": res.get("p200_values", []),
        "pctWindow": PCT_WINDOW,
        "pctWindow5y": PCT_WINDOW_5Y,
        "spreadBig": SPREAD_BIG,
        "indexNames": res.get("index_names", {}),
        "indexColors": res.get("index_colors", {}),
        "pool": pool,
    }, ensure_ascii=False)

    last = rows[-1]
    cards = "".join(
        f'<div class="card"><div class="lab">站上 {n} 日均线</div>'
        f'<div class="val">{float(last[f"pct_above_{n}"]):.1f}<span>%</span></div>'
        f'<div class="sub">{last[f"above_{n}"]} / {last[f"n_ma{n}"]} 只</div></div>'
        for n in MA_WINDOWS)
    aux_html = ""
    if aux:
        aux_html = (f'<div class="aux"><b>当日快照校验</b>（a-stock-api /api/market-breadth，'
                    f'独立数据源，口径略有差异）：{aux.get("date")} 上涨 {aux.get("up")} / '
                    f'下跌 {aux.get("down")} / 平 {aux.get("flat")}（合计 {aux.get("total")}）；'
                    f'涨停 {aux.get("limit_up")}、跌停 {aux.get("limit_down")}、'
                    f'最高连板 {aux.get("max_streak")} 板。</div>')
    trs = "".join(
        "<tr>" + "".join(f"<td>{r[k]}</td>" for k in
                         ["date", "n_stocks", "pct_above_5", "pct_above_20",
                          "pct_above_50", "pct_above_200", "advancers", "decliners"])
        + "</tr>" for r in reversed(rows[-15:]))

    src = pool.get("source", "unknown")
    age = pool.get("stale_days")
    if src.startswith("fallback"):
        pool_html = (f'<div class="pool warn">⚠️ <b>代码池兜底</b>：主源与备用源本次均不可用，'
                     f'使用仓库内 <code>codes.txt</code>（{pool.get("updated") or "无日期"}，'
                     f'已 {age if age is not None else "未知"} 天未刷新）。新上市 / 退市股票可能未同步；'
                     f'系统每次运行都会继续尝试刷新清单。</div>')
    elif src == "backup":
        pool_html = (f'<div class="pool warn">⚠️ <b>代码池来自备用源</b>：主源 push2 不可用，'
                     f'本次用纯在市清单 <b>{pool["count"]:,}</b> 只（不含退市 / 长期停牌股，通常少约 300 只）。'
                     f'刷新于 {pool.get("updated")}；新股仍会自动纳入。</div>')
    else:
        label = {"online": "在线快照 stock_zh_a_spot_em", "custom": "自定义股票池"}.get(src, src)
        pool_html = (f'<div class="pool">股票池：<b>{label}</b> · 刷新于 <b>{pool.get("updated")}</b> '
                     f'· <b>{pool["count"]:,}</b> 只。新股上市、退市摘牌由每日清单自动纳入 / 剔除；'
                     f'上市满 {MIN_BARS} 根日线进入 MA5/20/50 宽度，满 200 根进入 MA200 宽度。</div>')

    vals = [float(v) for v in res.get("p200_values", []) if v is not None]
    b_rank = None
    if vals:
        cur = vals[-1]
        rank = 100.0 * sum(1 for v in vals if v <= cur) / len(vals)
        b_rank = rank
        lo, hi = min(vals), max(vals)
        med = sorted(vals)[len(vals) // 2]
        pct_band = (
            '<div class="pctband"><div class="pbtitle">当前站上 200 日线占比 · 近 5 年百分位</div>'
            '<div class="pbrow"><div class="pbbig">' + f'{rank:.1f}' +
            '<span>%</span></div><div class="pbdesc">当前值 <b>' + f'{cur:.2f}%' +
            '</b>，高于近 ' + f'{len(vals)}' + ' 个交易日中 <b>' + f'{rank:.1f}%' +
            '</b> 的读数<br><span>5 年区间 ' + f'{lo:.2f}% ~ {hi:.2f}%' +
            ' · 中位 ' + f'{med:.2f}%' + '</span></div></div>'
            '<div class="pbbar"><i style="width:' + f'{rank:.1f}%' + '"></i></div></div>')
    else:
        pct_band = ''

    # —— 指数近 5 年百分位（与 MA200% 百分位同类：当前值在历史序列中的 percentile rank）——
    idx_rank = {}
    idx_stats = {}
    for k, arr in (res.get("indices") or {}).items():
        xs = [float(v) for v in arr if v is not None]
        if xs:
            c = xs[-1]
            rk = 100.0 * sum(1 for v in xs if v <= c) / len(xs)
            idx_rank[k] = rk
            idx_stats[k] = (rk, c, min(xs), max(xs), sorted(xs)[len(xs) // 2])

    idx_cards = ""
    for k in ("sse", "hs300", "cyb"):
        if k in idx_stats:
            rk, c, lo, hi, med = idx_stats[k]
            name = res.get("index_names", {}).get(k, k)
            idx_cards += (
                '<div class="idxcard"><div class="ilab">' + name + ' · 近 5 年百分位</div>'
                '<div class="ival">' + f'{rk:.1f}' + '<span>%</span></div>'
                '<div class="isub">当前 ' + f'{c:,.0f}' + ' · 5 年区间 ' + f'{lo:,.0f}' + '~' + f'{hi:,.0f}' +
                ' · 中位 ' + f'{med:,.0f}' + '</div>'
                '<div class="ibar"><i style="width:' + f'{rk:.1f}%' + '"></i></div></div>')
    idx_card_html = '<div class="idxcards">' + idx_cards + '</div>' if idx_cards else ''

    # —— 用法说明框：怎么读 4 个百分位 & 出手规则（纯用法说明，无论证叙事）——
    guide_html = (
        '<div class="guidebox"><div class="gtitle">怎么读这 4 个百分位 &amp; 出手规则（用法说明）</div>'
        '<div class="gbody">'
        '<p><b>4 个百分位（都是近 5 年 percentile rank，0-100）：</b>头部百分位条里的 <b>MA200 占比</b> + 下方三张卡片的 '
        '<b>上证综指 / 沪深300 / 创业板指</b>。数值越低 = 越接近 5 年低位（越便宜 / 超卖）；越高 = 越接近 5 年高位（越贵 / 过热）。'
        '它们各自基于不同序列，彼此不可直接相减。</p>'
        '<p><b>真正的「宽度 vs 指数」背离</b>看三张背离子图里的 <b>Spread</b> = MA200 占比分位 − 该指数分位：背景 '
        '<b style="color:#E8453C">红（宽度领先）</b> / <b style="color:#1FB37A">绿（指数领先）</b>，'
        '|<b>Spread</b>| ≥ 25 为大劈叉；|<b>Spread</b>| &lt; 25 即宽度与指数收敛、结构健康。</p>'
        '<p><b>出手规则：</b>当某张子图 <b>|Spread| &lt; 25（收敛、无大劈叉）</b> 且 <b>对应指数百分位偏低（&lt;30%，最好 &lt;20%）</b> 时，'
        '<b>分批</b>考虑入手；若出现「指数领涨、宽度滞后」的绿色大劈叉，是分化反弹，勿追。低位 ≠ 立刻反转，宜分批 + 以「宽度回升 / Spread 收敛」作右侧确认。'
        '下方「结果判断栏」已按四百分位组合给出综合结论。</p>'
        '</div></div>')

    # —— 综合研判（结果栏目）——
    verdict_html = ''
    if b_rank is not None and idx_rank:
        label, level, summary, detail = build_verdict(b_rank, idx_rank)
        spread_repr = b_rank - sum(idx_rank.values()) / len(idx_rank)
        names = res.get("index_names", {})
        verdict_html = (
            '<div class="verdict v-' + level + '">'
            '<div class="vtag">' + label + '</div>'
            '<div class="vhead">综合研判（基于 4 个百分位）</div>'
            '<div class="vsub">' + summary + '</div>'
            '<div class="vbody">' + detail + '</div>'
            '<div class="vmeta">当前 · MA200 占比百分位 <b>' + f'{b_rank:.1f}%' + '</b>'
            ' · ' + names.get("sse", "上证综指") + ' <b>' + f'{idx_rank.get("sse", 0):.1f}%' + '</b>'
            ' · ' + names.get("hs300", "沪深300") + ' <b>' + f'{idx_rank.get("hs300", 0):.1f}%' + '</b>'
            ' · ' + names.get("cyb", "创业板指") + ' <b>' + f'{idx_rank.get("cyb", 0):.1f}%' + '</b>'
            ' · 宽度−指数相对位置差 <b>' + f'{spread_repr:+.1f}' + '</b>（|&lt;25| 视为收敛）</div>'
            '</div>')

    # —— 归一化背离对比：拆成 3 张独立子图（每张预绑定一个指数作背离基准）——
    idx_sect_html = (
        '<h2>三大指数 与 MA200 占比 · 归一化背离对比（分图）</h2>'
        '<div class="ctl ctl2">'
        '  <b>归一化周期</b>'
        '  <button data-pct="250">250 日滚动</button>'
        '  <button data-pct="5y" class="on">5 年周期</button>'
        '  <span class="pcthint" id="pctHint">5 年全序列滚动窗口：看长期历史位置，近端波动会被压平</span>'
        '  <b style="margin-left:14px">视图</b><button id="axisBtn">显示原始走势</button>'
        '</div>'
        '<p class="spread-note"><b id="pctWinDesc">滚动 __PCTWIN5Y__ 个交易日（约 5 年）</b> min-max 归一化到 0-100'
        '（只用当日及之前数据，口径不随图表缩放漂移；可切换周期）。每张子图背景 Spread = MA200 占比分位 − 该指数 占比分位：'
        '<b style="color:#E8453C">正值红（宽度领先）</b> / <b style="color:#1FB37A">负值绿（指数领先）</b>，'
        '|Spread| ≥ __SPREADBIG__ 为大劈叉。悬停查看每日明细。</p>')
    for k in ("sse", "hs300", "cyb"):
        if k in (res.get("indices") or {}):
            name = res.get("index_names", {}).get(k, k)
            idx_sect_html += (
                '<div class="idxwrap">'
                '<h3>' + name + ' 与 MA200 占比 · 归一化背离对比</h3>'
                '<p class="guid">指导：当本图 <b>|Spread|&lt;25（无大劈叉、宽度与指数收敛）</b> 且 '
                '<b>指数百分位偏低（&lt;30%，最好&lt;20%）</b> 时，分批考虑入手；'
                '若出现「指数领涨宽度」的绿色大劈叉，则是分化反弹，勿追。</p>'
                '<div class="panel"><div id="idx_' + k + '"></div></div>'
                '</div>')

    html = (HTML_TEMPLATE
            .replace("__CARDS__", cards)
            .replace("__PCTBAND__", pct_band)
            .replace("__IDXCARD__", idx_card_html)
            .replace("__GUIDE__", guide_html)
            .replace("__VERDICT__", verdict_html)
            .replace("__IDXSECT__", idx_sect_html)
            .replace("__POOL__", pool_html)
            .replace("__AUX__", aux_html)
            .replace("__TRS__", trs)
            .replace("__LATEST__", res["latest"])
            .replace("__NFETCH__", f'{res["n_fetched"]:,}')
            .replace("__NVALID__", f'{res["n_valid"]:,}')
            .replace("__NDAYS__", str(res["used_days"]))
            .replace("__MINBARS__", str(MIN_BARS))
            .replace("__PCTWINDOW__", str(PCT_WINDOW))
            .replace("__PCTWIN5Y__", str(PCT_WINDOW_5Y))
            .replace("__SPREADBIG__", str(SPREAD_BIG))
            .replace("__MAXROWS__", str(MAX_CSV_ROWS))
            .replace("__MAXMB__", str(int(MAX_DIR_MB)))
            .replace("__GEN__", res["generated_at"])
            .replace("__PAYLOAD__", payload))
    atomic_write(path, html)


# ----------------------------------------------------------------------------
def main() -> int:
    global MAX_WORKERS
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    ap = argparse.ArgumentParser(description="云端版 A 股市场宽度（5 年 + 指数对比 + 存储治理）")
    ap.add_argument("--out", default="docs")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--codes-file", default="")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS)
    ap.add_argument("--max-rows", type=int, default=MAX_CSV_ROWS)
    ap.add_argument("--max-mb", type=float, default=MAX_DIR_MB)
    ap.add_argument("--min-valid", type=int, default=MIN_VALID)
    ap.add_argument("--no-aux", action="store_true")
    ap.add_argument("--no-index", action="store_true", help="跳过三大指数（快速调试用）")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    MAX_WORKERS = a.workers

    os.makedirs(a.out, exist_ok=True)
    csv_path = os.path.join(a.out, "breadth.csv")
    html_path = os.path.join(a.out, "index.html")
    existing = load_existing_rows(csv_path)

    aux = None if a.no_aux else fetch_aux()
    # 代码池陈旧时禁止幂等跳过：否则一次长期兜底会把新上市股票永久挡在门外
    before = read_codes_meta(CODES_SNAPSHOT)
    stale_days = before.get("stale_days")
    pool_stale = stale_days is None or stale_days > POOL_MAX_AGE_DAYS
    if pool_stale:
        desc = f"已 {stale_days} 天未刷新" if stale_days is not None else "没有日期标记"
        print(f"[0/6] 代码池{desc}（阈值 {POOL_MAX_AGE_DAYS} 天）"
              f"→ 禁止幂等跳过，强制刷新清单", flush=True)
    if aux:
        print(f"[0/6] 辅助源当日快照：{aux['date']} 上涨 {aux['up']} / 下跌 {aux['down']} "
              f"/ 涨停 {aux['limit_up']}（仅校验）", flush=True)
        if not a.force and not pool_stale and existing and max(existing) == aux["date"]:
            print(f"已有结果覆盖 {aux['date']} 且代码池新鲜，幂等跳过（--force 可强制）", flush=True)
            return 0

    today = datetime.now()
    start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    codes, pool_meta = get_codes(a.limit, a.codes_file or None)

    print(f"[2/6] 拉取 {len(codes)} 只股票的 {LOOKBACK_DAYS} 天后复权日线（并发 {MAX_WORKERS}）...", flush=True)
    data = fetch_all(codes, start, end)
    if len(data) < 200:
        sys.exit(f"仅成功取到 {len(data)} 只，疑似接口被限流，本次不覆盖已有结果")

    print("[3/6] 拉取沪深300 / 上证综指 / 创业板指日线...", flush=True)
    idx_maps = {} if a.no_index else fetch_indices(start, end)

    res = compute(data, idx_maps)
    if res["n_valid"] < a.min_valid:
        sys.exit(f"有效样本仅 {res['n_valid']} 只（下限 {a.min_valid}），本次不覆盖已有结果")

    merged = merge_rows(existing, res["rows"], a.max_rows)
    write_csv(merged, csv_path)
    write_html(res, aux, pool_meta, html_path)
    notes = housekeeping(a.out, a.max_mb, a.max_rows)

    last = res["rows"][-1]
    print(f"[5/6] 数据截止 {res['latest']}，有效样本 {res['n_valid']:,} 只，"
          f"本次序列 {res['used_days']} 个交易日，CSV 累计 {len(merged)} 行", flush=True)
    for n in MA_WINDOWS:
        print(f"  站上 MA{n:<3d}: {last[f'pct_above_{n}']:6.2f}%  ({last[f'above_{n}']} 只)")
    print(f"  指数曲线：{', '.join(res['index_names'].get(k, k) for k in res['indices']) or '无'}")
    print("[6/6] 存储治理：" + "；".join(notes), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
