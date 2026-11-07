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
PCT_WINDOW = 250            # etf-lsi 归一化分位的滚动窗口（交易日）
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
.panel{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;margin-bottom:16px}
#main{height:420px}#idx{height:320px}#sub{height:250px}
select.ctlsel{border:1px solid #d8dde6;border-radius:6px;padding:5px 8px;background:#fff;color:#1f2733;font-size:12px;cursor:pointer}
table{width:100%;border-collapse:collapse;background:var(--card);font-size:12px}
th,td{padding:7px 9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
th{background:#f1f3f7;color:var(--muted);font-weight:600}th:first-child,td:first-child{text-align:left}
.src{color:var(--muted);font-size:12px;margin-top:20px;border-top:1px solid var(--line);padding-top:12px}
code{background:#eef1f6;padding:1px 5px;border-radius:4px;font-size:12px}
@media(max-width:760px){.cards{grid-template-columns:repeat(2,1fr)}.wrap{padding:18px 12px 40px}.hint{display:none}#main{height:360px}#idx{height:300px}.pbbig{font-size:30px}.headrow{align-items:flex-start}.datebadge{white-space:normal}.dbtop b{font-size:15px}}
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
__POOL__
__AUX__
<h2>宽度曲线（中长期）</h2>
<div class="ctl">
  <b>区间</b>
  <button data-days="125">半年</button><button data-days="250">1 年</button>
  <button data-days="750">3 年</button><button data-days="1300" class="on">5 年</button>
  <b>指数</b><button id="axisBtn">显示原始走势</button>
  <b>背离基准</b><select id="benchSel" class="ctlsel">
    <option value="hs300">沪深300</option><option value="sse">上证综指</option><option value="cyb">创业板指</option>
  </select>
  <b>图框</b><button id="sizeBtn">放大图框</button>
  <b>纵轴</b><button id="yBtn">纵轴自适应</button>
  <span class="hint">三图联动 · 鼠标滚轮缩放 · 拖动滑块平移 · 图例可勾选显隐</span>
</div>
<div class="panel"><div id="main"></div></div>
<h2>三大指数 与 MA200 占比 · 归一化背离对比</h2>
<p class="spread-note">滚动 __PCTWINDOW__ 交易日 min-max 归一化到 0-100（口径不随图表缩放漂移）。背景 Spread = MA200 占比分位 − <b id="benchName">沪深300</b> 分位：<b style="color:#E8453C">正值红（宽度领先）</b> / <b style="color:#1FB37A">负值绿（指数领先）</b>，|Spread| ≥ __SPREADBIG__ 为大劈叉。悬停查看每日明细。</p>
<div class="panel"><div id="idx"></div></div>
<h2>涨跌家数 与 McClellan 振荡器</h2>
<div class="panel"><div id="sub"></div></div>
<h2>最近 15 个交易日</h2>
<table><thead><tr><th>日期</th><th>样本数</th><th>&gt;MA5</th><th>&gt;MA20</th><th>&gt;MA50</th><th>&gt;MA200</th><th>上涨</th><th>下跌</th><th>McClellan</th></tr></thead><tbody>__TRS__</tbody></table>
<div class="src">
主数据源：AKShare → 东方财富 <code>stock_zh_a_spot_em</code>（代码清单主源）→ <code>stock_info_a_code_name</code>（备用源）→ <code>codes.txt</code>（带日期兜底）+ <code>stock_zh_a_hist(adjust="hfq")</code>（逐只后复权日线）+ <code>index_zh_a_hist</code>（沪深300 / 上证综指 / 创业板指）。辅助校验：<code>a-stock-api /api/market-breadth</code> 当日快照（不参与均线计算）。<br>
数据截止 <b>__LATEST__</b>；成功取数 <b>__NFETCH__</b> 只、计入计算 <b>__NVALID__</b> 只、本次序列 <b>__NDAYS__</b> 个交易日。等权、含北交所/科创创业、ST 未单独剔除。股票池由每日在线清单自动纳入新股、剔除退市股；上市满 __MINBARS__ 根日线进入 MA5/20/50 宽度，满 200 根进入 MA200 宽度；停牌日不计入分母。<br>
归一化分位参照 <code>wj624/etf-lsi</code>：每个点取自身及之前 __PCTWINDOW__ 个交易日的极值做 min-max 映射到 0-100，Spread = MA200 占比分位 − 基准指数分位（−100 ~ +100），正红负绿、±__SPREADBIG__ 为大劈叉；这是形状对比，不是收益率。头部百分位是「当前 MA200 占比在近 5 年序列中的 percentile rank」，与滚动归一化分位是两个不同指标。<br>
⚠️ 5 年历史是用「当前股票池」回算的，早期存在幸存者偏差（当时未上市 / 现已退市的股票不在池内），越往前偏差越大，不可表述为「当时的真实全市场宽度」。<br>
CSV 滚动保留 __MAXROWS__ 个交易日；docs/ 超过 __MAXMB__ MB 自动裁剪。GitHub Actions 生成于 __GEN__（北京时间）；仅为公开数据统计结果，不构成投资建议。
</div>
</div>
<script>
var DATA = __PAYLOAD__;
var COLORS = {'50':'#e0972f','200':'#d64545'};
var IDX_KEYS = Object.keys(DATA.indices || {});
var rawMode = false, autoY = false, bigFrame = false;
var curStart = 0, curEnd = 100, benchKey = 'hs300', CUR_SPREAD = [];
var chart = null, idxChart = null, subChart = null;

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
function spreadArray() {
  var a = DATA.p200Pct || [], b = DATA.indexPct[benchKey] || [];
  return a.map(function (v, i) {
    return (v == null || b[i] == null) ? null : +(v - b[i]).toFixed(2);
  });
}
// 默认：MA200 占比 + 沪深300 显示；上证 / 创业板默认关闭。
// 用户手动勾选后以图表实例的真实 selected 为准（不依赖 legendselectchanged 事件，
// 因此无论点击图例还是脚本 dispatchAction 都能保留）。切换「分位 / 基准 100」
// 时 series 名称会变，这里按指数 key 在两套命名之间做映射，避免选择被重置。
function idxLegendSelected() {
  var cur = {};
  try {
    if (idxChart) {
      var op = idxChart.getOption();
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
  IDX_KEYS.forEach(function(k){
    s[DATA.indexNames[k] + suffix] =
      pick(DATA.indexNames[k] + ' 分位', DATA.indexNames[k] + '（基准 100）', k === 'hs300');
  });
  s[rawMode ? 'MA200 占比' : 'MA200 占比分位'] =
    pick('MA200 占比分位', 'MA200 占比', true);
  return s;
}
function originalIndexSeries(startPct, endPct) {
  var r = bounds(startPct, endPct), x = r[0], y = r[1];
  return IDX_KEYS.map(function (k) {
    var arr = DATA.indices[k], base = null;
    for (var j = x; j <= y; j++) { if (arr[j] != null) { base = arr[j]; break; } }
    return {id:'idx_'+k, name: DATA.indexNames[k]+'（基准 100）', type:'line', yAxisIndex:1,
      data: arr.map(function(v){ return (v==null||!base)?null:+((v/base)*100).toFixed(2); }),
      showSymbol:false, smooth:true, connectNulls:true,
      lineStyle:{width:1.9,color:DATA.indexColors[k]}, itemStyle:{color:DATA.indexColors[k]}};
  });
}
function pctSeries() {
  var out = [{id:'p200pct', name:'MA200 占比分位', type:'line', yAxisIndex:0, z:6,
    data: DATA.p200Pct, showSymbol:false, smooth:true, connectNulls:true,
    lineStyle:{width:2.8,color:'#d64545'}, itemStyle:{color:'#d64545'}}];
  IDX_KEYS.forEach(function(k){
    out.push({id:'idx_'+k, name: DATA.indexNames[k]+' 分位', type:'line', yAxisIndex:0, z:5,
      data: DATA.indexPct[k]||[], showSymbol:false, smooth:true, connectNulls:true,
      lineStyle:{width:1.7,color:DATA.indexColors[k]}, itemStyle:{color:DATA.indexColors[k]}});
  });
  return out;
}
function spreadSeries() {
  return [
    {name:'Spread 背景', type:'bar', yAxisIndex:1, z:1, data:CUR_SPREAD, barWidth:'100%', silent:true,
     itemStyle:{color:function(p){
       if (p.value == null) return 'rgba(0,0,0,0)';
       return p.value >= 0 ? 'rgba(232,69,60,0.20)' : 'rgba(31,159,107,0.20)';
     }}},
    {name:'Spread', type:'line', yAxisIndex:1, z:2, data:CUR_SPREAD, showSymbol:false,
     lineStyle:{width:1,color:'rgba(107,118,136,0.55)'}, itemStyle:{color:'#6b7688'},
     markLine:{silent:true, symbol:'none', label:{show:false},
       lineStyle:{type:'dashed',width:1,color:'#aab2c0'},
       data:[{yAxis:DATA.spreadBig},{yAxis:-DATA.spreadBig},{yAxis:0,lineStyle:{color:'#c9ced8',width:1}}]}}
  ];
}
function rawSeries(startPct, endPct) {
  var s = [{id:'p200raw', name:'MA200 占比', type:'line', yAxisIndex:0, z:6,
    data: DATA.p200Values, showSymbol:false, smooth:true, connectNulls:true,
    lineStyle:{width:2.8,color:'#d64545'}, itemStyle:{color:'#d64545'}}];
  return s.concat(originalIndexSeries(startPct, endPct));
}
function idxTooltip(ps) {
  if (!ps || !ps.length) return '';
  var i = ps[0].dataIndex, out = ['<b>'+DATA.dates[i]+'</b>'];
  out.push('MA200 占比：'+fmt(DATA.p200Values[i],'%')+'　分位：'+fmt(DATA.p200Pct[i]));
  IDX_KEYS.forEach(function(k){
    out.push(DATA.indexNames[k]+'：'+fmt(DATA.indices[k][i])+'　分位：'+fmt(DATA.indexPct[k][i]));
  });
  var sp = CUR_SPREAD[i];
  if (sp != null) {
    var col = sp >= 0 ? '#E8453C' : '#1FB37A';
    var tag = Math.abs(sp) >= DATA.spreadBig ? ' · 大劈叉' : '';
    out.push('<span style="color:'+col+'">Spread（vs '+DATA.indexNames[benchKey]+'）：'+(sp>0?'+':'')+sp.toFixed(1)+tag+'</span>');
  }
  return out.join('<br>');
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
    if (rawMode && idxChart) refreshIdx();
  });
}

function idxChartInit() {
  var el = document.getElementById('idx');
  if (typeof echarts === 'undefined') { el.innerHTML='<p style="color:#6b7688">图表库未加载（离线）。</p>'; return; }
  idxChart = echarts.init(el);
  CUR_SPREAD = spreadArray();
  idxChart.setOption({
    animation:false, tooltip:{trigger:'axis',formatter:idxTooltip},
    legend:{top:0,textStyle:{color:'#1f2733'},selected:idxLegendSelected()},
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
    series: pctSeries().concat(spreadSeries())
  });
  // 不挂 legendselectchanged：显隐状态由 idxLegendSelected() 从 getOption() 读取，
  // 可覆盖点击与 dispatchAction 两种来源，也避免与 setOption 的 selected 互相打架。
}

function subChartInit() {
  var el = document.getElementById('sub');
  if (typeof echarts === 'undefined') return;
  subChart = echarts.init(el);
  subChart.setOption({
    animation:false, tooltip:{trigger:'axis'}, legend:{top:0, textStyle:{color:'#1f2733'}},
    grid:{left:60, right:60, top:38, bottom:58},
    xAxis:{type:'category', data: DATA.dates, axisLabel:{color:'#6b7688'}},
    yAxis:[{type:'value', name:'家数', axisLabel:{color:'#6b7688'}, splitLine:{lineStyle:{color:'#eef0f4'}}},
           {type:'value', name:'McClellan', axisLabel:{color:'#6b7688'}, splitLine:{show:false}}],
    dataZoom:[
      {type:'inside', xAxisIndex:0, zoomOnMouseWheel:true, moveOnMouseMove:true, moveOnMouseWheel:false},
      {type:'slider', xAxisIndex:0, bottom:10, height:18}
    ],
    series:[
      {name:'上涨家数', type:'bar', stack:'ad', data: DATA.adv, itemStyle:{color:'#d64545'}},
      {name:'下跌家数', type:'bar', stack:'ad', data: DATA.dec, itemStyle:{color:'#1f9d6b'}},
      {name:'McClellan', type:'line', yAxisIndex:1, data: DATA.mc, showSymbol:false, smooth:true,
       lineStyle:{width:1.8, color:'#2f6fd0'}, itemStyle:{color:'#2f6fd0'}}
    ]
  });
}

function refreshIdx() {
  if (!idxChart) return;
  var sel = idxLegendSelected();
  if (rawMode) {
    CUR_SPREAD = [];
    idxChart.setOption({legend:{selected:sel}, yAxis:[{},{name:'指数（基准 100）',min:'dataMin',max:'dataMax'}]});
    idxChart.setOption({series: rawSeries(curStart, curEnd)}, {replaceMerge:['series']});
  } else {
    CUR_SPREAD = spreadArray();
    idxChart.setOption({legend:{selected:sel}, yAxis:[{},{name:'Spread',min:-100,max:100}]});
    idxChart.setOption({series: pctSeries().concat(spreadSeries())}, {replaceMerge:['series']});
  }
}

function setRange(days) {
  var n = DATA.dates.length;
  var pct = Math.max(0, (n - days) / (n - 1) * 100);
  if (chart) chart.dispatchAction({type:'dataZoom', start:pct, end:100});
  curStart = pct; curEnd = 100;
  if (rawMode) refreshIdx();
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
  document.getElementById('axisBtn').onclick = function () {
    rawMode = !rawMode;
    this.textContent = rawMode ? '显示归一化分位' : '显示原始走势';
    this.classList.toggle('on', rawMode);
    refreshIdx();
  };
  document.getElementById('benchSel').onchange = function () {
    benchKey = this.value;
    document.getElementById('benchName').textContent = DATA.indexNames[benchKey];
    if (!rawMode) refreshIdx();
  };
  document.getElementById('sizeBtn').onclick = function () {
    bigFrame = !bigFrame;
    document.getElementById('main').style.height = bigFrame ? '680px' : '420px';
    document.getElementById('idx').style.height = bigFrame ? '500px' : '320px';
    this.textContent = bigFrame ? '还原图框' : '放大图框';
    this.classList.toggle('on', bigFrame);
    if (chart) chart.resize();
    if (idxChart) idxChart.resize();
  };
  document.getElementById('yBtn').onclick = function () {
    autoY = !autoY;
    this.textContent = autoY ? '纵轴固定 0-100' : '纵轴自适应';
    this.classList.toggle('on', autoY);
    if (chart) chart.setOption({yAxis:{min: autoY ? 'dataMin' : 0, max: autoY ? 'dataMax' : 100}});
  };
  window.addEventListener('resize', function () {
    if (chart) chart.resize();
    if (idxChart) idxChart.resize();
    if (subChart) subChart.resize();
  });
}

mainChart(); idxChartInit(); subChartInit();
if (typeof echarts !== 'undefined') {
  var g = [chart, idxChart, subChart].filter(Boolean);
  if (g.length > 1) echarts.connect(g);
}
wireControls();
markFreshness();
</script>
</body></html>"""


def write_html(res: dict, aux: dict | None, pool: dict, path: str) -> None:
    rows = res["rows"][-OUT_DAYS:]
    dates = [r["date"] for r in rows]
    payload = json.dumps({
        "dates": dates,
        "series": {str(n): [float(r[f"pct_above_{n}"]) for r in rows] for n in MA_WINDOWS},
        "adv": [int(r["advancers"]) for r in rows],
        "dec": [-int(r["decliners"]) for r in rows],
        "ema5": [float(r.get("ema5_pct200") or 0) for r in rows],
        "emaLong": [float(r.get("ema_long_pct200") or 0) for r in rows],
        "mc": [float(r.get("mcclellan") or 0) for r in rows],
        "indices": res.get("indices", {}),
        "p200Pct": res.get("p200_pct", []),
        "indexPct": res.get("index_pct", {}),
        "p200Values": res.get("p200_values", []),
        "pctWindow": PCT_WINDOW,
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
                          "pct_above_50", "pct_above_200", "advancers", "decliners", "mcclellan"])
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
    if vals:
        cur = vals[-1]
        rank = 100.0 * sum(1 for v in vals if v <= cur) / len(vals)
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

    html = (HTML_TEMPLATE
            .replace("__CARDS__", cards)
            .replace("__PCTBAND__", pct_band)
            .replace("__POOL__", pool_html)
            .replace("__AUX__", aux_html)
            .replace("__TRS__", trs)
            .replace("__LATEST__", res["latest"])
            .replace("__NFETCH__", f'{res["n_fetched"]:,}')
            .replace("__NVALID__", f'{res["n_valid"]:,}')
            .replace("__NDAYS__", str(res["used_days"]))
            .replace("__MINBARS__", str(MIN_BARS))
            .replace("__PCTWINDOW__", str(PCT_WINDOW))
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
