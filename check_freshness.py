#!/usr/bin/env python3
"""
死信开关（dead-man's switch）：断言看板数据未过期。

逻辑与页面 markFreshness 保持一致：
- 以北京时间为准；18:30 前期待「上一工作日」数据，之后期待「当日」数据；跳过周末。
- 读取 docs/breadth.csv 最后一行日期（date 列），若 < 应有交易日则判定过期。
- 过期 / 文件缺失 / 为空 / 解析失败 → 退出码 1（让 GitHub Actions 失败并通知仓库所有者）；否则 0。

零外部依赖（仅标准库），不依赖数据源是否在线，因此本检查本身不会因东财 / akshare 故障而失效——
这正是它作为「死信开关」的意义：数据源挂了，这个检查反而能发现。

注意：法定节假日 / 临时休市不在判断范围内（与页面一致）。节后首个交易日可能出现一次温和误报，属预期。
"""
import csv
import os
import sys
from datetime import datetime, timedelta, timezone

BJ = timezone(timedelta(hours=8))

# 脚本位于仓库根目录时，docs/breadth.csv 即相对路径；可用 CSV_PATH 覆盖。
CSV_PATH = os.environ.get("CSV_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "docs", "breadth.csv"
)


def expected_last_trading_day(now_bj: datetime) -> datetime:
    exp = now_bj.replace(hour=0, minute=0, second=0, microsecond=0)
    # 18:30 前：当日数据大概率还没生成，期待上一工作日
    if now_bj.hour * 60 + now_bj.minute < 18 * 60 + 30:
        exp -= timedelta(days=1)
    # 跳过周末（5=周六, 6=周日）
    while exp.weekday() >= 5:
        exp -= timedelta(days=1)
    return exp


def read_latest(csv_path: str):
    if not os.path.exists(csv_path):
        return None, f"文件不存在: {csv_path}"
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and "date" not in reader.fieldnames:
                date_col = reader.fieldnames[0]  # 退路：取第一列
            else:
                date_col = "date"
            rows = [r for r in reader if (r.get(date_col) or "").strip()]
        if not rows:
            return None, "CSV 为空（无任何数据行）"
        return rows[-1][date_col].strip(), None
    except Exception as e:  # noqa: BLE001 - 任何解析异常都按过期处理
        return None, f"读取/解析失败: {e}"


def write_summary(title: str, body: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"## {title}\n\n{body}\n\n")
        except Exception:  # noqa: BLE001
            pass


def main() -> None:
    now_bj = datetime.now(BJ)
    latest, err = read_latest(CSV_PATH)
    if err or not latest:
        msg = f"STALE: 无法确认数据日期 —— {err or '无日期'}"
        print(msg)
        print(f"[debug] now(BJ)={now_bj.strftime('%Y-%m-%d %H:%M')}  csv={CSV_PATH}")
        write_summary("⚠️ 数据可能过期", msg)
        sys.exit(1)

    exp = expected_last_trading_day(now_bj)
    exp_s = exp.strftime("%Y-%m-%d")
    gap = (now_bj.date() - datetime.strptime(latest, "%Y-%m-%d").date()).days

    print(f"now(BJ)       = {now_bj.strftime('%Y-%m-%d %H:%M')}")
    print(f"latest(data)  = {latest}")
    print(f"expected >=   = {exp_s}")
    print(f"gap(calendar) = {gap} 天")

    if latest >= exp_s:
        print("FRESH: 数据日期不早于应有交易日，看板最新。")
        write_summary("✅ 数据新鲜", f"数据日期 {latest} 不早于应有交易日 {exp_s}，看板最新。")
        sys.exit(0)

    msg = (
        f"STALE: 数据停留在 {latest}（应有交易日 >= {exp_s}，已 {gap} 天未更新）。"
        "如非法定节假日 / 周末，可能数据源异常，请检查仓库 Actions 是否失败。"
    )
    print(msg)
    write_summary("⚠️ 数据可能过期", msg)
    sys.exit(1)


if __name__ == "__main__":
    main()
