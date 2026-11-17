#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""零外部依赖的 workflow 静态体检。

背景（2026-11-14 真实事故）：给 job 加 `env: A_STOCK_API_KEY` 时，忘了已有
`env: TZ`，同一 mapping 出现两个同名键。PyYAML 会静默「后者覆盖前者」，
但 GitHub Actions 直接判 workflow 无效——push 时生成一个名为
`.github/workflows/breadth.yml` 的失败 run，之后 16:30 / 18:00 的定时触发
**完全消失且没有任何失败记录**，只能靠页面新鲜度横幅发现。

本脚本检查三件事：
  ① 同一缩进层级是否出现重复的 mapping 键（重点看 env / jobs / on / steps / name）；
  ② 四个定时 workflow 是否还保留 `schedule:`（防止误删后定时任务静默消失）；
  ③ requirements.txt 里 akshare / pandas / numpy 是否精确锁成 pkg==x.y.z。

第 ③ 条来自 2026-11-17 事故：原本 `numpy<3` 自动漂到 2.5.5（11-13 成功时是
2.5.4），26 个包里只有它变了，取数阶段并发调用 pandas/numpy C 层直接
Segmentation fault (exit 139)。范围约束 = 把 CI 环境交给 PyPI 抽签，必须禁止。

改完 `.github/workflows/*.yml` 必须本地跑一次；CI 里由 probe-sources.yml 每周跑一次。
退出码 0 = 健康，1 = 有问题（这里**故意**让 CI 变红，好让 GitHub 立刻通知）。
"""
from __future__ import annotations

import glob
import os
import re
import sys

WATCH_KEYS = {
    "env", "jobs", "on", "permissions", "concurrency", "name", "runs-on",
    "timeout-minutes", "steps", "strategy", "defaults", "outputs", "with",
}
NEEDS_SCHEDULE = {"breadth.yml", "freshness.yml", "maintenance.yml", "probe-sources.yml"}
KEY_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*)\s*:(?:\s|$)")


def check(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    problems: list[str] = []
    level_keys: dict[int, set[str]] = {}

    for i, raw in enumerate(lines, 1):
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if s.startswith("- ") or s == "-":
            # 每个列表项都是独立 mapping（steps 下每个 step 都有自己的 with/env），
            # 必须重置当前及更深层的键集合，否则会把不同 step 的同名键误判成重复。
            for k in [x for x in level_keys if x >= indent]:
                del level_keys[k]
            continue
        m = KEY_RE.match(s)
        if not m:
            continue
        key = m.group(1)
        if key not in WATCH_KEYS:
            continue
        for k in [x for x in level_keys if x > indent]:
            del level_keys[k]              # 缩进回退 = 退出更深的 mapping
        keys = level_keys.setdefault(indent, set())
        if key in keys:
            problems.append(f"第 {i} 行：重复键 '{key}'（缩进 {indent}），GitHub 会判 workflow 无效")
        keys.add(key)

    base = os.path.basename(path)
    if base in NEEDS_SCHEDULE and not any(re.match(r"^\s*schedule:\s*$", l) for l in lines):
        problems.append(f"缺少 schedule: 定时触发会消失（{base}）")
    return problems


def check_pins(root: str) -> list[str]:
    """akshare / pandas / numpy 必须是 pkg==x.y.z，不允许 >= / < / ~= / 裸包名。"""
    path = os.path.join(root, "requirements.txt")
    if not os.path.exists(path):
        return ["requirements.txt 缺失"]
    with open(path, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
    problems: list[str] = []
    for pkg in ("akshare", "pandas", "numpy"):
        found = [l for l in lines if re.match(rf"^{pkg}\b", l)]
        if len(found) != 1:
            problems.append(f"{pkg} 应恰好有 1 条约束，实际 {len(found)} 条")
        elif not re.fullmatch(rf"{pkg}==\d+\.\d+\.\d+", found[0]):
            problems.append(f"{pkg} 未精确锁版本：'{found[0]}'，必须写 {pkg}==x.y.z")
    return problems


def check_worker_key(root: str) -> list[str]:
    """Worker 鉴权链路不能被删：4 个 workflow 引用 Secret + 代码只读环境变量。

    背景：密钥是防止别人 fork / 复制仓库蹭自建 Worker 额度的唯一闸门。
    GitHub 的硬规则是 fork 不继承 Secrets，Worker 端对所有 /api/* 强制校验
    X-API-Key（见 wj624/a-stock-api worker.js:1197）。任何一环被删，别人
    复制仓库就能白嫖；而仓库里又不能写明文密钥，所以静态体检是唯一防线。
    """
    problems: list[str] = []
    workflows = ("breadth.yml", "freshness.yml", "maintenance.yml", "probe-sources.yml")
    ref = "A_STOCK_API_KEY: ${{ secrets.A_STOCK_API_KEY }}"
    for name in workflows:
        p = os.path.join(root, ".github", "workflows", name)
        if not os.path.exists(p):
            problems.append(f"缺少 {name}")
            continue
        with open(p, encoding="utf-8") as f:
            if ref not in f.read():
                problems.append(f"{name} 缺少 `{ref}`（Worker 鉴权失效）")
    code = os.path.join(root, "breadth.py")
    if not os.path.exists(code):
        problems.append("缺少 breadth.py")
    else:
        with open(code, encoding="utf-8") as f:
            src = f.read()
        for token in ('os.environ.get("A_STOCK_API_KEY"', '"X-API-Key"'):
            if token not in src:
                problems.append(f"breadth.py 缺少 `{token}`（Worker 鉴权失效）")
        if re.search(r"ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}", src):
            problems.append("breadth.py 出现明文令牌")
    return problems


def main() -> int:
    root = os.path.dirname(__file__)
    files = sorted(glob.glob(os.path.join(root,
                                         ".github", "workflows", "*.yml")))
    if not files:
        print("FAIL 没找到任何 workflow 文件")
        return 1
    bad = 0
    for p in files:
        issues = check(p)
        name = os.path.basename(p)
        if issues:
            bad += 1
            print(f"FAIL {name}")
            for x in issues:
                print("     -", x)
        else:
            print(f"OK   {name}")
    print("WORKFLOW_YAML_OK" if not bad else f"WORKFLOW_YAML_FAIL ({bad} 个文件有问题)")

    pin_issues = check_pins(root)
    if pin_issues:
        print("DEPS_PIN_FAIL  requirements.txt 存在版本漂移风险")
        for x in pin_issues:
            print("     -", x)
    else:
        print("DEPS_PIN_OK   akshare / pandas / numpy 均已精确锁版本")

    key_issues = check_worker_key(root)
    if key_issues:
        print("WORKER_KEY_FAIL  Worker 鉴权链路不完整（别人复制仓库可能蹭额度）")
        for x in key_issues:
            print("     -", x)
    else:
        print("WORKER_KEY_OK    4 个 workflow 引用 Secret，代码只读环境变量、无明文")

    ok = not bad and not pin_issues and not key_issues
    print("ALL_CHECKS_OK" if ok else "CHECKS_FAIL（见上方 FAIL 明细）")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
