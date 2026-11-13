#!/usr/bin/env bash
# 容器/本地一键验收：生成样例 -> 跑单元测试 -> 功能端到端演示 -> 并发修复专项演示。
# 在容器内：docker compose exec dicom-remap scripts/verify.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> [1/4] 生成合成 DICOM 样例"
python -I scripts/generate_samples.py "${DICOM_REMAP_DATA:-/data}/samples"

echo "==> [2/4] 运行 pytest 单元/集成测试（含并发等待、超时收尾、断连专项）"
python -I -m pytest tests/ -q

echo "==> [3/4] 运行功能端到端演示（跨批次引用、同源并发、项目隔离、失败回滚）"
python -I scripts/demo.py --samples "${DICOM_REMAP_DATA:-/data}/samples"

echo "==> [4/4] 运行并发修复专项演示（40 并发查询 SLA、慢转换、409 收尾、请求断连）"
python -I scripts/concurrency_demo.py

echo "==> 验收全部通过"
