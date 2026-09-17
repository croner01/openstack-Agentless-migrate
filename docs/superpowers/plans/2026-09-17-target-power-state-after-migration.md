# 迁移完成后目标机电源状态（逐 VM 开关）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development（每个 Task 先写失败测试）与 superpowers:verification-before-completion（收尾跑全量回归）。

**Goal:** 向导步骤④为每台 VM 提供「迁移完成后是否开机」开关，默认开机；关掉后数据照常迁移、状态照常成功，但目标机保持 `SHUTOFF`（含增量·手动切换的收尾）。

**Architecture:** 逐 VM 布尔值沿 `row_overrides → MigrationRow → VmTask → 迁移收尾分支` 传递。RBD 路径跳过 `os-start`（目标此时本就关机）；中转机路径因 Nova 建机必然先开机，改为等 `ACTIVE` 后 `os-stop` 并等 `SHUTOFF`。不新增 `VmStatus`。

**Tech Stack:** Python 3.10、Flask、`unittest` + `unittest.mock`、原生 JS（`templates/index.html`）。

设计文档：`docs/superpowers/specs/2026-09-17-target-power-state-after-migration-design.md`

涉及真实 socket 的用例需要 `require_escalated` 运行；其余在沙箱内即可。

## 文件结构

| 文件 | 状态 | 职责 |
| --- | --- | --- |
| `state_machine.py` | 改造 | `VmTask.start_target` 字段与持久化 |
| `excel_parser.py` | 改造 | `MigrationRow.start_target`、`parse_start_target()`、可选列 |
| `job_manager.py` | 改造 | `create_job` 透传 |
| `migration_manager.py` | 改造 | RBD 跳过开机、中转机关机收尾 |
| `app.py` | 改造 | `row_overrides` 解析、`_serialize_vm` 下发 |
| `templates/index.html` | 改造 | 步骤④复选框列、批量设置、摘要计数、详情 chip 与抽屉 |
| `README.md` | 改造 | 功能说明与中转机路径的"仍会开机一次"提示 |
| `tests/test_*.py` | 新增/扩展 | 见各 Task |

---

## Task 1: 状态模型与持久化

**Files:** Modify `state_machine.py` / Test `tests/test_state_machine.py`

- [x] Step 1（失败测试）：`VmTask(name="vm", target_az="az")` 的 `start_target` 为 `True`；`start_target=False` 经 `to_dict`/`from_dict` 往返仍为 `False`；`from_dict` 缺该键时为 `True`。
- [x] Step 2：实现字段与序列化。
- [x] Step 3：`python3 -m unittest tests.test_state_machine` 全绿。

## Task 2: Excel / JSON 行解析

**Files:** Modify `excel_parser.py` / Test `tests/test_excel_parser.py`

- [x] Step 1（失败测试）：`parse_start_target` 对 `None/""/true/1/yes/是/开机` 返回 `True`，对 `false/0/no/否/不开机` 返回 `False`，非法值抛 `ValueError`；`parse_rows` 缺列或留空=开机、非法值报错带行号；`parse_selected_rows` 支持 `start_target`。
- [x] Step 2：实现 `parse_start_target()` 与两处调用；`OPTIONAL_COLUMNS` 增列。
- [x] Step 3：`python3 -m unittest tests.test_excel_parser` 全绿。

## Task 3: 作业创建透传

**Files:** Modify `job_manager.py` / Test `tests/test_job_manager.py`

- [x] Step 1（失败测试）：`create_job` 传入 `start_target=False` 的 `MigrationRow` → `job.vms[0].start_target is False`。
- [x] Step 2：`create_job` 里带上 `start_target=row.start_target`。
- [x] Step 3：`python3 -m unittest tests.test_job_manager` 全绿。

## Task 4: RBD 路径收尾

**Files:** Modify `migration_manager.py` / Test `tests/test_migration_manager.py`

- [x] Step 1（失败测试）：新增 `TargetPowerStateTest`：
  - `start_target=True`（默认）→ `_start_target_if_requested(vm)` 调 `target_os.start_server`；
  - `start_target=False` → 不调 `start_server`、不调 `wait_server_status(ACTIVE)`、状态仍由 `mark_success()` 收敛为 `SUCCESS`。
- [x] Step 2：抽出 `_start_target_if_requested(vm)` 并替换 `_migrate_vm_inner` 里的内联收尾。
- [x] Step 3：`python3 -m unittest tests.test_migration_manager` 全绿。

## Task 5: 中转机路径收尾

**Files:** Modify `migration_manager.py` / Test `tests/test_relay_relay_power_state`（并入 `tests/test_migration_manager.py`）

- [x] Step 1（失败测试）：`start_target=False` 的 VM 走中转机收尾时 `stop_server` 被调且随后等待 `SHUTOFF`；`start_target=True` 时不调 `stop_server`。
- [x] Step 2：在 `_migrate_vm_via_relay` 的 `wait_server_booted` 之后按配置 `os-stop`。
- [x] Step 3：`python3 -m unittest tests.test_migration_manager` 全绿。

## Task 6: 接口层

**Files:** Modify `app.py` / Test 新增 `tests/test_target_power_state_api.py`

- [x] Step 1（失败测试）：`row_overrides[key].start_target=false` → 该 VM `start_target is False`，同批其它 VM 仍为 `True`；未传该字段时默认 `True`；`_serialize_vm` 输出该字段。
- [x] Step 2：`_create_job_and_files` 里应用覆盖 + `_serialize_vm` 增字段。
- [x] Step 3：`python3 -m unittest tests.test_target_power_state_api` 全绿。

## Task 7: 前端

**Files:** Modify `templates/index.html` / Test `tests/test_relay_ui_render.py`

- [x] Step 1（失败测试）：步骤④表头含「迁移后开机」；`renderPlan` 构建 `start_target` 复选框；`collectPreviewValues` 对复选框取 `.checked`；提交 `row_overrides` 带 `start_target`；工具条有批量控件；作业详情渲染「迁移后不开机」chip；抽屉有该行。
- [x] Step 2：实现（含 `colspan` 8→9 的三处空态行）。
- [x] Step 3：`python3 -m unittest tests.test_relay_ui_render` 全绿。

## Task 8: 文档与回归

- [x] Step 1：README 补功能说明与中转机路径"目标 OS 仍会启动一次"的提示。
- [x] Step 2：`timeout 600 python3 -m unittest discover -s tests`（需 escalated）全绿。
- [x] Step 3：浏览器验证步骤④勾选/取消与详情展示（Playwright + mock）。
