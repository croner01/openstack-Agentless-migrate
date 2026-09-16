# 源 VM 列表选择迁移 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持在 Step 2 直接加载源项目 VM 列表勾选迁移，Excel 降级为折叠的“高级批量导入”，所有任务最终进入同一套迁移清单。

**Architecture:** 前端新增“源 VM 选择器”，列表模式通过 `selected_rows` 直接创建任务；后端新增 `/api/source-vms`、扩展 `/api/catalog` 返回真实 AZ，并把 Excel/列表两条入口统一成带 `server_id` 的 `MigrationRow`。后端在已有 `source_server_id` 时按 ID 取源 VM。

**Tech Stack:** Flask、openstacksdk、Jinja 模板（原生 JS）、unittest。

---

## 说明

当前目录不是 git 仓库，所有任务里的 commit 步骤跳过。全部文件沿用现有
ConfigMap 挂载方式，不新增 Python 模块，不改 Deployment 挂载。

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `excel_parser.py` | `MigrationRow` 支持 `source_server_id`；新增 `parse_selected_rows()` |
| `job_manager.py` | `create_job()` 从 `MigrationRow.source_server_id` 写入 `VmTask` |
| `migration_manager.py` | 有 `source_server_id` 时按 ID 取源 VM |
| `openstack_utils.py` | 源 VM 列表/分页/补全、目标 AZ 查询 |
| `app.py` | `/api/source-vms`、catalog 增加 AZ、`/api/migrate` 支持 `selected_rows` |
| `templates/index.html` | 源 VM 选择器、AZ 下拉、Excel 折叠入口、提交逻辑 |
| 各 `tests/` | 上述行为的单元测试 |

---

### Task 1: MigrationRow 支持 server_id 与列表行解析

**Files:**
- Modify: `excel_parser.py`
- Test: `tests/test_excel_parser.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_excel_parser.py`:

```python
from excel_parser import parse_selected_rows


class SelectedRowsTest(unittest.TestCase):
    def test_parse_selected_rows_preserves_server_id(self):
        rows = parse_selected_rows(
            [
                {
                    "server_id": "srv-1",
                    "vm_name": "web-1",
                    "target_az": "az1",
                    "target_image": "img-1",
                    "target_flavor": "flavor-1",
                }
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].source_server_id, "srv-1")
        self.assertEqual(rows[0].vm_name, "web-1")
        self.assertEqual(rows[0].target_image, "img-1")

    def test_parse_selected_rows_requires_server_id(self):
        with self.assertRaisesRegex(ValueError, "server_id"):
            parse_selected_rows(
                [{"server_id": "", "vm_name": "web-1", "target_az": "az1"}]
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_excel_parser.SelectedRowsTest -v`
Expected: ImportError/AttributeError because `parse_selected_rows` does not exist.

- [ ] **Step 3: Implement**

In `excel_parser.py`:

```python
@dataclass
class MigrationRow:
    vm_name: str
    target_az: str
    source_server_id: str | None = None
    target_image: str | None = None
    target_flavor: str | None = None
    source_networks: list = None

    def __post_init__(self):
        if self.source_networks is None:
            self.source_networks = []


def parse_selected_rows(records: list[dict[str, Any]]) -> list[MigrationRow]:
    """Parse JSON rows produced by the source-VM picker."""
    rows: list[MigrationRow] = []
    for index, record in enumerate(records, start=1):
        server_id = str(record.get("server_id") or "").strip()
        vm_name = str(record.get("vm_name") or "").strip()
        target_az = str(record.get("target_az") or "").strip()
        if not server_id or not vm_name or not target_az:
            raise ValueError(
                f"第 {index} 条 selected_rows 缺少 server_id/vm_name/target_az"
            )
        rows.append(
            MigrationRow(
                vm_name=vm_name,
                target_az=target_az,
                source_server_id=server_id,
                target_image=str(record.get("target_image") or "").strip() or None,
                target_flavor=str(record.get("target_flavor") or "").strip() or None,
            )
        )
    return rows
```

Note: field order change keeps all existing Excel parser constructor keywords valid.

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest discover -s tests`
Expected: PASS, 原 74 个测试 + 新增 2 个。

---

### Task 2: VmTask 预填 source_server_id 且迁移器按 ID 取源 VM

**Files:**
- Modify: `job_manager.py`, `migration_manager.py`
- Test: `tests/test_migration_manager.py`, `tests/test_job_manager.py`

- [ ] **Step 1: Write failing tests**

In `tests/test_migration_manager.py` add:

```python
class SourceServerLookupTest(unittest.TestCase):
    def test_provided_server_id_skips_name_lookup(self):
        source_os = mock.Mock()
        server = mock.Mock()
        server.id = "srv-abc"
        source_os.get_server_detail.return_value = server
        manager = MigrationManager(source_os, None, ceph_utils=None)
        vm = VmTask(name="web-1", target_az="az1")
        vm.source_server_id = "srv-abc"

        found = manager._resolve_source_server(vm)

        self.assertEqual(found.id, "srv-abc")
        source_os.get_server_by_name.assert_not_called()
        source_os.get_server_detail.assert_called_once_with("srv-abc")
```

In `tests/test_job_manager.py`, update `test_new_job_contains_vm_task_for_each_row`
with a second row carrying `source_server_id`:

```python
def test_new_job_preserves_selected_server_id(self):
    job = self.manager.create_job(
        [
            MigrationRow(
                vm_name="web-1",
                target_az="az1",
                source_server_id="srv-abc",
            )
        ]
    )
    self.assertEqual(job.vms[0].source_server_id, "srv-abc")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_migration_manager.SourceServerLookupTest tests.test_job_manager.JobManagerTest -v`
Expected: AttributeError for `_resolve_source_server` and `source_server_id` is None.

- [ ] **Step 3: Implement**

In `job_manager.py`, change `create_job()`:

```python
        vms = [
            VmTask(
                name=row.vm_name,
                target_az=row.target_az,
                target_image=row.target_image,
                target_flavor=row.target_flavor,
                source_server_id=row.source_server_id,
            )
            for row in rows
        ]
```

In `migration_manager.py`:

```python
    def _resolve_source_server(self, vm: VmTask):
        if vm.source_server_id:
            server = self.source_os.get_server_detail(vm.source_server_id)
            if not server:
                raise RuntimeError(f"源 VM {vm.source_server_id} 不存在")
            return server
        server = self.source_os.get_server_by_name(vm.name)
        if not server:
            raise RuntimeError(f"源 VM {vm.name} 不存在")
        vm.source_server_id = server.id
        return server
```

In `_migrate_vm_inner()`, replace:

```python
        source_server = self.source_os.get_server_by_name(vm.name)
        if not source_server:
            raise RuntimeError(f"源 VM {vm.name} 不存在")
        vm.source_server_id = source_server.id
```

with:

```python
        source_server = self._resolve_source_server(vm)
```

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest discover -s tests`
Expected: PASS.

---

### Task 3: OpenStack 源 VM 列表与目录补全

**Files:**
- Modify: `openstack_utils.py`
- Test: `tests/test_openstack_utils.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_openstack_utils.py`:

```python
class FakeServer:
    def __init__(self, server_id, name, status, az):
        self.id = server_id
        self.name = name
        self.status = status
        self._az = az

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "OS-EXT-AZ:availability_zone": self._az,
            "flavor": {"id": "flavor-1"},
            "image": {"id": "image-1"},
        }


class FakeFlavor:
    def __init__(self):
        self.id = "flavor-1"
        self.name = "m1.small"
        self.vcpus = 1
        self.ram = 2048
        self.disk = 20


class SourceServerListTest(unittest.TestCase):
    def test_summary_enriches_flavor_and_image(self):
        server = FakeServer("srv-1", "web-1", "ACTIVE", "az1")
        summary = OpenStackUtils._summarize_source_server(
            server,
            flavor_by_id={"flavor-1": FakeFlavor()},
            image_by_id={"image-1": "cirros"},
        )
        self.assertEqual(summary["server_id"], "srv-1")
        self.assertEqual(summary["availability_zone"], "az1")
        self.assertEqual(summary["flavor"]["name"], "m1.small")
        self.assertEqual(summary["image_name"], "cirros")

    def test_list_source_servers_passes_paging_and_search(self):
        conn = mock.Mock()
        conn.compute.servers.return_value = [
            FakeServer("srv-1", "web-1", "ACTIVE", "az1")
        ]
        conn.compute.flavors.return_value = [FakeFlavor()]
        conn.image.images.return_value = [
            mock.Mock(id="image-1", name="cirros")
        ]
        utils = OpenStackUtils(conn=conn)
        result = utils.list_source_servers(search="web", marker="old", limit=20)
        conn.compute.servers.assert_called_once_with(
            search="web", marker="old", limit=20
        )
        self.assertEqual(result[0]["server_id"], "srv-1")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_openstack_utils.SourceServerListTest -v`
Expected: AttributeError.

- [ ] **Step 3: Implement**

Add to `openstack_utils.py`:

```python
    @staticmethod
    def _summarize_source_server(server, flavor_by_id, image_by_id):
        data = server.to_dict()
        flavor_id = (data.get("flavor") or {}).get("id") or ""
        image_id = (data.get("image") or {}).get("id") or ""
        flavor = flavor_by_id.get(flavor_id)
        return {
            "server_id": str(getattr(server, "id", "") or ""),
            "name": str(getattr(server, "name", "") or data.get("name") or ""),
            "status": str(getattr(server, "status", "") or data.get("status") or ""),
            "availability_zone": str(
                data.get("OS-EXT-AZ:availability_zone")
                or getattr(server, "availability_zone", None)
                or ""
            ),
            "flavor": {
                "id": flavor_id,
                "name": getattr(flavor, "name", None) or flavor_id,
                "vcpus": int(getattr(flavor, "vcpus", 0) or 0),
                "ram": int(getattr(flavor, "ram", 0) or 0),
                "disk": int(getattr(flavor, "disk", 0) or 0),
            },
            "image_id": image_id,
            "image_name": image_by_id.get(image_id) or image_id,
        }

    def list_source_servers(
        self,
        search: str = "",
        limit: int = 100,
        marker: str | None = None,
    ) -> list[dict[str, Any]]:
        filters: dict[str, Any] = {"limit": int(limit)}
        if search:
            filters["search"] = search
        if marker:
            filters["marker"] = marker
        servers = list(self.conn.compute.servers(**filters))
        flavor_ids = {
            (server.to_dict().get("flavor") or {}).get("id")
            for server in servers
        }
        image_ids = {
            (server.to_dict().get("image") or {}).get("id")
            for server in servers
        }
        flavor_by_id = {
            flavor.id: flavor
            for flavor in self.conn.compute.flavors()
            if flavor.id in flavor_ids
        }
        image_by_id = {
            image.id: getattr(image, "name", "") or ""
            for image in self.conn.image.images()
            if image.id in image_ids
        }
        return [
            self._summarize_source_server(server, flavor_by_id, image_by_id)
            for server in servers
        ]
```

Note: Nova 实际字段为 `name` 精确过滤；openstacksdk 会把 `search` 映射到 Nova。
实现完成后若 SDK 不接受该关键字，将 filters 中的键改为 `name` 并在
`conn.compute.servers.assert_called_once_with(name="web", marker="old", limit=20)`
同步更新测试。

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest discover -s tests`
Expected: PASS.

---

### Task 4: 目标真实 AZ 查询

**Files:**
- Modify: `openstack_utils.py`
- Test: `tests/test_openstack_utils.py`

- [ ] **Step 1: Write failing test**

```python
from types import SimpleNamespace


class FakeZone:
    def __init__(self, name, state):
        self.zoneName = name
        self.zoneState = state


class AvailabilityZoneTest(unittest.TestCase):
    def test_availability_zones_are_normalised(self):
        conn = mock.Mock()
        conn.compute.availability_zones.return_value = [
            FakeZone("az1", {"available": True}),
            FakeZone("az2", {"available": False}),
        ]
        utils = OpenStackUtils(conn=conn)
        result = utils.list_availability_zones()
        self.assertEqual(
            result,
            [
                {"id": "az1", "name": "az1", "available": True},
                {"id": "az2", "name": "az2", "available": False},
            ],
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_openstack_utils.AvailabilityZoneTest -v`
Expected: AttributeError.

- [ ] **Step 3: Implement**

```python
    def list_availability_zones(self) -> list[dict[str, Any]]:
        zones = []
        try:
            zones = list(self.conn.compute.availability_zones())
        except (AttributeError, TypeError):
            zones = []
        normalized = []
        for zone in zones:
            if isinstance(zone, dict):
                raw_name = zone.get("zoneName") or zone.get("name") or ""
                state = zone.get("zoneState") or {}
            else:
                raw_name = (
                    getattr(zone, "zoneName", None)
                    or getattr(zone, "name", None)
                    or ""
                )
                state = getattr(zone, "zoneState", {}) or {}
            name = str(raw_name).strip()
            if not name:
                continue
            available = bool(
                state.get("available", True) if isinstance(state, dict) else True
            )
            normalized.append(
                {"id": name, "name": name, "available": available}
            )
        return normalized
```

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest tests.test_openstack_utils.AvailabilityZoneTest -v`
Expected: PASS.

---

### Task 5: API 支持 source-vms、catalog AZ、selected_rows 建任务

**Files:**
- Modify: `app.py`

说明：本项目没有 Flask 单元测试，任务通过 py_compile 与后续手工验收验证。

- [ ] **Step 1: 修改 import**

At top of `app.py`:

```python
from excel_parser import parse_rows, parse_selected_rows
```

- [ ] **Step 2: 修改 `_create_job_and_files`**

Replace the current Excel-only branch:

```python
    excel_file = request.files.get("excel_file")
    source_conf_file = request.files.get("source_ceph_conf_file")
    target_conf_file = request.files.get("target_ceph_conf_file")
    selected_rows_raw = request.form.get("selected_rows") or "[]"
    selected_rows = json.loads(selected_rows_raw)
    if not isinstance(selected_rows, list):
        raise ValueError("selected_rows 必须是数组")
    if not source_conf_file or not target_conf_file:
        raise ValueError("缺少源/目标 Ceph conf 文件")
    if not excel_file and not selected_rows:
        raise ValueError("缺少 excel_file 或 selected_rows")

    source_conf_path = os.path.join(job_dir, "source_ceph.conf")
    target_conf_path = os.path.join(job_dir, "target_ceph.conf")
    source_conf_file.save(source_conf_path)
    target_conf_file.save(target_conf_path)

    if excel_file:
        excel_path = os.path.join(job_dir, "migration.xlsx")
        excel_file.save(excel_path)
        rows = _read_excel_rows(excel_path)
    else:
        rows = parse_selected_rows(selected_rows)
```

The following `row_overrides` merge loop remains unchanged.

- [ ] **Step 3: 新增 `/api/source-vms`**

Add before `/api/source-vm-networks`:

```python
@app.post("/api/source-vms")
def api_source_vms():
    """List source-project VMs for checklist selection."""
    try:
        payload = request.get_json(force=True)
        auth_args = _auth_args_from_payload(payload)
        source_os = OpenStackUtils(auth_args)
        search = str(payload.get("search") or "").strip()
        try:
            limit = max(1, min(int(payload.get("limit") or 100), 500))
        except (TypeError, ValueError):
            limit = 100
        marker = str(payload.get("marker") or "").strip() or None
        servers = source_os.list_source_servers(
            search=search,
            limit=limit,
            marker=marker,
        )
        next_marker = servers[-1]["server_id"] if len(servers) == limit else None
        return jsonify(
            {
                "ok": True,
                "servers": servers,
                "next_marker": next_marker,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 查询源 VM 列表失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
```

- [ ] **Step 4: `/api/catalog` 增加 AZ**

In the existing `api_catalog()` handler:

```python
        availability_zones = target_os.list_availability_zones()
        return jsonify(
            {
                "ok": True,
                "images": images,
                "flavors": flavors,
                "networks": networks,
                "subnets": subnets,
                "availability_zones": availability_zones,
            }
        )
```

- [ ] **Step 5: 编译验证**

Run: `python3 -m py_compile app.py`
Expected: exit 0.

---

### Task 6: 前端源 VM 选择器数据加载

**Files:**
- Modify: `templates/index.html`

- [ ] **Step 1: state 增加字段**

```javascript
const state = {
    targetCatalog: { images: [], flavors: [], networks: [], subnets: [], availability_zones: [] },
    previewRows: [],
    sourceServers: [],
    sourceServersNextMarker: null,
    sourceServerLoading: false,
    selectedServerIds: new Set(),
    jobId: null,
    pollTimer: null
};
```

- [ ] **Step 2: 新增加载函数**

```javascript
async function loadSourceServers(reset) {
    if (state.sourceServerLoading) return;
    state.sourceServerLoading = true;
    const btn = $('#load-source-vms');
    if (btn) btn.disabled = true;
    try {
        const payload = {
            ...readAuth('source'),
            search: ($('#source-vm-search').value || '').trim(),
            marker: reset ? '' : (state.sourceServersNextMarker || '')
        };
        const data = await postJson('/api/source-vms', payload);
        if (reset) state.sourceServers = [];
        state.sourceServers = state.sourceServers.concat(data.servers || []);
        state.sourceServersNextMarker = data.next_marker || null;
        renderSourceServers();
        toast('已加载 ' + data.servers.length + ' 台源 VM', 'success');
    } catch (err) {
        toast('源 VM 列表加载失败：' + err.message, 'error');
    } finally {
        state.sourceServerLoading = false;
        if (btn) btn.disabled = false;
    }
}

function sourceRowChecked(serverId) {
    return state.selectedServerIds.has(serverId);
}

function toggleSourceServer(server) {
    if (sourceRowChecked(server.server_id)) {
        state.selectedServerIds.delete(server.server_id);
        removePreviewRowByServerId(server.server_id);
    } else {
        state.selectedServerIds.add(server.server_id);
        addSelectedSourceServer(server);
    }
    renderSourceServers();
}

function renderSourceServers() {
    const box = $('#source-server-list');
    if (!box) return;
    box.innerHTML = '';
    if (!state.sourceServers.length) {
        box.appendChild(text('div', 'empty', '暂无 VM，或按名称搜索无结果。'));
        return;
    }
    state.sourceServers.forEach(server => {
        const label = document.createElement('label');
        label.className = 'source-server-row';
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = sourceRowChecked(server.server_id);
        checkbox.onchange = () => toggleSourceServer(server);
        const name = text('span', 'source-name', server.name);
        const meta = text(
            'span',
            'source-meta mono',
            `${server.status || ''} · ${server.availability_zone || '未知AZ'} · ` +
            `${server.flavor ? server.flavor.vcpus + 'vCPU/' + server.flavor.ram + 'MB' : '未知规格'} · ${server.image_name || '未知镜像'}`
        );
        label.append(checkbox, name, meta);
        box.appendChild(label);
    });
    const moreBtn = document.createElement('button');
    moreBtn.type = 'button';
    moreBtn.className = 'btn btn-ghost btn-sm';
    moreBtn.textContent = '加载更多';
    moreBtn.disabled = !state.sourceServersNextMarker;
    moreBtn.onclick = () => loadSourceServers(false);
    box.appendChild(moreBtn);
    const hint = $('#selected-count');
    if (hint) hint.textContent = `已选 ${state.selectedServerIds.size} 台`;
}
```

- [ ] **Step 3: HTML 增加选择器容器**

Replace the whole Step 2 `checklist-toolbar` block with:

```html
            <div class="checklist-toolbar">
                <div>
                    <div class="form-group mb-0">
                        <label class="form-label">源 VM 选择（当前源项目）</label>
                        <div class="d-flex gap-2">
                            <input class="form-control mono" id="source-vm-search" placeholder="按名称搜索">
                            <button type="button" class="btn btn-accent" id="load-source-vms">从源项目加载 VM</button>
                        </div>
                    </div>
                </div>
                <div>
                    <div class="form-group mb-0">
                        <label class="form-label">全局目标镜像</label>
                        <select class="form-control" id="global-image"><option value="">先加载目标目录</option></select>
                    </div>
                </div>
                <div class="d-flex align-items-end gap-2">
                    <span class="text-muted small mb-2" id="selected-count">已选 0 台</span>
                    <button type="button" class="btn btn-ghost" id="start-migration">开始迁移</button>
                </div>
            </div>
            <div id="source-server-list" class="source-server-list"></div>
            <details class="advanced-excel">
                <summary>高级：Excel 批量导入</summary>
                <div class="excel-body">
                    <div class="form-row align-items-end">
                        <div class="form-group flex-grow-1">
                            <label class="form-label">Excel 迁移清单（vm_name / target_az）</label>
                            <input type="file" class="form-control-file" id="excel_file" name="excel_file">
                        </div>
                        <button type="button" class="btn btn-ghost" id="parse-excel">解析清单</button>
                    </div>
                </div>
            </details>
```

同时补充 CSS：

```css
.source-server-row {
    display: grid;
    grid-template-columns: 24px 160px 1fr;
    align-items: center;
    gap: 12px;
    padding: 8px 10px;
    border: 1px solid var(--line);
    border-radius: 8px;
    margin-bottom: 6px;
    cursor: pointer;
}
.source-server-row:hover { border-color: var(--accent); }
.source-server-list { max-height: 360px; overflow: auto; margin: 10px 0; }
.advanced-excel { margin-top: 12px; }
.advanced-excel summary { color: var(--muted); cursor: pointer; font-size: 13px; }
.excel-body { margin-top: 10px; }
```

- [ ] **Step 4: 手工语法校验**

Run: 用 Node 解析 `<script>`（与前面验证一致），Expected: `inline js ok: 1`。

---

### Task 7: 前端目标 AZ 下拉、去重/移除清单

**Files:**
- Modify: `templates/index.html`

- [ ] **Step 1: 渲染 AZ 下拉**

In `renderPreview()`, replace the AZ text input construction with:

```javascript
const azField = fieldBox('目标 AZ');
const azSelect = document.createElement('select');
azSelect.className = 'form-control';
azSelect.appendChild(new Option('选择目标 AZ', ''));
(state.targetCatalog.availability_zones || []).forEach(az =>
    azSelect.appendChild(new Option(az.name, az.id))
);
if (row.target_az) azSelect.value = row.target_az;
azSelect.dataset.row = index;
azSelect.dataset.field = 'target_az';
azSelect.addEventListener('change', () => {
    row.target_az = azSelect.value;
    refreshChecklistUI();
});
azField.appendChild(azSelect);
```

- [ ] **Step 2: 勾选来源 server 去重**

Add:

```javascript
function removePreviewRowByServerId(serverId) {
    state.previewRows = state.previewRows.filter(row => row.server_id !== serverId);
    renderPreview();
}

function addSelectedSourceServer(server) {
    if (state.previewRows.some(row => row.server_id === server.server_id)) {
        return;
    }
    state.previewRows.push({
        server_id: server.server_id,
        vm_name: server.name,
        target_az: '',
        target_image: $('#global-image').value,
        target_flavor: '',
        source_networks: [],
        source_flavor: server.flavor,
        source_image: server.image_name,
        source_az: server.availability_zone
    });
    renderPreview();
}
```

- [ ] **Step 3: 校验 Excel AZ 是否真实存在**

`networkIssues()` 增加一段：

```javascript
    const zones = state.targetCatalog.availability_zones || [];
    if (!row.target_az || !zones.some(zone => zone.id === row.target_az)) {
        issues.push({
            rowIndex,
            nicIndex: null,
            vm: row.vm_name,
            message: '目标 AZ 不存在或未选择'
        });
    }
```

- [ ] **Step 4: JS 语法与行为手工验证**

Run: `node` 内联 script 解析 Expected: PASS。

---

### Task 8: 前端提交 selected_rows、加载更多与来源标记

**Files:**
- Modify: `templates/index.html`

- [ ] **Step 1: `startMigration` 分支提交**

Replace the Excel-only validation and FormData append block with:

```javascript
    const excel = $('#excel_file').files[0];
    if (!state.previewRows.length) {
        toast('请先勾选源 VM，或展开高级 Excel 导入并解析清单', 'error');
        return;
    }
```

After `collectPreviewValues()`:

```javascript
    const selectedRows = state.previewRows.map(row => ({
        server_id: row.server_id || '',
        vm_name: row.vm_name,
        target_az: row.target_az,
        target_image: row.target_image,
        target_flavor: row.target_flavor || ''
    }));
```

And in FormData append:

```javascript
    if (excel) {
        data.append('excel_file', excel);
    } else {
        data.append('selected_rows', JSON.stringify(selectedRows));
    }
```

Remove the unconditional `data.append('excel_file', excel)` line.

- [ ] **Step 2: `loadSourceServers` 搜索时重置分页**

```javascript
$('#source-vm-search').addEventListener('keydown', event => {
    if (event.key === 'Enter') {
        event.preventDefault();
        state.sourceServers = [];
        state.sourceServersNextMarker = null;
        loadSourceServers(true);
    }
});
```

- [ ] **Step 3: 卡片来源标记**

在 `renderPreview()` 的 `plan-sub` 中追加：

```javascript
            const sourceLabel = row.server_id ? '列表选择' : 'Excel 导入';
            nameWrap.appendChild(text(
                'div',
                'plan-sub',
                `${sourceLabel} · 源 server ${row.server_id || '未获取'} · ` +
                `源网卡 ${(row.source_networks || []).length}`
            ));
```

- [ ] **Step 4: DOMContentLoaded 注册**

```javascript
    $('#load-source-vms').addEventListener('click', () => {
        state.sourceServers = [];
        state.sourceServersNextMarker = null;
        loadSourceServers(true);
    });
```

- [ ] **Step 5: JS 语法与全量回归**

Run: Node inline JS check + `python3 -m unittest discover -s tests`
Expected: JS PASS，74+ Python 测试 PASS。

---

### Task 9: 全量回归、文档与上线同步

**Files:**
- `README.md`

- [ ] **Step 1: 回归**

Run:

```bash
python3 -m unittest discover -s tests
python3 -m py_compile app.py openstack_utils.py excel_parser.py migration_manager.py job_manager.py state_machine.py
```

Expected: 全部 PASS，编译 exit 0。

- [ ] **Step 2: 更新 README**

在功能/API 段落补充：

```markdown
- 支持直接加载源项目 VM 列表勾选迁移，Excel 作为高级批量导入入口；
- `POST /api/source-vms`：按源项目列出 VM（search/limit/marker）；
- `POST /api/catalog` 返回目标 `availability_zones`。
```

- [ ] **Step 3: 同步 ConfigMap 并重启（集群侧执行）**

```bash
kubectl -n migrate create configmap migrate-vm-bin \
  --from-file=app.py --from-file=openstack_utils.py --from-file=config.py \
  --from-file=ceph_utils.py --from-file=migration_manager.py \
  --from-file=job_manager.py --from-file=graceful_shutdown.py \
  --from-file=state_machine.py --from-file=excel_parser.py \
  --from-file=migration_planner.py --from-file=repro_create.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n migrate create configmap migrate-vm-html \
  --from-file=index.html=templates/index.html \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n migrate rollout restart deployment/openstack-vm-migration-deployment
```

---

## 自检

- Spec 覆盖：源列表加载、勾选/搜索/分页、去重、server_id 身份、AZ 下拉、
  Excel 折叠、selected_rows 提交、测试与上线均已对应到 Task 1-9。
- 无占位符：所有新增 Python 方法都有测试与代码；前端 JS 关键函数给出完整实现，
  HTML 局部改动给出可定位代码。
- 类型一致性：`source_server_id` 在 `MigrationRow`、`VmTask`、`selected_rows`
  中保持一致；`availability_zones` 字段在 catalog 与前端 state 中同名。
- 当前目录不是 git 仓库，任务内不包含 commit 步骤。
