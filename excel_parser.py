from dataclasses import dataclass
from typing import Any


@dataclass
class MigrationRow:
    vm_name: str
    target_az: str
    source_server_id: str | None = None
    target_image: str | None = None
    target_flavor: str | None = None
    mode: str = "full"
    source_networks: list = None  # [{network, ip, cidr}] 供 UI 展示/规划

    def __post_init__(self):
        if self.source_networks is None:
            self.source_networks = []


REQUIRED_COLUMNS = {"vm_name", "target_az"}
OPTIONAL_COLUMNS = {"target_image", "target_flavor", "mode"}
VALID_MODES = {"full", "incremental"}


def parse_mode(raw: Any) -> str:
    text = str(raw or "full").strip().lower()
    if text not in VALID_MODES:
        raise ValueError(f"不支持的迁移模式: {raw}（可选 full/incremental）")
    return text


def parse_rows(records: list[dict[str, Any]]) -> list[MigrationRow]:
    """Parse Excel rows (already converted to dicts) into MigrationRow objects.

    The caller is responsible for converting an uploaded workbook into a list
    of dicts, e.g. ``df.to_dict(orient="records")``.
    """
    if not records:
        return []

    first_keys = set(records[0].keys())
    missing = REQUIRED_COLUMNS - first_keys
    if missing:
        raise ValueError(f"Excel 缺少列: {', '.join(sorted(missing))}")

    rows: list[MigrationRow] = []
    for index, record in enumerate(records, start=2):
        vm_name = str(record.get("vm_name") or "").strip()
        target_az = str(record.get("target_az") or "").strip()
        if not vm_name or not target_az:
            raise ValueError(f"第 {index} 行 vm_name/target_az 不能为空")
        rows.append(
            MigrationRow(
                vm_name=vm_name,
                target_az=target_az,
                target_image=str(record.get("target_image") or "").strip() or None,
                target_flavor=str(record.get("target_flavor") or "").strip() or None,
                mode=parse_mode(record.get("mode")),
            )
        )
    return rows


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
                mode=parse_mode(record.get("mode")),
            )
        )
    return rows
