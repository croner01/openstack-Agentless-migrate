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
    #: 迁移收尾后是否把目标机开起来（默认开机，与历史行为一致）。
    start_target: bool = True
    source_networks: list = None  # [{network, ip, cidr}] 供 UI 展示/规划
    #: 逐 VM 数据通道：rbd / relay，空 = 跟随作业级方案。
    channel: str = ""
    #: 源网卡要落到哪个目标网络（名称或 ID）；空 = 由页面自动映射。
    target_network: str = ""
    #: 逐 VM 目标卷类型；空 = 跟随作业级默认。
    target_volume_type: str = ""
    #: 逐 VM 单卷限速（MiB/s，0 = 不限）。
    rate_limit_mb_s: float = 0.0
    #: Excel 行号（从 2 开始，含表头），用于逐行报错定位。
    row_number: int = 0

    def __post_init__(self):
        if self.source_networks is None:
            self.source_networks = []


REQUIRED_COLUMNS = {"vm_name", "target_az"}
OPTIONAL_COLUMNS = {
    "target_image",
    "target_flavor",
    "mode",
    "start_target",
    "power_on",
    "channel",
    "target_network",
    "target_volume_type",
    "rate_limit_mb_s",
}
VALID_MODES = {"full", "incremental"}

#: 「通道」列的别名：中文/大小写都收，避免用户改一格表头就被判非法。
_RBD_LITERALS = {"rbd", "direct", "直连", "rbd直连", "rbd 直连"}
_RELAY_LITERALS = {"relay", "中转机", "中转机通道", "中转"}


def parse_channel(raw: Any) -> str:
    """解析「通道」列：空 = 跟随作业级方案；非法值报错。"""
    text = str(raw or "").strip().lower().replace(" ", "")
    if not text:
        return ""
    if text in {item.replace(" ", "") for item in _RBD_LITERALS}:
        return "rbd"
    if text in {item.replace(" ", "") for item in _RELAY_LITERALS}:
        return "relay"
    raise ValueError(f"不支持的通道: {raw}（可选 rbd/直连、relay/中转机）")


def parse_rate_limit(raw: Any) -> float:
    """解析「单卷限速」列：空 = 0（不限），负数报错。"""
    if raw is None:
        return 0.0
    text = str(raw).strip()
    if not text:
        return 0.0
    try:
        value = float(text)
    except ValueError as exc:
        raise ValueError(f"单卷限速必须是数字（MiB/s）: {raw}") from exc
    if value < 0:
        raise ValueError(f"单卷限速不能为负数: {raw}")
    return value

#: 「迁移后开机」列/字段的真假字面量。中文与 yes/no 都收，避免用户改一行
#: 表头就被判非法；留空或缺列一律按"开机"处理，保证老清单继续可用。
_TRUE_LITERALS = {"true", "t", "yes", "y", "1", "on", "是", "开机", "开"}
_FALSE_LITERALS = {"false", "f", "no", "n", "0", "off", "否", "不开机", "关", "不开"}


def parse_mode(raw: Any) -> str:
    text = str(raw or "full").strip().lower()
    if text not in VALID_MODES:
        raise ValueError(f"不支持的迁移模式: {raw}（可选 full/incremental）")
    return text


def parse_start_target(raw: Any) -> bool:
    """解析「迁移后是否开机」：空值=开机（默认），非法值报错。"""
    if raw is None:
        return True
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if not text:
        return True
    if text in _TRUE_LITERALS:
        return True
    if text in _FALSE_LITERALS:
        return False
    raise ValueError(f"不支持的「迁移后开机」取值: {raw}（可选 是/否、开机/不开机）")


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
        try:
            rows.append(_row_from_record(record, row_number=index))
        except ValueError as exc:
            # 带上行号，否则用户不知道是哪一行的单元格写错了。
            raise ValueError(f"第 {index} 行：{exc}") from exc
    return rows


def parse_rows_tolerant(
    records: list[dict[str, Any]],
) -> tuple[list[MigrationRow], list[dict[str, Any]]]:
    """逐行解析：坏行只记错误并跳过，返回 (可用行, 错误列表)。

    批量迁移时"一行写错就整份清单不能提交"代价太大；预览/校验报告用这个
    版本，让用户按行号改完再提交。
    """
    if not records:
        return [], []
    first_keys = set(records[0].keys())
    missing = REQUIRED_COLUMNS - first_keys
    if missing:
        raise ValueError(f"Excel 缺少列: {', '.join(sorted(missing))}")

    rows: list[MigrationRow] = []
    errors: list[dict[str, Any]] = []
    seen: dict[str, int] = {}
    for index, record in enumerate(records, start=2):
        try:
            row = _row_from_record(record, row_number=index)
        except ValueError as exc:
            errors.append({"row": index, "message": str(exc)})
            continue
        previous = seen.get(row.vm_name)
        if previous is not None:
            errors.append(
                {"row": index, "message": f"与第 {previous} 行重复的 VM 名称：{row.vm_name}"}
            )
            continue
        seen[row.vm_name] = index
        rows.append(row)
    return rows, errors


def _row_from_record(record: dict[str, Any], *, row_number: int) -> MigrationRow:
    """把一行记录转成 MigrationRow；字段非法时抛 ValueError（带可读原因）。"""
    vm_name = str(record.get("vm_name") or "").strip()
    target_az = str(record.get("target_az") or "").strip()
    if not vm_name or not target_az:
        raise ValueError("vm_name/target_az 不能为空")
    # 「迁移后开机」支持 start_target 与 power_on 两种表头，前者优先。
    raw_power = record.get("start_target")
    if raw_power is None or str(raw_power).strip() == "":
        raw_power = record.get("power_on")
    return MigrationRow(
        vm_name=vm_name,
        target_az=target_az,
        target_image=str(record.get("target_image") or "").strip() or None,
        target_flavor=str(record.get("target_flavor") or "").strip() or None,
        mode=parse_mode(record.get("mode")),
        start_target=parse_start_target(raw_power),
        channel=parse_channel(record.get("channel")),
        target_network=str(record.get("target_network") or "").strip(),
        target_volume_type=str(record.get("target_volume_type") or "").strip(),
        rate_limit_mb_s=parse_rate_limit(record.get("rate_limit_mb_s")),
        row_number=row_number,
    )


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
                start_target=parse_start_target(record.get("start_target")),
                channel=parse_channel(record.get("channel")),
                target_network=str(record.get("target_network") or "").strip(),
                target_volume_type=str(record.get("target_volume_type") or "").strip(),
                rate_limit_mb_s=parse_rate_limit(record.get("rate_limit_mb_s")),
                row_number=index,
            )
        )
    return rows
