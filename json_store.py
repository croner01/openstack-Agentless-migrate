"""通用 JSON 持久化原语：原子落盘 + dataclass 记录装载。

平台里原本有 5 处几乎逐字重复的存储实现（节点清单、租约、台账、池参数、
云凭据），各自写了一遍 "mkstemp → chmod 0600 → os.replace" 与
"按 dataclass 字段过滤未知键再构造"。集中到这里有两个好处：

* 落盘语义只有一份（权限、原子性、容错），不会某个文件忘了 0600；
* 旧文件里多出来的字段统一按"忽略未知键"处理，升级不回滚数据。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")

#: 这些文件里可能带令牌/密码密文，默认只给属主读写。
DEFAULT_MODE = 0o600


def read_json_object(path: str | os.PathLike[str]) -> dict[str, Any]:
    """读一个顶层为对象的 JSON 文件；文件不存在或为空则返回空字典。"""
    target = Path(path)
    if not target.exists():
        return {}
    raw = json.loads(target.read_text(encoding="utf-8") or "{}")
    if not isinstance(raw, dict):
        raise ValueError(f"{target} 顶层必须是 JSON 对象")
    return raw


def atomic_write_json(
    path: str | os.PathLike[str],
    payload: Any,
    *,
    mode: int = DEFAULT_MODE,
    prefix: str = ".json-store-",
    indent: int | None = None,
) -> None:
    """原子写 JSON：同目录临时文件 → fsync → chmod → os.replace。

    中途失败会删掉临时文件，绝不留下半个文件覆盖原数据。
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_path = tempfile.mkstemp(
        dir=str(target.parent), prefix=prefix, suffix=".tmp"
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=indent)
            stream.flush()
            # 台账/租约是"资源不堆积"的依据，必须落稳了再替换。
            os.fsync(stream.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, target)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def known_field_names(cls: type) -> set[str]:
    """dataclass 的字段名集合；非 dataclass 抛 TypeError。"""
    if not is_dataclass(cls):
        raise TypeError(f"{cls!r} 不是 dataclass")
    return {field.name for field in fields(cls)}


def dataclass_from_mapping(cls: type[T], item: Mapping[str, Any]) -> T:
    """过滤掉 dataclass 里不存在的键后构造实例。

    旧版本文件多出来的字段直接忽略，避免升级/回滚时整个文件读不出来。
    """
    known = known_field_names(cls)
    payload = {key: value for key, value in item.items() if key in known}
    return cls(**payload)  # type: ignore[call-arg]


def load_dataclass_records(
    path: str | os.PathLike[str], *, key: str, cls: type[T]
) -> list[T]:
    """读取形如 ``{"<key>": [{...}]}`` 的记录列表并转成 dataclass。

    单条记录损坏时跳过它而不是整体失败：清单读不出来比少一条更糟。
    """
    raw = read_json_object(path)
    items = raw.get(key) or []
    if not isinstance(items, list):
        raise ValueError(f"{path} 的 {key!r} 必须是数组")
    records: list[T] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        try:
            records.append(dataclass_from_mapping(cls, item))
        except (TypeError, ValueError) as exc:
            # 单条记录缺字段/字段类型不对时跳过它，而不是让整个清单读不出来。
            logging.warning(
                "[MIGRATION] 跳过损坏的记录 %s（%s）: %s", path, cls.__name__, exc
            )
            continue
    return records


def dump_dataclass_records(records: list[Any], *, key: str) -> dict[str, Any]:
    """把 dataclass 列表包成可落盘的 ``{"<key>": [...]}``。"""
    from dataclasses import asdict

    return {key: [asdict(record) for record in records]}
