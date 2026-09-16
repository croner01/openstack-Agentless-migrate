"""环境变量读取：缺失或非法一律回退默认值。

原先各处是 ``int(os.environ.get("X") or 5)``，一旦运维把值写成 "5 " 以外的
东西（常见：误填 "10m"、带单位、粘贴进换行），导入期或后台线程就直接
ValueError；有的位置还被拆成两处各写一遍，容错行为并不一致。
"""
from __future__ import annotations

import os
from typing import Mapping


def _source(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def env_str(
    name: str, default: str = "", *, environ: Mapping[str, str] | None = None
) -> str:
    """取字符串；缺失取默认值，前后空白一律裁掉。"""
    raw = _source(environ).get(name)
    if raw is None:
        return default
    return str(raw).strip()


def env_int(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """取整数；非数字/空值回退默认值，可再按 minimum 兜底。"""
    raw = env_str(name, "", environ=environ)
    try:
        value = int(raw) if raw else int(default)
    except (TypeError, ValueError):
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    return value


def env_float(
    name: str,
    default: float,
    *,
    minimum: float | None = None,
    environ: Mapping[str, str] | None = None,
) -> float:
    """取浮点数；非数字/空值回退默认值，可再按 minimum 兜底。"""
    raw = env_str(name, "", environ=environ)
    try:
        value = float(raw) if raw else float(default)
    except (TypeError, ValueError):
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    return value
