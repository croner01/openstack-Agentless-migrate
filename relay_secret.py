"""平台级 relay 签名密钥：进程重启后必须保持不变。

密钥来源优先级：环境变量 MIGRATION_RELAY_SECRET > 磁盘文件 > 新建并落盘。
密钥必须持久化，否则重启后所有常驻节点令牌立即失效。
"""
from __future__ import annotations

import base64
import binascii
import os
from pathlib import Path
from typing import Mapping

SECRET_BYTES = 32


class SecretError(RuntimeError):
    """密钥缺失或格式非法。"""


def parse_secret(raw: str, *, source: str) -> bytes:
    """解析 base64 / hex / 原始 32 字节密钥。"""
    value = (raw or "").strip()
    if not value:
        raise SecretError(f"{source} 为空")
    for decode in (
        lambda text: base64.b64decode(text, validate=True),
        binascii.unhexlify,
    ):
        try:
            key = decode(value)
        except (binascii.Error, ValueError):
            continue
        if len(key) == SECRET_BYTES:
            return key
    raw_bytes = value.encode("utf-8")
    if len(raw_bytes) == SECRET_BYTES:
        return raw_bytes
    raise SecretError(f"{source} 必须是 32 字节（base64 / hex / 原始串）")


def load_or_create_secret(
    path: str | os.PathLike[str],
    *,
    env_var: str = "MIGRATION_RELAY_SECRET",
    env: Mapping[str, str] | None = None,
) -> bytes:
    """返回稳定的签名密钥；文件不存在时生成并以 0600 落盘。"""
    environment = os.environ if env is None else env
    raw = environment.get(env_var, "")
    if str(raw).strip():
        return parse_secret(str(raw), source=env_var)

    secret_path = Path(path)
    if secret_path.exists():
        return parse_secret(
            secret_path.read_text(encoding="utf-8"), source=str(secret_path)
        )

    secret = os.urandom(SECRET_BYTES)
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            secret_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
    except FileExistsError:
        # 并发启动时另一个进程刚写好：读它即可，不能因为竞态让本进程起不来。
        return parse_secret(
            secret_path.read_text(encoding="utf-8"), source=str(secret_path)
        )
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(base64.b64encode(secret).decode("ascii"))
        stream.flush()
        # 密钥必须落稳：掉电留下空文件会让重启后所有节点令牌失效。
        os.fsync(stream.fileno())
    return secret
