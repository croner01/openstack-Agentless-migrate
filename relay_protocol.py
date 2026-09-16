"""中转机数据面的共享协议：帧编解码、握手与注册令牌。

平台与 relay agent 共用本模块，模块内不做任何 I/O。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import struct
import zlib
from typing import IO

MAGIC = b"RLY1"
HOLE_MAGIC = b"RLYH"
HEADER = struct.Struct("!4sQQI")  # magic, offset, length, crc32
HEADER_SIZE = HEADER.size
DEFAULT_CHUNK = 4 * 1024 * 1024
MAX_CHUNK = 8 * 1024 * 1024
HANDSHAKE_LIMIT = 4096
SPARSE_MIN_AGENT_VERSION = (1, 1, 0)


class ProtocolError(ValueError):
    """帧格式非法、校验失败或握手内容非法。"""


def crc32(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_chunk(offset: int, payload: bytes) -> bytes:
    """把一个数据块编码成帧。"""
    if not payload:
        raise ProtocolError("empty payload")
    if len(payload) > MAX_CHUNK:
        raise ProtocolError("payload too large")
    return HEADER.pack(MAGIC, offset, len(payload), crc32(payload)) + payload


def encode_hole(offset: int, length: int) -> bytes:
    """把"源端该区间全是零"编码成无载荷帧。"""
    if length <= 0:
        raise ProtocolError("empty hole")
    if length > MAX_CHUNK:
        raise ProtocolError("hole too large")
    return HEADER.pack(HOLE_MAGIC, offset, length, 0)


def read_frame(stream: IO[bytes]) -> tuple[str, int, int, bytes]:
    """读取一帧，返回 (kind, offset, length, payload)。

    kind 为 "data" 或 "hole"；流结束时返回 ("eof", -1, 0, b"")。
    老 agent 不认识 HOLE_MAGIC，因此只有确认两端都支持新协议后才会下发空洞帧。
    """
    header = stream.read(HEADER_SIZE)
    if not header:
        return "eof", -1, 0, b""
    if len(header) < HEADER_SIZE:
        raise ProtocolError("truncated header")
    magic, offset, length, checksum = HEADER.unpack(header)
    if magic == HOLE_MAGIC:
        if 0 < length <= MAX_CHUNK:
            return "hole", offset, length, b""
        raise ProtocolError("bad hole length")
    if magic != MAGIC:
        raise ProtocolError("bad magic")
    if length == 0 or length > MAX_CHUNK:
        raise ProtocolError("bad length")
    payload = stream.read(length)
    if len(payload) != length:
        raise ProtocolError("truncated payload")
    if crc32(payload) != checksum:
        raise ProtocolError("crc mismatch")
    return "data", offset, length, payload


def read_chunk(stream: IO[bytes]) -> tuple[int, bytes]:
    """从二进制流读取一帧，流结束时返回 (-1, b"")。"""
    kind, offset, _length, payload = read_frame(stream)
    if kind == "eof":
        return -1, b""
    if kind != "data":
        raise ProtocolError("unexpected hole frame")
    return offset, payload


def agent_supports_sparse(version: str) -> bool:
    """agent 版本是否支持空洞帧；解析失败一律视为不支持。

    比较按数字段进行（1.10.0 > 1.9.0），不能按字符串比。
    """
    parts = str(version or "").split(".")
    try:
        numbers = tuple(int(part) for part in parts)
    except ValueError:
        return False
    padded = numbers + (0,) * max(0, 3 - len(numbers))
    return padded[:3] >= SPARSE_MIN_AGENT_VERSION


def encode_handshake(ticket: str, offset: int, length: int) -> bytes:
    """编码一行握手 JSON，用于声明本次传输的票据与区间。"""
    body = json.dumps({"ticket": ticket, "offset": offset, "length": length})
    return body.encode("utf-8") + b"\n"


def decode_handshake(line: bytes) -> dict[str, object]:
    """解析握手行，字段缺失或类型非法时抛 ProtocolError。"""
    try:
        body = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError("malformed handshake") from exc
    if not isinstance(body, dict):
        raise ProtocolError("handshake must be an object")
    ticket = body.get("ticket")
    offset = body.get("offset")
    length = body.get("length")
    if not isinstance(ticket, str) or not ticket:
        raise ProtocolError("handshake ticket missing")
    if not isinstance(offset, int) or offset < 0:
        raise ProtocolError("handshake offset invalid")
    if not isinstance(length, int) or length < 0:
        raise ProtocolError("handshake length invalid")
    return {"ticket": ticket, "offset": offset, "length": length}


def issue_token(secret: bytes, job_id: str, role: str, *, now: float, ttl: int) -> str:
    """签发绑定 job 与角色的注册令牌。"""
    expires_at = int(now) + int(ttl)
    body = f"{job_id}|{role}|{expires_at}"
    signature = hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{body}|{signature}"


def verify_token(secret: bytes, token: str, *, now: float) -> tuple[str, str]:
    """校验令牌并返回 (job_id, role)。"""
    parts = token.split("|")
    if len(parts) != 4:
        raise ProtocolError("malformed token")
    job_id, role, expires_raw, signature = parts
    if not job_id or role not in {"source", "target"}:
        raise ProtocolError("malformed token")
    body = f"{job_id}|{role}|{expires_raw}"
    expected = hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ProtocolError("bad signature")
    try:
        expires_at = int(expires_raw)
    except ValueError as exc:
        raise ProtocolError("malformed token") from exc
    if expires_at < int(now):
        raise ProtocolError("token expired")
    return job_id, role


NODE_TOKEN_ROLES = {"source", "target"}


def issue_node_token(
    secret: bytes,
    *,
    node_id: str,
    role: str,
    tenant_key: str,
    az: str,
) -> str:
    """签发节点级长期令牌，不设过期时间；轮换靠更新节点记录实现。"""
    if role not in NODE_TOKEN_ROLES:
        raise ProtocolError(f"unknown role: {role!r}")
    payload = {
        "node_id": node_id,
        "role": role,
        "tenant_key": tenant_key,
        "az": az,
    }
    body = (
        base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        .decode("ascii")
        .rstrip("=")
    )
    signature = hmac.new(secret, body.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{body}.{signature}"


def verify_node_token(secret: bytes, token: str) -> dict[str, str]:
    """校验签名并返回 payload；任何结构或签名问题都抛 ProtocolError。"""
    body, _, signature = (token or "").partition(".")
    if not body or not signature:
        raise ProtocolError("malformed node token")
    expected = hmac.new(secret, body.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ProtocolError("bad node token signature")
    padded = body + "=" * (-len(body) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProtocolError("malformed node token payload") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("node token payload must be an object")
    for field in ("node_id", "role", "tenant_key", "az"):
        if not str(payload.get(field) or ""):
            raise ProtocolError(f"node token missing {field}")
    if payload["role"] not in NODE_TOKEN_ROLES:
        raise ProtocolError("node token role invalid")
    return {key: str(payload[key]) for key in ("node_id", "role", "tenant_key", "az")}
