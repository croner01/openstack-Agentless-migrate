"""块流传输：源端发送、目标端接收，含限速与进度回调。

两端都按绝对偏移读写，因此中断后从任意已确认偏移重传都是幂等的。
"""
from __future__ import annotations

import hashlib
import os
import re
import socket
import stat
import struct
import time
from dataclasses import dataclass
from threading import Event
from typing import Callable

try:  # 仅 Linux 需要：块设备容量必须用 ioctl 取
    import fcntl
except ImportError:  # pragma: no cover - 中转机必然是 Linux
    fcntl = None

BLKGETSIZE64 = 0x80081272
BLKZEROOUT = 0x1277


@dataclass
class TransferStats:
    """按"是否真的传了数据"记账：数据字节与跳过字节。

    源端统计"发出去"的字节，目标端统计"收下来"的字节；空洞跳过的字节
    两边都计入 skipped_bytes，因此 processed = data + skipped 在两端一致。
    """

    data_bytes: int = 0
    skipped_bytes: int = 0

    @property
    def processed_bytes(self) -> int:
        return self.data_bytes + self.skipped_bytes


def is_zero_block(payload: bytes, zeros: bytes) -> bool:
    """判断数据块是否全零：memcmp 实现，4 MiB 块的开销可以忽略。"""
    if len(payload) == len(zeros):
        return payload == zeros
    return payload == zeros[: len(payload)]


def device_size(path: str) -> int:
    """返回设备/文件的字节长度。

    块设备（/dev/vdb 之类）的 ``st_size`` 是 0，直接 stat 会把整盘当成空设备，
    必须用 BLKGETSIZE64 取真实容量；普通文件仍走 stat。
    """
    info = os.stat(path)
    if stat.S_ISBLK(info.st_mode) and fcntl is not None:
        with open(path, "rb") as handle:
            raw = fcntl.ioctl(handle.fileno(), BLKGETSIZE64, b"\0" * 8)
        return int(struct.unpack("<Q", raw)[0])
    return int(info.st_size)

from relay_protocol import (
    DEFAULT_CHUNK,
    HANDSHAKE_LIMIT,
    ProtocolError,
    decode_handshake,
    encode_chunk,
    encode_hole,
    encode_handshake,
    read_chunk,
    read_frame,
)


class TransferError(RuntimeError):
    """传输过程中的协议或对端错误。"""


MIN_THROTTLE_SLEEP = 0.0005


#: 「限速时段」形如 ``22:00-06:00``，可追加重定位偏移 ``@+08:00`` 指定时区；
#: 不写偏移时按平台本地时区解释（容器里通常是 UTC，跨时区部署请显式写偏移）。
RATE_WINDOW_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*(?:@([+-])(\d{1,2}):(\d{2}))?\s*$")


def parse_rate_window(window: str) -> tuple[int, int, int] | None:
    """解析限速时段；返回 (起始分钟, 结束分钟, 时区偏移秒)。非法返回 None。"""
    match = RATE_WINDOW_RE.match(str(window or ""))
    if match is None:
        return None
    start_h, start_m, end_h, end_m, sign, off_h, off_m = match.groups()
    start = int(start_h) * 60 + int(start_m)
    end = int(end_h) * 60 + int(end_m)
    if start > 24 * 60 or end > 24 * 60:
        return None
    offset = 0
    if sign:
        offset = (int(off_h) * 60 + int(off_m)) * 60
        if sign == "-":
            offset = -offset
    return start, end, offset


def _minute_in_window(minute: int, start: int, end: int) -> bool:
    if start == end:
        return False
    if start < end:
        return start <= minute < end
    # 跨零点：22:00-06:00 = [22:00, 24:00) ∪ [00:00, 06:00)
    return minute >= start or minute < end


def rate_schedule(
    base_rate: float | None,
    offpeak_rate: float | None,
    window: str,
    *,
    now: float | None = None,
    hours: float = 48.0,
) -> list[dict[str, float]]:
    """生成未来 ``hours`` 小时的 (绝对时刻, 速率) 切换表。

    绝对时刻在平台侧算好再下发给中转机 agent：不同时区的节点不会因为"本地
    时间不同"而在错误的时段限速。没有任何限速时返回空表（= 不限速）。
    """
    parsed = parse_rate_window(window)
    if parsed is None:
        return []
    base = float(base_rate) if base_rate else None
    offpeak = float(offpeak_rate) if offpeak_rate else None
    if base is None and offpeak is None:
        return []
    start, end, offset = parsed
    current = float(now if now is not None else time.time())
    local_now = current + offset
    day_start = local_now - (local_now % 86400)

    transitions: list[float] = []
    for day in range(-1, int(hours // 24) + 2):
        base_day = day_start + day * 86400
        for minute in (start, end):
            moment = base_day + minute * 60 - offset
            if current - 60 <= moment <= current + hours * 3600:
                transitions.append(moment)
    transitions = sorted(set(transitions))

    schedule: list[dict[str, float]] = [{"at": 0.0, "rate": scheduled_rate_from(base, offpeak, start, end, offset, current)}]
    for moment in transitions:
        if moment <= current:
            continue
        schedule.append({
            "at": moment,
            "rate": scheduled_rate_from(base, offpeak, start, end, offset, moment + 1),
        })
    return schedule


def scheduled_rate_from(
    base: float | None,
    offpeak: float | None,
    start: int,
    end: int,
    offset: int,
    moment: float,
) -> float:
    """某一绝对时刻应有的速率：落在限速时段内用 offpeak，否则用 base。"""
    minute = int(((moment + offset) % 86400) // 60)
    if _minute_in_window(minute, start, end):
        return float(offpeak) if offpeak else float(base or 0.0)
    return float(base or 0.0)


def scheduled_rate(schedule: list[dict[str, float]] | None, now: float) -> float | None:
    """按切换表取当前速率；表为空返回 None（不限速）。"""
    if not schedule:
        return None
    rate = float(schedule[0].get("rate") or 0.0)
    for item in schedule:
        if float(item.get("at") or 0.0) <= now:
            rate = float(item.get("rate") or 0.0)
        else:
            break
    return rate or None


def schedule_rate_provider(schedule: list[dict[str, float]] | None, clock=None):
    """把切换表包成 TokenBucket 需要的 rate_provider（None = 不限速）。"""
    if not schedule:
        return None
    wall = clock or time.time
    return lambda: scheduled_rate(schedule, wall())


class TokenBucket:
    """简单令牌桶限速器，rate 为 None 表示不限速。"""

    def __init__(
        self,
        rate_bytes_per_sec: float | None,
        *,
        burst: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        rate_provider: Callable[[], float | None] | None = None,
        recheck_seconds: float = 5.0,
    ):
        if rate_bytes_per_sec is not None and rate_bytes_per_sec <= 0:
            raise ValueError("rate must be positive or None")
        self.rate = rate_bytes_per_sec
        # 容量至少 1 字节：capacity=0 时 consume() 永远凑不够令牌，会一直空转。
        self.capacity = max(float(burst or rate_bytes_per_sec or 0), 1.0)
        self._tokens = float(self.capacity)
        self._clock = clock
        self._sleep = sleeper
        self._last = clock()
        #: 时段限速：定时回读"当前时段应有的速率"，让长拷贝能在跨时段时自动切换。
        self._rate_provider = rate_provider
        self._recheck_seconds = max(float(recheck_seconds), 0.1)
        self._last_rate_check = clock()

    def _refresh_rate(self) -> None:
        if self._rate_provider is None:
            return
        now = self._clock()
        if now - self._last_rate_check < self._recheck_seconds:
            return
        self._last_rate_check = now
        try:
            value = self._rate_provider()
        except Exception:  # noqa: BLE001 - 限速读取失败不能打断迁移
            return
        self.set_rate(value)

    def set_rate(self, rate_bytes_per_sec: float | None) -> None:
        """切换速率：令牌桶容量跟着调整，避免放宽后仍被旧容量卡住。"""
        if rate_bytes_per_sec is not None and rate_bytes_per_sec <= 0:
            rate_bytes_per_sec = None
        self.rate = rate_bytes_per_sec
        self.capacity = max(float(self.rate or 0), 1.0)
        self._tokens = min(self._tokens, self.capacity)

    def consume(self, amount: int) -> None:
        self._refresh_rate()
        if not self.rate or amount <= 0:
            return
        remaining = amount
        while remaining > 0:
            now = self._clock()
            self._tokens = min(
                float(self.capacity), self._tokens + (now - self._last) * self.rate
            )
            self._last = now
            if self._tokens < 1:
                # 等到攒够本次能消耗的令牌；下限避免浮点漂移导致的空转。
                target = min(float(remaining), float(self.capacity))
                deficit = target - self._tokens
                self._sleep(max(deficit / self.rate, MIN_THROTTLE_SLEEP))
                continue
            take = int(min(remaining, self._tokens))
            self._tokens -= take
            remaining -= take


def send_device(
    src_path: str,
    *,
    peer_host: str,
    peer_port: int,
    ticket: str,
    offset: int = 0,
    length: int | None = None,
    chunk_size: int = DEFAULT_CHUNK,
    rate_limit_bytes_per_sec: float | None = None,
    rate_schedule: list[dict[str, float]] | None = None,
    progress_cb: Callable[[int], None] | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    skip_zero: bool = False,
    stats: TransferStats | None = None,
) -> int:
    """把 src_path 的 [offset, offset+length) 推送到对端，返回已处理字节数。

    ``skip_zero`` 为真时，全零块只发一个空洞帧（不占网络带宽），目标端
    按自己的空洞模式落地；非零块永远按原始字节发送。
    """
    total = device_size(src_path)
    if length is None:
        length = total - offset
    if offset < 0 or length < 0 or offset + length > total:
        raise ValueError("transfer range out of bounds")

    # 时段限速：切换表由平台按绝对时刻算好，agent 只是照表切速率，
    # 跨时区的节点不会在错误的时段限速。
    bucket = TokenBucket(
        rate_limit_bytes_per_sec,
        rate_provider=schedule_rate_provider(rate_schedule),
    )
    zeros = b"\0" * chunk_size if skip_zero else b""
    processed = 0
    with socket.create_connection((peer_host, peer_port), timeout=30) as sock:
        with sock.makefile("rb") as reader:
            sock.sendall(encode_handshake(ticket, offset, length))
            ack = reader.readline(HANDSHAKE_LIMIT)
            if ack.strip() != b"OK":
                raise TransferError(f"peer rejected handshake: {ack!r}")
            with open(src_path, "rb") as source:
                source.seek(offset)
                while processed < length:
                    if is_cancelled is not None and is_cancelled():
                        raise TransferError("cancelled")
                    chunk = source.read(min(chunk_size, length - processed))
                    if not chunk:
                        break
                    if skip_zero and is_zero_block(chunk, zeros):
                        sock.sendall(encode_hole(offset + processed, len(chunk)))
                        if stats is not None:
                            stats.skipped_bytes += len(chunk)
                        processed += len(chunk)
                        if progress_cb is not None:
                            progress_cb(processed)
                        continue
                    bucket.consume(len(chunk))
                    sock.sendall(encode_chunk(offset + processed, chunk))
                    if stats is not None:
                        stats.data_bytes += len(chunk)
                    processed += len(chunk)
                    if progress_cb is not None:
                        progress_cb(processed)
    if processed != length:
        raise TransferError(f"short read from source: {processed}/{length}")
    return processed


def _zero_out(handle, offset: int, length: int) -> bool:
    """优先用 BLKZEROOUT 让后端打洞；不支持时返回 False 由调用方写零。"""
    if fcntl is None:
        return False
    try:
        fcntl.ioctl(handle.fileno(), BLKZEROOUT, struct.pack("<QQ", offset, length))
        return True
    except OSError:
        return False


def _write_zeros(handle, offset: int, length: int, *, block: int = 1024 * 1024) -> None:
    """在目标设备上显式写零，用于 BLKZEROOUT 不可用或普通文件场景。"""
    handle.seek(offset)
    zeros = b"\0" * min(block, length)
    remaining = length
    while remaining > 0:
        piece = zeros[: min(len(zeros), remaining)]
        handle.write(piece)
        remaining -= len(piece)


def _apply_hole(handle, offset: int, length: int, mode: str) -> None:
    """把源端全零区间在目标设备上落地。

    ``skip`` 依赖"目标是本作业新建的空白卷"；``zero`` 显式写零，对已有
    数据的卷也安全；``off`` 表示平台未启用 sparse，收到空洞帧就是协议错误。
    """
    if mode == "skip":
        return
    if mode != "zero":
        raise TransferError(f"unexpected hole frame (hole_mode={mode!r})")
    if not _zero_out(handle, offset, length):
        _write_zeros(handle, offset, length)


def receive_device(
    dst_path: str,
    *,
    listen_host: str,
    listen_port: int,
    expected_ticket: str,
    offset: int,
    length: int,
    chunk_size: int = DEFAULT_CHUNK,
    progress_cb: Callable[[int], None] | None = None,
    ready_event: Event | None = None,
    on_listening: Callable[[], None] | None = None,
    accept_timeout: float | None = None,
    hole_mode: str = "skip",
    stats: TransferStats | None = None,
) -> int:
    """监听一次连接并把数据写入 dst_path，返回已处理字节数。"""
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((listen_host, listen_port))
        server.listen(1)
        if ready_event is not None:
            ready_event.set()
        if on_listening is not None:
            on_listening()
        # 没有 accept_timeout 时，目标端会永远卡在 accept，既占住槽位又让平台
        # 一直等结果；对端失败时尤其致命。
        if accept_timeout is not None:
            server.settimeout(float(accept_timeout))
        try:
            conn, _ = server.accept()
        except socket.timeout as exc:
            raise TransferError(
                f"目标端在 {accept_timeout}s 内没有等到发送端连接"
            ) from exc
        with conn, conn.makefile("rb") as reader:
            handshake = decode_handshake(reader.readline(HANDSHAKE_LIMIT))
            if handshake["ticket"] != expected_ticket:
                conn.sendall(b"DENIED\n")
                raise TransferError("ticket mismatch")
            if handshake["offset"] != offset or handshake["length"] != length:
                conn.sendall(b"DENIED\n")
                raise TransferError("range mismatch")
            conn.sendall(b"OK\n")
            processed = 0
            with open(dst_path, "r+b") as target:
                while processed < length:
                    kind, chunk_offset, chunk_length, payload = read_frame(reader)
                    if kind == "eof":
                        raise TransferError("truncated stream")
                    if chunk_offset != offset + processed:
                        raise TransferError("unexpected chunk offset")
                    if kind == "hole":
                        _apply_hole(target, chunk_offset, chunk_length, hole_mode)
                        if stats is not None:
                            stats.skipped_bytes += chunk_length
                        processed += chunk_length
                    else:
                        target.seek(chunk_offset)
                        target.write(payload)
                        if stats is not None:
                            stats.data_bytes += len(payload)
                        processed += len(payload)
                    if progress_cb is not None:
                        progress_cb(processed)
                target.flush()
    return processed


TAIL_WINDOW = 16 * 1024 * 1024


def hash_device(path: str, window: int = TAIL_WINDOW) -> tuple[str, int]:
    """计算设备摘要：window>0 取尾部窗口，window<=0 取整盘。返回 (摘要, 大小)。"""
    size = device_size(path)
    if window <= 0:
        start, length = 0, size
    else:
        start = max(0, size - window)
        length = size - start
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        stream.seek(start)
        remaining = length
        while remaining > 0:
            chunk = stream.read(min(DEFAULT_CHUNK, remaining))
            if not chunk:
                break
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest(), size


__all__ = [
    "ProtocolError",
    "TokenBucket",
    "TransferError",
    "hash_device",
    "parse_rate_window",
    "rate_schedule",
    "schedule_rate_provider",
    "scheduled_rate",
    "receive_device",
    "send_device",
]
