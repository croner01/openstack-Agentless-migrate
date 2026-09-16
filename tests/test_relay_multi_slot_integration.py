import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from relay_transfer import receive_device, send_device

CHUNK = 64 * 1024


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _run_pair(source_path: Path, target_path: Path, port: int, ticket: str) -> None:
    size = os.path.getsize(source_path)
    listening = threading.Event()
    errors: list[BaseException] = []

    def receive() -> None:
        try:
            receive_device(
                str(target_path),
                listen_host="127.0.0.1",
                listen_port=port,
                expected_ticket=ticket,
                offset=0,
                length=size,
                chunk_size=CHUNK,
                progress_cb=lambda copied: None,
                ready_event=listening,
            )
        except BaseException as exc:  # noqa: BLE001 - 测试里记录后统一断言
            errors.append(exc)

    thread = threading.Thread(target=receive, daemon=True)
    thread.start()
    if not listening.wait(timeout=10):
        raise AssertionError("目标端未能在超时前进入监听状态")

    send_device(
        str(source_path),
        peer_host="127.0.0.1",
        peer_port=port,
        ticket=ticket,
        offset=0,
        length=size,
        chunk_size=CHUNK,
        progress_cb=lambda copied: None,
    )
    thread.join(timeout=10)
    if errors:
        raise errors[0]


class MultiSlotTransferTest(unittest.TestCase):
    """同一个"节点"上开两个端口并发拷贝，验证端口池互不冲突且数据一致。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_two_concurrent_transfers_on_distinct_ports(self):
        base_port = _free_port()
        jobs = []
        for index in range(2):
            source = self.root / f"source-{index}.img"
            target = self.root / f"target-{index}.img"
            source.write_bytes(os.urandom(256 * 1024))
            # 真实场景里目标卷已存在（块设备），这里用等长文件模拟。
            target.write_bytes(b"\0" * os.path.getsize(source))
            jobs.append((source, target, base_port + index, f"ticket-{index}"))

        errors: list[BaseException] = []

        def run(source, target, port, ticket):
            try:
                _run_pair(source, target, port, ticket)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=run, args=(source, target, port, ticket))
            for source, target, port, ticket in jobs
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        for source, target, _, _ in jobs:
            self.assertTrue(target.exists())
            self.assertEqual(target.read_bytes(), source.read_bytes())
