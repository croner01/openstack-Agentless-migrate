import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from relay_transfer import (
    TransferStats,
    TokenBucket,
    device_size,
    TransferError,
    receive_device,
    send_device,
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TokenBucketTest(unittest.TestCase):
    def test_no_rate_means_no_sleep(self):
        slept = []
        bucket = TokenBucket(None, clock=lambda: 0.0, sleeper=slept.append)

        bucket.consume(1024)

        self.assertEqual(slept, [])

    def test_sleeps_when_tokens_exhausted(self):
        clock = _FakeClock()
        bucket = TokenBucket(
            1000.0, clock=clock, sleeper=clock.sleep, burst=1000
        )

        bucket.consume(1500)

        # 桶容量 1000，多出的 500 字节必须靠睡眠换来。
        self.assertAlmostEqual(clock.slept, 0.5, places=6)

    def test_rejects_negative_rate(self):
        with self.assertRaises(ValueError):
            TokenBucket(-1.0)


class _FakeClock:
    """睡眠会推进的假时钟，避免限速用例空转或误判。"""

    def __init__(self):
        self.now = 0.0
        self.slept = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept += seconds
        self.now += seconds


class DeviceTransferTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.payload = os.urandom(300 * 1024)
        self.src.write_bytes(self.payload)
        self.dst.write_bytes(b"\x00" * len(self.payload))
        self.port = _free_port()

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_copy_matches_source(self):
        ready = threading.Event()
        received = {}

        def run_receiver():
            received["bytes"] = receive_device(
                str(self.dst),
                listen_host="127.0.0.1",
                listen_port=self.port,
                expected_ticket="ticket-1",
                offset=0,
                length=len(self.payload),
                chunk_size=64 * 1024,
                ready_event=ready,
            )

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        sent = send_device(
            str(self.src),
            peer_host="127.0.0.1",
            peer_port=self.port,
            ticket="ticket-1",
            offset=0,
            length=len(self.payload),
            chunk_size=64 * 1024,
        )
        thread.join(timeout=10)

        self.assertEqual(sent, len(self.payload))
        self.assertEqual(received["bytes"], len(self.payload))
        self.assertEqual(self.dst.read_bytes(), self.payload)

    def test_receiver_rejects_wrong_ticket(self):
        ready = threading.Event()
        errors = []

        def run_receiver():
            try:
                receive_device(
                    str(self.dst),
                    listen_host="127.0.0.1",
                    listen_port=self.port,
                    expected_ticket="expected",
                    offset=0,
                    length=16,
                    chunk_size=16,
                    ready_event=ready,
                )
            except Exception as exc:  # noqa: BLE001 - 测试需要捕获任意协议错误
                errors.append(exc)

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        with self.assertRaises(TransferError):
            send_device(
                str(self.src),
                peer_host="127.0.0.1",
                peer_port=self.port,
                ticket="wrong",
                offset=0,
                length=16,
                chunk_size=16,
            )
        thread.join(timeout=10)

        self.assertTrue(errors)

    def test_progress_callback_reports_cumulative_bytes(self):
        ready = threading.Event()
        seen = []

        def run_receiver():
            receive_device(
                str(self.dst),
                listen_host="127.0.0.1",
                listen_port=self.port,
                expected_ticket="ticket-1",
                offset=0,
                length=len(self.payload),
                chunk_size=64 * 1024,
                ready_event=ready,
            )

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        send_device(
            str(self.src),
            peer_host="127.0.0.1",
            peer_port=self.port,
            ticket="ticket-1",
            offset=0,
            length=len(self.payload),
            chunk_size=64 * 1024,
            progress_cb=seen.append,
        )
        thread.join(timeout=10)

        self.assertTrue(seen)
        self.assertEqual(seen[-1], len(self.payload))

    def test_send_device_rejects_range_beyond_source(self):
        with self.assertRaises(ValueError):
            send_device(
                str(self.src),
                peer_host="127.0.0.1",
                peer_port=self.port,
                ticket="ticket-1",
                offset=0,
                length=len(self.payload) + 1,
            )


class ResumeAndVerifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.payload = os.urandom(200 * 1024)
        self.src.write_bytes(self.payload)
        self.dst.write_bytes(self.payload[:100 * 1024] + b"\x00" * (100 * 1024))
        self.port = _free_port()

    def tearDown(self):
        self.tmp.cleanup()

    def test_resumed_transfer_only_sends_remaining_range(self):
        ready = threading.Event()

        def run_receiver():
            receive_device(
                str(self.dst),
                listen_host="127.0.0.1",
                listen_port=self.port,
                expected_ticket="ticket-1",
                offset=100 * 1024,
                length=100 * 1024,
                chunk_size=32 * 1024,
                ready_event=ready,
            )

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        sent = send_device(
            str(self.src),
            peer_host="127.0.0.1",
            peer_port=self.port,
            ticket="ticket-1",
            offset=100 * 1024,
            length=100 * 1024,
            chunk_size=32 * 1024,
        )
        thread.join(timeout=10)

        self.assertEqual(sent, 100 * 1024)
        self.assertEqual(self.dst.read_bytes(), self.payload)

class SparseTransferTest(unittest.TestCase):
    """空洞跳过：全零块不过网、目标端按模式落地，非零数据必须原样到达。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.port = _free_port()

    def tearDown(self):
        self.tmp.cleanup()

    def _transfer(self, *, length, chunk_size, skip_zero, hole_mode):
        """并发跑一次真实 socket 传输，返回 (send_stats, recv_stats, errors)。"""
        ready = threading.Event()
        recv_stats = TransferStats()
        send_stats = TransferStats()
        errors: list = []

        def run_receiver():
            try:
                receive_device(
                    str(self.dst),
                    listen_host="127.0.0.1",
                    listen_port=self.port,
                    expected_ticket="ticket-1",
                    offset=0,
                    length=length,
                    chunk_size=chunk_size,
                    ready_event=ready,
                    hole_mode=hole_mode,
                    stats=recv_stats,
                )
            except Exception as exc:  # noqa: BLE001 - 测试需要看到对端异常
                errors.append(exc)

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))
        try:
            send_device(
                str(self.src),
                peer_host="127.0.0.1",
                peer_port=self.port,
                ticket="ticket-1",
                offset=0,
                length=length,
                chunk_size=chunk_size,
                skip_zero=skip_zero,
                stats=send_stats,
            )
        except OSError as exc:  # 对端拒绝时发送侧可能直接断开
            errors.append(exc)
        thread.join(timeout=10)
        return send_stats, recv_stats, errors

    def test_send_device_skips_zero_chunks(self):
        self.src.write_bytes(b"\0" * (8 * 1024 * 1024) + b"tail")
        self.dst.write_bytes(b"\0" * (8 * 1024 * 1024 + 4))

        send_stats, recv_stats, errors = self._transfer(
            length=8 * 1024 * 1024 + 4,
            chunk_size=4 * 1024 * 1024,
            skip_zero=True,
            hole_mode="skip",
        )

        self.assertEqual(errors, [])
        self.assertEqual(send_stats.skipped_bytes, 8 * 1024 * 1024)
        self.assertEqual(send_stats.data_bytes, 4)
        self.assertEqual(recv_stats.skipped_bytes, 8 * 1024 * 1024)
        self.assertEqual(self.dst.read_bytes(), self.src.read_bytes())

    def test_send_device_never_skips_chunk_with_single_nonzero_byte(self):
        payload = bytearray(4 * 1024 * 1024)
        payload[-1] = 1
        self.src.write_bytes(bytes(payload))
        self.dst.write_bytes(b"\0" * len(payload))

        send_stats, _recv_stats, errors = self._transfer(
            length=len(payload),
            chunk_size=4 * 1024 * 1024,
            skip_zero=True,
            hole_mode="skip",
        )

        self.assertEqual(errors, [])
        self.assertEqual(send_stats.skipped_bytes, 0)
        self.assertEqual(send_stats.data_bytes, len(payload))
        self.assertEqual(self.dst.read_bytes(), bytes(payload))

    def test_receive_device_zero_mode_overwrites_existing_data(self):
        self.src.write_bytes(b"\0" * 128 * 1024)
        self.dst.write_bytes(b"\xff" * (128 * 1024))

        _send_stats, recv_stats, errors = self._transfer(
            length=128 * 1024,
            chunk_size=64 * 1024,
            skip_zero=True,
            hole_mode="zero",
        )

        self.assertEqual(errors, [])
        self.assertEqual(recv_stats.skipped_bytes, 128 * 1024)
        self.assertEqual(self.dst.read_bytes(), b"\0" * 128 * 1024)

    def test_receive_device_skip_mode_leaves_target_untouched(self):
        self.src.write_bytes(b"\0" * 64 * 1024)
        self.dst.write_bytes(b"\xff" * (64 * 1024))

        self._transfer(
            length=64 * 1024,
            chunk_size=64 * 1024,
            skip_zero=True,
            hole_mode="skip",
        )

        # skip 模式依赖"目标卷是空白卷"这一前提，不写就是保持原样。
        self.assertEqual(self.dst.read_bytes(), b"\xff" * 64 * 1024)

    def test_receive_device_rejects_hole_when_mode_off(self):
        self.src.write_bytes(b"\0" * 64 * 1024)
        self.dst.write_bytes(b"\0" * (64 * 1024))

        _send_stats, _recv_stats, errors = self._transfer(
            length=64 * 1024,
            chunk_size=64 * 1024,
            skip_zero=True,
            hole_mode="off",
        )

        self.assertTrue(
            any(isinstance(exc, TransferError) for exc in errors),
            f"expected TransferError, got {errors!r}",
        )

    def test_sparse_copy_reproduces_source_exactly(self):
        source = bytearray(12 * 1024 * 1024)
        source[5 * 1024 * 1024:5 * 1024 * 1024 + 4096] = b"x" * 4096
        source[-4:] = b"tail"
        self.src.write_bytes(bytes(source))
        self.dst.write_bytes(b"\0" * len(source))

        send_stats, _recv_stats, errors = self._transfer(
            length=len(source),
            chunk_size=4 * 1024 * 1024,
            skip_zero=True,
            hole_mode="skip",
        )

        self.assertEqual(errors, [])
        # 第 2 块含 x 数据、第 3 块含 tail，首块全零被跳过。
        self.assertEqual(send_stats.data_bytes, 8 * 1024 * 1024)
        self.assertEqual(send_stats.skipped_bytes, 4 * 1024 * 1024)
        self.assertEqual(self.dst.read_bytes(), bytes(source))


class DeviceSizeTest(unittest.TestCase):
    """块设备不能用 st_size：那会得到 0，导致 range out of bounds。"""

    def test_regular_file_uses_stat(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "f"
            path.write_bytes(b"x" * 123)

            self.assertEqual(device_size(str(path)), 123)

    def test_block_device_uses_blkgetsize64(self):
        import struct as _struct
        from unittest import mock

        fake_stat = mock.Mock()
        fake_stat.st_mode = 0o60600  # S_IFBLK
        fake_stat.st_size = 0
        with mock.patch("relay_transfer.os.stat", return_value=fake_stat), \
                mock.patch("relay_transfer.open", mock.mock_open(read_data=b"")), \
                mock.patch("relay_transfer.fcntl") as fake_fcntl:
            fake_fcntl.ioctl.return_value = _struct.pack("<Q", 200 * 1024 ** 3)

            self.assertEqual(device_size("/dev/vdb"), 200 * 1024 ** 3)
