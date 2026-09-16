import threading
import time
import unittest

from ceph_utils import (
    CopyGate,
    parse_memory_value,
    parse_oom_count,
)


class FakeMemoryProvider:
    def __init__(self, current, limit):
        self.current = current
        self.limit = limit

    def __call__(self):
        return self.current, self.limit


class ParseMemoryTest(unittest.TestCase):
    def test_parse_memory_value_reads_bytes(self):
        self.assertEqual(parse_memory_value("1073741824"), 1073741824)

    def test_parse_memory_value_treats_max_as_unlimited(self):
        self.assertIsNone(parse_memory_value("max"))
        self.assertIsNone(parse_memory_value("9223372036854771712"))

    def test_parse_oom_count_reads_counter(self):
        self.assertEqual(parse_oom_count("oom_kill 3\nunder_oom 0\n"), 3)
        self.assertEqual(parse_oom_count("oom_kill_disable 0\nunder_oom 0\n"), 0)


class CopyGateTest(unittest.TestCase):
    def setUp(self):
        self.memory = FakeMemoryProvider(
            current=512 * 1024 * 1024,
            limit=16 * 1024 * 1024 * 1024,
        )

    def test_max_active_copies_limits_concurrent_copy_pairs(self):
        gate = CopyGate(
            max_active_copies=2,
            memory_provider=self.memory,
            high_water=0.9,
            reserve_bytes=1024 * 1024 * 1024,
        )
        self.assertTrue(gate.try_acquire())
        self.assertTrue(gate.try_acquire())
        self.assertFalse(gate.try_acquire())

    def test_release_restores_capacity(self):
        gate = CopyGate(
            max_active_copies=1,
            memory_provider=self.memory,
        )
        self.assertTrue(gate.try_acquire())
        self.assertFalse(gate.try_acquire())
        gate.release()
        self.assertTrue(gate.try_acquire())

    def test_memory_high_water_blocks_new_copy_when_reserve_would_overflow(self):
        gate = CopyGate(
            max_active_copies=3,
            memory_provider=FakeMemoryProvider(
                current=9 * 1024 * 1024 * 1024,
                limit=10 * 1024 * 1024 * 1024,
            ),
            high_water=0.9,
            reserve_bytes=1024 * 1024 * 1024,
        )
        self.assertTrue(gate.try_acquire())
        self.assertFalse(gate.try_acquire())

    def test_no_cgroup_limit_only_applies_active_copy_cap(self):
        gate = CopyGate(
            max_active_copies=2,
            memory_provider=lambda: (1024 * 1024 * 1024, None),
        )
        self.assertTrue(gate.try_acquire())
        self.assertTrue(gate.try_acquire())
        self.assertFalse(gate.try_acquire())

    def test_holders_track_owner_and_release(self):
        gate = CopyGate(max_active_copies=2, memory_provider=self.memory)
        self.assertTrue(gate.try_acquire("vm-a/volume-1"))

        holders = gate.holders()
        self.assertEqual(len(holders), 1)
        self.assertEqual(holders[0][0], "vm-a/volume-1")
        self.assertGreaterEqual(holders[0][1], 0.0)
        self.assertIn("vm-a/volume-1", gate.holder_summary())

        gate.release()
        self.assertEqual(gate.holders(), [])
        self.assertEqual(gate.holder_summary(), "无")

    def test_waiting_log_names_who_holds_the_slot(self):
        """排队日志必须能回答"名额被谁占着"，否则现场只能靠猜。"""
        gate = CopyGate(
            max_active_copies=1,
            memory_provider=self.memory,
            poll_interval_seconds=0.01,
            log_interval_seconds=0.0,
        )
        gate.acquire(owner="vm-a/volume-1")
        finished = threading.Event()

        def waiter():
            gate.acquire(owner="vm-b/volume-2")
            gate.release()
            finished.set()

        worker = threading.Thread(target=waiter)
        with self.assertLogs(level="INFO") as captured:
            worker.start()
            time.sleep(0.2)
            gate.release()
            worker.join(2.0)

        self.assertTrue(finished.is_set())
        self.assertTrue(
            any(
                "等待 RBD 拷贝名额" in line and "vm-a/volume-1" in line
                for line in captured.output
            ),
            captured.output,
        )


if __name__ == "__main__":
    unittest.main()
