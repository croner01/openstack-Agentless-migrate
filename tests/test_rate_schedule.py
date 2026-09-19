"""时段限速：窗口解析、跨零点切换表，以及令牌桶按表自动切速。"""
import unittest

from relay_transfer import (
    TokenBucket,
    parse_rate_window,
    rate_schedule,
    schedule_rate_provider,
    scheduled_rate,
)

#: 2023-11-14 22:13:20 UTC == 2023-11-15 06:13:20 @+08:00（低谷刚结束）
MOMENT = 1700000000.0
MB = 1024 * 1024


class ParseRateWindowTest(unittest.TestCase):
    def test_parses_plain_window(self):
        self.assertEqual(parse_rate_window("22:00-06:00"), (1320, 360, 0))

    def test_parses_window_with_offset(self):
        self.assertEqual(parse_rate_window("22:00 - 06:00 @+08:00"), (1320, 360, 28800))
        self.assertEqual(parse_rate_window("00:30-02:00@-05:30"), (30, 120, -19800))

    def test_rejects_garbage(self):
        for value in ("", "22:00", "25:00-30:00", "abc"):
            self.assertIsNone(parse_rate_window(value))


class RateScheduleTest(unittest.TestCase):
    def test_window_wraps_midnight(self):
        schedule = rate_schedule(0, 20 * MB, "22:00-06:00@+08:00", now=MOMENT, hours=30)

        self.assertEqual(schedule[0]["rate"], 0.0)
        # 切点必须落在本地 22:00 / 06:00 上，而不是把整天都限速。
        self.assertEqual(schedule[1]["rate"], 20 * MB)
        self.assertEqual(scheduled_rate(schedule, schedule[1]["at"] + 1), 20 * MB)
        # 速率 0 统一成 None（不限速），令牌桶据此彻底关掉节流。
        self.assertIsNone(scheduled_rate(schedule, schedule[2]["at"] + 1))

    def test_daytime_window_throttles_during_the_day(self):
        schedule = rate_schedule(None, 10 * MB, "09:00-18:00@+08:00", now=MOMENT, hours=24)

        self.assertTrue(schedule)
        # 首个切点之前（本地 06:13）在窗口外 = 不限速。
        self.assertIsNone(scheduled_rate(schedule, MOMENT))

    def test_no_throttle_configuration_yields_empty_schedule(self):
        self.assertEqual(rate_schedule(0, 0, "22:00-06:00"), [])
        self.assertEqual(rate_schedule(None, None, "22:00-06:00"), [])
        self.assertEqual(rate_schedule(100, 10, ""), [])

    def test_base_rate_applies_outside_window(self):
        schedule = rate_schedule(50 * MB, 5 * MB, "22:00-06:00@+08:00", now=MOMENT, hours=30)

        self.assertEqual(scheduled_rate(schedule, MOMENT), 50 * MB)


class TokenBucketRateProviderTest(unittest.TestCase):
    def test_bucket_follows_provider_when_rechecked(self):
        clock = {"now": 0.0}
        rates = {"value": None}
        bucket = TokenBucket(
            1 * MB,
            clock=lambda: clock["now"],
            sleeper=lambda _seconds: None,
            rate_provider=lambda: rates["value"],
            recheck_seconds=5.0,
        )
        self.assertEqual(bucket.rate, 1 * MB)

        rates["value"] = 8 * MB
        clock["now"] = 10.0
        bucket._refresh_rate()

        self.assertEqual(bucket.rate, 8 * MB)

    def test_failing_provider_keeps_previous_rate(self):
        bucket = TokenBucket(
            1 * MB,
            clock=lambda: 10.0,
            sleeper=lambda _seconds: None,
            rate_provider=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            recheck_seconds=0.1,
        )

        bucket._refresh_rate()

        self.assertEqual(bucket.rate, 1 * MB)

    def test_provider_from_schedule_returns_none_without_schedule(self):
        self.assertIsNone(schedule_rate_provider([]))
        provider = schedule_rate_provider([{"at": 0.0, "rate": 2 * MB}])
        self.assertEqual(provider(), 2 * MB)
