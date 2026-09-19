"""批量清单增强：通道/网络/卷类型/限速列，以及逐行容错解析。"""
import unittest

from excel_parser import parse_channel, parse_rate_limit, parse_rows, parse_rows_tolerant


def _row(**kwargs):
    payload = {"vm_name": "vm-1", "target_az": "az1"}
    payload.update(kwargs)
    return payload


class ExtendedColumnsTest(unittest.TestCase):
    def test_channel_aliases(self):
        self.assertEqual(parse_channel("relay"), "relay")
        self.assertEqual(parse_channel("中转机"), "relay")
        self.assertEqual(parse_channel("RBD 直连"), "rbd")
        self.assertEqual(parse_channel(""), "")

    def test_channel_rejects_unknown_value(self):
        with self.assertRaises(ValueError):
            parse_channel("smb")

    def test_rate_limit_accepts_numbers_and_blank(self):
        self.assertEqual(parse_rate_limit(""), 0.0)
        self.assertEqual(parse_rate_limit(None), 0.0)
        self.assertEqual(parse_rate_limit("12.5"), 12.5)

    def test_rate_limit_rejects_negative_and_text(self):
        with self.assertRaises(ValueError):
            parse_rate_limit("-1")
        with self.assertRaises(ValueError):
            parse_rate_limit("fast")

    def test_rows_carry_batch_columns_and_row_number(self):
        rows = parse_rows([_row(
            channel="中转机",
            target_network="net-web",
            target_volume_type="ssd",
            rate_limit_mb_s="30",
            power_on="否",
        )])

        row = rows[0]
        self.assertEqual(row.channel, "relay")
        self.assertEqual(row.target_network, "net-web")
        self.assertEqual(row.target_volume_type, "ssd")
        self.assertEqual(row.rate_limit_mb_s, 30.0)
        self.assertFalse(row.start_target)
        self.assertEqual(row.row_number, 2)

    def test_start_target_wins_over_power_on(self):
        rows = parse_rows([_row(start_target="是", power_on="否")])

        self.assertTrue(rows[0].start_target)


class TolerantParseTest(unittest.TestCase):
    def test_bad_rows_are_reported_without_dropping_good_ones(self):
        rows, errors = parse_rows_tolerant([
            _row(vm_name="vm-1"),
            _row(vm_name="", target_az="az1"),
            _row(vm_name="vm-3", channel="smb"),
            _row(vm_name="vm-4"),
        ])

        self.assertEqual([row.vm_name for row in rows], ["vm-1", "vm-4"])
        self.assertEqual([item["row"] for item in errors], [3, 4])
        self.assertTrue(any("通道" in item["message"] for item in errors))

    def test_duplicate_vm_name_is_flagged(self):
        rows, errors = parse_rows_tolerant([_row(), _row(), _row(vm_name="vm-2")])

        self.assertEqual(len(rows), 2)
        self.assertEqual(len(errors), 1)
        self.assertIn("重复", errors[0]["message"])

    def test_missing_required_column_fails_fast(self):
        with self.assertRaises(ValueError):
            parse_rows_tolerant([{"vm_name": "vm-1"}])
