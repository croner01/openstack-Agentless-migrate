import unittest

from excel_parser import parse_rows, parse_selected_rows


class ExcelParserTest(unittest.TestCase):
    def test_returns_one_row_per_input(self):
        rows = parse_rows(
            [
                {"vm_name": "vm-a", "target_az": "az1"},
                {"vm_name": "vm-b", "target_az": "az2"},
            ]
        )
        self.assertEqual(
            [(row.vm_name, row.target_az) for row in rows],
            [("vm-a", "az1"), ("vm-b", "az2")],
        )

    def test_blank_row_raises(self):
        with self.assertRaises(ValueError):
            parse_rows([{"vm_name": "", "target_az": "az1"}])

    def test_missing_required_column_raises(self):
        with self.assertRaises(ValueError):
            parse_rows([{"vm_name": "vm-a"}])

    def test_optional_columns_are_preserved(self):
        rows = parse_rows(
            [
                {
                    "vm_name": "vm-a",
                    "target_az": "az1",
                    "target_image": "img-1",
                    "target_flavor": "fl-1",
                }
            ]
        )
        self.assertEqual(rows[0].target_image, "img-1")
        self.assertEqual(rows[0].target_flavor, "fl-1")

    def test_empty_input_returns_empty_list(self):
        self.assertEqual(parse_rows([]), [])


class SelectedRowsTest(unittest.TestCase):
    def test_parse_selected_rows_preserves_server_id(self):
        rows = parse_selected_rows(
            [
                {
                    "server_id": "srv-1",
                    "vm_name": "web-1",
                    "target_az": "az1",
                    "target_image": "img-1",
                    "target_flavor": "flavor-1",
                }
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].source_server_id, "srv-1")
        self.assertEqual(rows[0].vm_name, "web-1")
        self.assertEqual(rows[0].target_image, "img-1")

    def test_parse_selected_rows_requires_server_id(self):
        with self.assertRaisesRegex(ValueError, "server_id"):
            parse_selected_rows(
                [{"server_id": "", "vm_name": "web-1", "target_az": "az1"}]
            )


class MigrationModeParsingTest(unittest.TestCase):
    def test_defaults_to_full(self):
        rows = parse_selected_rows(
            [{"server_id": "s1", "vm_name": "vm1", "target_az": "az1"}]
        )
        self.assertEqual(rows[0].mode, "full")

    def test_accepts_incremental(self):
        rows = parse_selected_rows(
            [
                {
                    "server_id": "s1",
                    "vm_name": "vm1",
                    "target_az": "az1",
                    "mode": "incremental",
                }
            ]
        )
        self.assertEqual(rows[0].mode, "incremental")

    def test_rejects_unknown_mode(self):
        with self.assertRaises(ValueError):
            parse_selected_rows(
                [
                    {
                        "server_id": "s1",
                        "vm_name": "vm1",
                        "target_az": "az1",
                        "mode": "fast",
                    }
                ]
            )


if __name__ == "__main__":
    unittest.main()
