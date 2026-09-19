"""Excel 模板下载与逐行容错预览接口。"""
import io
import unittest

import pandas as pd

import app as app_module
from excel_parser import REQUIRED_COLUMNS


class ExcelTemplateApiTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_template_download_has_expected_columns(self):
        resp = self.client.get("/api/excel-template")

        self.assertEqual(resp.status_code, 200)
        frame = pd.read_excel(io.BytesIO(resp.get_data()))
        self.assertTrue(REQUIRED_COLUMNS.issubset(set(frame.columns)))
        for column in ("channel", "target_network", "target_volume_type", "rate_limit_mb_s"):
            self.assertIn(column, frame.columns)


class PreviewTolerantApiTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _upload(self, records):
        buffer = io.BytesIO()
        pd.DataFrame(records).to_excel(buffer, index=False)
        buffer.seek(0)
        return self.client.post(
            "/api/preview",
            data={"excel_file": (buffer, "list.xlsx")},
            content_type="multipart/form-data",
        )

    def test_bad_rows_are_reported_and_good_rows_kept(self):
        resp = self._upload([
            {"vm_name": "vm-1", "target_az": "az1", "mode": "full", "channel": "relay"},
            {"vm_name": "", "target_az": "az1"},
            {"vm_name": "vm-3", "target_az": "az1", "channel": "smb"},
        ])

        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual([row["vm_name"] for row in data["rows"]], ["vm-1"])
        self.assertEqual([item["row"] for item in data["errors"]], [3, 4])

    def test_preview_keeps_per_vm_batch_fields(self):
        resp = self._upload([{
            "vm_name": "vm-1", "target_az": "az1", "mode": "incremental",
            "start_target": "否", "channel": "中转机", "target_network": "net-web",
            "target_volume_type": "ssd", "rate_limit_mb_s": 25,
        }])

        row = resp.get_json()["rows"][0]

        self.assertEqual(row["mode"], "incremental")
        self.assertFalse(row["start_target"])
        self.assertEqual(row["channel"], "relay")
        self.assertEqual(row["target_network"], "net-web")
        self.assertEqual(row["target_volume_type"], "ssd")
        self.assertEqual(row["rate_limit_mb_s"], 25.0)
        self.assertEqual(row["row_number"], 2)

    def test_missing_columns_fail_with_readable_error(self):
        resp = self._upload([{"vm_name": "vm-1"}])

        data = resp.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("target_az", data["error"])
