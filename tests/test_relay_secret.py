import base64
import os
import stat
import tempfile
import unittest
from pathlib import Path

from relay_secret import SecretError, load_or_create_secret, parse_secret


class ParseSecretTest(unittest.TestCase):
    def test_accepts_base64_key(self):
        raw = base64.b64encode(b"a" * 32).decode("ascii")
        self.assertEqual(parse_secret(raw, source="test"), b"a" * 32)

    def test_accepts_hex_key(self):
        self.assertEqual(parse_secret("ab" * 32, source="test"), bytes.fromhex("ab" * 32))

    def test_accepts_raw_32_byte_key(self):
        self.assertEqual(parse_secret("k" * 32, source="test"), b"k" * 32)

    def test_rejects_wrong_length(self):
        with self.assertRaises(SecretError):
            parse_secret("short", source="test")

    def test_rejects_empty(self):
        with self.assertRaises(SecretError):
            parse_secret("", source="test")


class LoadOrCreateSecretTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-secret"

    def tearDown(self):
        self.tmp.cleanup()

    def test_creates_0600_file_and_is_stable_across_calls(self):
        first = load_or_create_secret(self.path, env={})
        second = load_or_create_secret(self.path, env={})

        self.assertEqual(len(first), 32)
        self.assertEqual(first, second)
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_env_var_wins_over_file(self):
        load_or_create_secret(self.path, env={})
        raw = base64.b64encode(b"b" * 32).decode("ascii")

        result = load_or_create_secret(self.path, env={"MIGRATION_RELAY_SECRET": raw})

        self.assertEqual(result, b"b" * 32)

    def test_invalid_env_var_raises(self):
        with self.assertRaises(SecretError):
            load_or_create_secret(self.path, env={"MIGRATION_RELAY_SECRET": "nope"})
