import unittest

from relay_protocol import (
    ProtocolError,
    issue_node_token,
    verify_node_token,
)

SECRET = b"s" * 32


class NodeTokenTest(unittest.TestCase):
    def test_roundtrip_preserves_fields(self):
        token = issue_node_token(
            SECRET,
            node_id="node-1",
            role="source",
            tenant_key="http://keystone|project-1",
            az="nova-1",
        )

        payload = verify_node_token(SECRET, token)

        self.assertEqual(payload["node_id"], "node-1")
        self.assertEqual(payload["role"], "source")
        self.assertEqual(payload["tenant_key"], "http://keystone|project-1")
        self.assertEqual(payload["az"], "nova-1")

    def test_tenant_key_with_pipe_does_not_break_parsing(self):
        token = issue_node_token(
            SECRET, node_id="n", role="target", tenant_key="a|b|c", az="z"
        )
        self.assertEqual(verify_node_token(SECRET, token)["tenant_key"], "a|b|c")

    def test_wrong_secret_is_rejected(self):
        token = issue_node_token(
            SECRET, node_id="n", role="source", tenant_key="t", az="z"
        )
        with self.assertRaises(ProtocolError):
            verify_node_token(b"x" * 32, token)

    def test_tampered_payload_is_rejected(self):
        token = issue_node_token(
            SECRET, node_id="n", role="source", tenant_key="t", az="z"
        )
        body, _, signature = token.partition(".")
        with self.assertRaises(ProtocolError):
            verify_node_token(SECRET, f"{body}x.{signature}")

    def test_malformed_token_is_rejected(self):
        for bad in ("", "no-dot", "a.b"):
            with self.assertRaises(ProtocolError):
                verify_node_token(SECRET, bad)

    def test_unknown_role_is_rejected(self):
        import base64
        import hashlib
        import hmac
        import json

        payload = {"node_id": "n", "role": "bogus", "tenant_key": "t", "az": "z"}
        body = (
            base64.urlsafe_b64encode(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            )
            .decode("ascii")
            .rstrip("=")
        )
        signature = hmac.new(SECRET, body.encode("ascii"), hashlib.sha256).hexdigest()

        with self.assertRaises(ProtocolError):
            verify_node_token(SECRET, f"{body}.{signature}")

    def test_issue_rejects_unknown_role(self):
        with self.assertRaises(ProtocolError):
            issue_node_token(
                SECRET, node_id="n", role="bogus", tenant_key="t", az="z"
            )
