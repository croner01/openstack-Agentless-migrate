import unittest
from unittest import mock

from flask import Flask

from relay_api import create_blueprint
from relay_inventory import NodeInventory, RelayNodeRecord
from relay_protocol import issue_node_token
from relay_registry import RelayState

SECRET = b"s" * 32


class NodeTokenRegisterTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(secret=SECRET)
        self.inventory = mock.MagicMock(spec=NodeInventory)
        self.credentials = mock.MagicMock()
        self.record = RelayNodeRecord(
            node_id="n1",
            name="relay-source-1",
            role="source",
            tenant_key="t1",
            az="nova-1",
            slots_total=5,
            token_enc="sealed-token",
        )
        self.inventory.get.return_value = self.record
        self.token = issue_node_token(
            SECRET, node_id="n1", role="source", tenant_key="t1", az="nova-1"
        )
        self.credentials.unseal_text.return_value = self.token
        self.app = Flask(__name__)
        self.app.register_blueprint(
            create_blueprint(
                self.state,
                inventory=self.inventory,
                credentials=self.credentials,
            )
        )
        self.client = self.app.test_client()

    def _register(self, **overrides):
        body = {
            "token": self.token,
            "name": "relay-source-1",
            "agent_version": "1.0.0",
            "slots_total": 5,
        }
        body.update(overrides)
        return self.client.post("/api/relay/register", json=body)

    def test_register_with_node_token_sets_slots(self):
        response = self._register()

        self.assertEqual(response.status_code, 200)
        agent = self.state.find_by_name("relay-source-1")
        self.assertEqual(agent.slots_total, 5)
        self.assertEqual(agent.job_id, "")

    def test_register_rejects_token_after_rotation(self):
        self.credentials.unseal_text.return_value = "rotated-token"

        response = self._register()

        self.assertEqual(response.status_code, 401)

    def test_register_rejects_unknown_node(self):
        self.inventory.get.return_value = None

        response = self._register()

        self.assertEqual(response.status_code, 401)

    def test_register_rejects_tampered_node_token(self):
        response = self._register(token=self.token[:-1] + "0")

        self.assertEqual(response.status_code, 401)

    def test_register_accepts_legacy_job_token_without_inventory_lookup(self):
        from relay_protocol import issue_token

        legacy = issue_token(SECRET, "job-1", "source", now=self.state.now(), ttl=300)
        self.inventory.get.reset_mock()

        response = self._register(token=legacy)

        self.assertEqual(response.status_code, 200)
        self.inventory.get.assert_not_called()

    def test_bootstrap_accepts_node_token(self):
        # cloud-init 在常驻模式下用节点令牌拉安装脚本，必须放行。
        response = self.client.get(f"/api/relay/bootstrap?token={self.token}")

        self.assertEqual(response.status_code, 200)
        self.assertIn("relay-agent", response.get_data(as_text=True))

    def test_bootstrap_rejects_revoked_node_token(self):
        self.credentials.unseal_text.return_value = "rotated-token"

        response = self.client.get(f"/api/relay/bootstrap?token={self.token}")

        self.assertEqual(response.status_code, 401)

    def test_pkg_accepts_node_token(self):
        response = self.client.get(
            f"/api/relay/pkg/relay_protocol.py?token={self.token}"
        )

        self.assertEqual(response.status_code, 200)

    def test_pkg_still_rejects_bad_token(self):
        response = self.client.get("/api/relay/pkg/relay_protocol.py?token=bogus")

        self.assertEqual(response.status_code, 401)
