"""加密凭据存储：云认证与节点 SSH 密码。

AES-GCM，密文格式 "<b64(nonce)>.<b64(ciphertext+tag)>"，AAD 绑定记录键，
防止密文在不同租户/节点之间串用。主密钥来自 MIGRATION_SECRET_KEY。
未配置主密钥时常驻模式拒绝启动，不做明文降级。
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from json_store import atomic_write_json, read_json_object
from relay_secret import parse_secret

NONCE_BYTES = 12
KEY_BYTES = 32


class CredentialError(RuntimeError):
    """密文损坏、密钥不匹配或主密钥缺失。"""


class Sealer:
    """AES-GCM 封装；AAD 绑定记录键。"""

    def __init__(self, key: bytes):
        if len(key) != KEY_BYTES:
            raise CredentialError("主密钥必须是 32 字节")
        self._aead = AESGCM(key)

    def seal(self, plaintext: str, *, aad: str = "") -> str:
        nonce = os.urandom(NONCE_BYTES)
        payload = self._aead.encrypt(
            nonce, plaintext.encode("utf-8"), aad.encode("utf-8")
        )
        return (
            f"{base64.b64encode(nonce).decode('ascii')}."
            f"{base64.b64encode(payload).decode('ascii')}"
        )

    def unseal(self, token: str, *, aad: str = "") -> str:
        try:
            nonce_b64, payload_b64 = token.split(".", 1)
            nonce = base64.b64decode(nonce_b64, validate=True)
            payload = base64.b64decode(payload_b64, validate=True)
            return self._aead.decrypt(
                nonce, payload, aad.encode("utf-8")
            ).decode("utf-8")
        except (ValueError, InvalidTag, UnicodeDecodeError) as exc:
            raise CredentialError("凭据解密失败：密文或主密钥不匹配") from exc


class CredentialStore:
    """按 tenant_key 保存云认证字典；节点 SSH 密码用 seal_text/unseal_text。"""

    def __init__(self, path: str | os.PathLike[str], sealer: Sealer):
        self.path = Path(path)
        self.sealer = sealer
        self._records: dict[str, str] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str], sealer: Sealer) -> "CredentialStore":
        store = cls(path, sealer)
        raw = read_json_object(store.path)
        store._records = {
            str(key): str(value) for key, value in (raw.get("credentials") or {}).items()
        }
        return store

    @classmethod
    def from_env(
        cls,
        path: str | os.PathLike[str],
        *,
        env: Mapping[str, str] | None = None,
        env_var: str = "MIGRATION_SECRET_KEY",
    ) -> "CredentialStore":
        environment = os.environ if env is None else env
        raw = str(environment.get(env_var, ""))
        if not raw.strip():
            raise CredentialError(f"未配置 {env_var}，常驻中转机模式拒绝启动")
        return cls.load(path, Sealer(parse_secret(raw, source=env_var)))

    def save(self, tenant_key: str, auth: dict[str, Any]) -> None:
        """加密写入单个租户凭据（内存态），由 flush() 落盘。"""
        plaintext = json.dumps(auth, ensure_ascii=False, sort_keys=True)
        self._records[str(tenant_key)] = self.sealer.seal(
            plaintext, aad=str(tenant_key)
        )

    def get(self, tenant_key: str) -> dict[str, Any] | None:
        token = self._records.get(str(tenant_key))
        if token is None:
            return None
        return json.loads(self.sealer.unseal(token, aad=str(tenant_key)))

    def delete(self, tenant_key: str) -> bool:
        return self._records.pop(str(tenant_key), None) is not None

    def tenants(self) -> list[str]:
        return sorted(self._records)

    def seal_text(self, plaintext: str, *, aad: str) -> str:
        return self.sealer.seal(plaintext, aad=aad)

    def unseal_text(self, token: str, *, aad: str) -> str:
        return self.sealer.unseal(token, aad=aad)

    def flush(self) -> None:
        """原子落盘，0600。"""
        atomic_write_json(
            self.path,
            {"credentials": self._records},
            prefix=".relay-credentials-",
        )
