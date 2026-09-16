"""下发给中转机的三个文件必须自洽。

bootstrap 脚本只把 relay_protocol.py / relay_transfer.py / relay_agent.py
拷到中转机的 /opt/relay，平台侧模块（env_utils、json_store 等）不在包里。
一旦这三个文件引用了平台模块，中转机上的 agent 会在启动时 ImportError，
表现为"注册不上 / 建机后一直没回连"——线上最难查的一类故障。
"""
import ast
import sys
import unittest
from pathlib import Path

from relay_api import AGENT_PACKAGE_FILES

ROOT = Path(__file__).resolve().parent.parent
STDLIB = set(sys.stdlib_module_names)


class AgentPackageSelfContainedTest(unittest.TestCase):
    def _imported_modules(self, path):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        modules = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level:  # 相对导入 = 平台内的包结构，包外必然失败
                    modules.add("." * node.level + (node.module or ""))
                elif node.module:
                    modules.add(node.module.split(".")[0])
        return modules

    def test_shipped_files_only_use_stdlib_and_each_other(self):
        allowed_local = {Path(name).stem for name in AGENT_PACKAGE_FILES}
        for name in AGENT_PACKAGE_FILES:
            path = ROOT / name
            self.assertTrue(path.exists(), f"缺少下发文件 {name}")
            for module in self._imported_modules(path):
                if module in allowed_local:
                    continue
                self.assertIn(
                    module,
                    STDLIB,
                    f"{name} 引用了非标准库模块 {module!r}：它不会随 agent 包下发到"
                    "中转机，会在启动时报 ImportError。",
                )

    def test_every_relay_dependency_is_whitelisted(self):
        """agent 侧文件之间的 import 必须都在白名单里，否则会漏发文件。"""
        local = {Path(name).stem for name in AGENT_PACKAGE_FILES}
        for name in AGENT_PACKAGE_FILES:
            for module in self._imported_modules(ROOT / name):
                if module.startswith("relay_"):
                    self.assertIn(
                        module,
                        local,
                        f"{name} 依赖 {module}，但它不在 AGENT_PACKAGE_FILES 白名单里",
                    )


if __name__ == "__main__":
    unittest.main()
