from __future__ import annotations

import ast
import unittest
from pathlib import Path

from tools.probes.pmxt_ordering_audit.download import ARCHIVE_HOST


PACKAGE = Path(__file__).parents[3] / "tools" / "probes" / "pmxt_ordering_audit"


class SafetyBoundaryTests(unittest.TestCase):
    def test_only_fixed_public_archive_host_is_present(self) -> None:
        self.assertEqual(ARCHIVE_HOST, "r2v2.pmxt.dev")
        source = (PACKAGE / "download.py").read_text(encoding="utf-8")
        self.assertNotIn("Authorization", source)
        self.assertNotIn("api_key", source.lower())
        self.assertNotIn("wallet", source.lower())
        self.assertNotIn("POST", source)

    def test_probe_python_has_no_trading_client_imports(self) -> None:
        forbidden = {
            "py_clob_client",
            "web3",
            "eth_account",
            "polymarket",
        }
        for path in PACKAGE.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module.split(".")[0])
            self.assertTrue(imports.isdisjoint(forbidden), f"{path}: {imports & forbidden}")


if __name__ == "__main__":
    unittest.main()
