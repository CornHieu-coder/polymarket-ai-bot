from __future__ import annotations

import ast
import importlib
import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus, SecurityError
from websockets.http11 import Response

from tools.probes.market_feed_contract import live
from tools.probes.market_feed_contract.core import ContractAnalyzer, EvidenceStore


PACKAGE = Path(live.__file__).parent


class PublicBoundaryTests(unittest.TestCase):
    def test_only_exact_public_get_book_request_is_allowed(self) -> None:
        allowed = httpx.Request(
            "GET", live.PUBLIC_BOOK_URL, params={"token_id": "123"}
        )
        live.validate_public_request(allowed)
        for request in (
            httpx.Request("POST", live.PUBLIC_BOOK_URL, params={"token_id": "123"}),
            httpx.Request("GET", "https://example.com/book", params={"token_id": "123"}),
            httpx.Request("GET", live.PUBLIC_BOOK_URL, params={"token_id": "123", "x": "1"}),
            httpx.Request(
                "GET",
                live.PUBLIC_BOOK_URL,
                params={"token_id": "123"},
                headers={"Authorization": "forbidden"},
            ),
        ):
            with self.assertRaises(SecurityError):
                live.validate_public_request(request)

    def test_market_subscription_has_no_authentication_surface(self) -> None:
        payload = live.market_subscription(["123", "456"])
        self.assertEqual(payload["type"], "market")
        self.assertTrue(payload["initial_dump"])
        serialized = json.dumps(payload).lower()
        for forbidden in ("auth", "apikey", "secret", "passphrase", "wallet"):
            self.assertNotIn(forbidden, serialized)

    def test_websocket_redirect_is_rejected(self) -> None:
        response = Response(
            302,
            "Found",
            Headers([("Location", "wss://example.com/ws")]),
        )
        connector = live.FixedEndpointConnect(live.PUBLIC_WS_URL, proxy=None)
        result = connector.process_redirect(InvalidStatus(response))
        self.assertIsInstance(result, SecurityError)

    def test_cli_exposes_no_endpoint_header_or_credential_options(self) -> None:
        option_names = {
            option
            for action in live.build_parser()._actions
            for option in action.option_strings
        }
        joined = " ".join(sorted(option_names)).lower()
        for forbidden in (
            "url",
            "endpoint",
            "header",
            "api-key",
            "secret",
            "wallet",
            "private-key",
        ):
            self.assertNotIn(forbidden, joined)

    def test_probe_sources_have_no_trading_client_imports_or_write_verbs(self) -> None:
        forbidden_import_roots = {
            "py_clob_client",
            "py_clob_client_v2",
            "web3",
            "eth_account",
        }
        forbidden_calls = {"post", "put", "patch", "delete", "request"}
        async_client_constructors = 0
        for path in PACKAGE.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    roots = {alias.name.split(".")[0] for alias in node.names}
                    self.assertTrue(roots.isdisjoint(forbidden_import_roots), (path, roots))
                elif isinstance(node, ast.ImportFrom) and node.module:
                    self.assertNotIn(node.module.split(".")[0], forbidden_import_roots)
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    self.assertNotIn(node.func.attr, forbidden_calls, (path, node.lineno))
                    if (
                        isinstance(node.func.value, ast.Name)
                        and node.func.value.id == "httpx"
                        and node.func.attr == "AsyncClient"
                    ):
                        async_client_constructors += 1
        self.assertEqual(async_client_constructors, 1)

    def test_only_allowlisted_network_url_literals_exist_in_live_module(self) -> None:
        tree = ast.parse(Path(live.__file__).read_text(encoding="utf-8"))
        urls = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.startswith(("https://", "wss://"))
        }
        self.assertEqual(urls, {live.PUBLIC_BOOK_URL, live.PUBLIC_WS_URL})

    def test_probe_dependencies_are_transport_only(self) -> None:
        lines = {
            line.strip()
            for line in (PACKAGE / "requirements-probe.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        self.assertEqual(lines, {"httpx==0.28.1", "websockets==16.0"})

    def test_import_has_no_network_side_effect(self) -> None:
        with patch.object(socket.socket, "connect", side_effect=AssertionError("network on import")):
            importlib.reload(live)


class HttpClientConfigurationTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_disables_redirects_and_environment_trust(self) -> None:
        client = live.public_http_client(3)
        try:
            self.assertFalse(client.follow_redirects)
            self.assertFalse(client._trust_env)
            self.assertEqual(client.event_hooks["request"], [live._validate_request_hook])
        finally:
            await client.aclose()

    async def test_fetch_path_emits_only_public_get_without_credentials(self) -> None:
        seen: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.url.host, "clob.polymarket.com")
            self.assertEqual(request.url.path, "/book")
            for blocked in live.BLOCKED_REQUEST_HEADERS:
                self.assertNotIn(blocked, request.headers)
            return httpx.Response(
                200,
                json={
                    "market": "fixture",
                    "asset_id": "123",
                    "timestamp": "1",
                    "hash": "fixture",
                    "bids": [],
                    "asks": [],
                    "min_order_size": "1",
                    "tick_size": "0.01",
                    "neg_risk": False,
                    "last_trade_price": "0.5",
                },
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            event_hooks={"request": [live._validate_request_hook]},
        ) as client:
            with tempfile.TemporaryDirectory() as temporary:
                store = EvidenceStore(Path(temporary), "run", {})
                analyzer = ContractAnalyzer(["123"])
                analyzer.start_session("session-001")
                await live.fetch_public_book(client, store, analyzer, "123")
                store.close()
        self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
