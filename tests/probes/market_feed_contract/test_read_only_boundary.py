from __future__ import annotations

import ast
import asyncio
import hashlib
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


def public_book_fixture(token_id: str = "123") -> dict[str, object]:
    return {
        "market": "fixture",
        "asset_id": token_id,
        "timestamp": "1",
        "hash": "fixture",
        "bids": [],
        "asks": [],
        "min_order_size": "1",
        "tick_size": "0.01",
        "neg_risk": False,
        "last_trade_price": "0.5",
    }


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class PublicBoundaryTests(unittest.TestCase):
    def test_explicit_baseline_summary_loader_preserves_content_digest(self) -> None:
        raw = b'{"run":{"run_id":"historical"},"q1":{"status":"UNRESOLVED"}}\n'
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "summary.json"
            path.write_bytes(raw)
            summary, digest = live.load_baseline_summary(path)
        self.assertEqual(summary["run"]["run_id"], "historical")
        self.assertEqual(digest, hashlib.sha256(raw).hexdigest())

    def test_baseline_summary_must_remain_under_ignored_outputs(self) -> None:
        config = live.ProbeConfig(
            token_ids=("123",), baseline_summary_path=Path(__file__)
        )
        with self.assertRaisesRegex(ValueError, "baseline summary must remain"):
            config.validate()

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

    def test_market_subscription_is_exact_minimal_public_payload(self) -> None:
        payload = live.market_subscription(["123", "456"])
        self.assertEqual(
            payload,
            {"assets_ids": ["123", "456"], "type": "market"},
        )
        metadata = live.market_subscription_metadata(["123", "456"])
        self.assertEqual(metadata["market_subscription_payload"], payload)
        self.assertEqual(metadata["market_subscription_fields"], ["assets_ids", "type"])
        self.assertTrue(metadata["assets_ids_field_sent"])
        self.assertTrue(metadata["type_field_sent"])
        self.assertFalse(metadata["initial_dump_field_sent"])
        self.assertFalse(metadata["level_field_sent"])
        self.assertFalse(metadata["custom_feature_enabled_field_sent"])
        serialized = json.dumps(payload).lower()
        for forbidden in (
            "auth",
            "apikey",
            "secret",
            "passphrase",
            "wallet",
            "initial_dump",
            "level",
            "custom_feature_enabled",
        ):
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
                headers={"Set-Cookie": "operational=discard-me; Path=/"},
                json=public_book_fixture(),
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
                context = {
                    "request_purpose": "empty_side_boundary_candidate",
                    "candidate_id": "fixture-candidate",
                }
                await live.fetch_public_book(
                    client,
                    store,
                    analyzer,
                    "123",
                    request_context=context,
                )
                await live.fetch_public_book(client, store, analyzer, "123")
                run_dir = store.run_dir
                store.close()
                records = read_jsonl(run_dir / "raw-rest-book.jsonl")
        self.assertEqual(len(seen), 2)
        self.assertTrue(all("cookie" not in request.headers for request in seen))
        self.assertEqual(list(client.cookies), [])
        self.assertEqual(records[0]["request_context"], context)
        self.assertEqual(
            records[1]["request_context"], {"request_purpose": "periodic_poll"}
        )

    async def test_targeted_empty_side_reads_run_concurrently_and_retain_context(self) -> None:
        request_started = asyncio.Event()
        release_response = asyncio.Event()

        async def handler(request: httpx.Request) -> httpx.Response:
            request_started.set()
            await release_response.wait()
            return httpx.Response(200, json=public_book_fixture())

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
                context = {
                    "request_purpose": "empty_side_boundary_candidate",
                    "candidate_id": "1:0:best_ask",
                    "token_id": "123",
                }
                analyzer.pending_empty_side_probe_requests.append(context)
                tasks: set[asyncio.Task[None]] = set()
                live._schedule_empty_side_probe_requests(
                    client=client,
                    store=store,
                    analyzer=analyzer,
                    allowed_token_ids=("123",),
                    tasks=tasks,
                )
                self.assertEqual(len(tasks), 1)
                await asyncio.wait_for(request_started.wait(), timeout=1)
                self.assertFalse(next(iter(tasks)).done())
                self.assertEqual(analyzer.take_empty_side_probe_requests(), [])
                release_response.set()
                await live._drain_targeted_rest_tasks(tasks)
                self.assertEqual(tasks, set())
                run_dir = store.run_dir
                store.close()
                records = read_jsonl(run_dir / "raw-rest-book.jsonl")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["request_context"], context)

    async def test_targeted_read_cannot_expand_explicit_token_scope(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.fail(f"unexpected network request: {request.url}")

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
                context = {
                    "request_purpose": "empty_side_boundary_candidate",
                    "candidate_id": "out-of-scope",
                    "token_id": "999",
                }
                analyzer.pending_empty_side_probe_requests.append(context)
                tasks: set[asyncio.Task[None]] = set()
                live._schedule_empty_side_probe_requests(
                    client=client,
                    store=store,
                    analyzer=analyzer,
                    allowed_token_ids=("123",),
                    tasks=tasks,
                )
                self.assertEqual(tasks, set())
                run_dir = store.run_dir
                store.close()
                control = read_jsonl(run_dir / "control-events.jsonl")

        self.assertEqual(len(control), 1)
        self.assertEqual(control[0]["event"], "targeted_rest_request_rejected")
        self.assertEqual(control[0]["details"]["request_context"], context)

    async def test_rest_failures_and_non_2xx_retain_request_context(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            token_id = request.url.params["token_id"]
            if token_id == "111":
                raise httpx.ReadTimeout("fixture timeout", request=request)
            if token_id == "222":
                return httpx.Response(503, content=b"temporarily unavailable")
            raise httpx.ConnectError("fixture failure", request=request)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            event_hooks={"request": [live._validate_request_hook]},
        ) as client:
            with tempfile.TemporaryDirectory() as temporary:
                store = EvidenceStore(Path(temporary), "run", {})
                analyzer = ContractAnalyzer(["111", "222", "333"])
                analyzer.start_session("session-001")
                contexts = {
                    token: {
                        "request_purpose": "empty_side_boundary_candidate",
                        "candidate_id": f"candidate-{token}",
                        "token_id": token,
                    }
                    for token in ("111", "222", "333")
                }
                for token_id in ("111", "222", "333"):
                    await live.fetch_public_book(
                        client,
                        store,
                        analyzer,
                        token_id,
                        request_context=contexts[token_id],
                    )
                run_dir = store.run_dir
                store.close()
                control = read_jsonl(run_dir / "control-events.jsonl")
                rest = read_jsonl(run_dir / "raw-rest-book.jsonl")

        details_by_event = {
            str(record["event"]): record["details"]
            for record in control
            if record["event"] in {"rest_timeout", "rest_non_2xx", "rest_transport_error"}
        }
        self.assertEqual(details_by_event["rest_timeout"]["request_context"], contexts["111"])
        self.assertEqual(details_by_event["rest_non_2xx"]["request_context"], contexts["222"])
        self.assertEqual(
            details_by_event["rest_transport_error"]["request_context"], contexts["333"]
        )
        self.assertEqual(len(rest), 1)
        self.assertEqual(rest[0]["status_code"], 503)
        self.assertEqual(rest[0]["request_context"], contexts["222"])


if __name__ == "__main__":
    unittest.main()
