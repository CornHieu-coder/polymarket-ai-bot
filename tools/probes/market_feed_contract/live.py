"""Bounded public-market capture for IP-001.

The network surface is intentionally closed: one fixed public market WebSocket,
one fixed public REST GET endpoint, no redirects, no proxies, no credentials,
and no generic request interface.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, SecurityError

from .core import (
    ContractAnalyzer,
    EvidenceStore,
    EvidenceWriteError,
    PROBE_VERSION,
    evidence_manifest,
    utc_now,
    write_json_once,
    write_summary,
)
from .report import render_report, write_report


REPO_ROOT = Path(__file__).resolve().parents[3]
PUBLIC_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PUBLIC_BOOK_URL = "https://clob.polymarket.com/book"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "market-feed-contract"
REPORT_PATH = REPO_ROOT / "docs" / "experiments" / "market-feed-contract-probe.md"
BLOCKED_REQUEST_HEADERS = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
    "poly-address",
    "poly-api-key",
    "poly-passphrase",
    "poly-signature",
}


class FixedEndpointConnect(connect):
    """websockets connector that refuses every handshake redirect."""

    def process_redirect(self, exc: Exception) -> Exception | str:
        result = super().process_redirect(exc)
        if isinstance(result, str):
            return SecurityError(f"WebSocket redirect refused by probe: {result}")
        return result


@dataclass(frozen=True)
class ProbeConfig:
    token_ids: tuple[str, ...]
    duration_seconds: int = 180
    rest_interval_seconds: int = 15
    output_root: Path = DEFAULT_OUTPUT_ROOT
    sample_note: str = "Explicit token IDs supplied to the probe CLI."
    offline_test_command: str = (
        "python -m unittest discover -s tests/probes/market_feed_contract "
        "-p 'test_*.py' -v"
    )
    offline_test_result: str = "Not supplied to capture command."
    reconnect_pause_seconds: int = 1
    heartbeat_interval_seconds: int = 10
    heartbeat_timeout_seconds: int = 12
    rest_timeout_seconds: int = 8

    def validate(self) -> None:
        if not 1 <= len(self.token_ids) <= 10:
            raise ValueError("supply between 1 and 10 explicit token IDs")
        if len(set(self.token_ids)) != len(self.token_ids):
            raise ValueError("token IDs must be unique")
        if any(not token.isdigit() or len(token) > 100 for token in self.token_ids):
            raise ValueError("each token ID must be a decimal identifier of at most 100 digits")
        if not 30 <= self.duration_seconds <= 900:
            raise ValueError("duration must be between 30 and 900 seconds")
        if not 5 <= self.rest_interval_seconds <= 120:
            raise ValueError("REST interval must be between 5 and 120 seconds")
        resolved_output = self.output_root.resolve()
        ignored_root = (REPO_ROOT / "outputs").resolve()
        if not resolved_output.is_relative_to(ignored_root):
            raise ValueError(f"output root must remain under ignored path {ignored_root}")


def market_subscription(token_ids: Sequence[str]) -> dict[str, Any]:
    """Return the sole non-heartbeat message the probe may send."""

    return {
        "assets_ids": list(token_ids),
        "type": "market",
        "initial_dump": True,
        "level": 2,
        "custom_feature_enabled": False,
    }


def validate_public_request(request: httpx.Request) -> None:
    """Fail closed unless a request is exactly public REST GET /book."""

    url = request.url
    if request.method != "GET":
        raise SecurityError(f"HTTP method is not allowed: {request.method}")
    if url.scheme != "https" or url.host != "clob.polymarket.com" or url.path != "/book":
        raise SecurityError(f"HTTP endpoint is not allowed: {url.copy_with(query=None)}")
    if url.username or url.password:
        raise SecurityError("URL user information is forbidden")
    query_keys = set(url.params.keys())
    if query_keys != {"token_id"} or len(url.params.get_list("token_id")) != 1:
        raise SecurityError("REST /book requires exactly one token_id query parameter")
    forbidden = BLOCKED_REQUEST_HEADERS.intersection(
        {name.lower() for name in request.headers.keys()}
    )
    if forbidden:
        raise SecurityError(f"credential-bearing headers are forbidden: {sorted(forbidden)}")


async def _validate_request_hook(request: httpx.Request) -> None:
    validate_public_request(request)


def public_http_client(timeout_seconds: int) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=False,
        trust_env=False,
        event_hooks={"request": [_validate_request_hook]},
    )


def _run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def _git_output(*args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def software_provenance() -> dict[str, Any]:
    status = _git_output("status", "--porcelain")
    return {
        "probe_version": PROBE_VERSION,
        "git_commit": _git_output("rev-parse", "HEAD"),
        "git_dirty": None if status is None else bool(status),
        "python": sys.version.split()[0],
        "httpx": importlib.metadata.version("httpx"),
        "websockets": importlib.metadata.version("websockets"),
    }


def _elapsed_ms(start_ns: int, end_ns: int) -> str:
    return format(Decimal(end_ns - start_ns) / Decimal(1_000_000), "f")


async def fetch_public_book(
    client: httpx.AsyncClient,
    store: EvidenceStore,
    analyzer: ContractAnalyzer,
    token_id: str,
) -> None:
    before_version = analyzer.state_versions[token_id]
    before_session = analyzer.current_session
    before_state = analyzer.states.get(token_id)
    before_valid = before_state is not None and before_state.valid
    started_at = utc_now()
    started_ns = time.monotonic_ns()
    try:
        response = await client.get(PUBLIC_BOOK_URL, params={"token_id": token_id})
    except httpx.TimeoutException as exc:
        control = store.record_control(
            "rest_timeout",
            token_id=token_id,
            method="GET",
            endpoint=PUBLIC_BOOK_URL,
            request_started_at=started_at,
            error=type(exc).__name__,
        )
        analyzer.observe_rest_transport_failure(
            ingest_sequence=int(control["ingest_sequence"]),
            token_id=token_id,
            error_kind="rest_timeout",
            message=str(exc),
        )
        return
    except httpx.HTTPError as exc:
        control = store.record_control(
            "rest_transport_error",
            token_id=token_id,
            method="GET",
            endpoint=PUBLIC_BOOK_URL,
            request_started_at=started_at,
            error=type(exc).__name__,
            message=str(exc),
        )
        analyzer.observe_rest_transport_failure(
            ingest_sequence=int(control["ingest_sequence"]),
            token_id=token_id,
            error_kind="rest_transport_error",
            message=str(exc),
        )
        return

    # The public endpoint may set an operational cookie. Retain no server cookie
    # in the client jar, so the next request remains credential-free and the
    # request guard continues to reject any outbound Cookie header.
    client.cookies.clear()
    ended_ns = time.monotonic_ns()
    after_version = analyzer.state_versions[token_id]
    after_session = analyzer.current_session
    after_state = analyzer.states.get(token_id)
    after_valid = after_state is not None and after_state.valid
    record, decoded = store.record_rest(
        token_id=token_id,
        url=str(response.request.url),
        request_started_at=started_at,
        received_at=utc_now(),
        status_code=response.status_code,
        raw_response=response.content,
        elapsed_ms=_elapsed_ms(started_ns, ended_ns),
        state_version_before=before_version,
        state_version_after=after_version,
        state_session_before=before_session,
        state_session_after=after_session,
        state_valid_before=before_valid,
        state_valid_after=after_valid,
    )
    analyzer.observe_rest(record, decoded)


async def _rest_poller(
    client: httpx.AsyncClient,
    store: EvidenceStore,
    analyzer: ContractAnalyzer,
    config: ProbeConfig,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        for token_id in config.token_ids:
            if stop.is_set():
                return
            await fetch_public_book(client, store, analyzer, token_id)
        try:
            await asyncio.wait_for(stop.wait(), timeout=config.rest_interval_seconds)
        except TimeoutError:
            pass


async def _cancel_background(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _run_websocket_session(
    *,
    session_id: str,
    duration_seconds: float,
    store: EvidenceStore,
    analyzer: ContractAnalyzer,
    config: ProbeConfig,
    fatal_task: asyncio.Task[None],
) -> bool:
    analyzer.start_session(session_id)
    store.record_control(
        "websocket_session_started",
        session_id=session_id,
        endpoint=PUBLIC_WS_URL,
        token_ids=list(config.token_ids),
        initial_dump=True,
    )
    controlled_disconnect = False
    disconnect_error: str | None = None
    try:
        async with FixedEndpointConnect(
            PUBLIC_WS_URL,
            proxy=None,
            ping_interval=None,
            ping_timeout=None,
            open_timeout=10,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
            max_queue=1024,
        ) as websocket:
            subscription = market_subscription(config.token_ids)
            subscription_text = json.dumps(
                subscription, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            await websocket.send(subscription_text)
            store.record_control(
                "market_subscription_sent",
                session_id=session_id,
                payload=subscription_text,
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + duration_seconds
            last_ping = loop.time()
            ping_pending_since: float | None = None

            while loop.time() < deadline:
                if fatal_task.done():
                    await fatal_task
                now = loop.time()
                if now - last_ping >= config.heartbeat_interval_seconds:
                    await websocket.send("PING")
                    last_ping = now
                    ping_pending_since = now
                    store.record_control(
                        "heartbeat_sent", session_id=session_id, payload="PING"
                    )
                if (
                    ping_pending_since is not None
                    and now - ping_pending_since >= config.heartbeat_timeout_seconds
                ):
                    disconnect_error = "heartbeat_timeout"
                    control = store.record_control(
                        "heartbeat_timeout",
                        session_id=session_id,
                        timeout_seconds=config.heartbeat_timeout_seconds,
                    )
                    analyzer.observe_operational_failure(
                        ingest_sequence=int(control["ingest_sequence"]),
                        code="heartbeat_timeout",
                        message=f"no PONG within {config.heartbeat_timeout_seconds} seconds",
                        details={"session_id": session_id},
                    )
                    break

                timeout = min(1.0, max(0.01, deadline - loop.time()))
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except TimeoutError:
                    continue
                record, decoded = store.record_websocket(
                    raw,
                    session_id=session_id,
                    token_ids=config.token_ids,
                    received_at=utc_now(),
                    received_monotonic_ns=time.monotonic_ns(),
                )
                if raw == "PONG" or raw == b"PONG":
                    ping_pending_since = None
                errors = analyzer.observe_websocket(record, decoded)
                for error in errors:
                    store.record_control(
                        "analysis_error",
                        source_ingest_sequence=record["ingest_sequence"],
                        analysis=error,
                    )

            if disconnect_error is None:
                controlled_disconnect = True
                store.record_control(
                    "controlled_disconnect_requested", session_id=session_id
                )
    except ConnectionClosed as exc:
        disconnect_error = f"{type(exc).__name__}: {exc}"
        control = store.record_control(
            "websocket_disconnect",
            session_id=session_id,
            error=disconnect_error,
        )
        analyzer.observe_operational_failure(
            ingest_sequence=int(control["ingest_sequence"]),
            code="websocket_disconnect",
            message=disconnect_error,
            details={"session_id": session_id},
        )
    except (OSError, TimeoutError) as exc:
        disconnect_error = f"{type(exc).__name__}: {exc}"
        control = store.record_control(
            "websocket_connection_error",
            session_id=session_id,
            error=disconnect_error,
        )
        analyzer.observe_operational_failure(
            ingest_sequence=int(control["ingest_sequence"]),
            code="websocket_connection_error",
            message=disconnect_error,
            details={"session_id": session_id},
        )
    finally:
        analyzer.end_session(
            session_id,
            controlled_disconnect=controlled_disconnect,
            disconnect_error=disconnect_error,
        )
        store.record_control(
            "websocket_session_ended",
            session_id=session_id,
            controlled_disconnect=controlled_disconnect,
            disconnect_error=disconnect_error,
        )
    return controlled_disconnect


async def capture(config: ProbeConfig) -> dict[str, Any]:
    config.validate()
    run_id = _run_id()
    provenance = software_provenance()
    initial_config = {
        "run_id": run_id,
        "created_at": utc_now(),
        "mode": "explicit_token_ids",
        "configuration": {
            **asdict(config),
            "output_root": str(config.output_root.resolve()),
        },
        "software": provenance,
        "network_policy": {
            "websocket": PUBLIC_WS_URL,
            "websocket_redirects": False,
            "websocket_proxy": False,
            "websocket_outbound_messages": ["market subscription", "PING heartbeat"],
            "rest": PUBLIC_BOOK_URL,
            "rest_methods": ["GET"],
            "rest_redirects": False,
            "rest_environment_proxy_or_credentials": False,
            "authenticated_operations": 0,
            "trading_operations": 0,
        },
    }
    store = EvidenceStore(config.output_root, run_id, initial_config)
    analyzer = ContractAnalyzer(config.token_ids)
    started_at = utc_now()
    started_ns = time.monotonic_ns()
    completed = False
    stop_rest = asyncio.Event()
    rest_task: asyncio.Task[None] | None = None
    try:
        store.write_resolved_config(initial_config)
        store.record_control("run_started", run_id=run_id)
        async with public_http_client(config.rest_timeout_seconds) as client:
            rest_task = asyncio.create_task(
                _rest_poller(client, store, analyzer, config, stop_rest),
                name="public-rest-book-poller",
            )
            session_duration = (
                config.duration_seconds - config.reconnect_pause_seconds
            ) / 2
            await _run_websocket_session(
                session_id="session-001",
                duration_seconds=session_duration,
                store=store,
                analyzer=analyzer,
                config=config,
                fatal_task=rest_task,
            )
            store.record_control(
                "controlled_reconnect_pause",
                duration_seconds=config.reconnect_pause_seconds,
            )
            await asyncio.sleep(config.reconnect_pause_seconds)
            await _run_websocket_session(
                session_id="session-002",
                duration_seconds=session_duration,
                store=store,
                analyzer=analyzer,
                config=config,
                fatal_task=rest_task,
            )
            stop_rest.set()
            await rest_task
        completed = True
        store.record_control("run_completed", run_id=run_id)
    except EvidenceWriteError:
        stop_rest.set()
        await _cancel_background(rest_task)
        raise
    except Exception as exc:
        stop_rest.set()
        await _cancel_background(rest_task)
        try:
            store.record_control(
                "run_aborted", run_id=run_id, error=f"{type(exc).__name__}: {exc}"
            )
        except EvidenceWriteError:
            pass
        raise
    finally:
        store.close()

    ended_ns = time.monotonic_ns()
    ended_at = utc_now()
    manifest = evidence_manifest(
        store.run_dir,
        (
            "run-config.json",
            "resolved-config.json",
            "raw-websocket.jsonl",
            "raw-rest-book.jsonl",
            "control-events.jsonl",
        ),
    )
    write_json_once(store.run_dir / "evidence-manifest.json", manifest)
    run = {
        "run_id": run_id,
        "completed": completed,
        "started_at": started_at,
        "ended_at": ended_at,
        "requested_duration_seconds": config.duration_seconds,
        "actual_duration_seconds": format(
            Decimal(ended_ns - started_ns) / Decimal(1_000_000_000), "f"
        ),
        "token_ids": list(config.token_ids),
        "sample_note": config.sample_note,
        "offline_test_command": config.offline_test_command,
        "offline_test_result": config.offline_test_result,
        "rest_interval_seconds": config.rest_interval_seconds,
        "controlled_reconnects": 1,
        "initial_dump": True,
        "websocket_level": 2,
        "raw_boundary": "WebSocket application text/bytes after protocol decompression",
        "raw_evidence_path": str(store.run_dir.relative_to(REPO_ROOT)),
        "software": provenance,
    }
    summary = analyzer.summary(
        run,
        {
            "run_directory": str(store.run_dir.relative_to(REPO_ROOT)),
            "records": dict(sorted(store.counts.items())),
            "manifest": manifest,
        },
    )
    summary_path = store.run_dir / "summary.json"
    write_summary(summary_path, summary)
    report = render_report(summary)
    write_report(REPORT_PATH, report)
    return {
        "run_id": run_id,
        "run_directory": str(store.run_dir),
        "summary_path": str(summary_path),
        "report_path": str(REPORT_PATH),
        "statuses": {f"Q{index}": summary[f"q{index}"]["status"] for index in range(1, 9)},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.probes.market_feed_contract",
        description="Bounded public read-only Polymarket market-feed contract probe.",
    )
    parser.add_argument(
        "--token-id",
        action="append",
        required=True,
        dest="token_ids",
        help="Explicit public CLOB outcome token ID; repeat for each sampled asset.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=180,
        help="Total bounded capture duration (30-900; default: 180).",
    )
    parser.add_argument(
        "--rest-interval-seconds",
        type=int,
        default=15,
        help="Seconds between REST /book polling rounds (5-120; default: 15).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Raw evidence root, which must remain beneath the repository's ignored outputs/ path.",
    )
    parser.add_argument(
        "--sample-note",
        default="Explicit token IDs supplied to the probe CLI.",
        help="Non-semantic provenance note describing how explicit token IDs were selected.",
    )
    parser.add_argument(
        "--offline-test-result",
        default="Not supplied to capture command.",
        help="Recorded result of the offline test run completed before capture.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ProbeConfig(
        token_ids=tuple(args.token_ids),
        duration_seconds=args.duration_seconds,
        rest_interval_seconds=args.rest_interval_seconds,
        output_root=args.output_root,
        sample_note=args.sample_note,
        offline_test_result=args.offline_test_result,
    )
    try:
        result = asyncio.run(capture(config))
    except (ValueError, EvidenceWriteError, SecurityError) as exc:
        print(f"probe failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0
