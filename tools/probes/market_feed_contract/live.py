"""Bounded public-market capture for IP-001.

The network surface is intentionally closed: one fixed public market WebSocket,
one fixed public REST GET endpoint, no redirects, no proxies, no credentials,
and no generic request interface.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
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
from typing import Any, Mapping, Sequence

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
from .report import render_comparative_report, render_report, write_report


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
    baseline_summary_path: Path | None = None

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
        if self.baseline_summary_path is not None:
            resolved_baseline = self.baseline_summary_path.resolve()
            if not resolved_baseline.is_relative_to(ignored_root):
                raise ValueError(
                    f"baseline summary must remain under ignored path {ignored_root}"
                )
            if not resolved_baseline.is_file():
                raise ValueError(f"baseline summary does not exist: {resolved_baseline}")


@dataclass(frozen=True)
class WebSocketSessionResult:
    connection_opened: bool
    subscription_sent: bool
    controlled_disconnect: bool


def market_subscription(token_ids: Sequence[str]) -> dict[str, Any]:
    """Return the sole non-heartbeat message the probe may send."""

    return {
        "assets_ids": list(token_ids),
        "type": "market",
    }


def market_subscription_metadata(token_ids: Sequence[str]) -> dict[str, Any]:
    """Describe the exact subscription sent without asserting omitted defaults."""

    payload = market_subscription(token_ids)
    return {
        "market_subscription_payload": payload,
        "market_subscription_fields": list(payload),
        "assets_ids_field_sent": True,
        "type_field_sent": True,
        "initial_dump_field_sent": False,
        "level_field_sent": False,
        "custom_feature_enabled_field_sent": False,
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


def load_baseline_summary(path: Path) -> tuple[dict[str, Any], str]:
    """Load one explicitly named ignored summary and return its content digest."""

    try:
        raw = path.read_bytes()
        decoded = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load baseline summary {path}: {exc}") from exc
    if not isinstance(decoded, dict) or not isinstance(decoded.get("run"), dict):
        raise ValueError("baseline summary must be a JSON object with a run object")
    return decoded, hashlib.sha256(raw).hexdigest()


def _elapsed_ms(start_ns: int, end_ns: int) -> str:
    return format(Decimal(end_ns - start_ns) / Decimal(1_000_000), "f")


async def fetch_public_book(
    client: httpx.AsyncClient,
    store: EvidenceStore,
    analyzer: ContractAnalyzer,
    token_id: str,
    *,
    request_context: Mapping[str, Any] | None = None,
) -> None:
    context = dict(request_context or {"request_purpose": "periodic_poll"})
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
            request_context=context,
        )
        analyzer.observe_rest_transport_failure(
            ingest_sequence=int(control["ingest_sequence"]),
            token_id=token_id,
            error_kind="rest_timeout",
            message=str(exc),
            request_context=context,
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
            request_context=context,
        )
        analyzer.observe_rest_transport_failure(
            ingest_sequence=int(control["ingest_sequence"]),
            token_id=token_id,
            error_kind="rest_transport_error",
            message=str(exc),
            request_context=context,
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
        request_context=context,
    )
    if response.status_code < 200 or response.status_code >= 300:
        store.record_control(
            "rest_non_2xx",
            token_id=token_id,
            method="GET",
            endpoint=PUBLIC_BOOK_URL,
            status_code=response.status_code,
            request_started_at=started_at,
            request_context=context,
            rest_ingest_sequence=record["ingest_sequence"],
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
            await fetch_public_book(
                client,
                store,
                analyzer,
                token_id,
                request_context={"request_purpose": "periodic_poll"},
            )
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


def _raise_completed_targeted_rest_tasks(tasks: set[asyncio.Task[None]]) -> None:
    """Surface completed targeted-request failures without blocking WS receive."""

    for task in tuple(tasks):
        if not task.done():
            continue
        tasks.remove(task)
        task.result()


async def _drain_targeted_rest_tasks(tasks: set[asyncio.Task[None]]) -> None:
    """Finish all issued public reads and surface failures before closing evidence."""

    if not tasks:
        return
    pending = tuple(tasks)
    results = await asyncio.gather(*pending, return_exceptions=True)
    tasks.difference_update(pending)
    for result in results:
        if isinstance(result, BaseException):
            raise result


def _schedule_empty_side_probe_requests(
    *,
    client: httpx.AsyncClient,
    store: EvidenceStore,
    analyzer: ContractAnalyzer,
    allowed_token_ids: Sequence[str],
    tasks: set[asyncio.Task[None]],
) -> None:
    """Issue analyzer-requested public reads concurrently with WS receiving."""

    allowed = set(allowed_token_ids)
    for request in analyzer.take_empty_side_probe_requests():
        context = dict(request)
        token_id = str(context.get("token_id", ""))
        if token_id not in allowed:
            control = store.record_control(
                "targeted_rest_request_rejected",
                reason="token_id_not_in_subscription",
                token_id=token_id,
                request_context=context,
            )
            analyzer.observe_operational_failure(
                ingest_sequence=int(control["ingest_sequence"]),
                code="targeted_rest_request_rejected",
                message="targeted REST token was not in the explicit subscription",
                details={"token_id": token_id, "request_context": context},
            )
            continue
        task = asyncio.create_task(
            fetch_public_book(
                client,
                store,
                analyzer,
                token_id,
                request_context=context,
            ),
            name=f"targeted-public-rest-book-{context.get('candidate_id', token_id)}",
        )
        tasks.add(task)


async def _run_websocket_session(
    *,
    session_id: str,
    duration_seconds: float,
    store: EvidenceStore,
    analyzer: ContractAnalyzer,
    config: ProbeConfig,
    client: httpx.AsyncClient,
    fatal_task: asyncio.Task[None],
) -> WebSocketSessionResult:
    analyzer.start_session(session_id)
    subscription_metadata = market_subscription_metadata(config.token_ids)
    store.record_control(
        "websocket_session_started",
        session_id=session_id,
        endpoint=PUBLIC_WS_URL,
        token_ids=list(config.token_ids),
        **subscription_metadata,
    )
    connection_opened = False
    subscription_sent = False
    controlled_disconnect = False
    disconnect_error: str | None = None
    targeted_rest_tasks: set[asyncio.Task[None]] = set()
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
            connection_opened = True
            subscription = market_subscription(config.token_ids)
            subscription_text = json.dumps(
                subscription, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            await websocket.send(subscription_text)
            subscription_sent = True
            store.record_control(
                "market_subscription_sent",
                session_id=session_id,
                payload=subscription_text,
                **subscription_metadata,
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + duration_seconds
            last_ping = loop.time()
            ping_pending_since: float | None = None

            while loop.time() < deadline:
                if fatal_task.done():
                    await fatal_task
                _raise_completed_targeted_rest_tasks(targeted_rest_tasks)
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
                _schedule_empty_side_probe_requests(
                    client=client,
                    store=store,
                    analyzer=analyzer,
                    allowed_token_ids=config.token_ids,
                    tasks=targeted_rest_tasks,
                )

            if disconnect_error is None:
                await _drain_targeted_rest_tasks(targeted_rest_tasks)
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
        await _drain_targeted_rest_tasks(targeted_rest_tasks)
        store.record_control(
            "websocket_session_ended",
            session_id=session_id,
            controlled_disconnect=controlled_disconnect,
            disconnect_error=disconnect_error,
        )
    return WebSocketSessionResult(
        connection_opened=connection_opened,
        subscription_sent=subscription_sent,
        controlled_disconnect=controlled_disconnect,
    )


async def capture(config: ProbeConfig) -> dict[str, Any]:
    config.validate()
    run_id = _run_id()
    provenance = software_provenance()
    subscription_metadata = market_subscription_metadata(config.token_ids)
    baseline_summary: dict[str, Any] | None = None
    baseline_provenance: dict[str, Any] = {}
    if config.baseline_summary_path is not None:
        baseline_path = config.baseline_summary_path.resolve()
        baseline_summary, baseline_digest = load_baseline_summary(baseline_path)
        baseline_provenance = {
            "baseline_run_id": str(baseline_summary["run"].get("run_id", "")),
            "baseline_summary_path": str(baseline_path.relative_to(REPO_ROOT)),
            "baseline_summary_sha256": baseline_digest,
        }
    initial_config = {
        "run_id": run_id,
        "created_at": utc_now(),
        "mode": "explicit_token_ids",
        "configuration": {
            **asdict(config),
            "output_root": str(config.output_root.resolve()),
            "baseline_summary_path": (
                str(config.baseline_summary_path.resolve())
                if config.baseline_summary_path is not None
                else None
            ),
        },
        "software": provenance,
        "baseline": baseline_provenance,
        "network_policy": {
            "websocket": PUBLIC_WS_URL,
            "websocket_redirects": False,
            "websocket_proxy": False,
            "websocket_outbound_messages": ["market subscription", "PING heartbeat"],
            **subscription_metadata,
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
    controlled_reconnects = 0
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
            first_session = await _run_websocket_session(
                session_id="session-001",
                duration_seconds=session_duration,
                store=store,
                analyzer=analyzer,
                config=config,
                client=client,
                fatal_task=rest_task,
            )
            store.record_control(
                "reconnect_pause",
                duration_seconds=config.reconnect_pause_seconds,
                controlled_trigger=first_session.controlled_disconnect,
            )
            await asyncio.sleep(config.reconnect_pause_seconds)
            second_session = await _run_websocket_session(
                session_id="session-002",
                duration_seconds=session_duration,
                store=store,
                analyzer=analyzer,
                config=config,
                client=client,
                fatal_task=rest_task,
            )
            if first_session.controlled_disconnect and second_session.subscription_sent:
                controlled_reconnects += 1
                store.record_control(
                    "controlled_reconnect_completed",
                    from_session_id="session-001",
                    to_session_id="session-002",
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
        "controlled_reconnects": controlled_reconnects,
        **subscription_metadata,
        "subscription_control": (
            "Minimal current first-party example payload; optional initial_dump, "
            "level, and custom_feature_enabled fields omitted so documented "
            "server defaults, rather than explicit probe controls, govern the run."
        ),
        **baseline_provenance,
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
    corrective_report_path = store.run_dir / "corrective-report.md"
    write_report(corrective_report_path, render_report(summary))
    report = (
        render_comparative_report(baseline_summary, summary)
        if baseline_summary is not None
        else render_report(summary)
    )
    write_report(REPORT_PATH, report)
    return {
        "run_id": run_id,
        "run_directory": str(store.run_dir),
        "summary_path": str(summary_path),
        "corrective_report_path": str(corrective_report_path),
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
    parser.add_argument(
        "--baseline-summary",
        type=Path,
        default=None,
        help=(
            "Ignored summary.json from the original run. When supplied, the committed "
            "report compares that immutable baseline with the corrective run."
        ),
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
        baseline_summary_path=args.baseline_summary,
    )
    try:
        result = asyncio.run(capture(config))
    except (ValueError, EvidenceWriteError, SecurityError) as exc:
        print(f"probe failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0
