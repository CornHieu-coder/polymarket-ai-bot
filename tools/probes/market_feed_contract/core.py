"""Evidence persistence and deterministic analysis for IP-001.

This module contains no network code. It is deliberately probe-specific and is
not a production collector or replay engine.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


PROBE_VERSION = "0.2.0"
KNOWN_EVENT_TYPES = {
    "book",
    "price_change",
    "last_trade_price",
    "tick_size_change",
    "best_bid_ask",
    "new_market",
    "market_resolved",
}
OPTIONAL_BOOK_METADATA_FIELDS = (
    "min_order_size",
    "tick_size",
    "neg_risk",
    "last_trade_price",
)
EXPECTED_REST_FIELDS = (
    "hash",
    "tick_size",
    "min_order_size",
    "neg_risk",
    "last_trade_price",
)
OFFICIAL_HASH_FIELDS = (
    "market",
    "asset_id",
    "timestamp",
    "hash",
    "bids",
    "asks",
    "min_order_size",
    "tick_size",
    "neg_risk",
    "last_trade_price",
)
ORDERING_FIELD_NAMES = {
    "sequence",
    "sequence_id",
    "sequenceid",
    "seq",
    "seq_id",
    "seqid",
    "offset",
    "update_id",
    "updateid",
}


class ProbeError(RuntimeError):
    """Base class for explicit probe failures."""


class EvidenceWriteError(ProbeError):
    """Raised when raw evidence cannot be durably written."""


class ProbeParseError(ProbeError):
    """A structured parse/state error that must be recorded."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp with microsecond precision."""

    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def parse_decimal(value: Any, field_name: str = "value") -> Decimal:
    """Parse an exact wire decimal without passing through binary float."""

    if not isinstance(value, str):
        raise ProbeParseError(
            "decimal_not_string", f"{field_name} must be a decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ProbeParseError(
            "decimal_parse_failure", f"{field_name} is not a decimal: {value!r}"
        ) from exc
    if not parsed.is_finite():
        raise ProbeParseError(
            "decimal_not_finite", f"{field_name} must be finite: {value!r}"
        )
    return parsed


def decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _json_line(record: Mapping[str, Any]) -> str:
    return json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"


class DurableJsonl:
    """Append-only JSONL writer that flushes and fsyncs every record."""

    def __init__(self, path: Path) -> None:
        self.path = path
        try:
            self._file = path.open("x", encoding="utf-8", newline="\n")
        except OSError as exc:
            raise EvidenceWriteError(f"cannot create evidence file {path}: {exc}") from exc

    def append(self, record: Mapping[str, Any]) -> None:
        try:
            self._file.write(_json_line(record))
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:
            raise EvidenceWriteError(
                f"durable evidence write failed for {self.path}: {exc}"
            ) from exc

    def close(self) -> None:
        try:
            self._file.close()
        except OSError as exc:
            raise EvidenceWriteError(f"cannot close evidence file {self.path}: {exc}") from exc


def write_json_once(path: Path, value: Mapping[str, Any]) -> None:
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise EvidenceWriteError(f"cannot durably write {path}: {exc}") from exc


def _decoded_frame_metadata(decoded: Any) -> dict[str, Any]:
    events: list[Mapping[str, Any]] = []
    if isinstance(decoded, Mapping):
        events = [decoded]
    elif isinstance(decoded, list):
        events = [entry for entry in decoded if isinstance(entry, Mapping)]

    event_types = sorted(
        {str(event["event_type"]) for event in events if "event_type" in event}
    )
    timestamps = sorted(
        {str(event["timestamp"]) for event in events if "timestamp" in event}
    )
    asset_ids: set[str] = set()
    for event in events:
        if event.get("asset_id") is not None:
            asset_ids.add(str(event["asset_id"]))
        changes = event.get("price_changes")
        if isinstance(changes, list):
            for change in changes:
                if isinstance(change, Mapping) and change.get("asset_id") is not None:
                    asset_ids.add(str(change["asset_id"]))

    return {
        "event_types": event_types,
        "source_timestamp": timestamps[0] if len(timestamps) == 1 else None,
        "source_timestamps": timestamps,
        "asset_ids": sorted(asset_ids),
    }


class EvidenceStore:
    """Own one immutable raw-evidence run directory."""

    def __init__(self, output_root: Path, run_id: str, initial_config: Mapping[str, Any]) -> None:
        self.run_id = run_id
        self.run_dir = output_root / run_id
        try:
            self.run_dir.mkdir(parents=True, exist_ok=False)
        except OSError as exc:
            raise EvidenceWriteError(f"cannot create run directory {self.run_dir}: {exc}") from exc

        write_json_once(self.run_dir / "run-config.json", dict(initial_config))
        self._writers = {
            "ws": DurableJsonl(self.run_dir / "raw-websocket.jsonl"),
            "rest": DurableJsonl(self.run_dir / "raw-rest-book.jsonl"),
            "control": DurableJsonl(self.run_dir / "control-events.jsonl"),
        }
        self._sequence = 0
        self.counts: Counter[str] = Counter()

    @property
    def last_sequence(self) -> int:
        return self._sequence

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def record_websocket(
        self,
        raw: str | bytes,
        *,
        session_id: str,
        token_ids: Sequence[str],
        received_at: str | None = None,
        received_monotonic_ns: int | None = None,
    ) -> tuple[dict[str, Any], Any | None]:
        received_at = received_at or utc_now()
        decoded: Any | None = None
        parse_status: str
        parse_error: str | None = None

        if isinstance(raw, bytes):
            payload_kind = "bytes_base64"
            raw_payload = base64.b64encode(raw).decode("ascii")
            digest_input = raw
            try:
                decoded_text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                parse_status = "invalid_utf8"
                parse_error = str(exc)
            else:
                try:
                    decoded = json.loads(decoded_text)
                except json.JSONDecodeError as exc:
                    parse_status = "invalid_json"
                    parse_error = str(exc)
                else:
                    parse_status = "ok"
        else:
            payload_kind = "text"
            raw_payload = raw
            digest_input = raw.encode("utf-8")
            if raw == "PONG":
                parse_status = "expected_heartbeat"
            else:
                try:
                    decoded = json.loads(raw)
                except json.JSONDecodeError as exc:
                    parse_status = "invalid_json"
                    parse_error = str(exc)
                else:
                    parse_status = "ok"

        metadata = _decoded_frame_metadata(decoded)
        record = {
            "record_type": "websocket_frame",
            "source_name": "polymarket_market_ws",
            "source_version": "clob_v2",
            "probe_version": PROBE_VERSION,
            "parser_version": PROBE_VERSION,
            "collector_id": self.run_id,
            "collector_session_id": session_id,
            "ingest_sequence": self._next_sequence(),
            "received_at": received_at,
            "received_monotonic_ns": received_monotonic_ns,
            "subscribed_token_ids": list(token_ids),
            "payload_kind": payload_kind,
            "raw_payload": raw_payload,
            "raw_payload_sha256": hashlib.sha256(digest_input).hexdigest(),
            "parse_status": parse_status,
            "parse_error": parse_error,
            **metadata,
        }
        self._writers["ws"].append(record)
        self.counts["websocket_frames"] += 1
        return record, decoded

    def record_rest(
        self,
        *,
        token_id: str,
        url: str,
        request_started_at: str,
        received_at: str,
        status_code: int,
        raw_response: str | bytes,
        elapsed_ms: str,
        state_version_before: int | None = None,
        state_version_after: int | None = None,
        state_session_before: str | None = None,
        state_session_after: str | None = None,
        state_valid_before: bool = False,
        state_valid_after: bool = False,
        request_context: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], Any | None]:
        if isinstance(raw_response, bytes):
            payload_kind = "bytes_base64"
            stored_response = base64.b64encode(raw_response).decode("ascii")
            digest_input = raw_response
            try:
                response_text = raw_response.decode("utf-8")
            except UnicodeDecodeError as exc:
                response_text = ""
                parse_status = "invalid_utf8"
                parse_error: str | None = str(exc)
            else:
                parse_status = "ok"
                parse_error = None
        else:
            payload_kind = "text"
            stored_response = raw_response
            response_text = raw_response
            digest_input = raw_response.encode("utf-8")
            parse_status = "ok"
            parse_error = None

        decoded: Any | None = None
        if parse_status == "ok":
            try:
                decoded = json.loads(response_text)
            except json.JSONDecodeError as exc:
                parse_status = "invalid_json"
                parse_error = str(exc)

        source_timestamp = (
            str(decoded["timestamp"])
            if isinstance(decoded, Mapping) and decoded.get("timestamp") is not None
            else None
        )
        record = {
            "record_type": "rest_book_response",
            "source_name": "polymarket_clob_rest",
            "source_version": "clob_v2",
            "probe_version": PROBE_VERSION,
            "parser_version": PROBE_VERSION,
            "collector_id": self.run_id,
            "ingest_sequence": self._next_sequence(),
            "token_id": token_id,
            "url": url,
            "http_method": "GET",
            "request_started_at": request_started_at,
            "received_at": received_at,
            "elapsed_ms": elapsed_ms,
            "status_code": status_code,
            "source_timestamp": source_timestamp,
            "payload_kind": payload_kind,
            "raw_response": stored_response,
            "raw_response_sha256": hashlib.sha256(digest_input).hexdigest(),
            "parse_status": parse_status,
            "parse_error": parse_error,
            "state_version_before": state_version_before,
            "state_version_after": state_version_after,
            "state_session_before": state_session_before,
            "state_session_after": state_session_after,
            "state_valid_before": state_valid_before,
            "state_valid_after": state_valid_after,
            "request_context": dict(request_context or {}),
        }
        self._writers["rest"].append(record)
        self.counts["rest_responses"] += 1
        return record, decoded

    def record_control(self, event: str, **details: Any) -> dict[str, Any]:
        record = {
            "record_type": "control_event",
            "probe_version": PROBE_VERSION,
            "ingest_sequence": self._next_sequence(),
            "recorded_at": utc_now(),
            "event": event,
            "details": details,
        }
        self._writers["control"].append(record)
        self.counts["control_events"] += 1
        return record

    def write_resolved_config(self, config: Mapping[str, Any]) -> None:
        write_json_once(self.run_dir / "resolved-config.json", dict(config))

    def close(self) -> None:
        failures: list[Exception] = []
        for writer in self._writers.values():
            try:
                writer.close()
            except EvidenceWriteError as exc:
                failures.append(exc)
        if failures:
            raise failures[0]


@dataclass(frozen=True)
class Level:
    price: Decimal
    size: Decimal
    price_text: str
    size_text: str


def _parse_levels(value: Any, field_name: str) -> dict[Decimal, Level]:
    if not isinstance(value, list):
        raise ProbeParseError("missing_required_field", f"{field_name} must be a list")
    result: dict[Decimal, Level] = {}
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise ProbeParseError(
                "malformed_level", f"{field_name}[{index}] must be an object"
            )
        if "price" not in item or "size" not in item:
            raise ProbeParseError(
                "missing_required_field",
                f"{field_name}[{index}] requires price and size",
            )
        price = parse_decimal(item["price"], f"{field_name}[{index}].price")
        size = parse_decimal(item["size"], f"{field_name}[{index}].size")
        if price < 0 or price > 1:
            raise ProbeParseError(
                "price_out_of_bounds", f"{field_name}[{index}].price={price}"
            )
        if size < 0:
            raise ProbeParseError(
                "negative_size", f"{field_name}[{index}].size={size}"
            )
        if price in result:
            raise ProbeParseError(
                "duplicate_snapshot_level",
                f"{field_name} repeats price {item['price']!r}",
            )
        result[price] = Level(price, size, str(item["price"]), str(item["size"]))
    return result


def official_orderbook_hash(summary: Mapping[str, Any]) -> str:
    """Apply the official py-clob-client-v2 order-book-summary SHA-1 algorithm.

    The caller must supply every field used by the first-party implementation;
    missing fields are never fabricated.
    """

    missing = [field for field in OFFICIAL_HASH_FIELDS if field not in summary]
    if missing:
        raise ProbeParseError(
            "hash_fields_missing", f"official hash inputs missing: {', '.join(missing)}"
        )
    if not isinstance(summary["bids"], list) or not isinstance(summary["asks"], list):
        raise ProbeParseError("hash_levels_malformed", "bids and asks must be lists")

    def raw_levels(side: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for level in summary[side]:
            if not isinstance(level, Mapping) or "price" not in level or "size" not in level:
                raise ProbeParseError(
                    "hash_levels_malformed", f"{side} contains an invalid level"
                )
            result.append({"price": level["price"], "size": level["size"]})
        return result

    payload = {
        "market": summary["market"],
        "asset_id": summary["asset_id"],
        "timestamp": summary["timestamp"],
        "hash": "",
        "bids": raw_levels("bids"),
        "asks": raw_levels("asks"),
        "min_order_size": summary["min_order_size"],
        "tick_size": summary["tick_size"],
        "neg_risk": summary["neg_risk"],
        "last_trade_price": summary["last_trade_price"],
    }
    serialized = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


@dataclass
class BookState:
    asset_id: str
    market: str
    bids: dict[Decimal, Level]
    asks: dict[Decimal, Level]
    metadata: dict[str, Any]
    last_feed_hash: str | None
    last_source_timestamp: str | None
    valid: bool = True

    @classmethod
    def from_event(cls, event: Mapping[str, Any]) -> "BookState":
        for required in ("asset_id", "market", "bids", "asks", "timestamp", "hash"):
            if required not in event:
                raise ProbeParseError(
                    "missing_required_field", f"book is missing {required}"
                )
        asset_id = str(event["asset_id"])
        market = str(event["market"])
        if not asset_id or not market:
            raise ProbeParseError(
                "missing_required_field", "book market and asset_id must be nonempty"
            )
        parse_decimal(str(event["timestamp"]), "book.timestamp")
        return cls(
            asset_id=asset_id,
            market=market,
            bids=_parse_levels(event["bids"], "book.bids"),
            asks=_parse_levels(event["asks"], "book.asks"),
            metadata={
                key: event[key]
                for key in OPTIONAL_BOOK_METADATA_FIELDS
                if key in event
            },
            last_feed_hash=str(event["hash"]),
            last_source_timestamp=str(event["timestamp"]),
        )

    @property
    def best_bid(self) -> Decimal | None:
        populated = [price for price, level in self.bids.items() if level.size > 0]
        return max(populated) if populated else None

    @property
    def best_ask(self) -> Decimal | None:
        populated = [price for price, level in self.asks.items() if level.size > 0]
        return min(populated) if populated else None

    def apply_replacement(self, side: str, price_text: str, size_text: str) -> bool:
        price = parse_decimal(price_text, "price_change.price")
        size = parse_decimal(size_text, "price_change.size")
        if price < 0 or price > 1:
            raise ProbeParseError("price_out_of_bounds", f"price_change.price={price}")
        if size < 0:
            raise ProbeParseError("negative_size", f"price_change.size={size}")
        if side == "BUY":
            levels = self.bids
        elif side == "SELL":
            levels = self.asks
        else:
            raise ProbeParseError("invalid_side", f"unsupported side {side!r}")

        old = levels.get(price)
        if size == 0:
            levels.pop(price, None)
            return old is None
        replacement = Level(price, size, price_text, size_text)
        levels[price] = replacement
        return old is not None and old.size == replacement.size

    def hash_input(self, timestamp: Any) -> Mapping[str, Any] | None:
        if any(field not in self.metadata for field in OPTIONAL_BOOK_METADATA_FIELDS):
            return None

        def levels(mapping: Mapping[Decimal, Level], *, reverse: bool) -> list[dict[str, str]]:
            return [
                {"price": mapping[price].price_text, "size": mapping[price].size_text}
                for price in sorted(mapping, reverse=reverse)
                if mapping[price].size > 0
            ]

        return {
            "market": self.market,
            "asset_id": self.asset_id,
            "timestamp": timestamp,
            "hash": "",
            "bids": levels(self.bids, reverse=True),
            "asks": levels(self.asks, reverse=False),
            **self.metadata,
        }


def _wire_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _field_observations(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    observations: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            observations.append((path, _wire_type(nested)))
            observations.extend(_field_observations(nested, path))
    elif isinstance(value, list):
        array_path = f"{prefix}[]"
        for item in value:
            observations.append((array_path, _wire_type(item)))
            observations.extend(_field_observations(item, array_path))
    return observations


def _event_tokens(event: Mapping[str, Any]) -> list[str]:
    tokens: set[str] = set()
    if event.get("asset_id") is not None:
        tokens.add(str(event["asset_id"]))
    changes = event.get("price_changes")
    if isinstance(changes, list):
        for change in changes:
            if isinstance(change, Mapping) and change.get("asset_id") is not None:
                tokens.add(str(change["asset_id"]))
    assets = event.get("assets_ids")
    if isinstance(assets, list):
        tokens.update(str(asset) for asset in assets)
    return sorted(tokens)


def _quantile(sorted_values: Sequence[Decimal], proportion: Decimal) -> Decimal | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = int(
        (Decimal(len(sorted_values)) * proportion).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    index = max(0, rank - 1)
    return sorted_values[index]


@dataclass
class ContractAnalyzer:
    """Deterministic aggregation of persisted WebSocket and REST observations."""

    token_ids: Sequence[str]
    states: dict[str, BookState] = field(default_factory=dict)
    event_counts: Counter[str] = field(default_factory=Counter)
    schema_presence: dict[str, Counter[str]] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    schema_occurrences: dict[str, Counter[str]] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    schema_types: dict[str, dict[str, Counter[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(Counter))
    )
    ordering_fields: Counter[str] = field(default_factory=Counter)
    sessions: dict[str, dict[str, Any]] = field(default_factory=dict)
    current_session: str | None = None
    errors: Counter[str] = field(default_factory=Counter)
    error_examples: list[dict[str, Any]] = field(default_factory=list)
    raw_hashes: Counter[str] = field(default_factory=Counter)
    duplicate_raw_frames: int = 0
    comparison_entries: int = 0
    comparison_matches: int = 0
    comparison_mismatches: int = 0
    direct_comparison_mismatches: int = 0
    comparison_components: Counter[str] = field(default_factory=Counter)
    comparison_counterexamples: list[dict[str, Any]] = field(default_factory=list)
    empty_side_candidate_entries: int = 0
    empty_side_candidates: list[dict[str, Any]] = field(default_factory=list)
    empty_side_candidates_by_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    pending_empty_side_probe_requests: list[dict[str, Any]] = field(default_factory=list)
    empty_side_rest_confirmations: Counter[str] = field(default_factory=Counter)
    empty_side_rest_diagnostics: Counter[str] = field(default_factory=Counter)
    empty_side_rest_unaligned: int = 0
    zero_updates: int = 0
    nonzero_updates: int = 0
    side_updates: Counter[str] = field(default_factory=Counter)
    discriminating_nonzero_updates: int = 0
    discriminating_zero_deletes: int = 0
    validated_nonzero_replacements: int = 0
    validated_zero_deletes: int = 0
    pending_discriminating: dict[
        str, dict[tuple[str, Decimal], dict[str, Any]]
    ] = field(
        default_factory=lambda: defaultdict(dict)
    )
    superseded_before_validation: int = 0
    superseded_validation_examples: list[dict[str, Any]] = field(default_factory=list)
    idempotent_replacements: int = 0
    price_change_frames: int = 0
    price_change_entries_total: int = 0
    price_change_entries_applied: int = 0
    price_change_entries_excluded: int = 0
    price_changes_per_frame: Counter[int] = field(default_factory=Counter)
    multi_entry_frames: int = 0
    multi_same_asset_frames: int = 0
    duplicate_key_frames: int = 0
    order_sensitive_frames: int = 0
    multi_counterexamples: list[dict[str, Any]] = field(default_factory=list)
    book_hash_attempts: int = 0
    book_hash_matches: int = 0
    book_hash_mismatches: int = 0
    book_hash_skipped: int = 0
    change_hash_attempts: int = 0
    change_hash_matches: int = 0
    change_hash_mismatches: int = 0
    change_hash_skipped: int = 0
    rest_hash_attempts: int = 0
    rest_hash_matches: int = 0
    rest_hash_mismatches: int = 0
    hash_counterexamples: list[dict[str, Any]] = field(default_factory=list)
    rest_requests: int = 0
    rest_successes: int = 0
    rest_statuses: Counter[int] = field(default_factory=Counter)
    rest_field_presence: Counter[str] = field(default_factory=Counter)
    rest_aligned_comparisons: int = 0
    rest_aligned_matches: int = 0
    rest_aligned_mismatches: int = 0
    rest_unaligned: int = 0
    rest_counterexamples: list[dict[str, Any]] = field(default_factory=list)
    optional_book_presence: Counter[str] = field(default_factory=Counter)
    book_events: int = 0
    state_versions: Counter[str] = field(default_factory=Counter)
    last_timestamp_by_token: dict[str, Decimal] = field(default_factory=dict)
    timestamp_count_by_token: Counter[str] = field(default_factory=Counter)
    timestamp_events: int = 0
    timestamp_regressions: int = 0
    same_timestamp_adjacent: int = 0
    timestamp_group_counts: Counter[tuple[str, str]] = field(default_factory=Counter)
    delay_ms: list[Decimal] = field(default_factory=list)
    future_timestamps: int = 0
    timestamp_counterexamples: list[dict[str, Any]] = field(default_factory=list)
    websocket_records_observed: int = 0
    websocket_monotonic_timestamps_observed: int = 0
    websocket_ingest_sequence_regressions: int = 0
    websocket_monotonic_receive_regressions: int = 0
    last_websocket_ingest_sequence: int | None = None
    last_websocket_received_monotonic_ns: int | None = None

    def take_empty_side_probe_requests(self) -> list[dict[str, Any]]:
        """Return and clear targeted public REST requests created by wire ambiguities."""

        requests = self.pending_empty_side_probe_requests
        self.pending_empty_side_probe_requests = []
        return requests

    def start_session(self, session_id: str) -> None:
        self.current_session = session_id
        self.states = {}
        self.sessions[session_id] = {
            "started": True,
            "completed": False,
            "controlled_disconnect": False,
            "disconnect_error": None,
            "tokens": {
                token: {
                    "first_state_event": None,
                    "first_state_ingest_sequence": None,
                    "first_book_ingest_sequence": None,
                    "first_delta_ingest_sequence": None,
                    "book_events": 0,
                    "price_change_entries": 0,
                }
                for token in self.token_ids
            },
        }

    def end_session(
        self,
        session_id: str,
        *,
        controlled_disconnect: bool,
        disconnect_error: str | None = None,
    ) -> None:
        session = self.sessions[session_id]
        session["completed"] = True
        session["controlled_disconnect"] = controlled_disconnect
        session["disconnect_error"] = disconnect_error
        self.current_session = None
        self.states = {}

    def _record_error(
        self,
        code: str,
        *,
        ingest_sequence: int | None,
        event_type: str | None,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.errors[code] += 1
        example = {
            "code": code,
            "ingest_sequence": ingest_sequence,
            "event_type": event_type,
            "message": message,
            "details": dict(details or {}),
        }
        self.error_examples.append(example)
        return example

    def observe_websocket(self, record: Mapping[str, Any], decoded: Any | None) -> list[dict[str, Any]]:
        before = sum(self.errors.values())
        before_examples = len(self.error_examples)
        self._observe_websocket_receive_order(record)
        digest = str(record.get("raw_payload_sha256", ""))
        if digest:
            self.raw_hashes[digest] += 1
            if self.raw_hashes[digest] > 1:
                self.duplicate_raw_frames += 1

        parse_status = record.get("parse_status")
        if parse_status == "expected_heartbeat":
            return []
        if parse_status != "ok":
            self._record_error(
                str(parse_status),
                ingest_sequence=record.get("ingest_sequence"),
                event_type=None,
                message=str(record.get("parse_error") or parse_status),
            )
            return self.error_examples[before_examples:]

        if isinstance(decoded, Mapping):
            events: Iterable[Any] = [decoded]
        elif isinstance(decoded, list):
            events = decoded
        else:
            self._record_error(
                "unexpected_json_shape",
                ingest_sequence=record.get("ingest_sequence"),
                event_type=None,
                message=f"top-level JSON is {type(decoded).__name__}",
            )
            return self.error_examples[before_examples:]

        for event_index, event in enumerate(events):
            if not isinstance(event, Mapping):
                self._record_error(
                    "unexpected_event_shape",
                    ingest_sequence=record.get("ingest_sequence"),
                    event_type=None,
                    message=f"event[{event_index}] is {type(event).__name__}",
                )
                continue
            self._observe_event(record, event, event_index)

        if sum(self.errors.values()) == before:
            return []
        return self.error_examples[before_examples:]

    def _observe_websocket_receive_order(self, record: Mapping[str, Any]) -> None:
        """Measure the probe's processing order without inferring transport gaps."""

        sequence = int(record["ingest_sequence"])
        self.websocket_records_observed += 1
        if (
            self.last_websocket_ingest_sequence is not None
            and sequence <= self.last_websocket_ingest_sequence
        ):
            self.websocket_ingest_sequence_regressions += 1
            self._record_error(
                "websocket_ingest_sequence_regression",
                ingest_sequence=sequence,
                event_type=None,
                message="WebSocket record ingest_sequence did not increase",
                details={"previous": self.last_websocket_ingest_sequence},
            )
        self.last_websocket_ingest_sequence = sequence

        raw_monotonic = record.get("received_monotonic_ns")
        if raw_monotonic is None:
            return
        monotonic_ns = int(raw_monotonic)
        self.websocket_monotonic_timestamps_observed += 1
        if (
            self.last_websocket_received_monotonic_ns is not None
            and monotonic_ns < self.last_websocket_received_monotonic_ns
        ):
            self.websocket_monotonic_receive_regressions += 1
            self._record_error(
                "websocket_monotonic_receive_regression",
                ingest_sequence=sequence,
                event_type=None,
                message="WebSocket received_monotonic_ns regressed",
                details={"previous": self.last_websocket_received_monotonic_ns},
            )
        self.last_websocket_received_monotonic_ns = monotonic_ns

    def _observe_event(
        self, record: Mapping[str, Any], event: Mapping[str, Any], event_index: int
    ) -> None:
        sequence = int(record["ingest_sequence"])
        raw_event_type = event.get("event_type")
        if not isinstance(raw_event_type, str) or not raw_event_type:
            self._record_error(
                "missing_event_type",
                ingest_sequence=sequence,
                event_type=None,
                message=f"event[{event_index}] has no string event_type",
            )
            return
        event_type = raw_event_type
        self.event_counts[event_type] += 1
        observations = _field_observations(event)
        paths = [path for path, _ in observations]
        self.schema_presence[event_type].update(set(paths))
        self.schema_occurrences[event_type].update(paths)
        for path, observed_type in observations:
            self.schema_types[event_type][path][observed_type] += 1
        for path in set(paths):
            leaf = path.replace("[]", "").rsplit(".", 1)[-1].lower()
            if leaf in ORDERING_FIELD_NAMES or "sequence" in leaf:
                self.ordering_fields[path] += 1

        if event_type not in KNOWN_EVENT_TYPES:
            self._record_error(
                "unexpected_event_type",
                ingest_sequence=sequence,
                event_type=event_type,
                message=f"unexpected event_type {event_type!r}",
            )

        tokens = _event_tokens(event)
        self._observe_timestamp(record, event, tokens, event_type)

        if event_type == "book":
            self._observe_book(record, event)
        elif event_type == "price_change":
            self._observe_price_change(record, event)

    def _session_token(self, token: str) -> dict[str, Any] | None:
        if self.current_session is None:
            return None
        session = self.sessions.get(self.current_session)
        if session is None:
            return None
        return session["tokens"].get(token)

    def _mark_first_state_event(
        self, token: str, event_type: str, ingest_sequence: int
    ) -> None:
        item = self._session_token(token)
        if item is not None and item["first_state_event"] is None:
            item["first_state_event"] = event_type
            item["first_state_ingest_sequence"] = ingest_sequence
        if item is not None and event_type == "book" and item["first_book_ingest_sequence"] is None:
            item["first_book_ingest_sequence"] = ingest_sequence
        if (
            item is not None
            and event_type == "price_change"
            and item["first_delta_ingest_sequence"] is None
        ):
            item["first_delta_ingest_sequence"] = ingest_sequence

    def _record_superseded_validation(
        self, pending: Mapping[str, Any], *, superseding_sequence: int
    ) -> None:
        """Retire evidence whose exact level was changed before REST validation."""

        self.superseded_before_validation += 1
        if len(self.superseded_validation_examples) < 20:
            self.superseded_validation_examples.append(
                {
                    **dict(pending),
                    "superseding_ingest_sequence": superseding_sequence,
                }
            )

    def _observe_book(self, record: Mapping[str, Any], event: Mapping[str, Any]) -> None:
        sequence = int(record["ingest_sequence"])
        self.book_events += 1
        for field_name in OPTIONAL_BOOK_METADATA_FIELDS:
            if field_name in event:
                self.optional_book_presence[field_name] += 1

        try:
            state = BookState.from_event(event)
        except ProbeParseError as exc:
            self._record_error(
                exc.code,
                ingest_sequence=sequence,
                event_type="book",
                message=str(exc),
            )
            return
        self.states[state.asset_id] = state
        self._mark_first_state_event(state.asset_id, "book", sequence)
        item = self._session_token(state.asset_id)
        if item is not None:
            item["book_events"] += 1
        self.state_versions[state.asset_id] += 1
        pending = self.pending_discriminating[state.asset_id]
        for prior in pending.values():
            self._record_superseded_validation(prior, superseding_sequence=sequence)
        self.pending_discriminating[state.asset_id] = {}

        if all(field in event for field in OFFICIAL_HASH_FIELDS):
            self.book_hash_attempts += 1
            try:
                computed = official_orderbook_hash(event)
            except ProbeParseError as exc:
                self._record_error(
                    exc.code,
                    ingest_sequence=sequence,
                    event_type="book",
                    message=str(exc),
                )
                return
            observed = str(event["hash"])
            if computed == observed:
                self.book_hash_matches += 1
            else:
                self.book_hash_mismatches += 1
                if len(self.hash_counterexamples) < 20:
                    self.hash_counterexamples.append(
                        {
                            "shape": "websocket_book",
                            "ingest_sequence": sequence,
                            "asset_id": state.asset_id,
                            "observed_hash": observed,
                            "computed_hash": computed,
                        }
                    )
        else:
            self.book_hash_skipped += 1

    def _observe_price_change(
        self, record: Mapping[str, Any], event: Mapping[str, Any]
    ) -> None:
        sequence = int(record["ingest_sequence"])
        self.price_change_frames += 1
        changes = event.get("price_changes")
        if not isinstance(changes, list):
            self._record_error(
                "missing_required_field",
                ingest_sequence=sequence,
                event_type="price_change",
                message="price_change.price_changes must be a list",
            )
            return
        self.price_change_entries_total += len(changes)
        self.price_changes_per_frame[len(changes)] += 1
        if len(changes) > 1:
            self.multi_entry_frames += 1
        for change in changes:
            if isinstance(change, Mapping) and change.get("asset_id") is not None:
                observed_token = str(change["asset_id"])
                self._mark_first_state_event(observed_token, "price_change", sequence)
                observed_item = self._session_token(observed_token)
                if observed_item is not None:
                    observed_item["price_change_entries"] += 1
        if "market" not in event or "timestamp" not in event:
            self.price_change_entries_excluded += len(changes)
            self._record_error(
                "missing_required_field",
                ingest_sequence=sequence,
                event_type="price_change",
                message="price_change requires market and timestamp",
            )
            return

        assets = [
            str(change.get("asset_id"))
            for change in changes
            if isinstance(change, Mapping) and change.get("asset_id") is not None
        ]
        asset_entry_counts = Counter(assets)
        if any(count > 1 for count in asset_entry_counts.values()):
            self.multi_same_asset_frames += 1

        keyed_sizes: dict[tuple[str, str, Any], list[tuple[str, Any]]] = defaultdict(list)
        for change in changes:
            if isinstance(change, Mapping):
                raw_price = str(change.get("price", ""))
                raw_size = str(change.get("size", ""))
                try:
                    price_key: Any = parse_decimal(raw_price, "price_change.price")
                except ProbeParseError:
                    price_key = ("unparseable", raw_price)
                try:
                    size_key: Any = parse_decimal(raw_size, "price_change.size")
                except ProbeParseError:
                    size_key = ("unparseable", raw_size)
                key = (
                    str(change.get("asset_id", "")),
                    str(change.get("side", "")),
                    price_key,
                )
                keyed_sizes[key].append((raw_size, size_key))
        duplicated = {key: sizes for key, sizes in keyed_sizes.items() if len(sizes) > 1}
        if duplicated:
            self.duplicate_key_frames += 1
            order_sensitive = any(
                sizes[0][1] != sizes[-1][1] for sizes in duplicated.values()
            )
            if order_sensitive:
                self.order_sensitive_frames += 1
            if len(self.multi_counterexamples) < 20:
                self.multi_counterexamples.append(
                    {
                        "ingest_sequence": sequence,
                        "duplicate_keys": [
                            {
                                "asset_id": key[0],
                                "side": key[1],
                                "price": (
                                    decimal_text(key[2])
                                    if isinstance(key[2], Decimal)
                                    else str(key[2][1])
                                ),
                                "sizes_in_order": [item[0] for item in sizes],
                            }
                            for key, sizes in sorted(
                                duplicated.items(),
                                key=lambda item: (
                                    item[0][0],
                                    item[0][1],
                                    str(item[0][2]),
                                ),
                            )
                        ],
                        "order_sensitive": order_sensitive,
                    }
                )

        for change_index, change in enumerate(changes):
            if not isinstance(change, Mapping):
                self._record_error(
                    "malformed_price_change",
                    ingest_sequence=sequence,
                    event_type="price_change",
                    message=f"price_changes[{change_index}] must be an object",
                )
                self.price_change_entries_excluded += 1
                continue
            missing = [
                field_name
                for field_name in ("asset_id", "price", "size", "side", "hash")
                if field_name not in change
            ]
            if missing:
                self._record_error(
                    "missing_required_field",
                    ingest_sequence=sequence,
                    event_type="price_change",
                    message=(
                        f"price_changes[{change_index}] missing {', '.join(missing)}"
                    ),
                )
                self.price_change_entries_excluded += 1
                continue

            token = str(change["asset_id"])
            state = self.states.get(token)
            if state is None or not state.valid:
                self._record_error(
                    "price_change_without_valid_book",
                    ingest_sequence=sequence,
                    event_type="price_change",
                    message=f"cannot apply change for uninitialized/invalid asset {token}",
                )
                self.price_change_entries_excluded += 1
                continue

            side = str(change["side"])
            try:
                size = parse_decimal(change["size"], "price_change.size")
                price = parse_decimal(change["price"], "price_change.price")
                old_level = (
                    state.bids.get(price)
                    if side == "BUY"
                    else state.asks.get(price)
                    if side == "SELL"
                    else None
                )
                idempotent = state.apply_replacement(
                    side, str(change["price"]), str(change["size"])
                )
            except ProbeParseError as exc:
                state.valid = False
                self._record_error(
                    exc.code,
                    ingest_sequence=sequence,
                    event_type="price_change",
                    message=str(exc),
                    details={"asset_id": token, "change_index": change_index},
                )
                self.price_change_entries_excluded += 1
                continue

            pending_key = (side, price)
            superseded = self.pending_discriminating[token].pop(pending_key, None)
            if superseded is not None:
                self._record_superseded_validation(
                    superseded, superseding_sequence=sequence
                )

            if size == 0:
                self.zero_updates += 1
                if old_level is not None and old_level.size > 0:
                    self.discriminating_zero_deletes += 1
                    self.pending_discriminating[token][pending_key] = {
                        "kind": "zero_delete",
                        "ingest_sequence": sequence,
                        "side": side,
                        "price": str(change["price"]),
                    }
            else:
                self.nonzero_updates += 1
                if old_level is not None and old_level.size != size:
                    self.discriminating_nonzero_updates += 1
                    self.pending_discriminating[token][pending_key] = {
                        "kind": "nonzero_replacement",
                        "ingest_sequence": sequence,
                        "side": side,
                        "price": str(change["price"]),
                        "previous_size": old_level.size_text,
                        "observed_size": str(change["size"]),
                    }
            self.side_updates[side] += 1
            if idempotent:
                self.idempotent_replacements += 1
            self.price_change_entries_applied += 1

            state.last_feed_hash = str(change["hash"])
            state.last_source_timestamp = str(event["timestamp"])
            self.state_versions[token] += 1
            self._compare_best(
                sequence,
                change_index,
                len(changes),
                asset_entry_counts[token],
                token,
                state,
                change,
            )
            self._compare_change_hash(sequence, token, state, event, change)

    def _compare_best(
        self,
        sequence: int,
        change_index: int,
        frame_entry_count: int,
        asset_entry_count: int,
        token: str,
        state: BookState,
        change: Mapping[str, Any],
    ) -> None:
        supplied = [name for name in ("best_bid", "best_ask") if name in change]
        if not supplied:
            return
        self.comparison_entries += 1
        mismatches: dict[str, dict[str, str | None]] = {}
        candidates: list[dict[str, Any]] = []
        for name in supplied:
            raw_value = change[name]
            try:
                observed = (
                    None
                    if raw_value is None or raw_value == ""
                    else parse_decimal(raw_value, f"price_change.{name}")
                )
            except ProbeParseError as exc:
                state.valid = False
                self._record_error(
                    exc.code,
                    ingest_sequence=sequence,
                    event_type="price_change",
                    message=str(exc),
                    details={"asset_id": token, "change_index": change_index},
                )
                return
            derived = state.best_bid if name == "best_bid" else state.best_ask
            self.comparison_components[f"{name}_total"] += 1
            if observed == derived:
                self.comparison_components[f"{name}_matches"] += 1
            elif derived is None and (
                (name == "best_ask" and observed == Decimal("1"))
                or (name == "best_bid" and observed == Decimal("0"))
            ):
                kind = (
                    "ask_one_when_empty"
                    if name == "best_ask"
                    else "bid_zero_when_empty"
                )
                candidate_id = f"{sequence}:{change_index}:{name}"
                candidate = {
                    "candidate_id": candidate_id,
                    "kind": kind,
                    "ingest_sequence": sequence,
                    "change_index": change_index,
                    "asset_id": token,
                    "component": name,
                    "wire_value": decimal_text(observed),
                    "derived_value": None,
                    "state_version": self.state_versions[token],
                    "session_id": self.current_session,
                    "price": str(change.get("price")),
                    "size": str(change.get("size")),
                    "side": str(change.get("side")),
                    "rest_validation": "UNRESOLVED",
                    "rest_ingest_sequence": None,
                    "rest_attempts": [],
                }
                self.comparison_components[f"{name}_empty_side_boundary_candidates"] += 1
                self.empty_side_candidates.append(candidate)
                self.empty_side_candidates_by_id[candidate_id] = candidate
                self.pending_empty_side_probe_requests.append(
                    {
                        "request_purpose": "empty_side_boundary_candidate",
                        "candidate_id": candidate_id,
                        "candidate_kind": kind,
                        "candidate_ingest_sequence": sequence,
                        "candidate_state_version": self.state_versions[token],
                        "candidate_session_id": self.current_session,
                        "candidate_component": name,
                        "token_id": token,
                    }
                )
                candidates.append(candidate)
            else:
                self.comparison_components[f"{name}_mismatches"] += 1
                mismatches[name] = {
                    "observed": decimal_text(observed),
                    "derived": decimal_text(derived),
                }

        if mismatches:
            self.comparison_mismatches += 1
            if asset_entry_count == 1:
                self.direct_comparison_mismatches += 1
            state.valid = False
            example = {
                "ingest_sequence": sequence,
                "change_index": change_index,
                "asset_id": token,
                "price": str(change.get("price")),
                "size": str(change.get("size")),
                "side": str(change.get("side")),
                "frame_entry_count": frame_entry_count,
                "asset_entry_count": asset_entry_count,
                "multi_entry_semantics_ambiguous": asset_entry_count > 1,
                "mismatches": mismatches,
            }
            if len(self.comparison_counterexamples) < 20:
                self.comparison_counterexamples.append(example)
            self._record_error(
                "best_bid_ask_mismatch",
                ingest_sequence=sequence,
                event_type="price_change",
                message="reconstructed best bid/ask did not match event values",
                details=example,
            )
        elif candidates:
            self.empty_side_candidate_entries += 1
        else:
            self.comparison_matches += 1

    def _compare_change_hash(
        self,
        sequence: int,
        token: str,
        state: BookState,
        event: Mapping[str, Any],
        change: Mapping[str, Any],
    ) -> None:
        candidate = state.hash_input(event["timestamp"])
        if candidate is None:
            self.change_hash_skipped += 1
            return
        self.change_hash_attempts += 1
        computed = official_orderbook_hash(candidate)
        observed = str(change["hash"])
        if computed == observed:
            self.change_hash_matches += 1
        else:
            self.change_hash_mismatches += 1
            if len(self.hash_counterexamples) < 20:
                self.hash_counterexamples.append(
                    {
                        "shape": "price_change_post_update",
                        "ingest_sequence": sequence,
                        "asset_id": token,
                        "observed_hash": observed,
                        "computed_hash": computed,
                    }
                )

    def _observe_timestamp(
        self,
        record: Mapping[str, Any],
        event: Mapping[str, Any],
        tokens: Sequence[str],
        event_type: str,
    ) -> None:
        if "timestamp" not in event:
            return
        sequence = int(record["ingest_sequence"])
        try:
            source_ms = parse_decimal(str(event["timestamp"]), f"{event_type}.timestamp")
        except ProbeParseError as exc:
            self._record_error(
                exc.code,
                ingest_sequence=sequence,
                event_type=event_type,
                message=str(exc),
            )
            return
        try:
            received = datetime.fromisoformat(str(record["received_at"]).replace("Z", "+00:00"))
        except (ValueError, TypeError) as exc:
            self._record_error(
                "received_timestamp_parse_failure",
                ingest_sequence=sequence,
                event_type=event_type,
                message=str(exc),
            )
            return
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        since_epoch = received - epoch
        received_ms = (
            Decimal(since_epoch.days) * Decimal(86_400_000)
            + Decimal(since_epoch.seconds) * Decimal(1_000)
            + Decimal(since_epoch.microseconds) / Decimal(1_000)
        )
        delay = received_ms - source_ms
        self.delay_ms.append(delay)
        self.timestamp_events += 1
        if delay < 0:
            self.future_timestamps += 1
            if len(self.timestamp_counterexamples) < 20:
                self.timestamp_counterexamples.append(
                    {
                        "kind": "source_after_local_receive",
                        "ingest_sequence": sequence,
                        "event_type": event_type,
                        "source_timestamp": str(event["timestamp"]),
                        "received_at": record["received_at"],
                        "delay_ms": decimal_text(delay),
                    }
                )

        for token in tokens:
            self.timestamp_count_by_token[token] += 1
            previous = self.last_timestamp_by_token.get(token)
            if previous is not None:
                if source_ms < previous:
                    self.timestamp_regressions += 1
                    if len(self.timestamp_counterexamples) < 20:
                        self.timestamp_counterexamples.append(
                            {
                                "kind": "source_timestamp_regression",
                                "ingest_sequence": sequence,
                                "asset_id": token,
                                "previous": decimal_text(previous),
                                "current": decimal_text(source_ms),
                            }
                        )
                    self._record_error(
                        "source_timestamp_regression",
                        ingest_sequence=sequence,
                        event_type=event_type,
                        message=f"source timestamp regressed for {token}",
                        details={
                            "previous": decimal_text(previous),
                            "current": decimal_text(source_ms),
                        },
                    )
                elif source_ms == previous:
                    self.same_timestamp_adjacent += 1
            self.last_timestamp_by_token[token] = source_ms
            self.timestamp_group_counts[(token, str(event["timestamp"]))] += 1

    def _record_empty_side_rest_attempt(
        self,
        context: Any,
        *,
        ingest_sequence: int,
        outcome: str,
        details: Mapping[str, Any] | None = None,
    ) -> bool:
        """Attach a targeted REST outcome without promoting diagnostics to proof."""

        if not isinstance(context, Mapping) or context.get("request_purpose") != (
            "empty_side_boundary_candidate"
        ):
            return False
        candidate_id = str(context.get("candidate_id", ""))
        candidate = self.empty_side_candidates_by_id.get(candidate_id)
        if candidate is None:
            self.empty_side_rest_diagnostics["unknown_candidate"] += 1
            return True
        attempt = {
            "ingest_sequence": ingest_sequence,
            "outcome": outcome,
            **dict(details or {}),
        }
        candidate["rest_attempts"].append(attempt)
        candidate["rest_ingest_sequence"] = ingest_sequence
        if outcome == "CONFIRMED":
            if candidate["rest_validation"] != "CONFIRMED":
                candidate["rest_validation"] = "CONFIRMED"
                self.empty_side_rest_confirmations[str(candidate["kind"])] += 1
        else:
            self.empty_side_rest_diagnostics[outcome] += 1
        return True

    def observe_rest(self, record: Mapping[str, Any], decoded: Any | None) -> None:
        self.rest_requests += 1
        status = int(record["status_code"])
        self.rest_statuses[status] += 1
        sequence = int(record["ingest_sequence"])
        token = str(record["token_id"])
        context = record.get("request_context")
        if status < 200 or status >= 300:
            self._record_empty_side_rest_attempt(
                context,
                ingest_sequence=sequence,
                outcome="non_2xx",
                details={"status_code": status},
            )
            self._record_error(
                "rest_non_2xx",
                ingest_sequence=sequence,
                event_type=None,
                message=f"REST /book returned HTTP {status}",
                details={"asset_id": token},
            )
            if len(self.rest_counterexamples) < 20:
                self.rest_counterexamples.append(
                    {"kind": "non_2xx", "ingest_sequence": sequence, "status": status}
                )
            return
        if record.get("parse_status") != "ok" or not isinstance(decoded, Mapping):
            self._record_empty_side_rest_attempt(
                context,
                ingest_sequence=sequence,
                outcome="invalid_response",
            )
            self._record_error(
                "rest_invalid_json",
                ingest_sequence=sequence,
                event_type=None,
                message=str(record.get("parse_error") or "REST JSON was not an object"),
                details={"asset_id": token},
            )
            return

        self.rest_successes += 1
        for field_name in EXPECTED_REST_FIELDS:
            if field_name in decoded:
                self.rest_field_presence[field_name] += 1
            elif len(self.rest_counterexamples) < 20:
                self.rest_counterexamples.append(
                    {
                        "kind": "missing_expected_field",
                        "ingest_sequence": sequence,
                        "asset_id": token,
                        "field": field_name,
                    }
                )

        if all(field in decoded for field in OFFICIAL_HASH_FIELDS):
            self.rest_hash_attempts += 1
            try:
                computed = official_orderbook_hash(decoded)
            except ProbeParseError as exc:
                self._record_error(
                    exc.code,
                    ingest_sequence=sequence,
                    event_type=None,
                    message=str(exc),
                )
            else:
                observed = str(decoded["hash"])
                if computed == observed:
                    self.rest_hash_matches += 1
                else:
                    self.rest_hash_mismatches += 1
                    if len(self.hash_counterexamples) < 20:
                        self.hash_counterexamples.append(
                            {
                                "shape": "rest_book",
                                "ingest_sequence": sequence,
                                "asset_id": token,
                                "observed_hash": observed,
                                "computed_hash": computed,
                            }
                        )

        state = self.states.get(token)
        stable_observed_window = (
            state is not None
            and state.valid
            and bool(record.get("state_valid_before"))
            and bool(record.get("state_valid_after"))
            and record.get("state_session_before") is not None
            and record.get("state_session_before") == record.get("state_session_after")
            and record.get("state_session_after") == self.current_session
            and record.get("state_version_before") == record.get("state_version_after")
            and record.get("state_version_after") == self.state_versions[token]
        )
        if not stable_observed_window:
            self.rest_unaligned += 1
            if self._record_empty_side_rest_attempt(
                context,
                ingest_sequence=sequence,
                outcome="unaligned_request_window",
            ):
                self.empty_side_rest_unaligned += 1
            return

        try:
            rest_bids = _parse_levels(decoded.get("bids"), "rest.bids")
            rest_asks = _parse_levels(decoded.get("asks"), "rest.asks")
        except ProbeParseError as exc:
            self._record_empty_side_rest_attempt(
                context,
                ingest_sequence=sequence,
                outcome="malformed_rest_book",
            )
            self._record_error(
                exc.code,
                ingest_sequence=sequence,
                event_type=None,
                message=str(exc),
                details={"asset_id": token},
            )
            return
        self.rest_aligned_comparisons += 1
        rest_bid_values = {price: level.size for price, level in rest_bids.items()}
        rest_ask_values = {price: level.size for price, level in rest_asks.items()}
        state_bid_values = {price: level.size for price, level in state.bids.items()}
        state_ask_values = {price: level.size for price, level in state.asks.items()}
        full_depth_match = (
            rest_bid_values == state_bid_values and rest_ask_values == state_ask_values
        )
        if isinstance(context, Mapping) and context.get("request_purpose") == (
            "empty_side_boundary_candidate"
        ):
            candidate_id = str(context.get("candidate_id", ""))
            candidate = self.empty_side_candidates_by_id.get(candidate_id)
            candidate_aligned = (
                candidate is not None
                and candidate["asset_id"] == token
                and candidate["session_id"] == self.current_session
                and candidate["state_version"] == record.get("state_version_before")
                and candidate["state_version"] == record.get("state_version_after")
            )
            if candidate_aligned:
                component = str(candidate["component"])
                rest_side_empty = (
                    not rest_asks if component == "best_ask" else not rest_bids
                )
                if full_depth_match and rest_side_empty:
                    self._record_empty_side_rest_attempt(
                        context,
                        ingest_sequence=sequence,
                        outcome="CONFIRMED",
                        details={
                            "full_depth_match": True,
                            "relevant_rest_side_empty": True,
                        },
                    )
                else:
                    self._record_empty_side_rest_attempt(
                        context,
                        ingest_sequence=sequence,
                        outcome="aligned_full_depth_mismatch",
                        details={
                            "full_depth_match": full_depth_match,
                            "relevant_rest_side_empty": rest_side_empty,
                        },
                    )
            else:
                self.empty_side_rest_unaligned += 1
                self._record_empty_side_rest_attempt(
                    context,
                    ingest_sequence=sequence,
                    outcome="candidate_state_superseded",
                )

        if full_depth_match:
            self.rest_aligned_matches += 1
            for pending in self.pending_discriminating[token].values():
                if pending["kind"] == "nonzero_replacement":
                    self.validated_nonzero_replacements += 1
                elif pending["kind"] == "zero_delete":
                    self.validated_zero_deletes += 1
            self.pending_discriminating[token] = {}
        else:
            self.rest_aligned_mismatches += 1
            if len(self.rest_counterexamples) < 20:
                self.rest_counterexamples.append(
                    {
                        "kind": "hash_aligned_level_mismatch",
                        "ingest_sequence": sequence,
                        "asset_id": token,
                        "rest_hash": str(decoded.get("hash", "")),
                        "feed_hash": state.last_feed_hash,
                        "alignment": "stable_observed_request_window",
                    }
                )

    def observe_rest_transport_failure(
        self,
        *,
        ingest_sequence: int,
        token_id: str,
        error_kind: str,
        message: str,
        request_context: Mapping[str, Any] | None = None,
    ) -> None:
        self.rest_requests += 1
        self.rest_statuses[0] += 1
        self._record_empty_side_rest_attempt(
            request_context,
            ingest_sequence=ingest_sequence,
            outcome=error_kind,
            details={"message": message},
        )
        self._record_error(
            error_kind,
            ingest_sequence=ingest_sequence,
            event_type=None,
            message=message,
            details={
                "asset_id": token_id,
                "request_context": dict(request_context or {}),
            },
        )
        if len(self.rest_counterexamples) < 20:
            self.rest_counterexamples.append(
                {
                    "kind": error_kind,
                    "ingest_sequence": ingest_sequence,
                    "asset_id": token_id,
                    "message": message,
                }
            )

    def observe_operational_failure(
        self,
        *,
        ingest_sequence: int,
        code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self._record_error(
            code,
            ingest_sequence=ingest_sequence,
            event_type=None,
            message=message,
            details=details,
        )

    def summary(self, run: Mapping[str, Any], evidence: Mapping[str, Any]) -> dict[str, Any]:
        schema: dict[str, Any] = {}
        for event_type in sorted(self.event_counts):
            schema[event_type] = {
                "events": self.event_counts[event_type],
                "fields": {
                    path: {
                        "event_presence": self.schema_presence[event_type][path],
                        "occurrences": self.schema_occurrences[event_type][path],
                        "types": dict(sorted(self.schema_types[event_type][path].items())),
                    }
                    for path in sorted(self.schema_occurrences[event_type])
                },
            }

        q2_counterexamples: list[dict[str, Any]] = []
        ordered_session_ids = sorted(self.sessions)
        enough_sessions = len(ordered_session_ids) >= 2
        all_sessions_completed = enough_sessions and all(
            self.sessions[session_id]["completed"]
            for session_id in ordered_session_ids
        )
        clean_controlled_reconnect = False
        if enough_sessions:
            first_session = self.sessions[ordered_session_ids[0]]
            clean_controlled_reconnect = bool(
                first_session["completed"]
                and first_session["controlled_disconnect"]
                and first_session["disconnect_error"] is None
            )
            if not clean_controlled_reconnect:
                q2_counterexamples.append(
                    {
                        "session_id": ordered_session_ids[0],
                        "kind": "first_session_not_cleanly_controlled",
                        "completed": first_session["completed"],
                        "controlled_disconnect": first_session[
                            "controlled_disconnect"
                        ],
                        "disconnect_error": first_session["disconnect_error"],
                    }
                )
        all_first_books = enough_sessions
        all_exercised_after_book = enough_sessions
        for session_id, session in sorted(self.sessions.items()):
            if not session["completed"]:
                all_first_books = False
            for token, token_result in sorted(session["tokens"].items()):
                if token_result["first_state_event"] != "book":
                    all_first_books = False
                    q2_counterexamples.append(
                        {
                            "session_id": session_id,
                            "asset_id": token,
                            "first_state_event": token_result["first_state_event"],
                        }
                    )
                if token_result["price_change_entries"] == 0:
                    all_exercised_after_book = False
                    q2_counterexamples.append(
                        {
                            "session_id": session_id,
                            "asset_id": token,
                            "kind": "no_delta_observed_after_fresh_book",
                            "first_state_event": token_result["first_state_event"],
                            "first_book_ingest_sequence": token_result[
                                "first_book_ingest_sequence"
                            ],
                        }
                    )
        if any(
            item.get("first_state_event") == "price_change"
            for item in q2_counterexamples
        ):
            q2_status = "CONTRADICTED"
        elif (
            all_sessions_completed
            and clean_controlled_reconnect
            and all_first_books
            and all_exercised_after_book
        ):
            q2_status = "CONFIRMED"
        else:
            q2_status = "UNRESOLVED"

        candidate_kind_counts = Counter(
            str(candidate["kind"]) for candidate in self.empty_side_candidates
        )
        observed_candidate_kinds = sorted(candidate_kind_counts)
        confirmed_candidate_kinds = sorted(
            kind
            for kind in observed_candidate_kinds
            if self.empty_side_rest_confirmations[kind] > 0
        )
        empty_side_kind_support = bool(observed_candidate_kinds) and (
            confirmed_candidate_kinds == observed_candidate_kinds
        )
        empty_side_kind_support_status = (
            "CONFIRMED" if empty_side_kind_support else "UNRESOLVED"
        )
        unresolved_candidate_count = sum(
            candidate["rest_validation"] != "CONFIRMED"
            for candidate in self.empty_side_candidates
        )
        empty_side_interpretation_supported = (
            empty_side_kind_support and unresolved_candidate_count == 0
        )
        empty_side_interpretation_status = (
            "CONFIRMED" if empty_side_interpretation_supported else "UNRESOLVED"
        )
        empty_side_requirement_met = (
            not self.empty_side_candidates or empty_side_interpretation_supported
        )
        if self.direct_comparison_mismatches:
            q3_status = "CONTRADICTED"
        elif (
            self.comparison_entries > 0
            and self.comparison_mismatches == 0
            and self.validated_nonzero_replacements > 0
            and self.validated_zero_deletes > 0
            and self.side_updates["BUY"] > 0
            and self.side_updates["SELL"] > 0
            and empty_side_requirement_met
        ):
            q3_status = "CONFIRMED"
        else:
            q3_status = "UNRESOLVED"

        if self.duplicate_key_frames:
            q4_status = "CONFIRMED"
        else:
            q4_status = "UNRESOLVED"

        if (
            self.book_hash_attempts > 0
            and self.book_hash_matches == self.book_hash_attempts
            and self.change_hash_attempts > 0
            and self.change_hash_matches == self.change_hash_attempts
        ):
            q5_status = "CONFIRMED"
        else:
            q5_status = "UNRESOLVED"

        missing_rest_fields = {
            field_name: self.rest_successes - self.rest_field_presence[field_name]
            for field_name in EXPECTED_REST_FIELDS
        }
        if self.rest_successes and any(missing_rest_fields.values()):
            q6_status = "CONTRADICTED"
        elif self.rest_aligned_mismatches:
            q6_status = "UNRESOLVED"
        elif self.rest_aligned_comparisons and self.rest_successes:
            q6_status = "CONFIRMED"
        else:
            q6_status = "UNRESOLVED"

        delays = sorted(self.delay_ms)
        same_groups = [
            {"asset_id": token, "source_timestamp": timestamp, "events": count}
            for (token, timestamp), count in sorted(self.timestamp_group_counts.items())
            if count > 1
        ]
        timestamps_exercised = bool(self.token_ids) and all(
            self.timestamp_count_by_token[token] >= 2 for token in self.token_ids
        )
        if (
            self.timestamp_regressions
            or self.websocket_ingest_sequence_regressions
            or self.websocket_monotonic_receive_regressions
        ):
            q8_status = "CONTRADICTED"
        elif self.timestamp_events and timestamps_exercised:
            q8_status = "CONFIRMED"
        else:
            q8_status = "UNRESOLVED"

        return {
            "probe_version": PROBE_VERSION,
            "run": dict(run),
            "evidence": {
                **dict(evidence),
                "duplicate_raw_frames": self.duplicate_raw_frames,
            },
            "q1": {
                "status": (
                    "CONFIRMED"
                    if self.event_counts and self.ordering_fields
                    else "UNRESOLVED"
                ),
                "event_schema": schema,
                "candidate_ordering_fields": dict(sorted(self.ordering_fields.items())),
                "scope_note": (
                    "Field absence is reported only for this bounded sample and is not "
                    "a claim that a field can never appear."
                ),
                "counterexamples": [],
            },
            "q2": {
                "status": q2_status,
                "sessions": self.sessions,
                "sessions_observed": len(self.sessions),
                "all_sessions_completed": all_sessions_completed,
                "clean_controlled_reconnect": clean_controlled_reconnect,
                "scope_note": (
                    "This confirms only that fresh full books preceded later deltas "
                    "for every sampled token in two completed corrective sessions "
                    "using the minimal subscription payload and a clean controlled "
                    "reconnect. It is not a universal guarantee about server defaults."
                ),
                "counterexamples": q2_counterexamples,
            },
            "q3": {
                "status": q3_status,
                "comparison_entries": self.comparison_entries,
                "observed_change_entries": self.price_change_entries_total,
                "applied_change_entries": self.price_change_entries_applied,
                "excluded_change_entries": self.price_change_entries_excluded,
                "exact_matches": self.comparison_matches,
                "mismatches": self.comparison_mismatches,
                "genuine_mismatches": self.comparison_mismatches,
                "direct_single-entry_mismatches": self.direct_comparison_mismatches,
                "components": dict(sorted(self.comparison_components.items())),
                "zero_size_updates": self.zero_updates,
                "nonzero_replacements": self.nonzero_updates,
                "side_updates": dict(sorted(self.side_updates.items())),
                "discriminating_nonzero_updates": self.discriminating_nonzero_updates,
                "discriminating_zero_deletes": self.discriminating_zero_deletes,
                "independently_validated_nonzero_replacements": (
                    self.validated_nonzero_replacements
                ),
                "independently_validated_zero_deletes": self.validated_zero_deletes,
                "superseded_before_validation": self.superseded_before_validation,
                "superseded_validation_examples": (
                    self.superseded_validation_examples
                ),
                "idempotent_replacements": self.idempotent_replacements,
                "empty_side_boundary_interpretation": {
                    "status": empty_side_interpretation_status,
                    "kind_level_status": empty_side_kind_support_status,
                    "all_observed_candidates_confirmed": (
                        bool(self.empty_side_candidates)
                        and unresolved_candidate_count == 0
                    ),
                    "scope": (
                        "Probe-only, observation-bounded candidate interpretation. "
                        "Numeric 1/0 is never normalized globally and first-party "
                        "materials do not define it as an empty-side sentinel."
                    ),
                    "observed_candidates": len(self.empty_side_candidates),
                    "observed_kinds": {
                        kind: candidate_kind_counts[kind]
                        for kind in observed_candidate_kinds
                    },
                    "confirmed_candidates": sum(
                        candidate["rest_validation"] == "CONFIRMED"
                        for candidate in self.empty_side_candidates
                    ),
                    "confirmed_kinds": {
                        kind: self.empty_side_rest_confirmations[kind]
                        for kind in confirmed_candidate_kinds
                    },
                    "unresolved_candidates": unresolved_candidate_count,
                    "targeted_rest_unaligned": self.empty_side_rest_unaligned,
                    "targeted_rest_diagnostics": dict(
                        sorted(self.empty_side_rest_diagnostics.items())
                    ),
                    "confirmation_rule": (
                        "A candidate kind is supported only after at least one public "
                        "REST /book response exactly matches full reconstructed depth "
                        "across an unchanged session/state-version request window and "
                        "the relevant REST side is empty. Kind-level support is diagnostic "
                        "only; the bounded run-level interpretation, event-best-price "
                        "component, and Q3 cannot be CONFIRMED while any observed "
                        "candidate remains unresolved. Every other REST outcome remains "
                        "diagnostic and UNRESOLVED."
                    ),
                    "candidates": self.empty_side_candidates,
                },
                "component_statuses": {
                    "nonzero_aggregate_replacement": (
                        "CONFIRMED"
                        if self.validated_nonzero_replacements > 0
                        else "UNRESOLVED"
                    ),
                    "zero_size_delete": (
                        "CONFIRMED"
                        if self.validated_zero_deletes > 0
                        else "UNRESOLVED"
                    ),
                    "buy_bid_sell_ask": (
                        "CONFIRMED"
                        if self.side_updates["BUY"] > 0
                        and self.side_updates["SELL"] > 0
                        and self.comparison_mismatches == 0
                        else "CONTRADICTED"
                        if self.direct_comparison_mismatches
                        else "UNRESOLVED"
                    ),
                    "event_best_prices": (
                        "CONTRADICTED"
                        if self.direct_comparison_mismatches
                        else "CONFIRMED"
                        if self.comparison_entries > 0
                        and self.comparison_mismatches == 0
                        and empty_side_requirement_met
                        else "UNRESOLVED"
                    ),
                    "empty_side_boundary_candidates": (
                        empty_side_interpretation_status
                        if self.empty_side_candidates
                        else "NOT_OBSERVED"
                    ),
                },
                "validation_note": (
                    "Best-price agreement alone cannot distinguish aggregate replacement "
                    "from arithmetic increment. Independent validation requires exact full-"
                    "depth agreement in a stable observed REST request window."
                ),
                "counterexamples": self.comparison_counterexamples,
            },
            "q4": {
                "status": q4_status,
                "price_change_frames": self.price_change_frames,
                "entries_per_frame": {
                    str(count): frames
                    for count, frames in sorted(self.price_changes_per_frame.items())
                },
                "multi_entry_frames": self.multi_entry_frames,
                "multi_same_asset_frames": self.multi_same_asset_frames,
                "duplicate_asset_side_price_frames": self.duplicate_key_frames,
                "order_sensitive_frames": self.order_sensitive_frames,
                "counterexamples": self.multi_counterexamples,
            },
            "q5": {
                "status": q5_status,
                "algorithm": (
                    "py-clob-client-v2 generate_orderbook_summary_hash: SHA-1 of "
                    "compact ordered JSON with hash set to an empty string"
                ),
                "websocket_book": {
                    "attempts": self.book_hash_attempts,
                    "matches": self.book_hash_matches,
                    "mismatches": self.book_hash_mismatches,
                    "skipped_missing_fields": self.book_hash_skipped,
                },
                "price_change_post_update": {
                    "attempts": self.change_hash_attempts,
                    "matches": self.change_hash_matches,
                    "mismatches": self.change_hash_mismatches,
                    "skipped_missing_fields": self.change_hash_skipped,
                },
                "rest_book_diagnostic": {
                    "attempts": self.rest_hash_attempts,
                    "matches": self.rest_hash_matches,
                    "mismatches": self.rest_hash_mismatches,
                },
                "counterexamples": self.hash_counterexamples,
            },
            "q6": {
                "status": q6_status,
                "requests": self.rest_requests,
                "successful_responses": self.rest_successes,
                "status_codes": {
                    str(code): count for code, count in sorted(self.rest_statuses.items())
                },
                "expected_field_presence": {
                    field_name: self.rest_field_presence[field_name]
                    for field_name in EXPECTED_REST_FIELDS
                },
                "expected_field_missing": missing_rest_fields,
                "alignment_rule": (
                    "Compare levels only when a valid reconstructed state exists before "
                    "the request and its session/version remain unchanged through receipt."
                ),
                "aligned_comparisons": self.rest_aligned_comparisons,
                "aligned_matches": self.rest_aligned_matches,
                "aligned_mismatches": self.rest_aligned_mismatches,
                "unaligned_diagnostics": self.rest_unaligned,
                "counterexamples": self.rest_counterexamples,
            },
            "q7": {
                "status": (
                    "CONFIRMED"
                    if self.book_events
                    and all(self.optional_book_presence[field] > 0 for field in OPTIONAL_BOOK_METADATA_FIELDS)
                    else "UNRESOLVED"
                ),
                "websocket_book_events": self.book_events,
                "optional_field_presence": {
                    field_name: self.optional_book_presence[field_name]
                    for field_name in OPTIONAL_BOOK_METADATA_FIELDS
                },
                "scope_note": (
                    "Observed absence in this sample does not establish universal absence."
                ),
                "counterexamples": [],
            },
            "q8": {
                "status": q8_status,
                "timestamped_events": self.timestamp_events,
                "timestamp_events_by_token": {
                    token: self.timestamp_count_by_token[token]
                    for token in sorted(self.token_ids)
                },
                "source_timestamp_regressions": self.timestamp_regressions,
                "adjacent_same_timestamp_events": self.same_timestamp_adjacent,
                "same_timestamp_groups": same_groups[:20],
                "source_after_local_receive": self.future_timestamps,
                "processing_order": {
                    "websocket_records_observed": self.websocket_records_observed,
                    "ingest_sequence_regressions": (
                        self.websocket_ingest_sequence_regressions
                    ),
                    "records_with_received_monotonic_ns": (
                        self.websocket_monotonic_timestamps_observed
                    ),
                    "received_monotonic_ns_regressions": (
                        self.websocket_monotonic_receive_regressions
                    ),
                    "scope_note": (
                        "These counters measure probe processing order only; they do "
                        "not establish gap-free network delivery."
                    ),
                },
                "probe_observed_source_to_processing_delay_ms": {
                    "quantile_method": "nearest-rank without interpolation",
                    "count": len(delays),
                    "min": decimal_text(delays[0]) if delays else None,
                    "p50": decimal_text(_quantile(delays, Decimal("0.50"))),
                    "p95": decimal_text(_quantile(delays, Decimal("0.95"))),
                    "max": decimal_text(delays[-1]) if delays else None,
                },
                "delay_measurement_definition": (
                    "Local UTC received_at minus the event source timestamp, sampled "
                    "after the WebSocket receive completed and before the current raw "
                    "frame's durable write."
                ),
                "delay_caveat": (
                    "This probe-observed source-to-processing delay is contaminated "
                    "by local clock offset, event-loop scheduling, socket/library "
                    "buffering, backpressure, and synchronous flush/fsync in the "
                    "receive path. It is not a venue/network latency estimate."
                ),
                "counterexamples": self.timestamp_counterexamples,
            },
            "errors": {
                "counts": dict(sorted(self.errors.items())),
                "examples": self.error_examples,
            },
            "safety": {
                "authenticated_operations": 0,
                "trading_operations": 0,
                "wallets_or_credentials_used": False,
                "http_methods": ["GET"],
                "websocket_channel": "market",
                "trading_performance_result": False,
            },
        }


def write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    write_json_once(path, summary)


def evidence_manifest(run_dir: Path, names: Sequence[str]) -> dict[str, Any]:
    files: dict[str, Any] = {}
    for name in names:
        path = run_dir / name
        digest = hashlib.sha256()
        size = 0
        try:
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    size += len(block)
                    digest.update(block)
        except OSError as exc:
            raise EvidenceWriteError(f"cannot hash evidence file {path}: {exc}") from exc
        files[name] = {"bytes": size, "sha256": digest.hexdigest()}
    return {"algorithm": "sha256", "files": files}
