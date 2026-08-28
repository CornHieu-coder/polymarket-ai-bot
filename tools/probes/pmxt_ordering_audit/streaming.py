"""Exact sorted-streaming A1-A8 analyzer and bounded IP-002S pilot."""

from __future__ import annotations

import hashlib
import json
import math
import sys
import time
from array import array
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import duckdb
import pyarrow
import pyarrow.parquet as pq

from .core import (
    KNOWN_EVENT_TYPES,
    classify_availability_group,
    distribution_from_histogram,
    normalize_json,
    sha256_file,
)
from .engine import (
    _book_counterexample,
    _schema,
    enforce_a1_a8_schema,
    git_provenance,
    require_non_null_grouping_values,
)
from .recovery import (
    ORIGINAL_ANALYSIS_SHA256,
    _atomic_create_json,
    _canonical_json,
    _monitor_process,
    verify_checkpoint,
    verify_preserved_evidence,
)


ARROW_BATCH_SIZE = 65_536
WINDOW_MIN_ROWS = 2_000_000
AUGUST_ROWS = 82_705_648
STREAMING_MAX_RSS_BYTES = 4 * 1024**3
STREAMING_MAX_TEMP_BYTES = 2 * 1024**3
STREAMING_MAX_COMBINED_SECONDS = 10 * 60
STREAMING_MAX_PROJECTED_SECONDS = 30 * 60
SAFETY_FACTOR = 1.5
REFERENCE_SCAN_MAX_ROWS = 10_000_000
EXPECTED_COLUMNS = (
    "timestamp_received",
    "timestamp",
    "market",
    "event_type",
    "asset_id",
    "bids",
    "asks",
    "price",
    "size",
    "side",
    "best_bid",
    "best_ask",
    "fee_rate_bps",
    "transaction_hash",
    "old_tick_size",
    "new_tick_size",
)


class SortContractError(ValueError):
    """Raised when tested physical rows violate the documented composite sort."""


class StreamingDeadlineExceeded(RuntimeError):
    """Raised before another batch is processed after an authorized deadline."""


_FULL_AUGUST_STREAM_CAPABILITY = object()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _hour_bounds(hour: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(hour, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    return start, start + timedelta(hours=1)


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    return float(numerator / denominator) if denominator else None


def _bucket(group_size: int) -> str:
    if group_size <= 4:
        return str(group_size)
    if group_size <= 9:
        return "5-9"
    if group_size <= 99:
        return "10-99"
    return "100+"


def _composition(counts: Mapping[str, int], unknown: int) -> str:
    return (
        f"book={counts.get('book', 0)},price_change={counts.get('price_change', 0)}"
        f",last_trade_price={counts.get('last_trade_price', 0)}"
        f",tick_size_change={counts.get('tick_size_change', 0)},unknown={unknown}"
    )


def _bounded_insert(
    values: list[dict[str, Any]], value: dict[str, Any], key
) -> None:
    values.append(value)
    values.sort(key=key)
    if len(values) > 20:
        values.pop()


def _count_grid_points(
    *, hour_start_ms: int, segment_start_ms: int, segment_end_ms: int, step_ms: int
) -> int:
    if segment_end_ms <= segment_start_ms:
        return 0
    offset = segment_start_ms - hour_start_ms
    first = hour_start_ms + max(0, math.ceil(offset / step_ms)) * step_ms
    if first >= segment_end_ms:
        return 0
    return ((segment_end_ms - 1 - first) // step_ms) + 1


def _signature(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(name) for name in EXPECTED_COLUMNS)


def _normalized_timestamp_values(values: Iterable[datetime | None]) -> list[Any]:
    materialized = list(values)
    present = sorted({value for value in materialized if value is not None})
    result = [normalize_json(value) for value in present]
    if any(value is None for value in materialized):
        result.append(None)
    return result


class StreamingAggregates:
    """Incremental exact aggregates with only current group/asset state buffered."""

    def __init__(self, *, hour: str, schema: Mapping[str, Any]) -> None:
        self.start, self.end = _hour_bounds(hour)
        self.start_ms, self.end_ms = _epoch_ms(self.start), _epoch_ms(self.end)
        self.schema = schema
        self.previous_sort_key: tuple[bytes, str, datetime] | None = None
        self.group_key: tuple[bytes, str, datetime] | None = None
        self.group_rows: list[dict[str, Any]] = []
        self.current_asset: tuple[bytes, str] | None = None
        self.asset_rows = 0
        self.asset_has_in_hour_group = False
        self.asset_group_sequence = 0
        self.asset_state = "UNINITIALIZED"
        self.asset_segment_start_ms = self.start_ms
        self.asset_segments: list[tuple[int, int, str]] = []
        self.asset_first_book_ms: int | None = None
        self.asset_pending_recovery: dict[str, Any] | None = None
        self.asset_count = 0
        self.max_asset_rows = 0
        self.max_group_size = 0

        self.row_count = 0
        self.in_hour_timestamp_counts = array("I", [0]) * 3_600_000
        self.out_hour_timestamp_counts: Counter[datetime] = Counter()
        self.timestamp_min: datetime | None = None
        self.timestamp_max: datetime | None = None
        self.source_min: datetime | None = None
        self.source_max: datetime | None = None
        self.null_required = Counter()
        self.received_outside = 0
        self.source_outside = 0
        self.markets: set[bytes] = set()
        self.assets: set[str] = set()
        self.event_counts: Counter[str] = Counter()
        self.group_buckets: dict[str, dict[str, int]] = {}

        self.a2_total = 0
        self.a2_multi = 0
        self.a2_l2_rows = 0
        self.a2_all_rows = 0
        self.compositions: dict[str, dict[str, int]] = {}

        self.a3 = Counter()
        self.a3_examples: list[dict[str, Any]] = []
        self.a4 = Counter()
        self.a4_examples: list[dict[str, Any]] = []
        self.a5 = Counter()
        self.a5_examples: list[dict[str, Any]] = []
        self.a6 = Counter()
        self.a6_span_min: int | None = None
        self.a6_span_max: int | None = None
        self.a6_distinct_hist: Counter[int] = Counter()

        self.recovery_time_hist: Counter[int] = Counter()
        self.recovery_group_hist: Counter[int] = Counter()
        self.recovery_events: list[dict[str, Any]] = []
        self.per_asset: list[dict[str, Any]] = []
        self.aggregate_time = Counter()
        self.coverage = Counter()
        self.cadence = {
            "1": Counter(),
            "5": Counter(),
            "30": Counter(),
        }

    def consume(self, row: dict[str, Any]) -> None:
        require_non_null_grouping_values(row, context="stream row")
        market = row["market"]
        asset = row["asset_id"]
        received = row["timestamp_received"]
        key = (market, asset, received)
        if self.previous_sort_key is not None and key < self.previous_sort_key:
            raise SortContractError(
                f"composite sort regression: {normalize_json(key)!r} after "
                f"{normalize_json(self.previous_sort_key)!r}"
            )
        self.previous_sort_key = key
        asset_key = (market, asset)
        if self.current_asset is None:
            self._start_asset(asset_key)
        elif asset_key != self.current_asset:
            self._finish_group()
            self._finish_asset()
            self._start_asset(asset_key)
        if self.group_key is None:
            self.group_key = key
        elif key != self.group_key:
            self._finish_group()
            self.group_key = key
        self.group_rows.append(row)
        self.max_group_size = max(self.max_group_size, len(self.group_rows))
        self.asset_rows += 1
        self._consume_a1_row(row)

    def _start_asset(self, key: tuple[bytes, str]) -> None:
        self.current_asset = key
        self.asset_rows = 0
        self.asset_has_in_hour_group = False
        self.asset_group_sequence = 0
        self.asset_state = "UNINITIALIZED"
        self.asset_segment_start_ms = self.start_ms
        self.asset_segments = []
        self.asset_first_book_ms = None
        self.asset_pending_recovery = None

    def _consume_a1_row(self, row: Mapping[str, Any]) -> None:
        self.row_count += 1
        received = row["timestamp_received"]
        source = row.get("timestamp")
        self.timestamp_min = received if self.timestamp_min is None else min(self.timestamp_min, received)
        self.timestamp_max = received if self.timestamp_max is None else max(self.timestamp_max, received)
        if source is not None:
            self.source_min = source if self.source_min is None else min(self.source_min, source)
            self.source_max = source if self.source_max is None else max(self.source_max, source)
        for name in ("timestamp_received", "timestamp", "market", "asset_id", "event_type"):
            if row.get(name) is None:
                self.null_required[name] += 1
        if self.start <= received < self.end:
            offset = _epoch_ms(received) - self.start_ms
            self.in_hour_timestamp_counts[offset] += 1
        else:
            self.out_hour_timestamp_counts[received] += 1
            self.received_outside += 1
        if source is not None and not (self.start <= source < self.end):
            self.source_outside += 1
        self.markets.add(row["market"])
        self.assets.add(row["asset_id"])
        self.event_counts[str(row.get("event_type"))] += 1

    def _finish_group(self) -> None:
        if not self.group_rows:
            return
        rows = self.group_rows
        group_key = self.group_key
        assert group_key is not None
        counts = Counter(str(row.get("event_type")) for row in rows)
        unknown = sum(
            1
            for row in rows
            if row.get("event_type") is None
            or str(row.get("event_type")) not in KNOWN_EVENT_TYPES
        )
        l2_rows = counts["book"] + counts["price_change"]
        group_size = len(rows)
        bucket = self.group_buckets.setdefault(_bucket(group_size), {"groups": 0, "rows": 0})
        bucket["groups"] += 1
        bucket["rows"] += group_size
        self.a2_total += 1
        if l2_rows > 1:
            self.a2_multi += 1
            self.a2_l2_rows += l2_rows
            self.a2_all_rows += group_size
            name = _composition(counts, unknown)
            item = self.compositions.setdefault(name, {"groups": 0, "l2_rows": 0, "all_rows": 0})
            item["groups"] += 1
            item["l2_rows"] += l2_rows
            item["all_rows"] += group_size
        price_stats = self._a3_group(rows, counts)
        classification: dict[str, Any] | None = None
        if counts["book"]:
            classification = classify_availability_group(rows)
            self._a4_group(rows, classification)
        self._a5_group(rows, counts)
        self._a6_group(rows)
        self._a7_group(
            rows=rows,
            counts=counts,
            unknown=unknown,
            price_stats=price_stats,
            classification=classification,
        )
        self.group_rows = []
        self.group_key = None

    def _a3_group(
        self, rows: Sequence[Mapping[str, Any]], counts: Mapping[str, int]
    ) -> dict[str, Any]:
        malformed = False
        key_sizes: dict[tuple[str, Decimal], set[Decimal]] = {}
        key_counts: Counter[tuple[str, Decimal]] = Counter()
        price_rows = [row for row in rows if row.get("event_type") == "price_change"]
        for row in price_rows:
            side, price, size = row.get("side"), row.get("price"), row.get("size")
            if (
                side not in {"BUY", "SELL"}
                or price is None
                or size is None
                or size < 0
            ):
                malformed = True
                continue
            key = (str(side), price)
            key_counts[key] += 1
            key_sizes.setdefault(key, set()).add(size)
        repeated = sum(value > 1 for value in key_counts.values())
        conflicting = sum(len(value) > 1 for value in key_sizes.values())
        if counts["book"] == 0 and counts["price_change"] > 1:
            self.a3["pure"] += 1
            if malformed:
                self.a3["malformed"] += 1
            else:
                if repeated == 0:
                    self.a3["distinct"] += 1
                elif conflicting == 0:
                    self.a3["idempotent"] += 1
                if conflicting:
                    self.a3["conflicting"] += 1
            signatures = Counter(_signature(row) for row in price_rows)
            self.a3["duplicate_rows"] += sum(value - 1 for value in signatures.values())
            self.a3["duplicate_signatures"] += sum(value > 1 for value in signatures.values())
            for (side, price), sizes in key_sizes.items():
                if malformed or len(sizes) <= 1:
                    continue
                matching = [
                    row
                    for row in price_rows
                    if row.get("side") == side and row.get("price") == price
                ]
                example = {
                    "market": normalize_json(rows[0]["market"]),
                    "asset_id": rows[0]["asset_id"],
                    "timestamp_received": normalize_json(rows[0]["timestamp_received"]),
                    "side": side,
                    "price": normalize_json(price),
                    "replacement_sizes": normalize_json(sorted(sizes)),
                    "source_timestamps": _normalized_timestamp_values(
                        row.get("timestamp") for row in matching
                    ),
                    "note": (
                        "Source timestamps are retained only as diagnostics and did not resolve "
                        "the conflicting replacement order."
                    ),
                }
                _bounded_insert(
                    self.a3_examples,
                    example,
                    lambda value: (
                        value["timestamp_received"],
                        value["market"],
                        value["asset_id"],
                        value["side"],
                        Decimal(value["price"]),
                    ),
                )
        return {"malformed": malformed, "conflicting": conflicting and not malformed}

    def _a4_group(
        self, rows: Sequence[Mapping[str, Any]], classification: Mapping[str, Any]
    ) -> None:
        self.a4["book_groups"] += 1
        books = int(classification["book_count"])
        changes = int(classification["price_change_count"])
        status = classification["l2_status"]
        if books > 1:
            self.a4["multi_book"] += 1
            self.a4["multi_identical" if status == "INVARIANT" else "multi_other"] += 1
        if changes:
            self.a4["mixed"] += 1
            if status == "INVARIANT":
                self.a4["mixed_invariant"] += 1
            elif status == "AMBIGUOUS":
                self.a4["mixed_ambiguous"] += 1
            else:
                self.a4["mixed_unresolved"] += 1
        if status in {"AMBIGUOUS", "UNRESOLVED"}:
            example = _book_counterexample(rows, classification)
            _bounded_insert(
                self.a4_examples,
                example,
                lambda value: tuple(value["group_key"]),
            )

    def _a5_group(
        self, rows: Sequence[Mapping[str, Any]], counts: Mapping[str, int]
    ) -> None:
        ticks = [row for row in rows if row.get("event_type") == "tick_size_change"]
        if not ticks or len(rows) <= 1:
            return
        self.a5["tied"] += 1
        malformed = any(
            row.get("old_tick_size") is None or row.get("new_tick_size") is None
            for row in ticks
        )
        transitions = {
            (row.get("old_tick_size"), row.get("new_tick_size")) for row in ticks
        }
        if malformed:
            self.a5["unresolved"] += 1
        elif len(ticks) == 1:
            self.a5["single"] += 1
        elif len(transitions) == 1:
            self.a5["duplicate"] += 1
        else:
            self.a5["ambiguous"] += 1
            example = {
                "market": normalize_json(rows[0]["market"]),
                "asset_id": rows[0]["asset_id"],
                "timestamp_received": normalize_json(rows[0]["timestamp_received"]),
                "transitions": normalize_json(
                    [
                        {"old_tick_size": old, "new_tick_size": new}
                        for old, new in sorted(transitions)
                    ]
                ),
            }
            _bounded_insert(
                self.a5_examples,
                example,
                lambda value: (
                    value["timestamp_received"], value["market"], value["asset_id"]
                ),
            )

    def _a6_group(self, rows: Sequence[Mapping[str, Any]]) -> None:
        if len(rows) <= 1:
            return
        self.a6["tied"] += 1
        source_values = [row.get("timestamp") for row in rows]
        present = [value for value in source_values if value is not None]
        distinct = len(set(present))
        self.a6_distinct_hist[distinct] += 1
        if distinct == 1 and len(present) == len(rows):
            self.a6["equal"] += 1
        if distinct > 1:
            self.a6["different"] += 1
        if len(present) != len(rows):
            self.a6["null"] += 1
        if present:
            span = _epoch_ms(max(present)) - _epoch_ms(min(present))
            self.a6_span_min = span if self.a6_span_min is None else min(self.a6_span_min, span)
            self.a6_span_max = span if self.a6_span_max is None else max(self.a6_span_max, span)
        diagnostic_rows = sorted(
            rows,
            key=lambda row: row.get("_ip002r_source_row_ordinal", row["_physical_row"]),
        )
        previous: datetime | None = None
        regressed = False
        for row in diagnostic_rows:
            current = row.get("timestamp")
            if previous is not None and current is not None and current < previous:
                self.a6["regression_rows"] += 1
                regressed = True
            previous = current
        if regressed:
            self.a6["regression_groups"] += 1

    def _a7_group(
        self,
        *,
        rows: Sequence[Mapping[str, Any]],
        counts: Mapping[str, int],
        unknown: int,
        price_stats: Mapping[str, Any],
        classification: Mapping[str, Any] | None,
    ) -> None:
        received = rows[0]["timestamp_received"]
        if not (self.start <= received < self.end):
            return
        self.asset_has_in_hour_group = True
        self.asset_group_sequence += 1
        l2_rows = counts["book"] + counts["price_change"]
        if l2_rows == 0 and unknown == 0:
            return
        if unknown:
            status, reason, accepted = "UNRESOLVED", "unknown_event_type", False
        elif counts["book"]:
            assert classification is not None
            status = str(classification["l2_status"])
            reason = str(classification["l2_reason"])
            accepted = bool(classification["accepted_book"])
        elif price_stats["malformed"]:
            status, reason, accepted = "UNRESOLVED", "price_change_parse_failure", False
        elif price_stats["conflicting"]:
            status, reason, accepted = "AMBIGUOUS", "repeated_price_key_multiple_sizes", False
        else:
            status, reason, accepted = "INVARIANT", "pure_price_change_order_invariant", False
        received_ms = _epoch_ms(received)
        if received_ms > self.asset_segment_start_ms:
            self.asset_segments.append(
                (self.asset_segment_start_ms, received_ms, self.asset_state)
            )
        state_before = self.asset_state
        audit_rows = l2_rows + unknown
        ambiguity = status != "INVARIANT"
        if accepted:
            self.coverage["processed"] += audit_rows
        elif status == "INVARIANT" and state_before == "VALID":
            self.coverage["processed"] += audit_rows
        elif state_before == "INVALID" or (ambiguity and state_before == "VALID"):
            self.coverage["excluded"] += audit_rows
        elif state_before == "UNINITIALIZED":
            self.coverage["uninitialized"] += audit_rows
        self.coverage["total"] += audit_rows
        if accepted:
            if self.asset_pending_recovery is not None:
                event = self.asset_pending_recovery
                recovery_ms = received_ms - int(event["_received_ms"])
                recovery_groups = self.asset_group_sequence - int(event["_group_sequence"])
                event["recovery_time_ms"] = recovery_ms
                event["subsequent_archive_groups_to_recovery"] = recovery_groups
                self.recovery_time_hist[recovery_ms] += 1
                self.recovery_group_hist[recovery_groups] += 1
                self.asset_pending_recovery = None
            if self.asset_first_book_ms is None:
                self.asset_first_book_ms = received_ms
            self.asset_state = "VALID"
        elif ambiguity and state_before == "VALID":
            self.asset_state = "INVALID"
            event = {
                "market": normalize_json(rows[0]["market"]),
                "asset_id": rows[0]["asset_id"],
                "timestamp_received": normalize_json(received),
                "reason": reason,
                "audit_state_rows": audit_rows,
                "recovery_time_ms": None,
                "subsequent_archive_groups_to_recovery": None,
                "_received_ms": received_ms,
                "_group_sequence": self.asset_group_sequence,
            }
            self.recovery_events.append(event)
            self.asset_pending_recovery = event
        self.asset_segment_start_ms = received_ms

    def _finish_asset(self) -> None:
        if self.current_asset is None:
            return
        self.max_asset_rows = max(self.max_asset_rows, self.asset_rows)
        if self.asset_has_in_hour_group:
            self.asset_count += 1
            if self.end_ms > self.asset_segment_start_ms:
                self.asset_segments.append(
                    (self.asset_segment_start_ms, self.end_ms, self.asset_state)
                )
            durations = Counter()
            for segment_start, segment_end, state in self.asset_segments:
                durations[state] += segment_end - segment_start
                if self.asset_first_book_ms is None or segment_end <= self.asset_first_book_ms:
                    continue
                eligible_start = max(segment_start, self.asset_first_book_ms)
                for minutes in (1, 5, 30):
                    count = _count_grid_points(
                        hour_start_ms=self.start_ms,
                        segment_start_ms=eligible_start,
                        segment_end_ms=segment_end,
                        step_ms=minutes * 60_000,
                    )
                    self.cadence[str(minutes)]["eligible"] += count
                    if state == "VALID":
                        self.cadence[str(minutes)]["valid"] += count
            valid_ms = durations["VALID"]
            invalid_ms = durations["INVALID"]
            self.aggregate_time["uninitialized_ms"] += durations["UNINITIALIZED"]
            self.aggregate_time["valid_ms"] += valid_ms
            self.aggregate_time["invalid_ms"] += invalid_ms
            self.per_asset.append(
                {
                    "market": normalize_json(self.current_asset[0]),
                    "asset_id": self.current_asset[1],
                    "uninitialized_ms": durations["UNINITIALIZED"],
                    "valid_ms": valid_ms,
                    "invalid_ms": invalid_ms,
                    "initialized_valid_share": _ratio(valid_ms, valid_ms + invalid_ms),
                }
            )
        self.current_asset = None

    def finish(self) -> dict[str, Any]:
        self._finish_group()
        self._finish_asset()
        timestamp_hist: Counter[int] = Counter()
        distinct_received = 0
        for count in self.in_hour_timestamp_counts:
            if count:
                distinct_received += 1
                timestamp_hist[int(count)] += 1
        for count in self.out_hour_timestamp_counts.values():
            distinct_received += 1
            timestamp_hist[int(count)] += 1
        group_total = sum(value["groups"] for value in self.group_buckets.values())
        buckets = {
            key: {
                **value,
                "group_share": _ratio(value["groups"], group_total),
            }
            for key, value in self.group_buckets.items()
        }
        compositions = [
            {"composition": key, **value}
            for key, value in sorted(
                self.compositions.items(),
                key=lambda item: (-item[1]["groups"], item[0]),
            )
        ]
        a6_hist = dict(sorted(self.a6_distinct_hist.items()))
        events = sorted(
            self.recovery_events,
            key=lambda value: (
                value["timestamp_received"], value["market"], value["asset_id"]
            ),
        )
        for event in events:
            event.pop("_received_ms", None)
            event.pop("_group_sequence", None)
        recovered = sum(event["recovery_time_ms"] is not None for event in events)
        valid_ms = self.aggregate_time["valid_ms"]
        invalid_ms = self.aggregate_time["invalid_ms"]
        processed = self.coverage["processed"]
        excluded = self.coverage["excluded"]
        cadence = {
            key: {
                "decision_points_after_first_book": values["eligible"],
                "valid_decision_points": values["valid"],
                "valid_fraction": _ratio(values["valid"], values["eligible"]),
            }
            for key, values in self.cadence.items()
        }
        result = {
            "a1": {
                "status": (
                    "EXPECTED_SCHEMA"
                    if self.schema["column_names_match_expected"]
                    and self.schema["column_types_match_expected"]
                    else "SCHEMA_DRIFT"
                ),
                "schema": self.schema,
                "row_count": self.row_count,
                "distinct_timestamp_received": distinct_received,
                "timestamp_received_min": normalize_json(self.timestamp_min),
                "timestamp_received_max": normalize_json(self.timestamp_max),
                "source_timestamp_min": normalize_json(self.source_min),
                "source_timestamp_max": normalize_json(self.source_max),
                "null_required_fields": {
                    key: self.null_required[key]
                    for key in ("timestamp_received", "timestamp", "market", "asset_id", "event_type")
                },
                "timestamp_received_outside_object_hour": self.received_outside,
                "source_timestamp_outside_object_hour": self.source_outside,
                "distinct_markets": len(self.markets),
                "distinct_assets": len(self.assets),
                "event_type_counts": dict(sorted(self.event_counts.items())),
                "unknown_event_type_rows": sum(
                    count for name, count in self.event_counts.items() if name not in KNOWN_EVENT_TYPES
                ),
                "rows_per_timestamp_received": distribution_from_histogram(timestamp_hist),
                "rows_per_timestamp_received_histogram": dict(sorted(timestamp_hist.items())),
                "availability_group_size_buckets": buckets,
                "plateau_diagnostic": (
                    "Equal timestamp_received plateaus are diagnostic of coarse/batched "
                    "availability but do not establish collector implementation history."
                ),
            },
            "a2": {
                "total_archive_availability_groups": self.a2_total,
                "groups_with_multiple_l2_rows": self.a2_multi,
                "l2_rows_in_multiple_l2_groups": self.a2_l2_rows,
                "all_rows_in_multiple_l2_groups": self.a2_all_rows,
                "multiple_l2_group_rate": _ratio(self.a2_multi, self.a2_total),
                "composition_distribution": compositions,
            },
            "a3": {
                "pure_price_change_tied_groups": self.a3["pure"],
                "all_distinct_price_level_keys": self.a3["distinct"],
                "repeated_keys_one_unique_size": self.a3["idempotent"],
                "repeated_keys_multiple_sizes": self.a3["conflicting"],
                "malformed_groups": self.a3["malformed"],
                "order_ambiguous_rate": _ratio(self.a3["conflicting"], self.a3["pure"]),
                "exact_duplicate_rows_beyond_first": self.a3["duplicate_rows"],
                "exact_duplicate_signatures": self.a3["duplicate_signatures"],
                "representative_counterexamples": self.a3_examples,
            },
            "a4": {
                "groups_containing_book": self.a4["book_groups"],
                "groups_with_multiple_books": self.a4["multi_book"],
                "multiple_book_identical": self.a4["multi_identical"],
                "multiple_book_differing_or_unresolved": self.a4["multi_other"],
                "book_plus_price_change_groups": self.a4["mixed"],
                "book_plus_price_change_invariant": self.a4["mixed_invariant"],
                "book_plus_price_change_ambiguous": self.a4["mixed_ambiguous"],
                "book_plus_price_change_unresolved": self.a4["mixed_unresolved"],
                "representative_counterexamples": self.a4_examples,
            },
            "a5": {
                "tied_groups_containing_tick_transition": self.a5["tied"],
                "single_transition_deterministic": self.a5["single"],
                "duplicate_identical_transitions_deterministic": self.a5["duplicate"],
                "distinct_transitions_ambiguous": self.a5["ambiguous"],
                "malformed_or_unresolved": self.a5["unresolved"],
                "independent_h2_metadata_blocking_groups": self.a5["ambiguous"] + self.a5["unresolved"],
                "representative_counterexamples": self.a5_examples,
                "scope_note": (
                    "Tick ambiguity is reported independently and does not invalidate L2 state "
                    "in this audit."
                ),
            },
            "a6": {
                "tied_groups": self.a6["tied"],
                "all_source_timestamps_equal": self.a6["equal"],
                "different_source_timestamps": self.a6["different"],
                "groups_with_null_source_timestamp": self.a6["null"],
                "source_time_span_ms": {"min": self.a6_span_min, "max": self.a6_span_max},
                "distinct_source_timestamps_per_group": distribution_from_histogram(a6_hist),
                "distinct_source_timestamps_per_group_histogram": a6_hist,
                "published_row_order_source_regression_rows": self.a6["regression_rows"],
                "published_row_order_source_regression_groups": self.a6["regression_groups"],
                "diagnostic_only": (
                    "Published Parquet row order and source timestamp are reported only as "
                    "diagnostics; neither resolves A3, A4, or A5 ambiguity."
                ),
            },
            "a7": {
                "ambiguity_triggered_invalidations": len(events),
                "recovered_invalidations": recovered,
                "right_censored_invalidations": len(events) - recovered,
                "recovery_time_ms": distribution_from_histogram(self.recovery_time_hist),
                "recovery_time_ms_histogram": dict(sorted(self.recovery_time_hist.items())),
                "subsequent_archive_groups_to_recovery": distribution_from_histogram(self.recovery_group_hist),
                "subsequent_archive_groups_to_recovery_histogram": dict(sorted(self.recovery_group_hist.items())),
                "aggregate_asset_time": {
                    "uninitialized_ms": self.aggregate_time["uninitialized_ms"],
                    "valid_ms": valid_ms,
                    "invalid_ms": invalid_ms,
                    "initialized_valid_share": _ratio(valid_ms, valid_ms + invalid_ms),
                },
                "per_asset_wall_clock": self.per_asset,
                "state_changing_row_coverage": {
                    "processed": processed,
                    "excluded_after_ambiguity": excluded,
                    "excluded_while_uninitialized": self.coverage["uninitialized"],
                    "total_audited_state_rows": self.coverage["total"],
                    "processed_share_after_initialization": _ratio(processed, processed + excluded),
                    "excluded_after_ambiguity_share": _ratio(excluded, processed + excluded),
                },
                "recovery_events": events,
                "representative_invalidations": events[:20],
                "scope_note": (
                    "Each file starts UNINITIALIZED. Unknown event groups are unresolved and "
                    "fail closed, but tick and last-trade ordering do not invalidate L2."
                ),
            },
            "a8": {
                "utc_aligned_cadences_minutes": cadence,
                "scope_note": (
                    "Points before an asset's first accepted in-file book are excluded. These "
                    "are sensitivity diagnostics, not a strategy or cadence selection."
                ),
            },
        }
        return normalize_json(result)


def _duckdb_schema(path: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        return _schema(connection, path)
    finally:
        connection.close()


def _row_group_starts(parquet: pq.ParquetFile) -> list[int]:
    starts = [0]
    for index in range(parquet.num_row_groups):
        starts.append(starts[-1] + parquet.metadata.row_group(index).num_rows)
    return starts


def _row_group_for(starts: Sequence[int], row_index: int) -> int:
    for index in range(len(starts) - 1):
        if starts[index] <= row_index < starts[index + 1]:
            return index
    raise ValueError(f"physical row index outside file: {row_index}")


def _enforce_scan_boundary(
    *,
    mode: str,
    total_rows: int,
    reference_scan: bool,
    capability: object | None,
) -> None:
    if mode != "all":
        return
    if capability is _FULL_AUGUST_STREAM_CAPABILITY:
        if reference_scan:
            raise RuntimeError(
                "authorized full-August streaming is separate from reference scans"
            )
        return
    if not reference_scan:
        raise RuntimeError(
            "whole-file streaming is reference-only and requires explicit authorization"
        )
    if total_rows > REFERENCE_SCAN_MAX_ROWS:
        raise RuntimeError(
            "reference scan exceeds the bounded 10,000,000-row safety limit"
        )


def _analyze_stream(
    path: Path,
    *,
    actual_hour: str,
    mode: str = "all",
    batch_size: int = ARROW_BATCH_SIZE,
    source_schema_path: Path | None = None,
    reference_scan: bool = False,
    capability: object | None = None,
    deadline_monotonic: float | None = None,
    progress_callback: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Analyze a whole sorted file or one mechanical complete-asset pilot window."""

    if mode not in {"all", "W1", "W2"}:
        raise ValueError("stream mode must be all, W1, or W2")
    parquet = pq.ParquetFile(path)
    arrow_names = tuple(parquet.schema_arrow.names)
    unexpected = [name for name in arrow_names if name not in EXPECTED_COLUMNS and name != "_ip002r_source_row_ordinal"]
    missing = [name for name in EXPECTED_COLUMNS if name not in arrow_names]
    if missing or unexpected:
        raise ValueError(f"Arrow schema mismatch; missing={missing}, unexpected={unexpected}")
    starts = _row_group_starts(parquet)
    total_rows = starts[-1]
    _enforce_scan_boundary(
        mode=mode,
        total_rows=total_rows,
        reference_scan=reference_scan,
        capability=capability,
    )
    midpoint = total_rows // 2
    first_group = 0 if mode != "W2" else _row_group_for(starts, midpoint)
    columns = list(EXPECTED_COLUMNS)
    if "_ip002r_source_row_ordinal" in arrow_names:
        columns.append("_ip002r_source_row_ordinal")
    schema_path = source_schema_path or path
    schema = _duckdb_schema(schema_path)
    analysis_schema = (
        schema if schema_path.resolve() == path.resolve() else _duckdb_schema(path)
    )
    enforce_a1_a8_schema(schema, source_label=str(schema_path))
    enforce_a1_a8_schema(analysis_schema, source_label=str(path))
    analyzer = StreamingAggregates(hour=actual_hour, schema=schema)
    started = time.perf_counter()
    batches_read = 0
    analyzed_batches = 0
    physical_index = starts[first_group]
    tested_start = physical_index
    window_start: int | None = None
    window_end: int | None = None
    discarded_midpoint_asset: tuple[bytes, str] | None = None
    active_asset: tuple[bytes, str] | None = None
    last_included_asset: tuple[bytes, str] | None = None
    complete_asset_count = 0
    rows_analyzed = 0
    stop = False
    last_tested = physical_index
    previous_tested_key: tuple[bytes, str, datetime] | None = None
    row_groups = list(range(first_group, parquet.num_row_groups))
    for batch in parquet.iter_batches(
        batch_size=batch_size,
        row_groups=row_groups,
        columns=columns,
        use_threads=False,
    ):
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            raise StreamingDeadlineExceeded(
                "authorized eight-hour streaming wall-clock budget expired"
            )
        batches_read += 1
        batch_had_rows = False
        rows = batch.to_pylist()
        for values in rows:
            row_index = physical_index
            physical_index += 1
            last_tested = physical_index
            require_non_null_grouping_values(
                values, context=f"stream physical row {row_index}"
            )
            market = values["market"]
            asset_id = values["asset_id"]
            received = values["timestamp_received"]
            tested_key = (market, asset_id, received)
            if previous_tested_key is not None and tested_key < previous_tested_key:
                raise SortContractError(
                    f"composite sort regression at physical row {row_index}: "
                    f"{normalize_json(tested_key)!r} after "
                    f"{normalize_json(previous_tested_key)!r}"
                )
            previous_tested_key = tested_key
            asset_key = (market, asset_id)
            if mode == "W2" and window_start is None:
                if row_index < midpoint:
                    continue
                if discarded_midpoint_asset is None:
                    discarded_midpoint_asset = asset_key
                    continue
                if asset_key == discarded_midpoint_asset:
                    continue
                window_start = row_index
                active_asset = asset_key
            elif window_start is None:
                window_start = row_index
                active_asset = asset_key
            if mode in {"W1", "W2"} and rows_analyzed >= WINDOW_MIN_ROWS and asset_key != active_asset:
                window_end = row_index
                stop = True
                break
            active_asset = asset_key
            values["_physical_row"] = row_index
            if asset_key != last_included_asset:
                complete_asset_count += 1
                last_included_asset = asset_key
            analyzer.consume(values)
            rows_analyzed += 1
            batch_had_rows = True
        if batch_had_rows:
            analyzed_batches += 1
        if progress_callback is not None:
            progress_callback(
                {
                    "batches_read": batches_read,
                    "batches_with_analyzed_rows": analyzed_batches,
                    "physical_rows_read": physical_index,
                    "rows_analyzed": rows_analyzed,
                    "parquet_total_rows": total_rows,
                    "maximum_buffered_logical_group_size": analyzer.max_group_size,
                    "maximum_asset_rows_observed": analyzer.max_asset_rows,
                }
            )
        if stop:
            break
    if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
        raise StreamingDeadlineExceeded(
            "authorized eight-hour streaming wall-clock budget expired"
        )
    if mode in {"W1", "W2"} and rows_analyzed < WINDOW_MIN_ROWS:
        raise RuntimeError(f"{mode} could not obtain 2,000,000 complete-asset rows")
    if window_start is None:
        raise RuntimeError("stream selected no rows")
    if window_end is None:
        window_end = physical_index
    scientific = analyzer.finish()
    elapsed = time.perf_counter() - started
    final_group = _row_group_for(starts, max(window_start, window_end - 1))
    tested_final_group = _row_group_for(starts, max(tested_start, last_tested - 1))
    return {
        "status": "ANALYZED",
        "scientific": scientific,
        "streaming": {
            "mode": mode,
            "arrow_batch_size": batch_size,
            "batches_read": batches_read,
            "batches_with_analyzed_rows": analyzed_batches,
            "physical_row_range": [window_start, window_end],
            "physical_rows_tested_range": [tested_start, last_tested],
            "row_groups_read": list(range(first_group, tested_final_group + 1)),
            "window_row_groups": list(range(_row_group_for(starts, window_start), final_group + 1)),
            "rows_analyzed": rows_analyzed,
            "complete_asset_count": complete_asset_count,
            "maximum_buffered_logical_group_size": analyzer.max_group_size,
            "maximum_asset_rows_observed": analyzer.max_asset_rows,
            "sort_contract_monotonic": True,
            "elapsed_seconds": elapsed,
            "parquet_total_rows": total_rows,
            "physical_midpoint": midpoint if mode == "W2" else None,
            "discarded_midpoint_asset": normalize_json(discarded_midpoint_asset),
        },
    }


def analyze_stream(
    path: Path,
    *,
    actual_hour: str,
    mode: str = "all",
    batch_size: int = ARROW_BATCH_SIZE,
    source_schema_path: Path | None = None,
    reference_scan: bool = False,
) -> dict[str, Any]:
    """Analyze a pilot window or a bounded reference-only whole file."""

    return _analyze_stream(
        path,
        actual_hour=actual_hour,
        mode=mode,
        batch_size=batch_size,
        source_schema_path=source_schema_path,
        reference_scan=reference_scan,
    )


def _analyze_authorized_full_august(
    path: Path,
    *,
    actual_hour: str,
    batch_size: int = ARROW_BATCH_SIZE,
    deadline_monotonic: float,
    progress_callback: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Private capability route used only by the full-August authorization worker."""

    return _analyze_stream(
        path,
        actual_hour=actual_hour,
        mode="all",
        batch_size=batch_size,
        reference_scan=False,
        capability=_FULL_AUGUST_STREAM_CAPABILITY,
        deadline_monotonic=deadline_monotonic,
        progress_callback=progress_callback,
    )


def scientific_a1_a8(result: Mapping[str, Any]) -> dict[str, Any]:
    return {key: result["scientific"][key] for key in [f"a{index}" for index in range(1, 9)]}


def streaming_gate(
    *,
    exact_equivalence: bool,
    monotonic: bool,
    w1_seconds: float,
    w2_seconds: float,
    w1_rows: int,
    w2_rows: int,
    peak_rss_bytes: int,
    temp_growth_bytes: int,
    failed: bool,
) -> dict[str, Any]:
    r1 = w1_seconds / w1_rows
    r2 = w2_seconds / w2_rows
    projected = max(r1, r2) * AUGUST_ROWS * SAFETY_FACTOR
    checks = {
        "exact_equivalence": exact_equivalence,
        "tested_sort_keys_monotonic": monotonic,
        "combined_analysis_within_10_minutes": w1_seconds + w2_seconds <= STREAMING_MAX_COMBINED_SECONDS,
        "peak_rss_within_4_gib": peak_rss_bytes <= STREAMING_MAX_RSS_BYTES,
        "temporary_storage_within_2_gib": temp_growth_bytes <= STREAMING_MAX_TEMP_BYTES,
        "no_unrecoverable_failure": not failed,
        "projected_full_august_within_30_minutes": projected <= STREAMING_MAX_PROJECTED_SECONDS,
    }
    return {
        "status": "STREAMING_PILOT_PASS" if all(checks.values()) else "STREAMING_NOT_FEASIBLE",
        "checks": checks,
        "r1_seconds_per_row": r1,
        "r2_seconds_per_row": r2,
        "conservative_projected_full_august_seconds": projected,
        "safety_factor": SAFETY_FACTOR,
    }


def compare_preserved_checkpoint(
    *, original_output: Path, recovery_root: Path
) -> dict[str, Any]:
    partition = json.loads(
        (recovery_root / "august-32-shards" / "partition-manifest.json").read_text(encoding="utf-8")
    )
    shards = {int(item["shard_id"]): item for item in partition["shards"]}
    raw_path = original_output / "raw" / "polymarket_orderbook_2026-08-01T12.parquet"
    comparisons: list[dict[str, Any]] = []
    for shard_id in (0, 1, 2, 18):
        shard = shards[shard_id]
        if len(shard["files"]) != 1 or len(shard["file_sha256"]) != 1:
            raise ValueError(f"preserved shard {shard_id} has an unexpected file layout")
        path = recovery_root / "august-32-shards" / shard["files"][0]
        checkpoint_path = recovery_root / "checkpoints-32" / f"shard-{shard_id:03d}.json"
        partition_hash = sha256_file(path)
        if partition_hash != shard["file_sha256"][0]:
            raise ValueError(f"preserved shard {shard_id} hash mismatch")
        checkpoint = verify_checkpoint(checkpoint_path)
        try:
            streamed = analyze_stream(
                path,
                actual_hour="2026-08-01T12",
                mode="all",
                source_schema_path=raw_path,
                reference_scan=True,
            )
        except SortContractError as exc:
            comparisons.append({"shard_id": shard_id, "sort_contract_monotonic": False, "reason": str(exc)})
            continue
        expected = {key: checkpoint["payload"]["analysis"][key] for key in [f"a{index}" for index in range(1, 9)]}
        observed = scientific_a1_a8(streamed)
        equal = observed == expected
        comparisons.append(
            {
                "shard_id": shard_id,
                "sort_contract_monotonic": True,
                "exact_a1_a8_equal": equal,
                "streaming_a1_a8_sha256": hashlib.sha256(_canonical_json(observed)).hexdigest(),
                "checkpoint_a1_a8_sha256": hashlib.sha256(_canonical_json(expected)).hexdigest(),
                "partition_file_sha256": partition_hash,
                "checkpoint_file_sha256": sha256_file(checkpoint_path),
                "rows": streamed["streaming"]["rows_analyzed"],
            }
        )
        if not equal:
            raise RuntimeError(f"streaming result differs from preserved shard {shard_id}")
        return {"status": "EXACT_MATCH", "comparisons": comparisons}
    return {"status": "NO_SORTED_PRESERVED_SHARD", "comparisons": comparisons}


def run_streaming_pilot(
    *,
    original_output: Path,
    recovery_root: Path,
    pilot_root: Path,
    repository: Path,
    offline_test_result: str,
    exact_equivalence_passed: bool,
    real_shard_comparison: Mapping[str, Any],
) -> dict[str, Any]:
    git = git_provenance(repository)
    if git["dirty"]:
        raise RuntimeError("streaming pilot requires clean committed code")
    evidence = verify_preserved_evidence(original_output)
    august = next(item for item in evidence["samples"] if item["requested_hour"] == "2026-08-01T12")
    raw_path = Path(str(august["local_path"]))
    if sha256_file(raw_path) != august["sha256"]:
        raise ValueError("preserved August hash mismatch")
    pilot_root.mkdir(parents=True, exist_ok=True)
    worker = [sys.executable, "-m", "tools.probes.pmxt_ordering_audit"]
    resources: dict[str, dict[str, Any]] = {}
    for mode in ("W1", "W2"):
        output = pilot_root / f"{mode.lower()}-result.json"
        temp = pilot_root / f"{mode.lower()}-temp"
        if output.exists():
            raise FileExistsError(f"pilot window output already exists: {output}")
        temp.mkdir(parents=True, exist_ok=True)
        monitored = _monitor_process(
            worker + [
                "streaming-window-worker",
                "--path", str(raw_path),
                "--actual-hour", "2026-08-01T12",
                "--mode", mode,
                "--output", str(output),
            ],
            temp,
        )
        if monitored["exit_code"]:
            raise RuntimeError(f"{mode} streaming worker failed")
        value = json.loads(output.read_text(encoding="utf-8"))
        resources[mode] = {
            **value["streaming"],
            "process_wall_seconds": monitored["elapsed_seconds"],
            "peak_process_tree_rss_bytes": monitored["peak_rss_bytes"],
            "temporary_storage_growth_bytes": monitored["peak_temp_growth_bytes"],
            "checkpoint_path": str(output.resolve()),
            "checkpoint_size_bytes": output.stat().st_size,
            "checkpoint_sha256": sha256_file(output),
        }
    peak_rss = max(item["peak_process_tree_rss_bytes"] for item in resources.values())
    temp_growth = max(item["temporary_storage_growth_bytes"] for item in resources.values())
    real_comparison_valid = real_shard_comparison.get("status") in {
        "EXACT_MATCH",
        "NO_SORTED_PRESERVED_SHARD",
    }
    exact = exact_equivalence_passed and real_comparison_valid
    gate = streaming_gate(
        exact_equivalence=exact,
        monotonic=all(item["sort_contract_monotonic"] for item in resources.values()),
        w1_seconds=float(resources["W1"]["process_wall_seconds"]),
        w2_seconds=float(resources["W2"]["process_wall_seconds"]),
        w1_rows=int(resources["W1"]["rows_analyzed"]),
        w2_rows=int(resources["W2"]["rows_analyzed"]),
        peak_rss_bytes=int(peak_rss),
        temp_growth_bytes=int(temp_growth),
        failed=False,
    )
    result = {
        "format": "ip-002s-streaming-pilot-v1",
        "status": gate["status"],
        "completed_at": _utc_now(),
        "pilot_only": True,
        "full_august_launched": False,
        "scientific_outcomes_used_for_tuning_or_conclusion": False,
        "offline_test_result": offline_test_result,
        "git": git,
        "python_version": sys.version,
        "pyarrow_version": pyarrow.__version__,
        "preserved_evidence": evidence,
        "original_analysis_sha256": ORIGINAL_ANALYSIS_SHA256,
        "real_shard_comparison": real_shard_comparison,
        "windows": resources,
        "peak_process_tree_rss_bytes": peak_rss,
        "maximum_temporary_storage_growth_bytes": temp_growth,
        "gate": gate,
    }
    output = pilot_root / "streaming-pilot.json"
    _atomic_create_json(output, result)
    return result
