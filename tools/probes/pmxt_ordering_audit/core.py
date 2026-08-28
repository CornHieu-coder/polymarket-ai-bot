"""Pure, order-independent classification primitives for the IP-002 audit."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


AUDIT_VERSION = "0.1.0"
PREDECLARED_SAMPLE_HOURS = (
    "2026-05-01T12",
    "2026-06-15T12",
    "2026-08-01T12",
)
KNOWN_EVENT_TYPES = {
    "book",
    "price_change",
    "last_trade_price",
    "tick_size_change",
}
L2_EVENT_TYPES = {"book", "price_change"}
GROUPING_FIELDS = ("market", "asset_id", "timestamp_received")


class AuditParseError(ValueError):
    """Raised when a value cannot be parsed exactly enough for classification."""


@dataclass(frozen=True)
class CanonicalBook:
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]

    def level_size(self, side: str, price: Decimal) -> Decimal | None:
        levels = self.bids if side == "BUY" else self.asks
        for level_price, level_size in levels:
            if level_price == price:
                return level_size
        return None


def parse_decimal(value: Any, field: str) -> Decimal:
    """Parse an archive scalar without permitting binary-float conclusions."""

    if value is None or isinstance(value, bool) or isinstance(value, float):
        raise AuditParseError(f"{field} must be a non-null exact decimal value")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise AuditParseError(f"{field} is not a valid decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise AuditParseError(f"{field} must be finite")
    return parsed


def decimal_text(value: Decimal) -> str:
    """Return a deterministic non-exponent decimal representation."""

    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def _canonical_levels(raw: Any, field: str) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(raw, str):
        raise AuditParseError(f"{field} must be a JSON string")
    try:
        decoded = json.loads(raw, parse_float=Decimal, parse_int=Decimal)
    except (json.JSONDecodeError, TypeError) as exc:
        raise AuditParseError(f"{field} is malformed JSON") from exc
    if not isinstance(decoded, list):
        raise AuditParseError(f"{field} must decode to a list")
    levels: dict[Decimal, Decimal] = {}
    for index, item in enumerate(decoded):
        if not isinstance(item, list) or len(item) != 2:
            raise AuditParseError(f"{field}[{index}] must be [price, size]")
        price = parse_decimal(item[0], f"{field}[{index}].price")
        size = parse_decimal(item[1], f"{field}[{index}].size")
        if price in levels:
            raise AuditParseError(f"{field} repeats price {decimal_text(price)}")
        if size < 0:
            raise AuditParseError(f"{field}[{index}].size must be non-negative")
        levels[price] = size
    return tuple(sorted(levels.items()))


def canonical_book(row: Mapping[str, Any]) -> CanonicalBook:
    return CanonicalBook(
        bids=_canonical_levels(row.get("bids"), "book.bids"),
        asks=_canonical_levels(row.get("asks"), "book.asks"),
    )


def group_key(row: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    """Return the exact IP-002 archive-availability grouping key."""

    return tuple(row.get(field) for field in GROUPING_FIELDS)


def candidate_hours(requested_hour: str) -> tuple[str, ...]:
    """Return the requested hour and only later hours on the same UTC date."""

    try:
        requested = datetime.strptime(requested_hour, "%Y-%m-%dT%H").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError("sample hour must use YYYY-MM-DDTHH") from exc
    return tuple(
        requested.replace(hour=hour).strftime("%Y-%m-%dT%H")
        for hour in range(requested.hour, 24)
    )


def _row_signature(row: Mapping[str, Any]) -> str:
    return json.dumps(
        normalize_json(dict(row)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def classify_availability_group(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Classify a logical group without using input order or source timestamps.

    The result intentionally treats source timestamps as payload provenance only. Any
    list rendering is sorted by canonical content so reversing published row order cannot
    alter the classification or its representative details.
    """

    if not rows:
        raise ValueError("an availability group must contain at least one row")
    keys = {group_key(row) for row in rows}
    if len(keys) != 1 or any(value is None for value in next(iter(keys))):
        raise ValueError("rows must share a non-null archive-availability key")

    event_counts = Counter(str(row.get("event_type")) for row in rows)
    unknown_event_types = sorted(set(event_counts) - KNOWN_EVENT_TYPES)
    l2_rows = [row for row in rows if row.get("event_type") in L2_EVENT_TYPES]
    book_rows = [row for row in l2_rows if row.get("event_type") == "book"]
    price_rows = [
        row for row in l2_rows if row.get("event_type") == "price_change"
    ]
    tick_rows = [
        row for row in rows if row.get("event_type") == "tick_size_change"
    ]

    signatures = Counter(_row_signature(row) for row in rows)
    exact_duplicate_rows = sum(count - 1 for count in signatures.values())

    l2_status = "NOT_APPLICABLE"
    l2_reason = "no_l2_rows"
    accepted_book = False
    price_key_sizes: dict[tuple[str, Decimal], set[Decimal]] = defaultdict(set)
    repeated_price_keys = 0
    conflicting_price_keys = 0
    parse_errors: list[str] = []

    if unknown_event_types:
        l2_status = "UNRESOLVED"
        l2_reason = "unknown_event_type"
    elif price_rows and not book_rows:
        price_key_counts: Counter[tuple[str, Decimal]] = Counter()
        try:
            for row in price_rows:
                side = str(row.get("side"))
                if side not in {"BUY", "SELL"}:
                    raise AuditParseError(f"price_change.side is invalid: {side!r}")
                price = parse_decimal(row.get("price"), "price_change.price")
                size = parse_decimal(row.get("size"), "price_change.size")
                if size < 0:
                    raise AuditParseError(
                        "price_change.size must be a non-negative replacement size"
                    )
                key = (side, price)
                price_key_counts[key] += 1
                price_key_sizes[key].add(size)
        except AuditParseError as exc:
            price_key_sizes.clear()
            parse_errors.append(str(exc))
            l2_status = "UNRESOLVED"
            l2_reason = "price_change_parse_failure"
        else:
            repeated_price_keys = sum(count > 1 for count in price_key_counts.values())
            conflicting_price_keys = sum(
                len(sizes) > 1 for sizes in price_key_sizes.values()
            )
            if conflicting_price_keys:
                l2_status = "AMBIGUOUS"
                l2_reason = "repeated_price_key_multiple_sizes"
            else:
                l2_status = "INVARIANT"
                l2_reason = (
                    "repeated_price_keys_idempotent"
                    if repeated_price_keys
                    else "distinct_price_keys_commute"
                )
    elif book_rows:
        try:
            books = [canonical_book(row) for row in book_rows]
        except AuditParseError as exc:
            parse_errors.append(str(exc))
            l2_status = "UNRESOLVED"
            l2_reason = "book_parse_failure"
        else:
            unique_books = set(books)
            if len(unique_books) != 1:
                l2_status = "AMBIGUOUS"
                l2_reason = "differing_book_snapshots"
            elif not price_rows:
                l2_status = "INVARIANT"
                l2_reason = "equivalent_book_snapshots"
                accepted_book = True
            else:
                snapshot = books[0]
                try:
                    idempotent = True
                    for row in price_rows:
                        side = str(row.get("side"))
                        if side not in {"BUY", "SELL"}:
                            raise AuditParseError(
                                f"price_change.side is invalid: {side!r}"
                            )
                        price = parse_decimal(row.get("price"), "price_change.price")
                        size = parse_decimal(row.get("size"), "price_change.size")
                        if size < 0:
                            raise AuditParseError(
                                "price_change.size must be a non-negative replacement size"
                            )
                        existing = snapshot.level_size(side, price)
                        if size == 0:
                            idempotent = idempotent and existing is None
                        else:
                            idempotent = idempotent and existing == size
                except AuditParseError as exc:
                    parse_errors.append(str(exc))
                    l2_status = "UNRESOLVED"
                    l2_reason = "book_price_change_parse_failure"
                else:
                    if idempotent:
                        l2_status = "INVARIANT"
                        l2_reason = "price_changes_idempotent_to_snapshot"
                        accepted_book = True
                    else:
                        l2_status = "AMBIGUOUS"
                        l2_reason = "book_price_change_order_sensitive"

    tick_status = "NOT_APPLICABLE"
    tick_reason = "no_tick_transition"
    tick_transitions: set[tuple[Decimal, Decimal]] = set()
    if tick_rows:
        try:
            for row in tick_rows:
                tick_transitions.add(
                    (
                        parse_decimal(
                            row.get("old_tick_size"), "tick_size_change.old_tick_size"
                        ),
                        parse_decimal(
                            row.get("new_tick_size"), "tick_size_change.new_tick_size"
                        ),
                    )
                )
        except AuditParseError as exc:
            parse_errors.append(str(exc))
            tick_status = "UNRESOLVED"
            tick_reason = "tick_parse_failure"
        else:
            if len(tick_rows) == 1:
                tick_status = "DETERMINISTIC"
                tick_reason = "single_tick_transition"
            elif len(tick_transitions) == 1:
                tick_status = "DETERMINISTIC"
                tick_reason = "duplicate_tick_transitions"
            else:
                tick_status = "AMBIGUOUS"
                tick_reason = "distinct_tick_transitions"

    repeated_details = [
        {
            "side": side,
            "price": decimal_text(price),
            "sizes": sorted(decimal_text(size) for size in sizes),
        }
        for (side, price), sizes in sorted(
            price_key_sizes.items(), key=lambda item: (item[0][0], item[0][1])
        )
        if len(sizes) > 1
    ]
    return {
        "group_key": [normalize_json(value) for value in next(iter(keys))],
        "group_size": len(rows),
        "event_counts": dict(sorted(event_counts.items())),
        "l2_row_count": len(l2_rows),
        "book_count": len(book_rows),
        "price_change_count": len(price_rows),
        "last_trade_price_count": event_counts.get("last_trade_price", 0),
        "tick_size_change_count": len(tick_rows),
        "unknown_event_types": unknown_event_types,
        "l2_status": l2_status,
        "l2_reason": l2_reason,
        "accepted_book": accepted_book,
        "repeated_price_keys": repeated_price_keys,
        "conflicting_price_keys": conflicting_price_keys,
        "conflicting_replacements": repeated_details,
        "exact_duplicate_rows": exact_duplicate_rows,
        "tick_status": tick_status,
        "tick_reason": tick_reason,
        "distinct_tick_transitions": len(tick_transitions),
        "parse_errors": sorted(parse_errors),
    }


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def weighted_nearest_rank(
    histogram: Mapping[int, int], probability: float
) -> int | None:
    """Nearest-rank quantile over an integer value->frequency histogram."""

    total = sum(histogram.values())
    if total == 0:
        return None
    rank = max(1, math.ceil(probability * total))
    seen = 0
    for value, frequency in sorted(histogram.items()):
        seen += frequency
        if seen >= rank:
            return value
    raise AssertionError("histogram rank was not reached")


def distribution_from_histogram(histogram: Mapping[int, int]) -> dict[str, Any]:
    if not histogram:
        return {
            "count": 0,
            "min": None,
            "p50": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "max": None,
            "quantile_method": "nearest-rank without interpolation",
        }
    return {
        "count": sum(histogram.values()),
        "min": min(histogram),
        "p50": weighted_nearest_rank(histogram, 0.50),
        "p90": weighted_nearest_rank(histogram, 0.90),
        "p95": weighted_nearest_rank(histogram, 0.95),
        "p99": weighted_nearest_rank(histogram, 0.99),
        "max": max(histogram),
        "quantile_method": "nearest-rank without interpolation",
    }


def normalize_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, datetime):
        rendered = value.astimezone(timezone.utc).isoformat(timespec="milliseconds")
        return rendered.replace("+00:00", "Z")
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError:
            return value.hex()
    if isinstance(value, Mapping):
        return {
            str(key): normalize_json(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [normalize_json(item) for item in value]
    return value


def merge_histograms(histograms: Iterable[Mapping[int, int]]) -> dict[int, int]:
    pooled: Counter[int] = Counter()
    for histogram in histograms:
        pooled.update({int(key): int(value) for key, value in histogram.items()})
    return dict(sorted(pooled.items()))
