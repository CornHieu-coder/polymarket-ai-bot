"""DuckDB-backed offline analysis for the bounded IP-002 Parquet samples."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb

from .core import (
    AUDIT_VERSION,
    classify_availability_group,
    distribution_from_histogram,
    merge_histograms,
    normalize_json,
    sha256_file,
)


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
EXPECTED_DUCKDB_TYPES = {
    "timestamp_received": {"TIMESTAMP WITH TIME ZONE"},
    "timestamp": {"TIMESTAMP WITH TIME ZONE"},
    "market": {"BLOB"},
    "event_type": {"VARCHAR"},
    "asset_id": {"VARCHAR"},
    "bids": {"VARCHAR"},
    "asks": {"VARCHAR"},
    "price": {"DECIMAL(9,4)"},
    "size": {"DECIMAL(18,6)"},
    "side": {"VARCHAR"},
    "best_bid": {"DECIMAL(9,4)"},
    "best_ask": {"DECIMAL(9,4)"},
    "fee_rate_bps": {"USMALLINT"},
    "transaction_hash": {"VARCHAR"},
    "old_tick_size": {"DECIMAL(9,4)"},
    "new_tick_size": {"DECIMAL(9,4)"},
}


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _hour_bounds(hour: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(hour, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    return start, start + timedelta(hours=1)


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _histogram(rows: Iterable[Sequence[Any]]) -> dict[int, int]:
    return {int(row[0]): int(row[1]) for row in rows}


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    return float(numerator / denominator) if denominator else None


def _schema(connection: duckdb.DuckDBPyConnection, path: Path) -> dict[str, Any]:
    described = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
    ).fetchall()
    columns = [
        {
            "name": row[0],
            "duckdb_type": row[1],
            "nullable": row[2],
            "key": row[3],
            "default": row[4],
            "extra": row[5],
        }
        for row in described
    ]
    cursor = connection.execute("SELECT * FROM parquet_schema(?)", [str(path)])
    parquet_names = [item[0] for item in cursor.description]
    parquet_rows = [
        dict(zip(parquet_names, row, strict=True)) for row in cursor.fetchall()
    ]
    actual_names = tuple(column["name"] for column in columns)
    type_mismatches = [
        {
            "name": column["name"],
            "observed": column["duckdb_type"],
            "expected": sorted(EXPECTED_DUCKDB_TYPES.get(column["name"], set())),
        }
        for column in columns
        if column["name"] in EXPECTED_DUCKDB_TYPES
        and column["duckdb_type"] not in EXPECTED_DUCKDB_TYPES[column["name"]]
    ]
    return {
        "expected_column_names": list(EXPECTED_COLUMNS),
        "columns": columns,
        "parquet_schema": normalize_json(parquet_rows),
        "column_names_match_expected": actual_names == EXPECTED_COLUMNS,
        "column_types_match_expected": not type_mismatches,
        "type_mismatches": type_mismatches,
        "missing_columns": [name for name in EXPECTED_COLUMNS if name not in actual_names],
        "unexpected_columns": [name for name in actual_names if name not in EXPECTED_COLUMNS],
    }


def _book_counterexample(
    rows: Sequence[Mapping[str, Any]], classification: Mapping[str, Any]
) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    for row in sorted(
        rows,
        key=lambda item: json.dumps(
            normalize_json(dict(item)), sort_keys=True, separators=(",", ":")
        ),
    ):
        event_type = str(row.get("event_type"))
        if event_type == "book":
            bids = str(row.get("bids"))
            asks = str(row.get("asks"))
            evidence.append(
                {
                    "event_type": event_type,
                    "source_timestamp": normalize_json(row.get("timestamp")),
                    "bids_sha256": hashlib.sha256(bids.encode()).hexdigest(),
                    "asks_sha256": hashlib.sha256(asks.encode()).hexdigest(),
                    "bids_bytes": len(bids.encode()),
                    "asks_bytes": len(asks.encode()),
                }
            )
        elif event_type == "price_change":
            evidence.append(
                {
                    "event_type": event_type,
                    "source_timestamp": normalize_json(row.get("timestamp")),
                    "side": normalize_json(row.get("side")),
                    "price": normalize_json(row.get("price")),
                    "size": normalize_json(row.get("size")),
                }
            )
        if len(evidence) == 20:
            break
    return {
        "group_key": classification["group_key"],
        "l2_reason": classification["l2_reason"],
        "event_counts": classification["event_counts"],
        "parse_errors": classification["parse_errors"],
        "representative_rows": evidence,
    }


def _load_book_groups(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    columns = (
        "market",
        "asset_id",
        "timestamp_received",
        "timestamp",
        "event_type",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
        "old_tick_size",
        "new_tick_size",
    )
    cursor = connection.execute(
        """
        SELECT e.market, e.asset_id, e.timestamp_received, e.timestamp,
               e.event_type, e.bids, e.asks, e.price, e.size, e.side,
               e.best_bid, e.best_ask, e.old_tick_size, e.new_tick_size
        FROM events AS e
        JOIN group_base AS g
          USING (market, asset_id, timestamp_received)
        WHERE g.book_count > 0
        ORDER BY e.market, e.asset_id, e.timestamp_received, e.event_type,
                 e.side, e.price, e.size, e.bids, e.asks, e.timestamp
        """
    )
    classifications: list[dict[str, Any]] = []
    counterexamples: list[dict[str, Any]] = []
    current_key: tuple[Any, Any, Any] | None = None
    current_rows: list[dict[str, Any]] = []

    def finish() -> None:
        if not current_rows:
            return
        classification = classify_availability_group(current_rows)
        classification["_raw_group_key"] = current_key
        classifications.append(classification)
        if (
            classification["l2_status"] in {"AMBIGUOUS", "UNRESOLVED"}
            and len(counterexamples) < 20
        ):
            counterexamples.append(_book_counterexample(current_rows, classification))

    while batch := cursor.fetchmany(100_000):
        for values in batch:
            row = dict(zip(columns, values, strict=True))
            key = (row["market"], row["asset_id"], row["timestamp_received"])
            if current_key is not None and key != current_key:
                finish()
                current_rows = []
            current_key = key
            current_rows.append(row)
    finish()
    return classifications, counterexamples


def _create_analysis_tables(
    connection: duckdb.DuckDBPyConnection, path: Path
) -> None:
    connection.execute(
        f"""
        CREATE VIEW events AS
        SELECT * FROM read_parquet('{_sql_path(path)}', file_row_number=true)
        """
    )
    connection.execute(
        """
        CREATE TABLE group_base AS
        SELECT market, asset_id, timestamp_received,
               COUNT(*)::BIGINT AS group_size,
               COUNT_IF(event_type = 'book')::BIGINT AS book_count,
               COUNT_IF(event_type = 'price_change')::BIGINT AS price_change_count,
               COUNT_IF(event_type = 'last_trade_price')::BIGINT AS last_trade_count,
               COUNT_IF(event_type = 'tick_size_change')::BIGINT AS tick_count,
               COUNT_IF(
                   event_type IS NULL OR event_type NOT IN (
                       'book', 'price_change', 'last_trade_price', 'tick_size_change'
                   )
               )::BIGINT AS unknown_count,
               COUNT_IF(event_type IN ('book', 'price_change'))::BIGINT AS l2_rows,
               COUNT_IF(
                   event_type = 'price_change'
                   AND (
                       side IS NULL OR side NOT IN ('BUY', 'SELL')
                       OR price IS NULL OR size IS NULL
                   )
               )::BIGINT AS malformed_price_changes
        FROM events
        WHERE market IS NOT NULL
          AND asset_id IS NOT NULL
          AND timestamp_received IS NOT NULL
        GROUP BY market, asset_id, timestamp_received
        """
    )
    connection.execute(
        """
        CREATE TABLE price_key_stats AS
        SELECT market, asset_id, timestamp_received, side, price,
               COUNT(*)::BIGINT AS row_count,
               COUNT(DISTINCT size)::BIGINT AS unique_size_count
        FROM events
        WHERE event_type = 'price_change'
          AND market IS NOT NULL
          AND asset_id IS NOT NULL
          AND timestamp_received IS NOT NULL
          AND side IN ('BUY', 'SELL')
          AND price IS NOT NULL
          AND size IS NOT NULL
        GROUP BY market, asset_id, timestamp_received, side, price
        """
    )
    connection.execute(
        """
        CREATE TABLE price_group_stats AS
        SELECT market, asset_id, timestamp_received,
               COUNT_IF(row_count > 1)::BIGINT AS repeated_price_keys,
               COUNT_IF(unique_size_count > 1)::BIGINT AS conflicting_price_keys
        FROM price_key_stats
        GROUP BY market, asset_id, timestamp_received
        """
    )


def _a1(
    connection: duckdb.DuckDBPyConnection,
    *,
    schema: Mapping[str, Any],
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    overview = connection.execute(
        """
        SELECT COUNT(*)::BIGINT,
               COUNT(DISTINCT timestamp_received)::BIGINT,
               MIN(timestamp_received), MAX(timestamp_received),
               MIN(timestamp), MAX(timestamp),
               COUNT_IF(timestamp_received IS NULL)::BIGINT,
               COUNT_IF(timestamp IS NULL)::BIGINT,
               COUNT_IF(market IS NULL)::BIGINT,
               COUNT_IF(asset_id IS NULL)::BIGINT,
               COUNT_IF(event_type IS NULL)::BIGINT,
               COUNT_IF(timestamp_received < ? OR timestamp_received >= ?)::BIGINT,
               COUNT_IF(timestamp < ? OR timestamp >= ?)::BIGINT,
               COUNT(DISTINCT market)::BIGINT,
               COUNT(DISTINCT asset_id)::BIGINT
        FROM events
        """,
        [start, end, start, end],
    ).fetchone()
    assert overview is not None
    rows_per_timestamp_histogram = _histogram(
        connection.execute(
            """
            SELECT rows_per_timestamp, COUNT(*)::BIGINT
            FROM (
                SELECT timestamp_received, COUNT(*)::BIGINT AS rows_per_timestamp
                FROM events
                WHERE timestamp_received IS NOT NULL
                GROUP BY timestamp_received
            )
            GROUP BY rows_per_timestamp
            ORDER BY rows_per_timestamp
            """
        ).fetchall()
    )
    group_buckets = {
        row[0]: {"groups": int(row[1]), "rows": int(row[2])}
        for row in connection.execute(
            """
            SELECT CASE
                       WHEN group_size = 1 THEN '1'
                       WHEN group_size = 2 THEN '2'
                       WHEN group_size = 3 THEN '3'
                       WHEN group_size = 4 THEN '4'
                       WHEN group_size BETWEEN 5 AND 9 THEN '5-9'
                       WHEN group_size BETWEEN 10 AND 99 THEN '10-99'
                       ELSE '100+'
                   END AS bucket,
                   COUNT(*)::BIGINT AS groups,
                   SUM(group_size)::BIGINT AS rows
            FROM group_base
            GROUP BY bucket
            ORDER BY MIN(group_size)
            """
        ).fetchall()
    }
    total_groups = sum(item["groups"] for item in group_buckets.values())
    for item in group_buckets.values():
        item["group_share"] = _ratio(item["groups"], total_groups)
    event_counts = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT event_type, COUNT(*)::BIGINT FROM events GROUP BY event_type ORDER BY event_type"
        ).fetchall()
    }
    return {
        "status": (
            "EXPECTED_SCHEMA"
            if schema["column_names_match_expected"]
            and schema["column_types_match_expected"]
            else "SCHEMA_DRIFT"
        ),
        "schema": schema,
        "row_count": int(overview[0]),
        "distinct_timestamp_received": int(overview[1]),
        "timestamp_received_min": normalize_json(overview[2]),
        "timestamp_received_max": normalize_json(overview[3]),
        "source_timestamp_min": normalize_json(overview[4]),
        "source_timestamp_max": normalize_json(overview[5]),
        "null_required_fields": {
            "timestamp_received": int(overview[6]),
            "timestamp": int(overview[7]),
            "market": int(overview[8]),
            "asset_id": int(overview[9]),
            "event_type": int(overview[10]),
        },
        "timestamp_received_outside_object_hour": int(overview[11]),
        "source_timestamp_outside_object_hour": int(overview[12]),
        "distinct_markets": int(overview[13]),
        "distinct_assets": int(overview[14]),
        "event_type_counts": event_counts,
        "unknown_event_type_rows": sum(
            count
            for name, count in event_counts.items()
            if name not in {
                "book",
                "price_change",
                "last_trade_price",
                "tick_size_change",
            }
        ),
        "rows_per_timestamp_received": distribution_from_histogram(
            rows_per_timestamp_histogram
        ),
        "rows_per_timestamp_received_histogram": rows_per_timestamp_histogram,
        "availability_group_size_buckets": group_buckets,
        "plateau_diagnostic": (
            "Equal timestamp_received plateaus are diagnostic of coarse/batched "
            "availability but do not establish collector implementation history."
        ),
    }


def _a2(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    totals = connection.execute(
        """
        SELECT COUNT(*)::BIGINT,
               COUNT_IF(l2_rows > 1)::BIGINT,
               COALESCE(SUM(CASE WHEN l2_rows > 1 THEN l2_rows ELSE 0 END), 0)::BIGINT,
               COALESCE(SUM(CASE WHEN l2_rows > 1 THEN group_size ELSE 0 END), 0)::BIGINT
        FROM group_base
        """
    ).fetchone()
    assert totals is not None
    compositions = [
        {
            "composition": row[0],
            "groups": int(row[1]),
            "l2_rows": int(row[2]),
            "all_rows": int(row[3]),
        }
        for row in connection.execute(
            """
            SELECT 'book=' || book_count || ',price_change=' || price_change_count
                   || ',last_trade_price=' || last_trade_count
                   || ',tick_size_change=' || tick_count
                   || ',unknown=' || unknown_count AS composition,
                   COUNT(*)::BIGINT,
                   SUM(l2_rows)::BIGINT,
                   SUM(group_size)::BIGINT
            FROM group_base
            WHERE l2_rows > 1
            GROUP BY book_count, price_change_count, last_trade_count, tick_count,
                     unknown_count
            ORDER BY COUNT(*) DESC, composition
            """
        ).fetchall()
    ]
    return {
        "total_archive_availability_groups": int(totals[0]),
        "groups_with_multiple_l2_rows": int(totals[1]),
        "l2_rows_in_multiple_l2_groups": int(totals[2]),
        "all_rows_in_multiple_l2_groups": int(totals[3]),
        "multiple_l2_group_rate": _ratio(totals[1], totals[0]),
        "composition_distribution": compositions,
    }


def _a3(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    totals = connection.execute(
        """
        SELECT COUNT(*)::BIGINT,
               COUNT_IF(COALESCE(p.repeated_price_keys, 0) = 0)::BIGINT,
               COUNT_IF(
                   COALESCE(p.repeated_price_keys, 0) > 0
                   AND COALESCE(p.conflicting_price_keys, 0) = 0
               )::BIGINT,
               COUNT_IF(COALESCE(p.conflicting_price_keys, 0) > 0)::BIGINT,
               COUNT_IF(g.malformed_price_changes > 0)::BIGINT
        FROM group_base AS g
        LEFT JOIN price_group_stats AS p
          USING (market, asset_id, timestamp_received)
        WHERE g.book_count = 0 AND g.price_change_count > 1
        """
    ).fetchone()
    assert totals is not None
    duplicate = connection.execute(
        """
        WITH duplicate_signatures AS (
            SELECT e.market, e.asset_id, e.timestamp_received, e.timestamp,
                   e.event_type, e.bids, e.asks, e.price, e.size, e.side,
                   e.best_bid, e.best_ask, e.fee_rate_bps, e.transaction_hash,
                   e.old_tick_size, e.new_tick_size, COUNT(*)::BIGINT AS copies
            FROM events AS e
            JOIN group_base AS g
              USING (market, asset_id, timestamp_received)
            WHERE g.book_count = 0
              AND g.price_change_count > 1
              AND e.event_type = 'price_change'
            GROUP BY ALL
        )
        SELECT COALESCE(SUM(copies - 1), 0)::BIGINT,
               COUNT_IF(copies > 1)::BIGINT
        FROM duplicate_signatures
        """
    ).fetchone()
    assert duplicate is not None
    counterexamples = [
        {
            "market": normalize_json(row[0]),
            "asset_id": row[1],
            "timestamp_received": normalize_json(row[2]),
            "side": row[3],
            "price": normalize_json(row[4]),
            "replacement_sizes": normalize_json(row[5]),
            "source_timestamps": normalize_json(row[6]),
            "note": (
                "Source timestamps are retained only as diagnostics and did not resolve "
                "the conflicting replacement order."
            ),
        }
        for row in connection.execute(
            """
            SELECT e.market, e.asset_id, e.timestamp_received, e.side, e.price,
                   LIST(DISTINCT e.size ORDER BY e.size),
                   LIST(DISTINCT e.timestamp ORDER BY e.timestamp)
            FROM events AS e
            JOIN price_key_stats AS p
              USING (market, asset_id, timestamp_received, side, price)
            JOIN group_base AS g
              USING (market, asset_id, timestamp_received)
            WHERE g.book_count = 0
              AND g.price_change_count > 1
              AND p.unique_size_count > 1
            GROUP BY e.market, e.asset_id, e.timestamp_received, e.side, e.price
            ORDER BY e.timestamp_received, e.market, e.asset_id, e.side, e.price
            LIMIT 20
            """
        ).fetchall()
    ]
    pure_groups = int(totals[0] or 0)
    conflicting = int(totals[3] or 0)
    return {
        "pure_price_change_tied_groups": pure_groups,
        "all_distinct_price_level_keys": int(totals[1] or 0),
        "repeated_keys_one_unique_size": int(totals[2] or 0),
        "repeated_keys_multiple_sizes": conflicting,
        "malformed_groups": int(totals[4] or 0),
        "order_ambiguous_rate": _ratio(conflicting, pure_groups),
        "exact_duplicate_rows_beyond_first": int(duplicate[0] or 0),
        "exact_duplicate_signatures": int(duplicate[1] or 0),
        "representative_counterexamples": counterexamples,
    }


def _a4(
    classifications: Sequence[Mapping[str, Any]],
    counterexamples: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    multiple_books = [item for item in classifications if item["book_count"] > 1]
    mixed = [
        item
        for item in classifications
        if item["book_count"] > 0 and item["price_change_count"] > 0
    ]
    return {
        "groups_containing_book": len(classifications),
        "groups_with_multiple_books": len(multiple_books),
        "multiple_book_identical": sum(
            item["l2_status"] == "INVARIANT" for item in multiple_books
        ),
        "multiple_book_differing_or_unresolved": sum(
            item["l2_status"] != "INVARIANT" for item in multiple_books
        ),
        "book_plus_price_change_groups": len(mixed),
        "book_plus_price_change_invariant": sum(
            item["l2_status"] == "INVARIANT" for item in mixed
        ),
        "book_plus_price_change_ambiguous": sum(
            item["l2_status"] == "AMBIGUOUS" for item in mixed
        ),
        "book_plus_price_change_unresolved": sum(
            item["l2_status"] == "UNRESOLVED" for item in mixed
        ),
        "representative_counterexamples": list(counterexamples),
    }


def _a5(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = connection.execute(
        """
        WITH tied_tick_groups AS (
            SELECT e.market, e.asset_id, e.timestamp_received,
                   COUNT(*)::BIGINT AS transitions,
                   COUNT(DISTINCT STRUCT_PACK(
                       old_tick_size := e.old_tick_size,
                       new_tick_size := e.new_tick_size
                   ))::BIGINT AS distinct_transitions,
                   COUNT_IF(e.old_tick_size IS NULL OR e.new_tick_size IS NULL)::BIGINT
                       AS malformed
            FROM events AS e
            JOIN group_base AS g
              USING (market, asset_id, timestamp_received)
            WHERE e.event_type = 'tick_size_change' AND g.group_size > 1
            GROUP BY e.market, e.asset_id, e.timestamp_received
        )
        SELECT COUNT(*)::BIGINT,
               COUNT_IF(transitions = 1 AND malformed = 0)::BIGINT,
               COUNT_IF(
                   transitions > 1 AND distinct_transitions = 1 AND malformed = 0
               )::BIGINT,
               COUNT_IF(distinct_transitions > 1 AND malformed = 0)::BIGINT,
               COUNT_IF(malformed > 0)::BIGINT
        FROM tied_tick_groups
        """
    ).fetchone()
    assert rows is not None
    counterexamples = [
        {
            "market": normalize_json(row[0]),
            "asset_id": row[1],
            "timestamp_received": normalize_json(row[2]),
            "transitions": normalize_json(row[3]),
        }
        for row in connection.execute(
            """
            WITH ambiguous AS (
                SELECT e.market, e.asset_id, e.timestamp_received,
                       LIST(DISTINCT STRUCT_PACK(
                           old_tick_size := e.old_tick_size,
                           new_tick_size := e.new_tick_size
                       ) ORDER BY STRUCT_PACK(
                           old_tick_size := e.old_tick_size,
                           new_tick_size := e.new_tick_size
                       )) AS transitions
                FROM events AS e
                JOIN group_base AS g
                  USING (market, asset_id, timestamp_received)
                WHERE e.event_type = 'tick_size_change' AND g.group_size > 1
                  AND e.old_tick_size IS NOT NULL AND e.new_tick_size IS NOT NULL
                GROUP BY e.market, e.asset_id, e.timestamp_received
                HAVING COUNT(DISTINCT STRUCT_PACK(
                    old_tick_size := e.old_tick_size,
                    new_tick_size := e.new_tick_size
                )) > 1
            )
            SELECT * FROM ambiguous
            ORDER BY timestamp_received, market, asset_id
            LIMIT 20
            """
        ).fetchall()
    ]
    return {
        "tied_groups_containing_tick_transition": int(rows[0] or 0),
        "single_transition_deterministic": int(rows[1] or 0),
        "duplicate_identical_transitions_deterministic": int(rows[2] or 0),
        "distinct_transitions_ambiguous": int(rows[3] or 0),
        "malformed_or_unresolved": int(rows[4] or 0),
        "independent_h2_metadata_blocking_groups": int(rows[3] or 0)
        + int(rows[4] or 0),
        "representative_counterexamples": counterexamples,
        "scope_note": (
            "Tick ambiguity is reported independently and does not invalidate L2 state "
            "in this audit."
        ),
    }


def _a6(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    summary = connection.execute(
        """
        WITH tied AS (
            SELECT e.market, e.asset_id, e.timestamp_received,
                   COUNT(DISTINCT e.timestamp)::BIGINT AS distinct_source_timestamps,
                   COUNT_IF(e.timestamp IS NULL)::BIGINT AS null_source_timestamps,
                   EPOCH_MS(MAX(e.timestamp)) - EPOCH_MS(MIN(e.timestamp)) AS span_ms
            FROM events AS e
            JOIN group_base AS g
              USING (market, asset_id, timestamp_received)
            WHERE g.group_size > 1
            GROUP BY e.market, e.asset_id, e.timestamp_received
        )
        SELECT COUNT(*)::BIGINT,
               COUNT_IF(distinct_source_timestamps = 1 AND null_source_timestamps = 0)::BIGINT,
               COUNT_IF(distinct_source_timestamps > 1)::BIGINT,
               COUNT_IF(null_source_timestamps > 0)::BIGINT,
               MIN(span_ms), MAX(span_ms)
        FROM tied
        """
    ).fetchone()
    assert summary is not None
    distinct_hist = _histogram(
        connection.execute(
            """
            WITH tied AS (
                SELECT e.market, e.asset_id, e.timestamp_received,
                       COUNT(DISTINCT e.timestamp)::BIGINT AS distinct_source_timestamps
                FROM events AS e
                JOIN group_base AS g
                  USING (market, asset_id, timestamp_received)
                WHERE g.group_size > 1
                GROUP BY e.market, e.asset_id, e.timestamp_received
            )
            SELECT distinct_source_timestamps, COUNT(*)::BIGINT
            FROM tied GROUP BY distinct_source_timestamps
            ORDER BY distinct_source_timestamps
            """
        ).fetchall()
    )
    regressions = connection.execute(
        """
        WITH ordered AS (
            SELECT e.market, e.asset_id, e.timestamp_received, e.file_row_number,
                   e.timestamp,
                   LAG(e.timestamp) OVER (
                       PARTITION BY e.market, e.asset_id, e.timestamp_received
                       ORDER BY e.file_row_number
                   ) AS previous_source_timestamp
            FROM events AS e
            JOIN group_base AS g
              USING (market, asset_id, timestamp_received)
            WHERE g.group_size > 1
        ), marked AS (
            SELECT *, timestamp < previous_source_timestamp AS regressed
            FROM ordered
        )
        SELECT COUNT_IF(regressed)::BIGINT,
               COUNT(DISTINCT CASE WHEN regressed THEN STRUCT_PACK(
                   market := market,
                   asset_id := asset_id,
                   timestamp_received := timestamp_received
               ) END)::BIGINT
        FROM marked
        """
    ).fetchone()
    assert regressions is not None
    return {
        "tied_groups": int(summary[0] or 0),
        "all_source_timestamps_equal": int(summary[1] or 0),
        "different_source_timestamps": int(summary[2] or 0),
        "groups_with_null_source_timestamp": int(summary[3] or 0),
        "source_time_span_ms": {
            "min": int(summary[4]) if summary[4] is not None else None,
            "max": int(summary[5]) if summary[5] is not None else None,
        },
        "distinct_source_timestamps_per_group": distribution_from_histogram(
            distinct_hist
        ),
        "distinct_source_timestamps_per_group_histogram": distinct_hist,
        "published_row_order_source_regression_rows": int(regressions[0] or 0),
        "published_row_order_source_regression_groups": int(regressions[1] or 0),
        "diagnostic_only": (
            "Published Parquet row order and source timestamp are reported only as "
            "diagnostics; neither resolves A3, A4, or A5 ambiguity."
        ),
    }


def _insert_book_flags(
    connection: duckdb.DuckDBPyConnection,
    classifications: Sequence[Mapping[str, Any]],
) -> None:
    connection.execute(
        """
        CREATE TABLE book_flags AS
        SELECT market, asset_id, timestamp_received,
               ''::VARCHAR AS l2_status,
               ''::VARCHAR AS l2_reason,
               FALSE::BOOLEAN AS accepted_book
        FROM events
        WHERE FALSE
        """
    )
    if classifications:
        connection.executemany(
            "INSERT INTO book_flags VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    item["_raw_group_key"][0],
                    item["_raw_group_key"][1],
                    item["_raw_group_key"][2],
                    item["l2_status"],
                    item["l2_reason"],
                    item["accepted_book"],
                )
                for item in classifications
            ],
        )


def _create_l2_timeline(
    connection: duckdb.DuckDBPyConnection,
    *,
    start: datetime,
    end: datetime,
) -> None:
    connection.execute(
        """
        CREATE TABLE l2_groups AS
        SELECT g.market, g.asset_id, g.timestamp_received, g.group_size,
               g.book_count, g.price_change_count, g.unknown_count, g.l2_rows,
               (g.l2_rows + g.unknown_count)::BIGINT AS audit_state_rows,
               CASE
                   WHEN g.unknown_count > 0 THEN 'UNRESOLVED'
                   WHEN g.book_count > 0 THEN b.l2_status
                   WHEN g.malformed_price_changes > 0 THEN 'UNRESOLVED'
                   WHEN COALESCE(p.conflicting_price_keys, 0) > 0 THEN 'AMBIGUOUS'
                   ELSE 'INVARIANT'
               END AS l2_status,
               CASE
                   WHEN g.unknown_count > 0 THEN 'unknown_event_type'
                   WHEN g.book_count > 0 THEN b.l2_reason
                   WHEN g.malformed_price_changes > 0 THEN 'price_change_parse_failure'
                   WHEN COALESCE(p.conflicting_price_keys, 0) > 0
                       THEN 'repeated_price_key_multiple_sizes'
                   ELSE 'pure_price_change_order_invariant'
               END AS l2_reason,
               CASE
                   WHEN g.unknown_count = 0 AND g.book_count > 0
                       THEN COALESCE(b.accepted_book, FALSE)
                   ELSE FALSE
               END AS accepted_book
        FROM group_base AS g
        LEFT JOIN price_group_stats AS p
          USING (market, asset_id, timestamp_received)
        LEFT JOIN book_flags AS b
          USING (market, asset_id, timestamp_received)
        WHERE g.l2_rows > 0 OR g.unknown_count > 0
        """
    )
    connection.execute(
        """
        CREATE TABLE asset_groups AS
        SELECT market, asset_id, timestamp_received,
               ROW_NUMBER() OVER (
                   PARTITION BY market, asset_id ORDER BY timestamp_received
               )::BIGINT AS archive_group_sequence
        FROM group_base
        WHERE timestamp_received >= ? AND timestamp_received < ?
        """,
        [start, end],
    )
    connection.execute(
        """
        CREATE TABLE l2_timeline AS
        WITH base AS (
            SELECT l.*, a.archive_group_sequence,
                   EPOCH_MS(l.timestamp_received)::BIGINT AS received_ms,
                   l.l2_status != 'INVARIANT' AS ambiguity_trigger
            FROM l2_groups AS l
            JOIN asset_groups AS a
              USING (market, asset_id, timestamp_received)
        ), markers AS (
            SELECT *,
                   MAX(CASE WHEN accepted_book THEN received_ms END) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS last_book_before,
                   MAX(CASE WHEN ambiguity_trigger THEN received_ms END) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS last_ambiguity_before,
                   MAX(CASE WHEN accepted_book THEN received_ms END) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS last_book_after,
                   MAX(CASE WHEN ambiguity_trigger THEN received_ms END) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS last_ambiguity_after
            FROM base
        )
        SELECT *,
               CASE
                   WHEN last_book_before IS NULL THEN 'UNINITIALIZED'
                   WHEN last_ambiguity_before IS NOT NULL
                        AND last_ambiguity_before > last_book_before THEN 'INVALID'
                   ELSE 'VALID'
               END AS state_before,
               CASE
                   WHEN last_book_after IS NULL THEN 'UNINITIALIZED'
                   WHEN last_ambiguity_after IS NOT NULL
                        AND last_ambiguity_after > last_book_after THEN 'INVALID'
                   ELSE 'VALID'
               END AS state_after
        FROM markers
        """
    )


def _a7_a8(
    connection: duckdb.DuckDBPyConnection,
    *,
    start: datetime,
    end: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    start_ms, end_ms = _epoch_ms(start), _epoch_ms(end)
    connection.execute(
        """
        CREATE TABLE recovery_candidates AS
        WITH future AS (
            SELECT *,
                   MIN(CASE WHEN accepted_book THEN received_ms END) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                       ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                   ) AS next_book_ms,
                   MIN(CASE WHEN accepted_book THEN archive_group_sequence END) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                       ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                   ) AS next_book_group_sequence
            FROM l2_timeline
        )
        SELECT *,
               next_book_ms - received_ms AS recovery_time_ms,
               next_book_group_sequence - archive_group_sequence
                   AS subsequent_archive_groups_to_recovery
        FROM future
        WHERE ambiguity_trigger AND state_before = 'VALID'
        """
    )
    invalidation_count, recovered, censored = connection.execute(
        """
        SELECT COUNT(*)::BIGINT,
               COUNT_IF(next_book_ms IS NOT NULL)::BIGINT,
               COUNT_IF(next_book_ms IS NULL)::BIGINT
        FROM recovery_candidates
        """
    ).fetchone()
    recovery_time_hist = _histogram(
        connection.execute(
            """
            SELECT recovery_time_ms, COUNT(*)::BIGINT
            FROM recovery_candidates
            WHERE recovery_time_ms IS NOT NULL
            GROUP BY recovery_time_ms ORDER BY recovery_time_ms
            """
        ).fetchall()
    )
    recovery_group_hist = _histogram(
        connection.execute(
            """
            SELECT subsequent_archive_groups_to_recovery, COUNT(*)::BIGINT
            FROM recovery_candidates
            WHERE subsequent_archive_groups_to_recovery IS NOT NULL
            GROUP BY subsequent_archive_groups_to_recovery
            ORDER BY subsequent_archive_groups_to_recovery
            """
        ).fetchall()
    )
    connection.execute(
        """
        CREATE TABLE state_segments AS
        WITH assets AS (
            SELECT DISTINCT market, asset_id FROM group_base
            WHERE timestamp_received >= ? AND timestamp_received < ?
        ), transition_segments AS (
            SELECT market, asset_id, received_ms AS segment_start_ms,
                   LEAD(received_ms, 1, ?) OVER (
                       PARTITION BY market, asset_id ORDER BY received_ms
                   ) AS segment_end_ms,
                   state_after AS state
            FROM l2_timeline
        ), initial_segments AS (
            SELECT a.market, a.asset_id, ?::BIGINT AS segment_start_ms,
                   COALESCE(MIN(t.received_ms), ?::BIGINT) AS segment_end_ms,
                   'UNINITIALIZED'::VARCHAR AS state
            FROM assets AS a
            LEFT JOIN l2_timeline AS t USING (market, asset_id)
            GROUP BY a.market, a.asset_id
        )
        SELECT * FROM initial_segments WHERE segment_end_ms > segment_start_ms
        UNION ALL
        SELECT * FROM transition_segments WHERE segment_end_ms > segment_start_ms
        """,
        [start, end, end_ms, start_ms, end_ms],
    )
    per_asset = [
        {
            "market": normalize_json(row[0]),
            "asset_id": row[1],
            "uninitialized_ms": int(row[2]),
            "valid_ms": int(row[3]),
            "invalid_ms": int(row[4]),
            "initialized_valid_share": _ratio(row[3], int(row[3]) + int(row[4])),
        }
        for row in connection.execute(
            """
            SELECT market, asset_id,
                   SUM(CASE WHEN state = 'UNINITIALIZED'
                            THEN segment_end_ms - segment_start_ms ELSE 0 END)::BIGINT,
                   SUM(CASE WHEN state = 'VALID'
                            THEN segment_end_ms - segment_start_ms ELSE 0 END)::BIGINT,
                   SUM(CASE WHEN state = 'INVALID'
                            THEN segment_end_ms - segment_start_ms ELSE 0 END)::BIGINT
            FROM state_segments
            GROUP BY market, asset_id
            ORDER BY market, asset_id
            """
        ).fetchall()
    ]
    aggregate_duration = {
        "uninitialized_ms": sum(item["uninitialized_ms"] for item in per_asset),
        "valid_ms": sum(item["valid_ms"] for item in per_asset),
        "invalid_ms": sum(item["invalid_ms"] for item in per_asset),
    }
    initialized_ms = aggregate_duration["valid_ms"] + aggregate_duration["invalid_ms"]
    aggregate_duration["initialized_valid_share"] = _ratio(
        aggregate_duration["valid_ms"], initialized_ms
    )
    row_coverage = connection.execute(
        """
        SELECT
            COALESCE(SUM(CASE
                WHEN accepted_book THEN audit_state_rows
                WHEN l2_status = 'INVARIANT' AND state_before = 'VALID'
                    THEN audit_state_rows ELSE 0 END), 0)::BIGINT AS processed,
            COALESCE(SUM(CASE
                WHEN NOT accepted_book AND state_before = 'INVALID'
                    THEN audit_state_rows
                WHEN ambiguity_trigger AND state_before = 'VALID'
                    THEN audit_state_rows ELSE 0 END), 0)::BIGINT AS excluded_after_ambiguity,
            COALESCE(SUM(CASE
                WHEN state_before = 'UNINITIALIZED' AND NOT accepted_book
                    THEN audit_state_rows ELSE 0 END), 0)::BIGINT AS uninitialized_excluded,
            COALESCE(SUM(audit_state_rows), 0)::BIGINT AS total
        FROM l2_timeline
        """
    ).fetchone()
    assert row_coverage is not None
    initialized_row_denominator = int(row_coverage[0]) + int(row_coverage[1])
    recovery_events = [
        {
            "market": normalize_json(row[0]),
            "asset_id": row[1],
            "timestamp_received": normalize_json(row[2]),
            "reason": row[3],
            "audit_state_rows": int(row[4]),
            "recovery_time_ms": int(row[5]) if row[5] is not None else None,
            "subsequent_archive_groups_to_recovery": (
                int(row[6]) if row[6] is not None else None
            ),
        }
        for row in connection.execute(
            """
            SELECT market, asset_id, timestamp_received, l2_reason, audit_state_rows,
                   recovery_time_ms, subsequent_archive_groups_to_recovery
            FROM recovery_candidates
            ORDER BY timestamp_received, market, asset_id
            """
        ).fetchall()
    ]

    connection.execute(
        "CREATE TABLE cadence_points(cadence_minutes INTEGER, point_ms BIGINT)"
    )
    cadence_rows = [
        (cadence, point)
        for cadence in (1, 5, 30)
        for point in range(start_ms, end_ms, cadence * 60_000)
    ]
    connection.executemany("INSERT INTO cadence_points VALUES (?, ?)", cadence_rows)
    cadence_results = {
        str(row[0]): {
            "decision_points_after_first_book": int(row[1]),
            "valid_decision_points": int(row[2]),
            "valid_fraction": _ratio(row[2], row[1]),
        }
        for row in connection.execute(
            """
            WITH first_books AS (
                SELECT market, asset_id, MIN(received_ms)::BIGINT AS first_book_ms
                FROM l2_timeline WHERE accepted_book
                GROUP BY market, asset_id
            ), eligible AS (
                SELECT f.market, f.asset_id, c.cadence_minutes, c.point_ms
                FROM first_books AS f
                CROSS JOIN cadence_points AS c
                WHERE c.point_ms >= f.first_book_ms
            )
            SELECT e.cadence_minutes, COUNT(*)::BIGINT,
                   COUNT_IF(s.state = 'VALID')::BIGINT
            FROM eligible AS e
            JOIN state_segments AS s
              ON e.market = s.market AND e.asset_id = s.asset_id
             AND e.point_ms >= s.segment_start_ms
             AND e.point_ms < s.segment_end_ms
            GROUP BY e.cadence_minutes
            ORDER BY e.cadence_minutes
            """
        ).fetchall()
    }
    for cadence in (1, 5, 30):
        cadence_results.setdefault(
            str(cadence),
            {
                "decision_points_after_first_book": 0,
                "valid_decision_points": 0,
                "valid_fraction": None,
            },
        )

    a7 = {
        "ambiguity_triggered_invalidations": int(invalidation_count or 0),
        "recovered_invalidations": int(recovered or 0),
        "right_censored_invalidations": int(censored or 0),
        "recovery_time_ms": distribution_from_histogram(recovery_time_hist),
        "recovery_time_ms_histogram": recovery_time_hist,
        "subsequent_archive_groups_to_recovery": distribution_from_histogram(
            recovery_group_hist
        ),
        "subsequent_archive_groups_to_recovery_histogram": recovery_group_hist,
        "aggregate_asset_time": aggregate_duration,
        "per_asset_wall_clock": per_asset,
        "state_changing_row_coverage": {
            "processed": int(row_coverage[0]),
            "excluded_after_ambiguity": int(row_coverage[1]),
            "excluded_while_uninitialized": int(row_coverage[2]),
            "total_audited_state_rows": int(row_coverage[3]),
            "processed_share_after_initialization": _ratio(
                row_coverage[0], initialized_row_denominator
            ),
            "excluded_after_ambiguity_share": _ratio(
                row_coverage[1], initialized_row_denominator
            ),
        },
        "recovery_events": recovery_events,
        "representative_invalidations": recovery_events[:20],
        "scope_note": (
            "Each file starts UNINITIALIZED. Unknown event groups are unresolved and "
            "fail closed, but tick and last-trade ordering do not invalidate L2."
        ),
    }
    a8 = {
        "utc_aligned_cadences_minutes": cadence_results,
        "scope_note": (
            "Points before an asset's first accepted in-file book are excluded. These "
            "are sensitivity diagnostics, not a strategy or cadence selection."
        ),
    }
    return a7, a8


def analyze_file(
    sample: Mapping[str, Any],
    *,
    working_directory: Path,
) -> dict[str, Any]:
    """Analyze one valid downloaded sample and return deterministic A1-A8 evidence."""

    path = Path(str(sample["local_path"])).resolve()
    start, end = _hour_bounds(str(sample["actual_hour"]))
    file_hash = sha256_file(path)
    if file_hash != sample["sha256"]:
        raise ValueError(f"raw file hash changed before analysis: {path}")
    connection = duckdb.connect()
    try:
        file_meta = connection.execute(
            "SELECT num_rows, num_row_groups FROM parquet_file_metadata(?)", [str(path)]
        ).fetchone()
        if file_meta is None:
            raise ValueError("missing Parquet file metadata")
        schema = _schema(connection, path)
    finally:
        connection.close()
    missing_required = [
        name
        for name in ("market", "asset_id", "timestamp_received", "timestamp", "event_type")
        if name in schema["missing_columns"]
    ]
    if missing_required:
        raise ValueError(f"required columns missing: {', '.join(missing_required)}")

    working_directory.mkdir(parents=True, exist_ok=True)
    temp_directory = working_directory / f"duckdb-temp-{sample['sha256'][:16]}"
    temp_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute("SET preserve_insertion_order = false")
        connection.execute(
            f"SET temp_directory = '{_sql_path(temp_directory)}'"
        )
        _create_analysis_tables(connection, path)
        a1 = _a1(connection, schema=schema, start=start, end=end)
        a2 = _a2(connection)
        a3 = _a3(connection)
        book_classifications, book_counterexamples = _load_book_groups(connection)
        a4 = _a4(book_classifications, book_counterexamples)
        a5 = _a5(connection)
        a6 = _a6(connection)
        _insert_book_flags(connection, book_classifications)
        _create_l2_timeline(connection, start=start, end=end)
        a7, a8 = _a7_a8(connection, start=start, end=end)
        result = {
            "status": "ANALYZED",
            "requested_hour": sample["requested_hour"],
            "actual_hour": sample["actual_hour"],
            "requested_object_key": sample["requested_object_key"],
            "actual_object_key": sample["actual_object_key"],
            "fallback_applied": sample["fallback_applied"],
            "provenance": {
                "download_url": sample["download_url"],
                "local_path": str(path),
                "byte_length": path.stat().st_size,
                "sha256": file_hash,
                "row_count": int(file_meta[0]),
                "row_group_count": int(file_meta[1]),
                "download_attempts": sample["attempts"],
            },
            "a1": a1,
            "a2": a2,
            "a3": a3,
            "a4": a4,
            "a5": a5,
            "a6": a6,
            "a7": a7,
            "a8": a8,
        }
        return normalize_json(result)
    finally:
        connection.close()


def _sum_path(files: Sequence[Mapping[str, Any]], *path: str) -> int:
    total = 0
    for item in files:
        value: Any = item
        for key in path:
            value = value[key]
        total += int(value)
    return total


def pool_results(
    files: Sequence[Mapping[str, Any]],
    *,
    feasibility_label: str,
    feasibility_rationale: str,
) -> dict[str, Any]:
    valid_labels = {
        "PRACTICALLY_LOW_COST",
        "MATERIAL_COVERAGE_COST",
        "SEVERE_COVERAGE_COST",
        "UNRESOLVED",
    }
    if feasibility_label not in valid_labels:
        raise ValueError(f"invalid A9 feasibility label: {feasibility_label}")
    rows_per_timestamp_hist = merge_histograms(
        item["a1"]["rows_per_timestamp_received_histogram"] for item in files
    )
    distinct_source_hist = merge_histograms(
        item["a6"]["distinct_source_timestamps_per_group_histogram"]
        for item in files
    )
    group_buckets: dict[str, dict[str, Any]] = {}
    event_counts: Counter[str] = Counter()
    compositions: dict[str, dict[str, int]] = {}
    for item in files:
        event_counts.update(item["a1"]["event_type_counts"])
        for bucket, values in item["a1"]["availability_group_size_buckets"].items():
            pooled_bucket = group_buckets.setdefault(bucket, {"groups": 0, "rows": 0})
            pooled_bucket["groups"] += int(values["groups"])
            pooled_bucket["rows"] += int(values["rows"])
        for composition in item["a2"]["composition_distribution"]:
            pooled_composition = compositions.setdefault(
                composition["composition"],
                {"groups": 0, "l2_rows": 0, "all_rows": 0},
            )
            for key in ("groups", "l2_rows", "all_rows"):
                pooled_composition[key] += int(composition[key])
    total_bucket_groups = sum(value["groups"] for value in group_buckets.values())
    for values in group_buckets.values():
        values["group_share"] = _ratio(values["groups"], total_bucket_groups)
    recovery_time_hist = merge_histograms(
        item["a7"]["recovery_time_ms_histogram"] for item in files
    )
    recovery_group_hist = merge_histograms(
        item["a7"]["subsequent_archive_groups_to_recovery_histogram"]
        for item in files
    )
    processed = _sum_path(files, "a7", "state_changing_row_coverage", "processed")
    excluded = _sum_path(
        files, "a7", "state_changing_row_coverage", "excluded_after_ambiguity"
    )
    valid_ms = _sum_path(files, "a7", "aggregate_asset_time", "valid_ms")
    invalid_ms = _sum_path(files, "a7", "aggregate_asset_time", "invalid_ms")
    cadence: dict[str, Any] = {}
    for minutes in (1, 5, 30):
        key = str(minutes)
        total = _sum_path(
            files,
            "a8",
            "utc_aligned_cadences_minutes",
            key,
            "decision_points_after_first_book",
        )
        valid = _sum_path(
            files,
            "a8",
            "utc_aligned_cadences_minutes",
            key,
            "valid_decision_points",
        )
        cadence[key] = {
            "decision_points_after_first_book": total,
            "valid_decision_points": valid,
            "valid_fraction": _ratio(valid, total),
        }
    pure_groups = _sum_path(files, "a3", "pure_price_change_tied_groups")
    conflicts = _sum_path(files, "a3", "repeated_keys_multiple_sizes")
    total_groups = _sum_path(files, "a2", "total_archive_availability_groups")
    multi_l2 = _sum_path(files, "a2", "groups_with_multiple_l2_rows")
    return {
        "sample_count": len(files),
        "a1": {
            "row_count": _sum_path(files, "a1", "row_count"),
            "distinct_timestamp_received_sum_across_files": _sum_path(
                files, "a1", "distinct_timestamp_received"
            ),
            "schema_statuses": [item["a1"]["status"] for item in files],
            "event_type_counts": dict(sorted(event_counts.items())),
            "rows_per_timestamp_received": distribution_from_histogram(
                rows_per_timestamp_hist
            ),
            "availability_group_size_buckets": group_buckets,
            "timestamp_received_ranges": [
                {
                    "actual_hour": item["actual_hour"],
                    "min": item["a1"]["timestamp_received_min"],
                    "max": item["a1"]["timestamp_received_max"],
                    "rows_per_timestamp_received": item["a1"][
                        "rows_per_timestamp_received"
                    ],
                }
                for item in files
            ],
        },
        "a2": {
            "total_archive_availability_groups": total_groups,
            "groups_with_multiple_l2_rows": multi_l2,
            "multiple_l2_group_rate": _ratio(multi_l2, total_groups),
            "l2_rows_in_multiple_l2_groups": _sum_path(
                files, "a2", "l2_rows_in_multiple_l2_groups"
            ),
            "composition_distribution": [
                {"composition": key, **value}
                for key, value in sorted(
                    compositions.items(),
                    key=lambda item: (-item[1]["groups"], item[0]),
                )
            ],
        },
        "a3": {
            "pure_price_change_tied_groups": pure_groups,
            "all_distinct_price_level_keys": _sum_path(
                files, "a3", "all_distinct_price_level_keys"
            ),
            "repeated_keys_one_unique_size": _sum_path(
                files, "a3", "repeated_keys_one_unique_size"
            ),
            "repeated_keys_multiple_sizes": conflicts,
            "order_ambiguous_rate": _ratio(conflicts, pure_groups),
            "exact_duplicate_rows_beyond_first": _sum_path(
                files, "a3", "exact_duplicate_rows_beyond_first"
            ),
            "exact_duplicate_signatures": _sum_path(
                files, "a3", "exact_duplicate_signatures"
            ),
            "malformed_groups": _sum_path(files, "a3", "malformed_groups"),
        },
        "a4": {
            key: _sum_path(files, "a4", key)
            for key in (
                "groups_containing_book",
                "groups_with_multiple_books",
                "multiple_book_identical",
                "multiple_book_differing_or_unresolved",
                "book_plus_price_change_groups",
                "book_plus_price_change_invariant",
                "book_plus_price_change_ambiguous",
                "book_plus_price_change_unresolved",
            )
        },
        "a5": {
            key: _sum_path(files, "a5", key)
            for key in (
                "tied_groups_containing_tick_transition",
                "single_transition_deterministic",
                "duplicate_identical_transitions_deterministic",
                "distinct_transitions_ambiguous",
                "malformed_or_unresolved",
                "independent_h2_metadata_blocking_groups",
            )
        },
        "a6": {
            "tied_groups": _sum_path(files, "a6", "tied_groups"),
            "all_source_timestamps_equal": _sum_path(
                files, "a6", "all_source_timestamps_equal"
            ),
            "different_source_timestamps": _sum_path(
                files, "a6", "different_source_timestamps"
            ),
            "published_row_order_source_regression_rows": _sum_path(
                files, "a6", "published_row_order_source_regression_rows"
            ),
            "published_row_order_source_regression_groups": _sum_path(
                files, "a6", "published_row_order_source_regression_groups"
            ),
            "source_time_span_ms": {
                "min": min(
                    (
                        value
                        for value in (
                            item["a6"]["source_time_span_ms"]["min"]
                            for item in files
                        )
                        if value is not None
                    ),
                    default=None,
                ),
                "max": max(
                    (
                        value
                        for value in (
                            item["a6"]["source_time_span_ms"]["max"]
                            for item in files
                        )
                        if value is not None
                    ),
                    default=None,
                ),
            },
            "distinct_source_timestamps_per_group": distribution_from_histogram(
                distinct_source_hist
            ),
            "diagnostic_only": files[0]["a6"]["diagnostic_only"],
        },
        "a7": {
            "ambiguity_triggered_invalidations": _sum_path(
                files, "a7", "ambiguity_triggered_invalidations"
            ),
            "recovered_invalidations": _sum_path(
                files, "a7", "recovered_invalidations"
            ),
            "right_censored_invalidations": _sum_path(
                files, "a7", "right_censored_invalidations"
            ),
            "recovery_time_ms": distribution_from_histogram(recovery_time_hist),
            "subsequent_archive_groups_to_recovery": distribution_from_histogram(
                recovery_group_hist
            ),
            "aggregate_asset_time": {
                "valid_ms": valid_ms,
                "invalid_ms": invalid_ms,
                "uninitialized_ms": _sum_path(
                    files, "a7", "aggregate_asset_time", "uninitialized_ms"
                ),
                "initialized_valid_share": _ratio(valid_ms, valid_ms + invalid_ms),
            },
            "state_changing_row_coverage": {
                "processed": processed,
                "excluded_after_ambiguity": excluded,
                "excluded_while_uninitialized": _sum_path(
                    files,
                    "a7",
                    "state_changing_row_coverage",
                    "excluded_while_uninitialized",
                ),
                "processed_share_after_initialization": _ratio(
                    processed, processed + excluded
                ),
                "excluded_after_ambiguity_share": _ratio(
                    excluded, processed + excluded
                ),
            },
        },
        "a8": {"utc_aligned_cadences_minutes": cadence},
        "a9": {
            "classification": feasibility_label,
            "rationale": feasibility_rationale,
            "interpretation": (
                "Descriptive project interpretation after presentation of raw rates; "
                "not a pre-registered statistical threshold and not an architecture decision."
            ),
        },
    }


def git_provenance(repository: Path) -> dict[str, Any]:
    def run(*arguments: str) -> str:
        return subprocess.check_output(
            ["git", *arguments], cwd=repository, text=True
        ).strip()

    return {
        "commit": run("rev-parse", "HEAD"),
        "branch": run("branch", "--show-current"),
        "dirty": bool(run("status", "--porcelain")),
    }


def runtime_provenance(repository: Path) -> dict[str, Any]:
    return {
        "audit_version": AUDIT_VERSION,
        "git": git_provenance(repository),
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "duckdb": duckdb.__version__,
        "executable": sys.executable,
    }


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".part")
    payload = json.dumps(
        normalize_json(value), indent=2, ensure_ascii=False, sort_keys=True
    ) + "\n"
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
