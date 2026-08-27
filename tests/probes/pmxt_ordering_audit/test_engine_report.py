from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

from tools.probes.pmxt_ordering_audit.core import sha256_file
from tools.probes.pmxt_ordering_audit.engine import analyze_file, pool_results
from tools.probes.pmxt_ordering_audit.report import render_report


MARKET_A = b"0x" + b"a" * 64
MARKET_B = b"0x" + b"b" * 64
MARKET_C = b"0x" + b"c" * 64


def timestamp(minute: int, milliseconds: int = 0) -> datetime:
    return datetime(2026, 5, 1, 12, minute, 0, milliseconds * 1000, tzinfo=timezone.utc)


def archive_row(
    market: bytes,
    asset_id: str,
    received_minute: int,
    event_type: str,
    *,
    source_milliseconds: int = 0,
    bids: str | None = None,
    asks: str | None = None,
    price: str | None = None,
    size: str | None = None,
    side: str | None = None,
    old_tick_size: str | None = None,
    new_tick_size: str | None = None,
) -> tuple:
    return (
        timestamp(received_minute),
        timestamp(received_minute, source_milliseconds),
        market,
        event_type,
        asset_id,
        bids,
        asks,
        Decimal(price) if price is not None else None,
        Decimal(size) if size is not None else None,
        side,
        None,
        None,
        None,
        None,
        Decimal(old_tick_size) if old_tick_size is not None else None,
        Decimal(new_tick_size) if new_tick_size is not None else None,
    )


BOOK = {
    "bids": '[["0.40","10"]]',
    "asks": '[["0.60","12"]]',
}


def write_fixture(path: Path, *, reverse_rows: bool = False) -> None:
    rows = [
        archive_row(MARKET_A, "asset-a", 0, "book", **BOOK),
        archive_row(
            MARKET_A,
            "asset-a",
            1,
            "price_change",
            source_milliseconds=2,
            price="0.40",
            size="10",
            side="BUY",
        ),
        archive_row(
            MARKET_A,
            "asset-a",
            1,
            "price_change",
            source_milliseconds=1,
            price="0.40",
            size="25",
            side="BUY",
        ),
        archive_row(
            MARKET_A,
            "asset-a",
            2,
            "price_change",
            price="0.30",
            size="5",
            side="BUY",
        ),
        archive_row(MARKET_A, "asset-a", 3, "book", **BOOK),
        archive_row(MARKET_B, "asset-b", 0, "book", **BOOK),
        archive_row(
            MARKET_B,
            "asset-b",
            10,
            "price_change",
            price="0.60",
            size="12",
            side="SELL",
        ),
        archive_row(
            MARKET_B,
            "asset-b",
            10,
            "price_change",
            source_milliseconds=1,
            price="0.60",
            size="7",
            side="SELL",
        ),
        archive_row(
            MARKET_B,
            "asset-b",
            10,
            "last_trade_price",
            price="0.50",
            size="2",
            side="BUY",
        ),
        archive_row(
            MARKET_C,
            "asset-c",
            30,
            "price_change",
            price="0.30",
            size="4",
            side="BUY",
        ),
        archive_row(MARKET_C, "asset-c", 40, "book", **BOOK),
    ]
    if reverse_rows:
        rows.reverse()
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE events(
                timestamp_received TIMESTAMPTZ,
                timestamp TIMESTAMPTZ,
                market BLOB,
                event_type VARCHAR,
                asset_id VARCHAR,
                bids VARCHAR,
                asks VARCHAR,
                price DECIMAL(9,4),
                size DECIMAL(18,6),
                side VARCHAR,
                best_bid DECIMAL(9,4),
                best_ask DECIMAL(9,4),
                fee_rate_bps USMALLINT,
                transaction_hash VARCHAR,
                old_tick_size DECIMAL(9,4),
                new_tick_size DECIMAL(9,4)
            )
            """
        )
        connection.executemany(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        escaped = str(path).replace("'", "''")
        connection.execute(f"COPY events TO '{escaped}' (FORMAT PARQUET)")
    finally:
        connection.close()


def sample_for(path: Path) -> dict:
    return {
        "requested_hour": "2026-05-01T12",
        "actual_hour": "2026-05-01T12",
        "requested_object_key": "polymarket_orderbook_2026-05-01T12.parquet",
        "actual_object_key": "polymarket_orderbook_2026-05-01T12.parquet",
        "fallback_applied": False,
        "download_url": (
            "https://r2v2.pmxt.dev/polymarket_orderbook_2026-05-01T12.parquet"
        ),
        "local_path": str(path),
        "byte_length": path.stat().st_size,
        "sha256": sha256_file(path),
        "attempts": [
            {
                "http_status": 200,
                "outcome": "SYNTHETIC_OFFLINE_FIXTURE",
                "sha256": sha256_file(path),
            }
        ],
    }


class EngineAndReportTests(unittest.TestCase):
    def analyze_fixture(self, directory: Path, *, reverse_rows: bool = False) -> dict:
        path = directory / ("reversed.parquet" if reverse_rows else "fixture.parquet")
        write_fixture(path, reverse_rows=reverse_rows)
        return analyze_file(sample_for(path), working_directory=directory / "work")

    def test_state_machine_recovery_and_right_censoring(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.analyze_fixture(Path(temporary))
        a7 = result["a7"]
        self.assertEqual(a7["ambiguity_triggered_invalidations"], 2)
        self.assertEqual(a7["recovered_invalidations"], 1)
        self.assertEqual(a7["right_censored_invalidations"], 1)
        self.assertEqual(a7["recovery_time_ms"]["p50"], 120_000)
        assets = {item["asset_id"]: item for item in a7["per_asset_wall_clock"]}
        self.assertEqual(assets["asset-a"]["valid_ms"], 3_480_000)
        self.assertEqual(assets["asset-a"]["invalid_ms"], 120_000)
        self.assertEqual(assets["asset-b"]["invalid_ms"], 3_000_000)
        self.assertEqual(assets["asset-c"]["uninitialized_ms"], 2_400_000)

    def test_expected_schema_names_and_types_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.analyze_fixture(Path(temporary))
        self.assertEqual(result["a1"]["status"], "EXPECTED_SCHEMA")
        self.assertTrue(result["a1"]["schema"]["column_names_match_expected"])
        self.assertTrue(result["a1"]["schema"]["column_types_match_expected"])
        self.assertEqual(result["a1"]["schema"]["type_mismatches"], [])

    def test_rows_after_ambiguity_are_excluded_and_last_trade_is_auxiliary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.analyze_fixture(Path(temporary))
        coverage = result["a7"]["state_changing_row_coverage"]
        self.assertEqual(coverage["processed"], 4)
        self.assertEqual(coverage["excluded_after_ambiguity"], 5)
        self.assertEqual(coverage["excluded_while_uninitialized"], 1)
        self.assertEqual(result["a1"]["event_type_counts"]["last_trade_price"], 1)

    def test_fixed_cadence_points_start_only_after_first_book(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.analyze_fixture(Path(temporary))
        cadence = result["a8"]["utc_aligned_cadences_minutes"]
        self.assertEqual(cadence["1"]["decision_points_after_first_book"], 140)
        self.assertEqual(cadence["1"]["valid_decision_points"], 88)
        self.assertEqual(cadence["5"]["valid_decision_points"], 18)
        self.assertEqual(cadence["30"]["valid_decision_points"], 3)

    def test_parquet_row_order_does_not_change_a2_to_a5_or_a7_a8(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            forward = self.analyze_fixture(directory)
            reversed_result = self.analyze_fixture(directory, reverse_rows=True)
        for question in ("a2", "a3", "a4", "a5", "a7", "a8"):
            self.assertEqual(forward[question], reversed_result[question])
        self.assertNotEqual(
            forward["a6"]["published_row_order_source_regression_rows"],
            reversed_result["a6"]["published_row_order_source_regression_rows"],
        )

    def test_deterministic_report_contains_a1_through_a9_and_safety_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.analyze_fixture(Path(temporary))
        pooled = pool_results(
            [result],
            feasibility_label="UNRESOLVED",
            feasibility_rationale="Synthetic fixture is not empirical archive evidence.",
        )
        summary = {
            "status": "SYNTHETIC_FIXTURE",
            "run": {
                "started_at": "2026-05-01T00:00:00Z",
                "ended_at": "2026-05-01T00:00:01Z",
                "runtime": {
                    "git": {"commit": "abc", "branch": "fixture", "dirty": False},
                    "python": "3.12.4",
                    "duckdb": "1.4.0",
                },
                "offline_test_command": "offline fixture",
                "offline_test_result": "fixture passed",
                "summary_path": "outputs/fixture/summary.json",
            },
            "samples": [result],
            "pooled": pooled,
            "deviation_from_ip_002": "None",
        }
        first = render_report(summary, summary_sha256="fixture-sha")
        round_trip = json.loads(json.dumps(summary, sort_keys=True))
        second = render_report(round_trip, summary_sha256="fixture-sha")
        self.assertEqual(first, second)
        for index in range(1, 10):
            self.assertIn(f"A{index}", first)
        self.assertIn("never resolve", first)
        self.assertIn("No Polymarket endpoint", first)
        self.assertIn("not an architecture decision", first)


if __name__ == "__main__":
    unittest.main()
