from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from tests.probes.pmxt_ordering_audit.test_engine_report import sample_for, write_fixture
from tools.probes.pmxt_ordering_audit.engine import AuditPreflightError, analyze_file
from tools.probes.pmxt_ordering_audit.streaming import analyze_stream


class FailClosedPreflightTests(unittest.TestCase):
    def _copy_query(self, source: Path, destination: Path, query: str) -> None:
        escaped_source = str(source).replace("'", "''")
        escaped_destination = str(destination).replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                f"COPY ({query.format(source=escaped_source)}) "
                f"TO '{escaped_destination}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()

    def _assert_both_engines_reject(self, path: Path, pattern: str) -> None:
        sample = sample_for(path)
        with self.assertRaisesRegex(AuditPreflightError, pattern):
            analyze_file(sample, working_directory=path.parent / f"work-{path.stem}")
        with self.assertRaisesRegex(AuditPreflightError, pattern):
            analyze_stream(
                path,
                actual_hour="2026-05-01T12",
                reference_scan=True,
            )

    def test_null_grouping_fields_fail_the_entire_file_in_both_engines(self) -> None:
        replacements = {
            "market": "NULL::BLOB AS market",
            "asset_id": "NULL::VARCHAR AS asset_id",
            "timestamp_received": (
                "NULL::TIMESTAMP WITH TIME ZONE AS timestamp_received"
            ),
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.parquet"
            write_fixture(source)
            for field, replacement in replacements.items():
                with self.subTest(field=field):
                    path = root / f"null-{field}.parquet"
                    self._copy_query(
                        source,
                        path,
                        "SELECT * REPLACE (" + replacement + ") "
                        "FROM read_parquet('{source}')",
                    )
                    self._assert_both_engines_reject(path, field)

    def test_incompatible_a1_a8_types_fail_before_classification(self) -> None:
        replacements = {
            "price": "CAST(price AS DOUBLE) AS price",
            "size": "CAST(size AS DOUBLE) AS size",
            "old_tick_size": "CAST(old_tick_size AS DOUBLE) AS old_tick_size",
            "new_tick_size": "CAST(new_tick_size AS DOUBLE) AS new_tick_size",
            "timestamp": "CAST(timestamp AS TIMESTAMP) AS timestamp",
            "timestamp_received": (
                "CAST(timestamp_received AS TIMESTAMP) AS timestamp_received"
            ),
            "market": "CAST(market AS VARCHAR) AS market",
            "asset_id": "CAST(asset_id AS BLOB) AS asset_id",
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.parquet"
            write_fixture(source)
            for field, replacement in replacements.items():
                with self.subTest(field=field):
                    path = root / f"wrong-type-{field}.parquet"
                    self._copy_query(
                        source,
                        path,
                        "SELECT * REPLACE (" + replacement + ") "
                        "FROM read_parquet('{source}')",
                    )
                    self._assert_both_engines_reject(path, field)

    def _analyze_malformed_group(self, root: Path, replacement: str) -> tuple[dict, dict]:
        source = root / "source.parquet"
        if not source.exists():
            write_fixture(source)
        path = root / f"malformed-{len(list(root.glob('malformed-*.parquet')))}.parquet"
        self._copy_query(
            source,
            path,
            "SELECT * REPLACE (" + replacement + ") "
            "FROM read_parquet('{source}') "
            "WHERE asset_id = 'asset-a' AND event_type = 'price_change' "
            "QUALIFY COUNT(*) OVER (PARTITION BY timestamp_received) > 1",
        )
        reference = analyze_file(
            sample_for(path),
            working_directory=root / f"work-{path.stem}",
        )
        streamed = analyze_stream(
            path,
            actual_hour="2026-05-01T12",
            reference_scan=True,
            batch_size=1,
        )["scientific"]
        return reference, streamed

    def _assert_malformed_only(self, result: dict) -> None:
        a3 = result["a3"]
        self.assertEqual(a3["pure_price_change_tied_groups"], 1)
        self.assertEqual(a3["malformed_groups"], 1)
        self.assertEqual(a3["all_distinct_price_level_keys"], 0)
        self.assertEqual(a3["repeated_keys_one_unique_size"], 0)
        self.assertEqual(a3["repeated_keys_multiple_sizes"], 0)
        self.assertEqual(a3["representative_counterexamples"], [])

    def test_malformed_pure_price_group_has_no_other_a3_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, streamed = self._analyze_malformed_group(
                Path(temporary), "NULL::VARCHAR AS side"
            )
        self._assert_malformed_only(reference)
        self._assert_malformed_only(streamed)
        self.assertEqual(reference["a3"], streamed["a3"])

    def test_negative_replacement_size_is_malformed_in_both_engines(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, streamed = self._analyze_malformed_group(
                Path(temporary), "-1::DECIMAL(18,6) AS size"
            )
        self._assert_malformed_only(reference)
        self._assert_malformed_only(streamed)
        self.assertEqual(reference["a3"], streamed["a3"])
        self.assertEqual(
            reference["a7"]["state_changing_row_coverage"][
                "excluded_while_uninitialized"
            ],
            2,
        )


if __name__ == "__main__":
    unittest.main()
