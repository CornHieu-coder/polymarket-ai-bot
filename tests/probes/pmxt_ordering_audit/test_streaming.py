from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from tests.probes.pmxt_ordering_audit.test_engine_report import (
    sample_for,
    write_fixture,
)
from tools.probes.pmxt_ordering_audit.engine import analyze_file
from tools.probes.pmxt_ordering_audit.recovery import SOURCE_ROW_ORDINAL
from tools.probes.pmxt_ordering_audit.streaming import (
    AUGUST_ROWS,
    SAFETY_FACTOR,
    SortContractError,
    analyze_stream,
    scientific_a1_a8,
    streaming_gate,
)


QUESTIONS = tuple(f"a{index}" for index in range(1, 9))


class StreamingAnalyzerTests(unittest.TestCase):
    def _fixture_results(
        self, root: Path, *, batch_size: int = 2
    ) -> tuple[dict, dict]:
        path = root / "fixture.parquet"
        write_fixture(path)
        reference = analyze_file(
            sample_for(path),
            working_directory=root / "reference-work",
        )
        streamed = analyze_stream(
            path,
            actual_hour="2026-05-01T12",
            batch_size=batch_size,
            reference_scan=True,
        )
        return reference, streamed

    def test_monotonic_composite_sort_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, streamed = self._fixture_results(Path(temporary))
        self.assertTrue(streamed["streaming"]["sort_contract_monotonic"])
        self.assertEqual(streamed["streaming"]["physical_rows_tested_range"], [0, 11])

    def test_composite_sort_regression_fails_without_auto_sorting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "regressed.parquet"
            write_fixture(path, reverse_rows=True)
            with self.assertRaisesRegex(SortContractError, "composite sort regression"):
                analyze_stream(
                    path,
                    actual_hour="2026-05-01T12",
                    batch_size=2,
                    reference_scan=True,
                )

    def test_group_and_asset_state_cross_batch_boundaries_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, streamed = self._fixture_results(Path(temporary), batch_size=2)
        self.assertEqual(streamed["streaming"]["batches_read"], 6)
        self.assertEqual(streamed["streaming"]["maximum_buffered_logical_group_size"], 3)
        self.assertEqual(streamed["streaming"]["maximum_asset_rows_observed"], 5)
        self.assertEqual(streamed["scientific"]["a2"]["total_archive_availability_groups"], 8)
        self.assertEqual(streamed["scientific"]["a7"], reference["a7"])
        self.assertEqual(streamed["scientific"]["a8"], reference["a8"])

    def test_batch_size_invariance_and_whole_a1_a8_reference_equivalence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference, small = self._fixture_results(root, batch_size=1)
            path = root / "fixture.parquet"
            medium = analyze_stream(
                path,
                actual_hour="2026-05-01T12",
                batch_size=3,
                reference_scan=True,
            )
            large = analyze_stream(
                path,
                actual_hour="2026-05-01T12",
                batch_size=65_536,
                reference_scan=True,
            )
        expected = {key: reference[key] for key in QUESTIONS}
        self.assertEqual(scientific_a1_a8(small), expected)
        self.assertEqual(scientific_a1_a8(medium), expected)
        self.assertEqual(scientific_a1_a8(large), expected)

    def test_exact_a1_a6_a7_a8_sufficient_statistics_match_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, streamed = self._fixture_results(Path(temporary), batch_size=2)
        observed = streamed["scientific"]
        self.assertEqual(
            observed["a1"]["rows_per_timestamp_received_histogram"],
            reference["a1"]["rows_per_timestamp_received_histogram"],
        )
        self.assertEqual(observed["a1"]["rows_per_timestamp_received"], reference["a1"]["rows_per_timestamp_received"])
        self.assertEqual(observed["a6"], reference["a6"])
        self.assertEqual(observed["a7"]["recovery_time_ms_histogram"], reference["a7"]["recovery_time_ms_histogram"])
        self.assertEqual(observed["a7"]["recovery_time_ms"], reference["a7"]["recovery_time_ms"])
        self.assertEqual(observed["a8"], reference["a8"])

    def test_published_order_diagnostic_does_not_change_frozen_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference, streamed = self._fixture_results(root, batch_size=2)
        self.assertGreater(
            streamed["scientific"]["a6"]["published_row_order_source_regression_rows"],
            0,
        )
        self.assertEqual(streamed["scientific"]["a3"], reference["a3"])
        self.assertEqual(streamed["scientific"]["a4"], reference["a4"])
        self.assertEqual(streamed["scientific"]["a5"], reference["a5"])

    def test_recovery_ordinal_a6_diagnostic_matches_reference_engine(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.parquet"
            shard = root / "sorted-shard.parquet"
            write_fixture(source)
            connection = duckdb.connect()
            try:
                escaped_source = str(source).replace("'", "''")
                connection.execute(
                    f"CREATE VIEW source_rows AS SELECT * FROM "
                    f"read_parquet('{escaped_source}')"
                )
                escaped_shard = str(shard).replace("'", "''")
                connection.execute(
                    f"""
                    COPY (
                        SELECT *, ROW_NUMBER() OVER () - 1 AS {SOURCE_ROW_ORDINAL}
                        FROM source_rows
                        ORDER BY market, asset_id, timestamp_received
                    ) TO '{escaped_shard}' (FORMAT PARQUET)
                    """
                )
            finally:
                connection.close()
            reference = analyze_file(
                sample_for(shard),
                working_directory=root / "ordinal-reference-work",
                source_schema_path=source,
                source_row_ordinal_column=SOURCE_ROW_ORDINAL,
            )
            streamed = analyze_stream(
                shard,
                actual_hour="2026-05-01T12",
                batch_size=2,
                source_schema_path=source,
                reference_scan=True,
            )
        self.assertEqual(
            scientific_a1_a8(streamed),
            {key: reference[key] for key in QUESTIONS},
        )

    def test_gate_uses_exact_conservative_projection_and_stops_failures(self) -> None:
        passing = dict(
            exact_equivalence=True,
            monotonic=True,
            w1_seconds=10.0,
            w2_seconds=12.0,
            w1_rows=2_000_000,
            w2_rows=2_400_000,
            peak_rss_bytes=1024,
            temp_growth_bytes=0,
            failed=False,
        )
        result = streaming_gate(**passing)
        expected_r1 = 10.0 / 2_000_000
        expected_r2 = 12.0 / 2_400_000
        expected_projection = max(expected_r1, expected_r2) * AUGUST_ROWS * SAFETY_FACTOR
        self.assertEqual(result["status"], "STREAMING_PILOT_PASS")
        self.assertEqual(result["r1_seconds_per_row"], expected_r1)
        self.assertEqual(result["r2_seconds_per_row"], expected_r2)
        self.assertEqual(result["conservative_projected_full_august_seconds"], expected_projection)
        for key, bad_value in (
            ("exact_equivalence", False),
            ("monotonic", False),
            ("w1_seconds", 601.0),
            ("peak_rss_bytes", 4 * 1024**3 + 1),
            ("temp_growth_bytes", 2 * 1024**3 + 1),
            ("failed", True),
        ):
            arguments = {**passing, key: bad_value}
            self.assertEqual(
                streaming_gate(**arguments)["status"],
                "STREAMING_NOT_FEASIBLE",
                key,
            )

    def test_whole_file_path_is_blocked_without_reference_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fixture.parquet"
            write_fixture(path)
            with self.assertRaisesRegex(RuntimeError, "reference-only"):
                analyze_stream(path, actual_hour="2026-05-01T12")


if __name__ == "__main__":
    unittest.main()
