from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from tests.probes.pmxt_ordering_audit.test_engine_report import sample_for, write_fixture
from tools.probes.pmxt_ordering_audit.core import sha256_file
from tools.probes.pmxt_ordering_audit.engine import analyze_file
from tools.probes.pmxt_ordering_audit.engine import pool_results
from tools.probes.pmxt_ordering_audit.recovery import (
    SHARD_ALGORITHM,
    SOURCE_ROW_ORDINAL,
    _exact_intermediates,
    partition_sample,
    reduce_checkpoints,
    resource_gate,
    shard_sql,
    stable_shard_id,
    verify_checkpoint,
    verify_partition,
    write_checkpoint,
)
from tools.probes.pmxt_ordering_audit.recovery_run import (
    _validation_fixture,
    _validation_sample,
)
from tools.probes.pmxt_ordering_audit.report import render_report


class RecoveryTests(unittest.TestCase):
    def _partition_fixture(self, root: Path, shard_count: int = 4):
        raw = root / "fixture.parquet"
        write_fixture(raw)
        sample = sample_for(raw)
        connection = duckdb.connect()
        try:
            row_count = connection.execute(
                "SELECT num_rows FROM parquet_file_metadata(?)", [str(raw)]
            ).fetchone()[0]
        finally:
            connection.close()
        sample["attempts"][-1]["row_count"] = int(row_count)
        recovery = root / "recovery"
        manifest = partition_sample(sample, recovery, shard_count)
        return raw, sample, recovery, manifest

    def _analyze_partition(self, root: Path, shard_count: int = 4):
        raw, sample, recovery, manifest = self._partition_fixture(root, shard_count)
        checkpoints = []
        for shard in manifest["shards"]:
            if not shard["files"]:
                continue
            self.assertEqual(len(shard["files"]), 1)
            path = recovery / f"august-{shard_count:02d}-shards" / shard["files"][0]
            shard_sample = {
                **sample,
                "local_path": str(path),
                "sha256": sha256_file(path),
                "attempts": [{"outcome": "SYNTHETIC_SHARD", "row_count": shard["row_count"]}],
            }
            analysis = analyze_file(
                shard_sample,
                working_directory=root / "work" / str(shard["shard_id"]),
                source_schema_path=raw,
                source_row_ordinal_column=SOURCE_ROW_ORDINAL,
                duckdb_memory_limit="8GiB",
                duckdb_threads=2,
            )
            checkpoints.append(
                {
                    "raw_sample_sha256": sample["sha256"],
                    "recovery_code_git_sha": "synthetic",
                    "shard_algorithm": SHARD_ALGORITHM,
                    "shard_count": shard_count,
                    "shard_id": shard["shard_id"],
                    "row_count": shard["row_count"],
                    "partition_file_sha256": shard["file_sha256"],
                    "analysis": analysis,
                    "exact_reduction_intermediates": _exact_intermediates(path),
                }
            )
        reference = analyze_file(sample, working_directory=root / "reference-work")
        reduced = reduce_checkpoints(
            checkpoints,
            expected_raw_sha256=sample["sha256"],
            expected_shard_count=shard_count,
            require_all_shards=False,
        )
        return reference, reduced, checkpoints, manifest, sample, recovery

    def test_python_and_duckdb_stable_shard_mapping_match(self) -> None:
        values = [(b"market-a", "asset-a"), (b"\x00\xff", "café"), (None, None)]
        connection = duckdb.connect()
        try:
            for market, asset in values:
                observed = connection.execute(
                    f"SELECT {shard_sql(32)} FROM (SELECT ?::BLOB AS market, ?::VARCHAR AS asset_id)",
                    [market, asset],
                ).fetchone()[0]
                self.assertEqual(observed, stable_shard_id(market, asset, 32))
                self.assertEqual(observed, stable_shard_id(market, asset, 32))
        finally:
            connection.close()

    def test_no_archive_group_or_asset_trajectory_is_split(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, recovery, manifest = self._partition_fixture(Path(temporary))
            pattern = recovery / "august-04-shards" / "data" / "shard_id=*" / "*.parquet"
            connection = duckdb.connect()
            try:
                split_groups, split_assets = connection.execute(
                    """
                    WITH source AS (
                        SELECT *, regexp_extract(filename, 'shard_id=([0-9]+)', 1) AS shard
                        FROM read_parquet(?, filename=true, hive_partitioning=false)
                    )
                    SELECT
                      (SELECT COUNT(*) FROM (
                         SELECT market, asset_id, timestamp_received
                         FROM source GROUP BY ALL HAVING COUNT(DISTINCT shard) > 1
                      )),
                      (SELECT COUNT(*) FROM (
                         SELECT market, asset_id FROM source
                         GROUP BY ALL HAVING COUNT(DISTINCT shard) > 1
                      ))
                    """,
                    [str(pattern)],
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual(split_groups, 0)
            self.assertEqual(split_assets, 0)
            self.assertEqual(sum(item["row_count"] for item in manifest["shards"]), 11)

    def test_exact_reduction_matches_unsharded_a1_through_a8_in_any_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, reduced, checkpoints, _, sample, _ = self._analyze_partition(Path(temporary))
            reversed_reduction = reduce_checkpoints(
                list(reversed(checkpoints)),
                expected_raw_sha256=sample["sha256"],
                expected_shard_count=4,
                require_all_shards=False,
            )
        for question in ("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"):
            self.assertEqual(reference[question], reduced[question], question)
            self.assertEqual(reduced[question], reversed_reduction[question], question)

    def test_global_timestamp_distribution_is_merged_by_exact_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, reduced, _, _, _, _ = self._analyze_partition(Path(temporary))
        self.assertEqual(
            reduced["a1"]["rows_per_timestamp_received_histogram"],
            reference["a1"]["rows_per_timestamp_received_histogram"],
        )
        self.assertEqual(reduced["a1"]["rows_per_timestamp_received"]["max"], 3)
        self.assertIn("2", reduced["a1"]["rows_per_timestamp_received_histogram"])

    def test_a7_exact_percentiles_and_a8_cadence_counts_match(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, reduced, _, _, _, _ = self._analyze_partition(Path(temporary))
        self.assertEqual(reduced["a7"], reference["a7"])
        self.assertEqual(reduced["a8"], reference["a8"])

    def test_a6_published_source_order_diagnostic_uses_preserved_ordinal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            reference, reduced, _, _, _, _ = self._analyze_partition(Path(temporary))
        self.assertEqual(
            reduced["a6"]["published_row_order_source_regression_rows"],
            reference["a6"]["published_row_order_source_regression_rows"],
        )
        self.assertEqual(
            reduced["a6"]["published_row_order_source_regression_groups"],
            reference["a6"]["published_row_order_source_regression_groups"],
        )

    def test_checkpoint_is_atomic_hash_verified_and_reused_immutably(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "checkpoint.json"
            payload = {"shard_id": 1, "value": [1, 2, 3]}
            first = write_checkpoint(path, payload)
            second = write_checkpoint(path, payload)
            self.assertEqual(first, second)
            self.assertFalse(any(root.glob("*.part-*")))
            with self.assertRaises(FileExistsError):
                write_checkpoint(path, {"shard_id": 1, "value": [9]})
            tampered = json.loads(path.read_text(encoding="utf-8"))
            tampered["payload"]["value"] = [7]
            path.write_text(json.dumps(tampered), encoding="utf-8")
            with self.assertRaises(ValueError):
                verify_checkpoint(path)

    def test_partition_is_hash_verified_and_reused_without_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, sample, recovery, manifest = self._partition_fixture(root)
            path = recovery / "august-04-shards" / "partition-manifest.json"
            before_hash = sha256_file(path)
            before_mtime = path.stat().st_mtime_ns
            reused = partition_sample(sample, recovery, 4)
            self.assertEqual(manifest, reused)
            self.assertEqual(before_hash, sha256_file(path))
            self.assertEqual(before_mtime, path.stat().st_mtime_ns)
            verify_partition(path, sample["sha256"])

    def test_reducer_rejects_missing_duplicate_and_wrong_sample_checkpoints(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, checkpoints, _, sample, _ = self._analyze_partition(Path(temporary))
        with self.assertRaisesRegex(ValueError, "missing"):
            reduce_checkpoints(
                checkpoints,
                expected_raw_sha256=sample["sha256"],
                expected_shard_count=4,
            )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            reduce_checkpoints(
                [checkpoints[0], checkpoints[0]],
                expected_raw_sha256=sample["sha256"],
                expected_shard_count=4,
                require_all_shards=False,
            )
        wrong = json.loads(json.dumps(checkpoints[0]))
        wrong["raw_sample_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "wrong-sample"):
            reduce_checkpoints(
                [wrong],
                expected_raw_sha256=sample["sha256"],
                expected_shard_count=4,
                require_all_shards=False,
            )

    def test_resource_gate_blocks_each_declared_budget_failure(self) -> None:
        passing = dict(
            peak_rss_bytes=1024,
            failed=False,
            combined_elapsed_seconds=10,
            projected_remaining_seconds=10,
            projected_temp_bytes=1024,
            semantic_equivalence=True,
        )
        self.assertEqual(resource_gate(**passing)["status"], "PASS")
        for key, bad in (
            ("peak_rss_bytes", 13 * 1024**3),
            ("failed", True),
            ("combined_elapsed_seconds", 1801),
            ("projected_remaining_seconds", 5401),
            ("projected_temp_bytes", 51 * 1024**3),
            ("semantic_equivalence", False),
        ):
            arguments = {**passing, key: bad}
            self.assertEqual(resource_gate(**arguments)["status"], "FAIL", key)

    def test_june_validation_unit_matches_reference_and_recovery_engines(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "june.parquet"
            write_fixture(raw)
            sample = sample_for(raw)
            recovery_path, reference_path, manifest = _validation_fixture(
                sample, root / "recovery"
            )
            reference = analyze_file(
                _validation_sample(sample, reference_path, "REFERENCE"),
                working_directory=root / "reference-work",
                source_schema_path=raw,
            )
            recovered = analyze_file(
                _validation_sample(sample, recovery_path, "RECOVERY"),
                working_directory=root / "recovery-work",
                source_schema_path=raw,
                source_row_ordinal_column=SOURCE_ROW_ORDINAL,
                duckdb_memory_limit="8GiB",
                duckdb_threads=2,
            )
        self.assertGreater(manifest["row_count"], 0)
        for question in [f"a{index}" for index in range(1, 9)]:
            self.assertEqual(reference[question], recovered[question], question)

    def test_recovery_report_preserves_execution_stages_and_puts_a9_last(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sample = self._analyze_partition(Path(temporary))[0]
        pooled = pool_results(
            [sample],
            feasibility_label="UNRESOLVED",
            feasibility_rationale="Synthetic data cannot resolve A9.",
        )
        summary = {
            "status": "COMPLETE",
            "run": {
                "started_at": "2026-08-01T00:00:00Z",
                "ended_at": "2026-08-01T00:01:00Z",
                "runtime": {
                    "git": {"commit": "final", "branch": "test", "dirty": False},
                    "python": "3.12.4",
                    "duckdb": "1.4.0",
                },
                "offline_test_command": "offline",
                "offline_test_result": "passed",
                "summary_path": "ignored.json",
            },
            "download": {"samples": []},
            "samples": [sample],
            "failures": [],
            "pooled": pooled,
            "deviation_from_ip_002": "None",
            "deviation_from_ip_002r": "None",
            "recovery": {
                "original_ip_002": {
                    "implementation_commit": "original",
                    "status": "UNRESOLVED",
                    "result_sha256": "original-result",
                },
                "ip_002r_pilot": {
                    "proceed_gate": {"status": "PASS"},
                    "largest_shard_id": 3,
                    "largest_shard_row_count": 10,
                    "checkpoint_file_sha256": "pilot-checkpoint",
                },
                "august_recovery": {
                    "status": "COMPLETE",
                    "checkpoint_count": 32,
                    "checkpoint_integrity": "PASS",
                    "reducer_order_independence": "PASS",
                    "active_total_august_seconds": 1.0,
                    "peak_rss_bytes": 1,
                    "final_recovery_storage_bytes": 1,
                    "peak_duckdb_temp_growth_bytes": 0,
                    "recovery_code_git_shas": ["recovery"],
                    "recovery_module_blob_shas": {"recovery": "blob"},
                },
                "june_equivalence": {
                    "status": "PASS",
                    "validation_manifest": {
                        "selected_shard_id": 0,
                        "shard_count": 1024,
                        "row_count": 1,
                    },
                    "reference_a1_a8_sha256": "equal",
                },
            },
        }
        report = render_report(summary, summary_sha256="summary")
        self.assertIn("later recovery does not rewrite the failed first attempt", report)
        self.assertIn("May and August", report)
        self.assertLess(report.index("## Pooled metrics"), report.index("## A9"))
        self.assertIn("Deviation from IP-002R: **None**", report)


if __name__ == "__main__":
    unittest.main()
