from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.probes.pmxt_ordering_audit import long_run
from tools.probes.pmxt_ordering_audit.recovery import _atomic_create_json
from tools.probes.pmxt_ordering_audit.streaming import (
    REFERENCE_SCAN_MAX_ROWS,
    _enforce_scan_boundary,
)


class FullAugustAuthorizationTests(unittest.TestCase):
    def test_authorization_is_exactly_one_owner_approved_eight_hour_budget(self) -> None:
        self.assertEqual(long_run.AUTHORIZED_WALL_SECONDS, 8 * 60 * 60)
        self.assertEqual(
            long_run.OWNER_APPROVAL_PHRASE,
            "OWNER_APPROVED_IP002S_FULL_AUGUST_8_HOURS",
        )
        with self.assertRaisesRegex(long_run.FullAugustAuthorizationError, "phrase"):
            long_run.launch_full_august(
                expected_git_sha="a" * 40,
                owner_approval="not-approved",
            )

    def test_wrong_preserved_raw_hash_fails_before_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            original = Path(temporary)
            raw = original / "raw" / long_run.EXPECTED_AUGUST_OBJECT
            raw.parent.mkdir(parents=True)
            raw.write_bytes(b"not-the-preserved-august-file")
            provenance = {
                "samples": [
                    {
                        "requested_hour": long_run.EXPECTED_AUGUST_HOUR,
                        "actual_hour": long_run.EXPECTED_AUGUST_HOUR,
                        "requested_object_key": long_run.EXPECTED_AUGUST_OBJECT,
                        "actual_object_key": long_run.EXPECTED_AUGUST_OBJECT,
                        "status": "VALID",
                        "sha256": long_run.EXPECTED_AUGUST_SHA256,
                        "byte_length": raw.stat().st_size,
                        "attempts": [],
                    }
                ]
            }
            (original / "download-provenance.json").write_text(
                json.dumps(provenance), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                long_run.FullAugustAuthorizationError, "raw hash mismatch"
            ):
                long_run._load_preserved_august(original)

    def test_dirty_or_changed_git_provenance_fails_closed(self) -> None:
        repository = Path.cwd()
        with patch.object(
            long_run,
            "git_provenance",
            return_value={"commit": "a" * 40, "branch": "test", "dirty": True},
        ):
            with self.assertRaisesRegex(
                long_run.FullAugustAuthorizationError, "clean committed"
            ):
                long_run._require_clean_committed_git(repository)
        with patch.object(
            long_run,
            "git_provenance",
            return_value={"commit": "b" * 40, "branch": "test", "dirty": False},
        ):
            with self.assertRaisesRegex(
                long_run.FullAugustAuthorizationError, "Git head changed"
            ):
                long_run._require_clean_committed_git(
                    repository, expected_commit="a" * 40
                )

    def test_existing_run_artifact_or_result_blocks_restart_and_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for artifact in (
                "authorization.json",
                "process.json",
                "worker-claim.json",
                "progress.json",
                "full-august.log",
                "full-august-a1-a8.json",
                "failure.json",
            ):
                with self.subTest(artifact=artifact):
                    run_root = root / artifact.replace(".", "-")
                    run_root.mkdir()
                    target = run_root / artifact
                    target.write_text("preserved", encoding="utf-8")
                    with self.assertRaisesRegex(FileExistsError, "one-time"):
                        long_run._ensure_unused_output_root(run_root)
            result = root / "atomic-result.json"
            _atomic_create_json(result, {"status": "COMPLETE"})
            before = result.read_bytes()
            with self.assertRaises(FileExistsError):
                _atomic_create_json(result, {"status": "REPLACED"})
            self.assertEqual(result.read_bytes(), before)

    def test_worker_command_and_result_are_a1_a8_only(self) -> None:
        command = long_run._detached_command(Path("authorization.json"), "run-id")
        self.assertIn("streaming-full-august-worker", command)
        command_text = " ".join(command).lower()
        self.assertNotIn("finalize", command_text)
        self.assertNotIn("a9", command_text)
        analysis = {
            "streaming": {"sort_contract_monotonic": True},
            "scientific": {f"a{index}": {} for index in range(1, 9)},
        }
        result = long_run._final_result(
            authorization={"run_id": "run-id"},
            analysis=analysis,
            started_at="2026-08-28T00:00:00Z",
            ended_at="2026-08-28T01:00:00Z",
            elapsed_seconds=3600.0,
            worker_provenance={"git": {"commit": "a" * 40}},
            input_evidence={"sha256": long_run.EXPECTED_AUGUST_SHA256},
        )
        self.assertEqual(set(result["a1_a8"]), {f"a{index}" for index in range(1, 9)})
        self.assertNotIn("a9", result)
        self.assertFalse(result["scope"]["a9_invoked"])
        self.assertFalse(result["scope"]["report_finalization_invoked"])
        self.assertFalse(result["scope"]["adr_or_production_replay_invoked"])
        for forbidden in ("pool_results", "render_report", "write_report"):
            self.assertNotIn(forbidden, long_run.__dict__)

    def test_ordinary_reference_only_guard_remains_at_ten_million_rows(self) -> None:
        _enforce_scan_boundary(
            mode="all",
            total_rows=REFERENCE_SCAN_MAX_ROWS,
            reference_scan=True,
            capability=None,
        )
        with self.assertRaisesRegex(RuntimeError, "10,000,000-row"):
            _enforce_scan_boundary(
                mode="all",
                total_rows=REFERENCE_SCAN_MAX_ROWS + 1,
                reference_scan=True,
                capability=None,
            )
        with self.assertRaisesRegex(RuntimeError, "reference-only"):
            _enforce_scan_boundary(
                mode="all",
                total_rows=1,
                reference_scan=False,
                capability=None,
            )


if __name__ == "__main__":
    unittest.main()
