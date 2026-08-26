from __future__ import annotations

import json
import unittest

from tools.probes.market_feed_contract.report import render_report


def minimal_summary() -> dict:
    return {
        "run": {
            "run_id": "fixture-run",
            "started_at": "2026-08-26T00:00:00Z",
            "ended_at": "2026-08-26T00:03:00Z",
            "requested_duration_seconds": 180,
            "actual_duration_seconds": "180.0",
            "rest_interval_seconds": 15,
            "controlled_reconnects": 1,
            "initial_dump": True,
            "websocket_level": 2,
            "raw_boundary": "fixture application boundary",
            "raw_evidence_path": "outputs/market-feed-contract/fixture-run",
            "token_ids": ["123"],
            "sample_note": "Synthetic report fixture; not empirical evidence.",
            "offline_test_command": "offline fixture command",
            "offline_test_result": "fixture pass",
            "software": {
                "git_commit": "abc",
                "git_dirty": False,
                "python": "3.12.4",
                "httpx": "0.28.1",
                "websockets": "16.0",
            },
        },
        "evidence": {
            "records": {"websocket_frames": 1},
            "duplicate_raw_frames": 0,
            "manifest": {"files": {"raw.jsonl": {"bytes": 2, "sha256": "00"}}},
        },
        "q1": {"status": "UNRESOLVED", "event_schema": {}, "candidate_ordering_fields": {}, "scope_note": "sample only", "counterexamples": []},
        "q2": {"status": "UNRESOLVED", "sessions": {}, "counterexamples": []},
        "q3": {"status": "UNRESOLVED", "observed_change_entries": 0, "applied_change_entries": 0, "excluded_change_entries": 0, "comparison_entries": 0, "exact_matches": 0, "mismatches": 0, "direct_single-entry_mismatches": 0, "components": {}, "zero_size_updates": 0, "nonzero_replacements": 0, "side_updates": {}, "discriminating_nonzero_updates": 0, "discriminating_zero_deletes": 0, "independently_validated_nonzero_replacements": 0, "independently_validated_zero_deletes": 0, "idempotent_replacements": 0, "validation_note": "fixture", "counterexamples": []},
        "q4": {"status": "UNRESOLVED", "price_change_frames": 0, "entries_per_frame": {}, "multi_entry_frames": 0, "multi_same_asset_frames": 0, "duplicate_asset_side_price_frames": 0, "order_sensitive_frames": 0, "counterexamples": []},
        "q5": {"status": "UNRESOLVED", "algorithm": "fixture", "websocket_book": {"attempts": 0, "matches": 0, "mismatches": 0, "skipped_missing_fields": 1}, "price_change_post_update": {"attempts": 0, "matches": 0, "mismatches": 0, "skipped_missing_fields": 0}, "rest_book_diagnostic": {"attempts": 0, "matches": 0, "mismatches": 0}, "counterexamples": []},
        "q6": {"status": "UNRESOLVED", "requests": 0, "successful_responses": 0, "status_codes": {}, "expected_field_presence": {"hash": 0}, "expected_field_missing": {"hash": 0}, "alignment_rule": "fixture", "aligned_comparisons": 0, "aligned_matches": 0, "aligned_mismatches": 0, "unaligned_diagnostics": 0, "counterexamples": []},
        "q7": {"status": "UNRESOLVED", "websocket_book_events": 0, "optional_field_presence": {"tick_size": 0}, "scope_note": "sample only", "counterexamples": []},
        "q8": {"status": "UNRESOLVED", "timestamped_events": 0, "timestamp_events_by_token": {"123": 0}, "source_timestamp_regressions": 0, "adjacent_same_timestamp_events": 0, "same_timestamp_groups": [], "source_after_local_receive": 0, "delay_ms_local_clock_dependent": {"quantile_method": "nearest-rank without interpolation", "count": 0, "min": None, "p50": None, "p95": None, "max": None}, "counterexamples": []},
        "errors": {"counts": {}, "examples": []},
    }


class ReportTests(unittest.TestCase):
    def test_report_rendering_is_deterministic(self) -> None:
        summary = minimal_summary()
        json_round_trip = json.loads(json.dumps(summary, sort_keys=True))
        self.assertEqual(render_report(summary), render_report(json_round_trip))

    def test_report_contains_every_question_status_and_safety_caveats(self) -> None:
        report = render_report(minimal_summary())
        for index in range(1, 9):
            self.assertIn(f"Q{index}", report)
        self.assertGreaterEqual(report.count("**UNRESOLVED**"), 8)
        self.assertIn("No authenticated endpoint", report)
        self.assertIn("cannot establish gap-free", report)
        self.assertIn("not a trading-performance result", report)


if __name__ == "__main__":
    unittest.main()
