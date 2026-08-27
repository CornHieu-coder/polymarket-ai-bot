from __future__ import annotations

from copy import deepcopy
import json
import unittest

from tools.probes.market_feed_contract.report import (
    Q8_DELAY_LIMITATION,
    render_comparative_report,
    render_report,
)


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


def comparative_summaries() -> tuple[dict, dict]:
    original = minimal_summary()
    original["run"]["run_id"] = "historical-run-id"
    original["run"]["raw_evidence_path"] = "outputs/original"
    original["evidence"]["run_directory"] = "outputs/original"
    original["evidence"]["manifest"] = {
        "algorithm": "sha256",
        "files": {
            "historical-raw.jsonl": {
                "bytes": 11,
                "sha256": "historical-sha256",
            }
        },
    }
    original["q2"]["status"] = "CONFIRMED"
    original["q3"].update(
        {
            "observed_change_entries": 12,
            "applied_change_entries": 5,
            "excluded_change_entries": 7,
            "counterexamples": [
                {
                    "ingest_sequence": 41,
                    "reason": "historical-empty-side-boundary",
                }
            ],
        }
    )
    original["errors"] = {
        "counts": {"price_change_without_valid_book": 7},
        "examples": [
            {
                "code": "price_change_without_valid_book",
                "ingest_sequence": 42,
            }
        ],
    }

    corrective = deepcopy(original)
    corrective["probe_version"] = "0.2.0"
    corrective["run"].update(
        {
            "run_id": "corrective-run-id",
            "raw_evidence_path": "outputs/corrective",
            "market_subscription_payload": {
                "assets_ids": ["123"],
                "type": "market",
            },
            "market_subscription_fields": ["assets_ids", "type"],
            "initial_dump_field_sent": False,
            "level_field_sent": False,
            "subscription_control": (
                "minimal documented payload; documented defaults relied upon"
            ),
            "baseline_run_id": "historical-run-id",
            "baseline_summary_path": "outputs/original/summary.json",
            "baseline_summary_sha256": "baseline-summary-sha256",
        }
    )
    del corrective["run"]["initial_dump"]
    del corrective["run"]["websocket_level"]
    corrective["evidence"]["run_directory"] = "outputs/corrective"
    corrective["evidence"]["manifest"] = {
        "algorithm": "sha256",
        "files": {
            "corrective-raw.jsonl": {
                "bytes": 13,
                "sha256": "corrective-sha256",
            }
        },
    }
    corrective["q2"]["status"] = "UNRESOLVED"
    corrective["q3"].update(
        {
            "observed_change_entries": 18,
            "applied_change_entries": 16,
            "excluded_change_entries": 2,
            "genuine_mismatches": 0,
            "superseded_before_validation": 3,
            "empty_side_boundary_interpretation": {
                "status": "UNRESOLVED",
                "kind_level_status": "CONFIRMED",
                "all_observed_candidates_confirmed": False,
                "scope": "Confirmed only for aligned candidates in this run.",
                "observed_candidates": 2,
                "observed_kinds": {
                    "ask_one_when_empty": 1,
                    "bid_zero_when_empty": 1,
                },
                "confirmed_candidates": 1,
                "confirmed_kinds": {"ask_one_when_empty": 1},
                "unresolved_candidates": 1,
                "targeted_rest_unaligned": 1,
                "targeted_rest_diagnostics": 2,
                "candidates": [
                    {
                        "candidate_id": "17:0:best_ask",
                        "rest_validation": "CONFIRMED",
                    }
                ],
            },
            "counterexamples": [
                {
                    "ingest_sequence": 99,
                    "reason": "corrective-diagnostic",
                }
            ],
        }
    )
    old_delays = corrective["q8"].pop("delay_ms_local_clock_dependent")
    corrective["q8"]["probe_observed_source_to_processing_delay_ms"] = old_delays
    corrective["q8"]["processing_order"] = {
        "raw_flush_before_received_at": True,
        "synchronous_fsync": True,
    }
    corrective["errors"] = {"counts": {}, "examples": []}
    return original, corrective


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

    def test_render_report_accepts_corrective_summary_fields(self) -> None:
        _, corrective = comparative_summaries()
        report = render_report(corrective)
        self.assertIn("omitted; documented default relied upon", report)
        self.assertIn("Corrective-run baseline provenance", report)
        self.assertIn("Empty-side boundary observations", report)
        self.assertIn(Q8_DELAY_LIMITATION, report)

    def test_comparative_report_is_json_round_trip_deterministic(self) -> None:
        original, corrective = comparative_summaries()
        round_trip_original = json.loads(json.dumps(original, sort_keys=True))
        round_trip_corrective = json.loads(json.dumps(corrective, sort_keys=True))
        self.assertEqual(
            render_comparative_report(original, corrective),
            render_comparative_report(round_trip_original, round_trip_corrective),
        )

    def test_comparative_report_preserves_both_runs_and_counterexamples(self) -> None:
        original, corrective = comparative_summaries()
        report = render_comparative_report(original, corrective)
        self.assertIn("historical-run-id", report)
        self.assertIn("corrective-run-id", report)
        self.assertIn("historical-raw.jsonl", report)
        self.assertIn("corrective-raw.jsonl", report)
        self.assertIn("CONFIRMED -> UNRESOLVED", report)
        self.assertIn("Q3 exclusions preserved: **7** of **12**", report)
        self.assertIn("historical-empty-side-boundary", report)
        self.assertIn("corrective-diagnostic", report)
        self.assertIn("Established", report)
        self.assertIn("Suggested", report)
        self.assertIn("Project-derived", report)
        self.assertIn("new aggregate size", report)
        self.assertIn("empty string as the raw absent-value form", report)
        self.assertIn("initial_dump", report)
        self.assertIn("level", report)
        self.assertIn(
            "https://docs.polymarket.com/api-reference/wss/market.md", report
        )
        self.assertIn("`jsonPayloadSchema.properties`", report)
        self.assertIn("kind-level support remains diagnostic only", report)
        self.assertIn("not claim that numeric 0/1 values are universal", report)
        self.assertIn("Per-question evidence, differences, and applicability", report)
        for index in range(1, 9):
            self.assertIn(f"### Q{index} —", report)
        self.assertIn("## Deviations from IP-001", report)
        self.assertIn("No authenticated endpoint", report)

    def test_comparative_report_does_not_claim_latency_estimation(self) -> None:
        original, corrective = comparative_summaries()
        report = render_comparative_report(original, corrective)
        self.assertIn(Q8_DELAY_LIMITATION, report)
        self.assertIn("neither distribution estimates venue or network latency", report)
        self.assertNotIn("venue latency was", report.lower())
        self.assertNotIn("network latency was", report.lower())


if __name__ == "__main__":
    unittest.main()
