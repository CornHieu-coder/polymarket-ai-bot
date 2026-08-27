from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import unittest
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from tools.probes.market_feed_contract.core import (
    BookState,
    ContractAnalyzer,
    DurableJsonl,
    EvidenceStore,
    EvidenceWriteError,
    ProbeParseError,
    official_orderbook_hash,
    parse_decimal,
)


FIXTURES = Path(__file__).parent / "fixtures"
TOKEN = "12345678901234567890"
RECEIVED_AT = "2026-08-26T00:00:00.100000Z"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def record(
    sequence: int,
    payload: dict,
    digest_suffix: str = "",
    *,
    received_monotonic_ns: int | None = None,
) -> dict:
    raw = json.dumps(payload, separators=(",", ":")) + digest_suffix
    return {
        "ingest_sequence": sequence,
        "received_at": RECEIVED_AT,
        "raw_payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "parse_status": "ok",
        "received_monotonic_ns": received_monotonic_ns,
    }


class EvidenceStoreTests(unittest.TestCase):
    def test_raw_frame_persistence_preserves_exact_text_and_bytes(self) -> None:
        text_payload = '{"event_type":"fixture","unknown":"雪\\nline"}'
        binary_payload = b"\x00\xffexact\r\nbytes"
        with tempfile.TemporaryDirectory() as temporary:
            store = EvidenceStore(Path(temporary), "run", {"fixture": True})
            text_record, _ = store.record_websocket(
                text_payload, session_id="s1", token_ids=[TOKEN]
            )
            byte_record, _ = store.record_websocket(
                binary_payload, session_id="s2", token_ids=[TOKEN]
            )
            store.close()
            rows = [
                json.loads(line)
                for line in (Path(temporary) / "run" / "raw-websocket.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
        self.assertEqual(rows[0]["raw_payload"], text_payload)
        self.assertEqual(base64.b64decode(rows[1]["raw_payload"]), binary_payload)
        self.assertEqual(text_record["raw_payload_sha256"], hashlib.sha256(text_payload.encode()).hexdigest())
        self.assertEqual(byte_record["raw_payload_sha256"], hashlib.sha256(binary_payload).hexdigest())

    def test_ingest_sequence_is_global_and_monotonic_across_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = EvidenceStore(Path(temporary), "run", {})
            first, _ = store.record_websocket("PONG", session_id="s1", token_ids=[TOKEN])
            middle = store.record_control("controlled_disconnect", session_id="s1")
            last, _ = store.record_websocket("PONG", session_id="s2", token_ids=[TOKEN])
            store.close()
        self.assertEqual(
            [first["ingest_sequence"], middle["ingest_sequence"], last["ingest_sequence"]],
            [1, 2, 3],
        )

    def test_unknown_fields_survive_raw_capture(self) -> None:
        payload = '{"event_type":"book","future":{"nested":[{"x":7}]}}'
        with tempfile.TemporaryDirectory() as temporary:
            store = EvidenceStore(Path(temporary), "run", {})
            store.record_websocket(payload, session_id="s1", token_ids=[TOKEN])
            store.close()
            row = json.loads(
                (Path(temporary) / "run" / "raw-websocket.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()[0]
            )
        self.assertEqual(json.loads(row["raw_payload"])["future"]["nested"][0]["x"], 7)

    def test_durable_write_failure_is_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            writer = DurableJsonl(Path(temporary) / "evidence.jsonl")
            with patch.object(writer._file, "write", side_effect=OSError("disk full")):
                with self.assertRaises(EvidenceWriteError):
                    writer.append({"raw": "must not continue"})
            writer.close()

    def test_raw_rest_bytes_round_trip(self) -> None:
        body = b'{"asset_id":"123","unknown":"exact"}'
        with tempfile.TemporaryDirectory() as temporary:
            store = EvidenceStore(Path(temporary), "run", {})
            stored, decoded = store.record_rest(
                token_id="123",
                url="https://clob.polymarket.com/book?token_id=123",
                request_started_at=RECEIVED_AT,
                received_at=RECEIVED_AT,
                status_code=200,
                raw_response=body,
                elapsed_ms="1.25",
                request_context={"request_purpose": "fixture", "candidate_id": "c1"},
            )
            store.close()
        self.assertEqual(base64.b64decode(stored["raw_response"]), body)
        self.assertEqual(decoded["unknown"], "exact")
        self.assertEqual(
            stored["request_context"],
            {"request_purpose": "fixture", "candidate_id": "c1"},
        )


class DecimalAndBookTests(unittest.TestCase):
    def test_decimal_parsing_is_exact_and_rejects_float(self) -> None:
        parsed = parse_decimal("0.1000000000000000000000001")
        self.assertEqual(parsed.as_tuple().exponent, -25)
        self.assertEqual(parsed, Decimal("0.1000000000000000000000001"))
        with self.assertRaises(ProbeParseError):
            parse_decimal(0.1)

    def test_zero_size_deletes_level(self) -> None:
        state = BookState.from_event(fixture("documented_book_fixture.json"))
        self.assertEqual(state.best_bid, Decimal("0.40"))
        state.apply_replacement("BUY", "0.40", "0")
        self.assertEqual(state.best_bid, Decimal("0.30"))

    def test_nonzero_size_replaces_instead_of_incrementing(self) -> None:
        state = BookState.from_event(fixture("documented_book_fixture.json"))
        state.apply_replacement("BUY", "0.40", "3.25")
        self.assertEqual(state.bids[Decimal("0.40")].size, Decimal("3.25"))

    def test_bid_and_ask_updates_are_independent(self) -> None:
        state = BookState.from_event(fixture("documented_book_fixture.json"))
        original_asks = deepcopy(state.asks)
        state.apply_replacement("BUY", "0.55", "2")
        self.assertEqual(state.asks, original_asks)
        original_bids = deepcopy(state.bids)
        state.apply_replacement("SELL", "0.58", "3")
        self.assertEqual(state.bids, original_bids)

    def test_best_bid_ask_derivation_handles_empty_side(self) -> None:
        state = BookState.from_event(fixture("documented_book_fixture.json"))
        self.assertEqual((state.best_bid, state.best_ask), (Decimal("0.40"), Decimal("0.60")))
        state.asks.clear()
        self.assertIsNone(state.best_ask)

    def test_duplicate_replacement_and_deletion_are_idempotent(self) -> None:
        state = BookState.from_event(fixture("documented_book_fixture.json"))
        self.assertFalse(state.apply_replacement("BUY", "0.40", "8"))
        self.assertTrue(state.apply_replacement("BUY", "0.40", "8.0"))
        self.assertFalse(state.apply_replacement("BUY", "0.40", "0"))
        self.assertTrue(state.apply_replacement("BUY", "0.40", "0"))

    def test_snapshot_rejects_duplicate_price_levels(self) -> None:
        event = fixture("documented_book_fixture.json")
        event["bids"].append({"price": "0.400", "size": "1"})
        with self.assertRaisesRegex(ProbeParseError, "repeats price"):
            BookState.from_event(event)


class AnalyzerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.book = fixture("documented_book_fixture.json")
        self.change = fixture("documented_price_change_fixture.json")
        self.analyzer = ContractAnalyzer([TOKEN])
        self.analyzer.start_session("session-001")

    def observe_empty_ask_boundary_candidate(self) -> dict:
        book = deepcopy(self.book)
        book["asks"] = [{"price": "0.60", "size": "12.00"}]
        self.analyzer.observe_websocket(record(1, book), book)
        change = {
            "event_type": "price_change",
            "market": book["market"],
            "timestamp": str(int(book["timestamp"]) + 1),
            "price_changes": [
                {
                    "asset_id": TOKEN,
                    "price": "0.60",
                    "size": "0",
                    "side": "SELL",
                    "hash": "fixture-empty-ask",
                    "best_bid": "0.40",
                    "best_ask": "1",
                }
            ],
        }
        self.analyzer.observe_websocket(record(2, change), change)
        return self.analyzer.take_empty_side_probe_requests()[0]

    def aligned_rest_record(
        self,
        *,
        sequence: int,
        context: dict | None = None,
        stable: bool = True,
    ) -> dict:
        version = self.analyzer.state_versions[TOKEN]
        return {
            "ingest_sequence": sequence,
            "status_code": 200,
            "token_id": TOKEN,
            "parse_status": "ok",
            "state_version_before": version,
            "state_version_after": version if stable else version + 1,
            "state_session_before": "session-001",
            "state_session_after": "session-001",
            "state_valid_before": True,
            "state_valid_after": True,
            "request_context": context or {"request_purpose": "periodic_poll"},
        }

    def test_malformed_state_change_is_recorded_without_state_mutation(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        before = deepcopy(self.analyzer.states[TOKEN].bids)
        malformed = deepcopy(self.change)
        del malformed["price_changes"][0]["size"]
        errors = self.analyzer.observe_websocket(record(2, malformed), malformed)
        self.assertTrue(any(error["code"] == "missing_required_field" for error in errors))
        self.assertEqual(self.analyzer.states[TOKEN].bids, before)

    def test_invalid_json_frame_is_retained_and_classified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = EvidenceStore(Path(temporary), "run", {})
            raw_record, decoded = store.record_websocket(
                '{"event_type":', session_id="session-001", token_ids=[TOKEN]
            )
            errors = self.analyzer.observe_websocket(raw_record, decoded)
            store.close()
        self.assertEqual(raw_record["parse_status"], "invalid_json")
        self.assertEqual(errors[0]["code"], "invalid_json")

    def test_decimal_failure_is_recorded_and_invalidates_state(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        malformed = deepcopy(self.change)
        malformed["price_changes"][0]["size"] = "not-a-decimal"
        errors = self.analyzer.observe_websocket(record(2, malformed), malformed)
        self.assertTrue(any(error["code"] == "decimal_parse_failure" for error in errors))
        self.assertFalse(self.analyzer.states[TOKEN].valid)

    def test_aggregation_counts_matches_mismatches_and_field_frequency(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        self.analyzer.observe_websocket(record(2, self.change), self.change)
        mismatch = deepcopy(self.change)
        mismatch["timestamp"] = "1787702400002"
        mismatch["price_changes"][0].update(
            {"side": "SELL", "price": "0.55", "size": "2", "best_bid": "0.49", "best_ask": "0.55"}
        )
        self.analyzer.observe_websocket(record(3, mismatch), mismatch)
        self.analyzer.end_session("session-001", controlled_disconnect=True)
        summary = self.analyzer.summary({}, {})
        self.assertEqual(summary["q3"]["comparison_entries"], 2)
        self.assertEqual(summary["q3"]["exact_matches"], 1)
        self.assertEqual(summary["q3"]["mismatches"], 1)
        fields = summary["q1"]["event_schema"]["price_change"]["fields"]
        self.assertEqual(fields["price_changes[].best_bid"]["event_presence"], 2)
        self.assertEqual(fields["price_changes[].best_bid"]["occurrences"], 2)

    def test_empty_ask_one_is_a_traceable_candidate_not_an_integrity_failure(self) -> None:
        context = self.observe_empty_ask_boundary_candidate()
        state = self.analyzer.states[TOKEN]
        self.assertTrue(state.valid)
        self.assertIsNone(state.best_ask)
        self.assertEqual(context["candidate_kind"], "ask_one_when_empty")
        self.assertEqual(self.analyzer.comparison_mismatches, 0)
        q3 = self.analyzer.summary({}, {})["q3"]
        interpretation = q3["empty_side_boundary_interpretation"]
        self.assertEqual(interpretation["status"], "UNRESOLVED")
        self.assertEqual(interpretation["observed_candidates"], 1)
        self.assertEqual(interpretation["unresolved_candidates"], 1)

    def test_empty_bid_zero_is_a_traceable_candidate(self) -> None:
        book = deepcopy(self.book)
        book["bids"] = [{"price": "0.40", "size": "10.00"}]
        self.analyzer.observe_websocket(record(1, book), book)
        change = {
            "event_type": "price_change",
            "market": book["market"],
            "timestamp": str(int(book["timestamp"]) + 1),
            "price_changes": [
                {
                    "asset_id": TOKEN,
                    "price": "0.40",
                    "size": "0",
                    "side": "BUY",
                    "hash": "fixture-empty-bid",
                    "best_bid": "0",
                    "best_ask": "0.60",
                }
            ],
        }
        self.analyzer.observe_websocket(record(2, change), change)
        context = self.analyzer.take_empty_side_probe_requests()[0]
        self.assertEqual(context["candidate_kind"], "bid_zero_when_empty")
        self.assertTrue(self.analyzer.states[TOKEN].valid)
        self.assertIsNone(self.analyzer.states[TOKEN].best_bid)

    def test_exact_stable_rest_empty_side_confirms_only_bounded_candidate(self) -> None:
        context = self.observe_empty_ask_boundary_candidate()
        rest = deepcopy(self.book)
        rest.pop("event_type")
        rest["asks"] = []
        self.analyzer.observe_rest(
            self.aligned_rest_record(sequence=3, context=context), rest
        )
        q3 = self.analyzer.summary({}, {})["q3"]
        interpretation = q3["empty_side_boundary_interpretation"]
        self.assertEqual(interpretation["status"], "CONFIRMED")
        self.assertEqual(interpretation["confirmed_kinds"], {"ask_one_when_empty": 1})
        self.assertEqual(interpretation["unresolved_candidates"], 0)
        self.assertIn("never normalized globally", interpretation["scope"])

    def test_same_kind_unresolved_candidate_blocks_run_level_q3_confirmation(self) -> None:
        first_context = self.observe_empty_ask_boundary_candidate()
        first_rest = deepcopy(self.book)
        first_rest.pop("event_type")
        first_rest["asks"] = []
        self.analyzer.observe_rest(
            self.aligned_rest_record(sequence=3, context=first_context), first_rest
        )

        replacement = deepcopy(self.change)
        replacement["timestamp"] = str(int(replacement["timestamp"]) + 1)
        replacement["price_changes"][0].update(
            {
                "price": "0.40",
                "size": "8",
                "side": "BUY",
                "best_bid": "0.40",
                "best_ask": "1",
            }
        )
        self.analyzer.observe_websocket(record(4, replacement), replacement)
        second_context = self.analyzer.take_empty_side_probe_requests()[0]
        self.assertEqual(
            second_context["candidate_kind"], first_context["candidate_kind"]
        )

        second_rest = deepcopy(first_rest)
        second_rest["bids"][0]["size"] = "8"
        self.analyzer.observe_rest(
            self.aligned_rest_record(sequence=5), second_rest
        )

        q3 = self.analyzer.summary({}, {})["q3"]
        interpretation = q3["empty_side_boundary_interpretation"]
        self.assertEqual(
            q3["component_statuses"]["nonzero_aggregate_replacement"], "CONFIRMED"
        )
        self.assertEqual(q3["component_statuses"]["zero_size_delete"], "CONFIRMED")
        self.assertEqual(q3["component_statuses"]["buy_bid_sell_ask"], "CONFIRMED")
        self.assertEqual(interpretation["observed_kinds"], {"ask_one_when_empty": 2})
        self.assertEqual(interpretation["confirmed_kinds"], {"ask_one_when_empty": 1})
        self.assertEqual(interpretation["kind_level_status"], "CONFIRMED")
        self.assertEqual(interpretation["confirmed_candidates"], 1)
        self.assertEqual(interpretation["unresolved_candidates"], 1)
        self.assertFalse(interpretation["all_observed_candidates_confirmed"])
        self.assertEqual(interpretation["status"], "UNRESOLVED")
        self.assertEqual(q3["component_statuses"]["event_best_prices"], "UNRESOLVED")
        self.assertEqual(q3["status"], "UNRESOLVED")

    def test_rest_mismatch_leaves_empty_side_candidate_unresolved(self) -> None:
        context = self.observe_empty_ask_boundary_candidate()
        rest = deepcopy(self.book)
        rest.pop("event_type")
        self.analyzer.observe_rest(
            self.aligned_rest_record(sequence=3, context=context), rest
        )
        interpretation = self.analyzer.summary({}, {})["q3"][
            "empty_side_boundary_interpretation"
        ]
        self.assertEqual(interpretation["status"], "UNRESOLVED")
        self.assertEqual(interpretation["unresolved_candidates"], 1)
        self.assertEqual(
            interpretation["targeted_rest_diagnostics"],
            {"aligned_full_depth_mismatch": 1},
        )
        self.assertTrue(self.analyzer.states[TOKEN].valid)

    def test_unaligned_rest_leaves_empty_side_candidate_unresolved(self) -> None:
        context = self.observe_empty_ask_boundary_candidate()
        rest = deepcopy(self.book)
        rest.pop("event_type")
        rest["asks"] = []
        self.analyzer.observe_rest(
            self.aligned_rest_record(sequence=3, context=context, stable=False), rest
        )
        interpretation = self.analyzer.summary({}, {})["q3"][
            "empty_side_boundary_interpretation"
        ]
        self.assertEqual(interpretation["status"], "UNRESOLVED")
        self.assertEqual(interpretation["targeted_rest_unaligned"], 1)

    def test_numeric_boundary_is_not_globally_normalized(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        change = deepcopy(self.change)
        change["price_changes"][0]["best_ask"] = "1"
        self.analyzer.observe_websocket(record(2, change), change)
        self.assertFalse(self.analyzer.states[TOKEN].valid)
        self.assertEqual(self.analyzer.comparison_mismatches, 1)
        self.assertEqual(self.analyzer.empty_side_candidate_entries, 0)

    def test_genuine_bbo_mismatch_fails_closed_and_excludes_followup(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        mismatch = deepcopy(self.change)
        mismatch["price_changes"][0]["best_bid"] = "0.49"
        self.analyzer.observe_websocket(record(2, mismatch), mismatch)
        followup = deepcopy(self.change)
        followup["timestamp"] = str(int(followup["timestamp"]) + 1)
        self.analyzer.observe_websocket(record(3, followup), followup)
        q3 = self.analyzer.summary({}, {})["q3"]
        self.assertEqual(q3["status"], "CONTRADICTED")
        self.assertEqual(q3["genuine_mismatches"], 1)
        self.assertEqual(q3["excluded_change_entries"], 1)

    def test_schema_records_unexpected_nested_fields(self) -> None:
        event = deepcopy(self.book)
        event["future"] = {"nested": [{"sequence_id": 9, "value": "x"}]}
        self.analyzer.observe_websocket(record(1, event), event)
        self.analyzer.end_session("session-001", controlled_disconnect=True)
        q1 = self.analyzer.summary({}, {})["q1"]
        self.assertIn("future.nested[].value", q1["event_schema"]["book"]["fields"])
        self.assertEqual(q1["candidate_ordering_fields"]["future.nested[].sequence_id"], 1)

    def test_multiple_same_key_updates_preserve_order_measurement(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        event = deepcopy(self.change)
        event["price_changes"].append(deepcopy(event["price_changes"][0]))
        event["price_changes"][1]["size"] = "9"
        self.analyzer.observe_websocket(record(2, event), event)
        self.analyzer.end_session("session-001", controlled_disconnect=True)
        q4 = self.analyzer.summary({}, {})["q4"]
        self.assertEqual(q4["multi_entry_frames"], 1)
        self.assertEqual(q4["duplicate_asset_side_price_frames"], 1)
        self.assertEqual(q4["order_sensitive_frames"], 1)

    def test_multi_asset_frame_is_not_blanket_bbo_ambiguity(self) -> None:
        second_token = "98765432109876543210"
        analyzer = ContractAnalyzer([TOKEN, second_token])
        analyzer.start_session("session-001")
        first_book = deepcopy(self.book)
        second_book = deepcopy(self.book)
        second_book["asset_id"] = second_token
        analyzer.observe_websocket(record(1, first_book), first_book)
        analyzer.observe_websocket(record(2, second_book), second_book)
        event = deepcopy(self.change)
        second_change = deepcopy(event["price_changes"][0])
        second_change["asset_id"] = second_token
        second_change["best_bid"] = "0.49"
        event["price_changes"].append(second_change)
        analyzer.observe_websocket(record(3, event), event)
        q3 = analyzer.summary({}, {})["q3"]
        q4 = analyzer.summary({}, {})["q4"]
        self.assertEqual(q3["direct_single-entry_mismatches"], 1)
        self.assertEqual(q3["status"], "CONTRADICTED")
        self.assertEqual(q4["multi_entry_frames"], 1)
        self.assertEqual(q4["multi_same_asset_frames"], 0)
        self.assertFalse(
            analyzer.comparison_counterexamples[0][
                "multi_entry_semantics_ambiguous"
            ]
        )

    def test_same_level_update_supersedes_prior_independent_validation_claim(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        first = deepcopy(self.change)
        first["price_changes"][0].update(
            {"price": "0.40", "size": "8", "best_bid": "0.40"}
        )
        second = deepcopy(first)
        second["timestamp"] = str(int(first["timestamp"]) + 1)
        second["price_changes"][0]["size"] = "7"
        self.analyzer.observe_websocket(record(2, first), first)
        self.analyzer.observe_websocket(record(3, second), second)

        rest = deepcopy(self.book)
        rest.pop("event_type")
        rest["bids"][0]["size"] = "7"
        self.analyzer.observe_rest(self.aligned_rest_record(sequence=4), rest)
        q3 = self.analyzer.summary({}, {})["q3"]
        self.assertEqual(q3["discriminating_nonzero_updates"], 2)
        self.assertEqual(q3["superseded_before_validation"], 1)
        self.assertEqual(q3["independently_validated_nonzero_replacements"], 1)

    def test_two_sessions_require_fresh_book_and_later_delta(self) -> None:
        for number in (1, 2):
            if number == 2:
                self.analyzer.start_session("session-002")
            session = f"session-{number:03d}"
            self.analyzer.observe_websocket(record(number * 10, self.book, session), self.book)
            change = deepcopy(self.change)
            change["timestamp"] = str(int(change["timestamp"]) + number)
            self.analyzer.observe_websocket(record(number * 10 + 1, change, session), change)
            self.analyzer.end_session(session, controlled_disconnect=True)
        q2 = self.analyzer.summary({}, {})["q2"]
        self.assertEqual(q2["status"], "CONFIRMED")

    def test_two_sessions_without_clean_controlled_reconnect_remain_unresolved(self) -> None:
        for number in (1, 2):
            if number == 2:
                self.analyzer.start_session("session-002")
            session = f"session-{number:03d}"
            self.analyzer.observe_websocket(record(number * 10, self.book), self.book)
            change = deepcopy(self.change)
            change["timestamp"] = str(int(change["timestamp"]) + number)
            self.analyzer.observe_websocket(record(number * 10 + 1, change), change)
            self.analyzer.end_session(
                session, controlled_disconnect=number != 1
            )
        q2 = self.analyzer.summary({}, {})["q2"]
        self.assertEqual(q2["status"], "UNRESOLVED")
        self.assertFalse(q2["clean_controlled_reconnect"])

    def test_delta_before_book_contradicts_reconnect_ordering(self) -> None:
        self.analyzer.observe_websocket(record(1, self.change), self.change)
        self.analyzer.end_session("session-001", controlled_disconnect=True)
        self.analyzer.start_session("session-002")
        self.analyzer.observe_websocket(record(2, self.book), self.book)
        self.analyzer.end_session("session-002", controlled_disconnect=True)
        self.assertEqual(self.analyzer.summary({}, {})["q2"]["status"], "CONTRADICTED")

    def test_timestamp_regression_and_same_timestamp_are_counted(self) -> None:
        first = deepcopy(self.book)
        same = deepcopy(self.book)
        older = deepcopy(self.book)
        older["timestamp"] = str(int(first["timestamp"]) - 1)
        self.analyzer.observe_websocket(record(1, first), first)
        self.analyzer.observe_websocket(record(2, same, "same"), same)
        self.analyzer.observe_websocket(record(3, older, "older"), older)
        self.analyzer.end_session("session-001", controlled_disconnect=True)
        q8 = self.analyzer.summary({}, {})["q8"]
        self.assertEqual(q8["adjacent_same_timestamp_events"], 1)
        self.assertEqual(q8["source_timestamp_regressions"], 1)
        self.assertEqual(q8["status"], "CONTRADICTED")

    def test_q8_reports_probe_receive_order_without_gap_free_claim(self) -> None:
        first = record(1, self.book, received_monotonic_ns=100)
        second_book = deepcopy(self.book)
        second_book["timestamp"] = str(int(self.book["timestamp"]) + 1)
        second = record(2, second_book, received_monotonic_ns=200)
        self.analyzer.observe_websocket(first, self.book)
        self.analyzer.observe_websocket(second, second_book)
        q8 = self.analyzer.summary({}, {})["q8"]
        self.assertEqual(
            q8["processing_order"],
            {
                "websocket_records_observed": 2,
                "ingest_sequence_regressions": 0,
                "records_with_received_monotonic_ns": 2,
                "received_monotonic_ns_regressions": 0,
                "scope_note": (
                    "These counters measure probe processing order only; they do not "
                    "establish gap-free network delivery."
                ),
            },
        )
        self.assertIn(
            "not a venue/network latency estimate", q8["delay_caveat"]
        )

    def test_q8_receive_order_regression_is_explicitly_contradicted(self) -> None:
        self.analyzer.observe_websocket(
            record(2, self.book, received_monotonic_ns=200), self.book
        )
        later = deepcopy(self.book)
        later["timestamp"] = str(int(self.book["timestamp"]) + 1)
        self.analyzer.observe_websocket(
            record(1, later, received_monotonic_ns=100), later
        )
        q8 = self.analyzer.summary({}, {})["q8"]
        self.assertEqual(q8["status"], "CONTRADICTED")
        self.assertEqual(q8["processing_order"]["ingest_sequence_regressions"], 1)
        self.assertEqual(
            q8["processing_order"]["received_monotonic_ns_regressions"], 1
        )

    def test_stable_window_rest_exact_depth_comparison(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        version = self.analyzer.state_versions[TOKEN]
        rest = deepcopy(self.book)
        rest.pop("event_type")
        rest_record = {
            "ingest_sequence": 2,
            "status_code": 200,
            "token_id": TOKEN,
            "parse_status": "ok",
            "state_version_before": version,
            "state_version_after": version,
            "state_session_before": "session-001",
            "state_session_after": "session-001",
            "state_valid_before": True,
            "state_valid_after": True,
        }
        self.analyzer.observe_rest(rest_record, rest)
        self.assertEqual(self.analyzer.rest_aligned_comparisons, 1)
        self.assertEqual(self.analyzer.rest_aligned_matches, 1)

    def test_unstable_window_rest_is_diagnostic_only(self) -> None:
        self.analyzer.observe_websocket(record(1, self.book), self.book)
        rest = deepcopy(self.book)
        rest.pop("event_type")
        rest_record = {
            "ingest_sequence": 2,
            "status_code": 200,
            "token_id": TOKEN,
            "parse_status": "ok",
            "state_version_before": 1,
            "state_version_after": 2,
            "state_session_before": "session-001",
            "state_session_after": "session-001",
            "state_valid_before": True,
            "state_valid_after": True,
        }
        self.analyzer.observe_rest(rest_record, rest)
        self.assertEqual(self.analyzer.rest_aligned_comparisons, 0)
        self.assertEqual(self.analyzer.rest_unaligned, 1)

    def test_rest_non_2xx_is_recorded(self) -> None:
        self.analyzer.observe_rest(
            {
                "ingest_sequence": 1,
                "status_code": 503,
                "token_id": TOKEN,
                "parse_status": "ok",
            },
            {"error": "temporarily unavailable"},
        )
        self.assertEqual(self.analyzer.errors["rest_non_2xx"], 1)
        self.assertEqual(self.analyzer.rest_statuses[503], 1)

    def test_duplicate_raw_frames_are_retained_and_counted(self) -> None:
        repeated = record(1, self.book)
        self.analyzer.observe_websocket(repeated, self.book)
        repeated_second = dict(repeated, ingest_sequence=2)
        self.analyzer.observe_websocket(repeated_second, self.book)
        self.assertEqual(self.analyzer.duplicate_raw_frames, 1)


class OfficialHashTests(unittest.TestCase):
    def test_official_hash_is_stable_and_requires_every_field(self) -> None:
        event = fixture("documented_book_fixture.json")
        self.assertEqual(
            official_orderbook_hash(event), "d2c2cdbdef0adb03eef20be5cbf93626fc8e8df5"
        )
        del event["tick_size"]
        with self.assertRaisesRegex(ProbeParseError, "tick_size"):
            official_orderbook_hash(event)


if __name__ == "__main__":
    unittest.main()
