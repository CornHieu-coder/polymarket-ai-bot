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


def record(sequence: int, payload: dict, digest_suffix: str = "") -> dict:
    raw = json.dumps(payload, separators=(",", ":")) + digest_suffix
    return {
        "ingest_sequence": sequence,
        "received_at": RECEIVED_AT,
        "raw_payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "parse_status": "ok",
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
            )
            store.close()
        self.assertEqual(base64.b64decode(stored["raw_response"]), body)
        self.assertEqual(decoded["unknown"], "exact")


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
