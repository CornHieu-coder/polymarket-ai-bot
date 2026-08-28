from __future__ import annotations

import unittest
from copy import deepcopy
from decimal import Decimal

from tools.probes.pmxt_ordering_audit.core import (
    AuditParseError,
    candidate_hours,
    classify_availability_group,
    parse_decimal,
)


KEY = {
    "market": "0x" + "a" * 64,
    "asset_id": "123",
    "timestamp_received": "2026-05-01T12:00:00.000Z",
}


def row(event_type: str, **values: object) -> dict:
    return {
        **KEY,
        "timestamp": "2026-05-01T12:00:00.000Z",
        "event_type": event_type,
        "bids": None,
        "asks": None,
        "price": None,
        "size": None,
        "side": None,
        "best_bid": None,
        "best_ask": None,
        "old_tick_size": None,
        "new_tick_size": None,
        **values,
    }


def book(**values: object) -> dict:
    payload = {
        "bids": '[["0.30","2"],["0.40","10.00"]]',
        "asks": '[["0.70","3"],["0.60","12"]]',
        **values,
    }
    return row("book", **payload)


def change(side: str, price: str, size: str, **values: object) -> dict:
    return row("price_change", side=side, price=price, size=size, **values)


class SampleSelectionTests(unittest.TestCase):
    def test_candidate_hours_are_requested_then_later_same_date_only(self) -> None:
        hours = candidate_hours("2026-05-01T12")
        self.assertEqual(hours[0], "2026-05-01T12")
        self.assertEqual(hours[-1], "2026-05-01T23")
        self.assertEqual(len(hours), 12)
        self.assertTrue(all(value.startswith("2026-05-01T") for value in hours))

    def test_candidate_hours_reject_malformed_input(self) -> None:
        with self.assertRaises(ValueError):
            candidate_hours("2026-05-01")

    def test_grouping_key_is_exact_market_asset_receive_timestamp(self) -> None:
        first = change("BUY", "0.40", "10")
        different_market = {**first, "market": "0x" + "b" * 64}
        with self.assertRaisesRegex(ValueError, "availability key"):
            classify_availability_group([first, different_market])


class ExactParsingTests(unittest.TestCase):
    def test_exact_decimal_rejects_float(self) -> None:
        self.assertEqual(parse_decimal("0.1000", "price"), Decimal("0.1000"))
        with self.assertRaises(AuditParseError):
            parse_decimal(0.1, "price")


class L2ClassificationTests(unittest.TestCase):
    def test_distinct_price_level_replacements_are_invariant(self) -> None:
        result = classify_availability_group(
            [change("BUY", "0.40", "10"), change("SELL", "0.60", "12")]
        )
        self.assertEqual(result["l2_status"], "INVARIANT")
        self.assertEqual(result["l2_reason"], "distinct_price_keys_commute")

    def test_same_key_same_size_is_idempotent(self) -> None:
        result = classify_availability_group(
            [change("BUY", "0.40", "10.0"), change("BUY", "0.400", "10")]
        )
        self.assertEqual(result["l2_status"], "INVARIANT")
        self.assertEqual(result["repeated_price_keys"], 1)
        self.assertEqual(result["conflicting_price_keys"], 0)

    def test_same_key_different_size_is_order_ambiguous(self) -> None:
        result = classify_availability_group(
            [change("BUY", "0.40", "10"), change("BUY", "0.40", "25")]
        )
        self.assertEqual(result["l2_status"], "AMBIGUOUS")
        self.assertEqual(result["conflicting_price_keys"], 1)

    def test_negative_replacement_size_is_unresolved(self) -> None:
        result = classify_availability_group(
            [change("BUY", "0.40", "-1"), change("SELL", "0.60", "12")]
        )
        self.assertEqual(result["l2_status"], "UNRESOLVED")
        self.assertEqual(result["l2_reason"], "price_change_parse_failure")
        self.assertEqual(result["repeated_price_keys"], 0)
        self.assertEqual(result["conflicting_price_keys"], 0)
        self.assertIn("non-negative replacement size", result["parse_errors"][0])

    def test_malformed_price_change_result_is_order_independent(self) -> None:
        rows = [
            change("BUY", "0.40", "10"),
            change("BUY", "0.40", "25"),
            change("SELL", "0.60", "-1"),
        ]
        forward = classify_availability_group(rows)
        reverse = classify_availability_group(list(reversed(rows)))
        self.assertEqual(forward, reverse)
        self.assertEqual(forward["conflicting_replacements"], [])

    def test_identical_books_are_invariant_after_canonical_parsing(self) -> None:
        reordered = book(
            bids='[["0.400","10"],["0.3","2.0"]]',
            asks='[["0.600","12.0"],["0.7","3.00"]]',
        )
        result = classify_availability_group([book(), reordered])
        self.assertEqual(result["l2_status"], "INVARIANT")
        self.assertTrue(result["accepted_book"])

    def test_different_books_are_ambiguous(self) -> None:
        different = book(bids='[["0.40","11"]]')
        result = classify_availability_group([book(), different])
        self.assertEqual(result["l2_status"], "AMBIGUOUS")
        self.assertEqual(result["l2_reason"], "differing_book_snapshots")

    def test_snapshot_plus_matching_delta_is_invariant(self) -> None:
        result = classify_availability_group(
            [book(), change("BUY", "0.40", "10")]
        )
        self.assertEqual(result["l2_status"], "INVARIANT")
        self.assertTrue(result["accepted_book"])

    def test_snapshot_plus_changing_delta_is_ambiguous(self) -> None:
        result = classify_availability_group(
            [book(), change("BUY", "0.40", "11")]
        )
        self.assertEqual(result["l2_status"], "AMBIGUOUS")
        self.assertFalse(result["accepted_book"])

    def test_malformed_book_is_unresolved(self) -> None:
        malformed = book(bids="not json")
        result = classify_availability_group([malformed])
        self.assertEqual(result["l2_status"], "UNRESOLVED")
        self.assertTrue(result["parse_errors"])

    def test_auxiliary_last_trade_tie_does_not_invalidate_l2(self) -> None:
        result = classify_availability_group(
            [
                change("BUY", "0.40", "10"),
                row("last_trade_price", price="0.50", size="2", side="BUY"),
            ]
        )
        self.assertEqual(result["l2_status"], "INVARIANT")
        self.assertEqual(result["last_trade_price_count"], 1)

    def test_unknown_event_is_preserved_as_unresolved(self) -> None:
        result = classify_availability_group([row("future_state_event")])
        self.assertEqual(result["l2_status"], "UNRESOLVED")
        self.assertEqual(result["unknown_event_types"], ["future_state_event"])

    def test_source_timestamp_differences_do_not_resolve_conflict(self) -> None:
        first = change("BUY", "0.40", "10", timestamp="2026-05-01T12:00:00Z")
        second = change("BUY", "0.40", "25", timestamp="2026-05-01T12:00:01Z")
        result = classify_availability_group([first, second])
        self.assertEqual(result["l2_status"], "AMBIGUOUS")

    def test_row_reordering_does_not_change_classification(self) -> None:
        rows = [
            change("BUY", "0.40", "10", timestamp="2026-05-01T12:00:01Z"),
            change("BUY", "0.40", "25", timestamp="2026-05-01T12:00:00Z"),
            row("last_trade_price", price="0.50", size="2", side="SELL"),
        ]
        self.assertEqual(
            classify_availability_group(rows),
            classify_availability_group(list(reversed(rows))),
        )

    def test_exact_duplicate_rows_are_counted_without_dropping_them(self) -> None:
        duplicate = change("BUY", "0.40", "10")
        result = classify_availability_group([duplicate, deepcopy(duplicate)])
        self.assertEqual(result["exact_duplicate_rows"], 1)
        self.assertEqual(result["l2_status"], "INVARIANT")


class TickClassificationTests(unittest.TestCase):
    def test_single_tick_transition_is_deterministic(self) -> None:
        result = classify_availability_group(
            [row("tick_size_change", old_tick_size="0.01", new_tick_size="0.001")]
        )
        self.assertEqual(result["tick_status"], "DETERMINISTIC")

    def test_duplicate_tick_transitions_are_deterministic(self) -> None:
        transition = row(
            "tick_size_change", old_tick_size="0.01", new_tick_size="0.001"
        )
        result = classify_availability_group([transition, deepcopy(transition)])
        self.assertEqual(result["tick_status"], "DETERMINISTIC")
        self.assertEqual(result["tick_reason"], "duplicate_tick_transitions")

    def test_distinct_tick_transitions_are_ambiguous(self) -> None:
        result = classify_availability_group(
            [
                row(
                    "tick_size_change",
                    old_tick_size="0.01",
                    new_tick_size="0.001",
                ),
                row(
                    "tick_size_change",
                    old_tick_size="0.001",
                    new_tick_size="0.0001",
                ),
            ]
        )
        self.assertEqual(result["tick_status"], "AMBIGUOUS")


if __name__ == "__main__":
    unittest.main()
