"""Deterministic Markdown rendering for IP-001 summaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def _cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _status_line(question: str, section: Mapping[str, Any], conclusion: str) -> str:
    return f"| {question} | **{section['status']}** | {_cell(conclusion)} |"


def _q_conclusions(summary: Mapping[str, Any]) -> list[str]:
    q1 = summary["q1"]
    q2 = summary["q2"]
    q3 = summary["q3"]
    q4 = summary["q4"]
    q5 = summary["q5"]
    q6 = summary["q6"]
    q7 = summary["q7"]
    q8 = summary["q8"]
    event_total = sum(item["events"] for item in q1["event_schema"].values())
    ordering = q1["candidate_ordering_fields"]
    q1_text = (
        f"Enumerated {len(q1['event_schema'])} event types across {event_total} logical "
        f"events; {sum(ordering.values())} sequence/order-field presences observed. "
        "Absence is sample-bounded."
    )
    q2_text = (
        f"Tested {len(q2['sessions'])} sessions including one controlled reconnect; "
        f"{len(q2['counterexamples'])} tokens lacked book-first evidence."
    )
    q3_text = (
        f"Applied {q3['nonzero_replacements']} non-zero and {q3['zero_size_updates']} "
        f"zero-size updates; BBO {q3['exact_matches']}/{q3['comparison_entries']} exact, "
        f"with {q3['independently_validated_nonzero_replacements']} independently "
        "validated discriminating replacements."
    )
    q4_text = (
        f"Observed {q4['price_change_frames']} price-change frames, "
        f"{q4['multi_entry_frames']} multi-entry frames, and "
        f"{q4['duplicate_asset_side_price_frames']} repeated-key frames."
    )
    book_hash = q5["websocket_book"]
    change_hash = q5["price_change_post_update"]
    q5_text = (
        f"Official algorithm: WS book {book_hash['matches']}/{book_hash['attempts']} "
        f"matches; post-change {change_hash['matches']}/{change_hash['attempts']} matches; "
        f"{book_hash['skipped_missing_fields'] + change_hash['skipped_missing_fields']} "
        "payloads ineligible because inputs were absent."
    )
    q6_text = (
        f"REST /book: {q6['successful_responses']}/{q6['requests']} successful; "
        f"{q6['aligned_matches']}/{q6['aligned_comparisons']} stable-window comparisons "
        "matched exactly. Unaligned mismatches remain diagnostics only."
    )
    q7_text = (
        f"Measured optional metadata over {q7['websocket_book_events']} WS book events: "
        + ", ".join(
            f"{name}={q7['optional_field_presence'][name]}"
            for name in sorted(q7["optional_field_presence"])
        )
        + "."
    )
    delays = q8["delay_ms_local_clock_dependent"]
    q8_text = (
        f"Measured {q8['timestamped_events']} timestamped events; "
        f"regressions={q8['source_timestamp_regressions']}, future-source="
        f"{q8['source_after_local_receive']}, local-clock-dependent p50 delay="
        f"{delays['p50']} ms."
    )
    return [
        _status_line("Q1 — Current event schema", q1, q1_text),
        _status_line("Q2 — Initial dump/reconnect", q2, q2_text),
        _status_line("Q3 — Price-level update semantics", q3, q3_text),
        _status_line("Q4 — Multiple updates per frame", q4, q4_text),
        _status_line("Q5 — Hash semantics", q5, q5_text),
        _status_line("Q6 — REST `/book` comparison", q6, q6_text),
        _status_line("Q7 — Optional WS book metadata", q7, q7_text),
        _status_line("Q8 — Timestamp behavior", q8, q8_text),
    ]


def _append_counterexamples(lines: list[str], summary: Mapping[str, Any]) -> None:
    lines.extend(["## Counterexamples and errors", ""])
    emitted = False
    duplicate_count = summary["evidence"].get("duplicate_raw_frames", 0)
    if duplicate_count:
        emitted = True
        lines.append(
            f"- Repeated raw-payload hashes retained: **{duplicate_count}**. This "
            "all-frame count is not, by itself, evidence of duplicate data events."
        )
    for index in range(1, 9):
        question = f"q{index}"
        for example in summary[question].get("counterexamples", []):
            emitted = True
            lines.append(f"- Q{index}: `{_json(example)}`")
    for code in sorted(summary["errors"]["counts"]):
        count = summary["errors"]["counts"][code]
        emitted = True
        lines.append(f"- Parser/operational error `{code}`: **{count}** occurrence(s).")
    if summary["errors"]["examples"]:
        lines.extend(
            [
                "",
                "Representative traceable examples (up to three per category, linked "
                "by raw `ingest_sequence`; complete counts and instances remain in the "
                "ignored evidence summary):",
                "",
            ]
        )
        examples_by_code: dict[str, list[Mapping[str, Any]]] = {}
        for example in summary["errors"]["examples"]:
            examples_by_code.setdefault(str(example.get("code", "uncategorized")), []).append(
                example
            )
        for code in sorted(examples_by_code):
            examples = examples_by_code[code]
            for example in examples[:3]:
                lines.append(f"- `{_json(example)}`")
            if len(examples) > 3:
                lines.append(
                    f"- `{code}`: {len(examples) - 3} additional traceable instance(s) "
                    "retained outside Git."
                )
    if not emitted:
        lines.append("No significant counterexample or parser/operational error was observed.")
    lines.append("")


def render_report(summary: Mapping[str, Any]) -> str:
    """Render byte-stable Markdown from a machine-readable summary."""

    run = summary["run"]
    records = summary["evidence"]["records"]
    software = run["software"]
    lines: list[str] = [
        "# Market Feed Contract Probe",
        "",
        "Status: **completed bounded read-only research probe**",
        "",
        "This report is empirical evidence for IP-001, not production collector/replay "
        "documentation and not a trading-performance result.",
        "",
        "## Run identity and configuration",
        "",
        "| Property | Value |",
        "| --- | --- |",
        f"| Run ID | `{_cell(run['run_id'])}` |",
        f"| UTC start | `{_cell(run['started_at'])}` |",
        f"| UTC end | `{_cell(run['ended_at'])}` |",
        f"| Requested duration | {run['requested_duration_seconds']} seconds |",
        f"| Actual wall/monotonic duration | {run['actual_duration_seconds']} seconds |",
        f"| REST interval | {run['rest_interval_seconds']} seconds |",
        f"| Controlled reconnects | {run['controlled_reconnects']} |",
        f"| Initial dump | `{str(run['initial_dump']).lower()}` (explicit) |",
        f"| WebSocket level | {run['websocket_level']} |",
        f"| Git commit captured | `{_cell(software.get('git_commit'))}` |",
        f"| Git worktree dirty at capture | `{str(software.get('git_dirty')).lower()}` |",
        f"| Python | `{software['python']}` |",
        f"| Probe transports | `httpx {software['httpx']}`, `websockets {software['websockets']}` |",
        f"| Offline test command | `{_cell(run['offline_test_command'])}` |",
        f"| Offline test result | {_cell(run['offline_test_result'])} |",
        f"| Raw evidence (ignored by Git) | `{_cell(run['raw_evidence_path'])}` |",
        f"| Raw capture boundary | {_cell(run['raw_boundary'])} |",
        "",
        "Explicit sampled token IDs:",
        "",
    ]
    lines.extend(f"- `{token}`" for token in run["token_ids"])
    lines.extend(
        [
            "",
            f"Sample provenance: {run['sample_note']}",
            "",
            "## Methodology and safety boundary",
            "",
            "- Connected only to the fixed public Polymarket Market WebSocket and sent "
            "the documented market subscription plus `PING` heartbeats.",
            "- Performed only public `GET /book` requests. HTTP redirects, environment "
            "proxies, credential-bearing headers, and WebSocket redirects were rejected.",
            "- Used two independent WebSocket sessions with a clean client-controlled "
            "disconnect/reconnect; reconstruction state was not carried across the gap.",
            "- Persisted exact application frames before derived state analysis, with "
            "separate source timestamps and local UTC receive timestamps, a global "
            "monotonic ingest sequence, session IDs, and SHA-256 payload digests.",
            "- Parsed canonical price and size values with Python `Decimal`; binary float "
            "equality was not used for contract conclusions.",
            "- Applied the project hypothesis in observed entry order: BUY→bid, SELL→ask, "
            "zero→delete, non-zero→replace. Best-price agreement alone was not treated as "
            "proof of aggregate-size replacement.",
            "- Attempted only the cited first-party order-book-summary SHA-1 algorithm and "
            "only when all required fields existed in the same payload/state. No missing "
            "hash input was filled from REST or another message.",
            "- Compared REST depth only across a stable observed request window: the asset "
            "had valid state before the request, and its session/version did not change "
            "through receipt. Other responses remained diagnostics.",
            "",
            "No authenticated endpoint, user WebSocket, API credential, wallet, private "
            "key, order construction, order simulation, order placement, or cancellation "
            "was used.",
            "",
            "## Evidence volume",
            "",
            "| Record type | Count |",
            "| --- | ---: |",
        ]
    )
    for name, count in sorted(records.items()):
        lines.append(f"| `{_cell(name)}` | {count} |")

    lines.extend(
        [
            "",
            "## Q1–Q8 conclusions",
            "",
            "Labels apply only to this bounded sample. `CONFIRMED` means directly "
            "supported within this run with no eligible unexplained counterexample; "
            "`CONTRADICTED` means eligible evidence falsified the tested claim; "
            "`UNRESOLVED` means evidence was absent, ambiguous, ineligible, or "
            "non-discriminating.",
            "",
            "| Question | Status | Bounded conclusion |",
            "| --- | --- | --- |",
            *_q_conclusions(summary),
            "",
            f"### Q1 — Current event schema — **{summary['q1']['status']}**",
            "",
        ]
    )
    q1 = summary["q1"]
    for event_type, detail in q1["event_schema"].items():
        lines.extend(
            [
                f"#### `{event_type}` ({detail['events']} logical events)",
                "",
                "| Field path | Events containing field | Total occurrences | Observed types |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for path, counts in detail["fields"].items():
            lines.append(
                f"| `{_cell(path)}` | {counts['event_presence']} | {counts['occurrences']} | "
                f"`{_json(counts['types'])}` |"
            )
        lines.append("")
    lines.append(
        "Candidate ordering fields observed: "
        + (f"`{_json(q1['candidate_ordering_fields'])}`" if q1["candidate_ordering_fields"] else "none")
        + "."
    )
    lines.extend(["", q1["scope_note"], ""])

    q2 = summary["q2"]
    lines.extend(
        [
            f"### Q2 — Initial dump/reconnect — **{q2['status']}**",
            "",
            "| Session | Token | First state event (seq) | First book seq | First delta seq | Book events | Delta entries | Controlled close |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for session_id, session in sorted(q2["sessions"].items()):
        for token, item in sorted(session["tokens"].items()):
            lines.append(
                f"| `{session_id}` | `{token}` | `{item['first_state_event']}` "
                f"(`{item['first_state_ingest_sequence']}`) | "
                f"{item['first_book_ingest_sequence']} | {item['first_delta_ingest_sequence']} | "
                f"{item['book_events']} | {item['price_change_entries']} | "
                f"`{str(session['controlled_disconnect']).lower()}` |"
            )
    lines.extend(
        [
            "",
            "A token/session without a later delta can confirm receipt of a fresh snapshot "
            "but cannot fully exercise snapshot-before-delta precedence.",
            "",
        ]
    )

    q3 = summary["q3"]
    lines.extend(
        [
            f"### Q3 — Price-level update semantics — **{q3['status']}**",
            "",
            "| Measurement | Count |",
            "| --- | ---: |",
            f"| Observed price-change entries | {q3['observed_change_entries']} |",
            f"| Applied price-change entries | {q3['applied_change_entries']} |",
            f"| Excluded price-change entries | {q3['excluded_change_entries']} |",
            f"| Best bid/ask comparisons | {q3['comparison_entries']} |",
            f"| Exact best bid/ask matches | {q3['exact_matches']} |",
            f"| Best bid/ask mismatches | {q3['mismatches']} |",
            f"| Direct single-entry mismatches | {q3['direct_single-entry_mismatches']} |",
            f"| Non-zero updates | {q3['nonzero_replacements']} |",
            f"| Zero-size updates | {q3['zero_size_updates']} |",
            f"| Discriminating non-zero updates | {q3['discriminating_nonzero_updates']} |",
            f"| Independently validated non-zero replacements | {q3['independently_validated_nonzero_replacements']} |",
            f"| Discriminating zero deletions | {q3['discriminating_zero_deletes']} |",
            f"| Independently validated zero deletions | {q3['independently_validated_zero_deletes']} |",
            f"| Idempotent replacements/deletions | {q3['idempotent_replacements']} |",
            "",
            q3["validation_note"],
            "",
            f"Side counts: `{_json(q3['side_updates'])}`. Component comparisons: "
            f"`{_json(q3['components'])}`.",
            "",
        ]
    )

    q4 = summary["q4"]
    lines.extend(
        [
            f"### Q4 — Multiple updates per frame — **{q4['status']}**",
            "",
            f"Entries-per-frame histogram: `{_json(q4['entries_per_frame'])}`.",
            "",
            "| Measurement | Count |",
            "| --- | ---: |",
            f"| Price-change frames | {q4['price_change_frames']} |",
            f"| Multi-entry frames | {q4['multi_entry_frames']} |",
            f"| Multiple entries for the same asset | {q4['multi_same_asset_frames']} |",
            f"| Repeated asset/side/price key | {q4['duplicate_asset_side_price_frames']} |",
            f"| Observed order-sensitive repeated-key frames | {q4['order_sensitive_frames']} |",
            "",
            "A zero count establishes only non-observation in this run; it does not prove "
            "that the payload shape cannot occur.",
            "",
        ]
    )

    q5 = summary["q5"]
    lines.extend(
        [
            f"### Q5 — Hash semantics — **{q5['status']}**",
            "",
            f"Algorithm attempted: {q5['algorithm']}.",
            "",
            "| Payload shape | Attempts | Matches | Mismatches | Skipped: missing fields |",
            "| --- | ---: | ---: | ---: | ---: |",
            f"| WebSocket `book` | {q5['websocket_book']['attempts']} | {q5['websocket_book']['matches']} | {q5['websocket_book']['mismatches']} | {q5['websocket_book']['skipped_missing_fields']} |",
            f"| Post-update `price_change` | {q5['price_change_post_update']['attempts']} | {q5['price_change_post_update']['matches']} | {q5['price_change_post_update']['mismatches']} | {q5['price_change_post_update']['skipped_missing_fields']} |",
            f"| REST diagnostic | {q5['rest_book_diagnostic']['attempts']} | {q5['rest_book_diagnostic']['matches']} | {q5['rest_book_diagnostic']['mismatches']} | n/a |",
            "",
            "Non-reproduction or missing required inputs leaves semantics unresolved; no "
            "alternative hash interpretation was reverse engineered.",
            "",
        ]
    )

    q6 = summary["q6"]
    lines.extend(
        [
            f"### Q6 — REST `/book` comparison — **{q6['status']}**",
            "",
            f"HTTP status counts: `{_json(q6['status_codes'])}`.",
            "",
            "| Expected REST field | Present | Missing |",
            "| --- | ---: | ---: |",
        ]
    )
    for field_name in sorted(q6["expected_field_presence"]):
        present = q6["expected_field_presence"][field_name]
        lines.append(
            f"| `{field_name}` | {present} | {q6['expected_field_missing'][field_name]} |"
        )
    lines.extend(
        [
            "",
            f"Alignment rule: {q6['alignment_rule']}",
            "",
            f"Aligned comparisons: **{q6['aligned_comparisons']}**; exact matches: "
            f"**{q6['aligned_matches']}**; mismatches: **{q6['aligned_mismatches']}**; "
            f"unaligned diagnostics: **{q6['unaligned_diagnostics']}**.",
            "",
            "A mismatch outside the declared alignment window is not classified as a "
            "WebSocket reconstruction failure.",
            "",
        ]
    )

    q7 = summary["q7"]
    lines.extend(
        [
            f"### Q7 — Optional metadata in WS book frames — **{q7['status']}**",
            "",
            f"Denominator: **{q7['websocket_book_events']}** WebSocket `book` events.",
            "",
            "| Optional field | Presence count |",
            "| --- | ---: |",
        ]
    )
    for field_name in sorted(q7["optional_field_presence"]):
        count = q7["optional_field_presence"][field_name]
        lines.append(f"| `{field_name}` | {count} |")
    lines.extend(["", q7["scope_note"], ""])

    q8 = summary["q8"]
    delays = q8["delay_ms_local_clock_dependent"]
    lines.extend(
        [
            f"### Q8 — Timestamp behavior — **{q8['status']}**",
            "",
            f"Per-token timestamped event counts: `{_json(q8['timestamp_events_by_token'])}`.",
            "",
            "| Measurement | Value |",
            "| --- | ---: |",
            f"| Timestamped logical events | {q8['timestamped_events']} |",
            f"| Source timestamp regressions | {q8['source_timestamp_regressions']} |",
            f"| Adjacent same-timestamp events | {q8['adjacent_same_timestamp_events']} |",
            f"| Same-timestamp groups | {len(q8['same_timestamp_groups'])} |",
            f"| Source timestamps after local receive | {q8['source_after_local_receive']} |",
            f"| Delay samples | {delays['count']} |",
            f"| Minimum delay (ms) | {delays['min']} |",
            f"| P50 delay (ms) | {delays['p50']} |",
            f"| P95 delay (ms) | {delays['p95']} |",
            f"| Maximum delay (ms) | {delays['max']} |",
            "",
            f"Quantile method: {delays['quantile_method']}.",
            "",
            "Delay is local-clock dependent. Negative values are retained, not corrected, "
            "and may indicate clock skew. These measurements do not prove gap-free delivery.",
            "",
        ]
    )

    _append_counterexamples(lines, summary)

    lines.extend(
        [
            "## Raw evidence integrity",
            "",
            "Raw evidence is outside Git under the ignored run directory. The committed "
            "report contains only derived counts and evidence references.",
            "",
            "| File | Bytes | SHA-256 |",
            "| --- | ---: | --- |",
        ]
    )
    for name in sorted(summary["evidence"]["manifest"]["files"]):
        detail = summary["evidence"]["manifest"]["files"][name]
        lines.append(f"| `{name}` | {detail['bytes']} | `{detail['sha256']}` |")
    lines.extend(
        [
            "",
            "## Limitations",
            "",
            "- This bounded sample cannot establish that an unobserved field or payload "
            "shape never occurs.",
            "- A short successful run cannot establish gap-free WebSocket delivery.",
            "- Local-clock delay is not authoritative exchange/network latency.",
            "- REST and WebSocket observations are asynchronous; only predeclared stable "
            "observed windows were eligible for exact depth comparison.",
            "- Feed-derived trade direction was not evaluated and is not treated as "
            "authoritative.",
            "- No result in this report is a trading-performance result.",
            "",
            "## Deviations from IP-001",
            "",
            "None. The probe used the packet-authorized explicit-token input mode and did "
            "not implement discovery, a production collector, or a replay engine.",
            "",
            "## First-party references applied",
            "",
            "- https://docs.polymarket.com/api-reference/wss/market",
            "- https://github.com/Polymarket/agent-skills/blob/main/websocket.md",
            "- https://docs.polymarket.com/api-reference/market-data/get-order-book",
            "- https://docs.polymarket.com/v2-migration",
            "- https://docs.polymarket.com/market-data/overview",
            "- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(path: Path, report: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8", newline="\n")
