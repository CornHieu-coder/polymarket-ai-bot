"""Deterministic Markdown rendering for IP-001 summaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


Q8_DELAY_LIMITATION = (
    "probe-observed source-to-processing delay contaminated by local clock offset, "
    "event-loop scheduling, socket/library buffering, backpressure, and synchronous "
    "flush/fsync durable-persistence overhead; not a venue/network latency estimate."
)


def _cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _status_line(question: str, section: Mapping[str, Any], conclusion: str) -> str:
    return f"| {question} | **{section['status']}** | {_cell(conclusion)} |"


def _delay_metrics(q8: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return delay metrics from either the historical or corrective schema."""

    value = q8.get("probe_observed_source_to_processing_delay_ms")
    if isinstance(value, Mapping):
        return value
    value = q8.get("delay_ms_local_clock_dependent")
    if isinstance(value, Mapping):
        return value
    return {}


def _display(value: Any, *, default: str = "not recorded") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _subscription_payload(run: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("subscription_payload", "market_subscription_payload"):
        value = run.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def _subscription_fields(run: Mapping[str, Any]) -> list[Any]:
    for key in ("subscription_fields", "market_subscription_fields"):
        value = run.get(key)
        if isinstance(value, (list, tuple)):
            return list(value)
    return []


def _subscription_setting(run: Mapping[str, Any], name: str) -> str:
    sent_key = f"{name}_field_sent"
    legacy_key = "websocket_level" if name == "level" else name
    if sent_key in run:
        if run[sent_key]:
            payload = _subscription_payload(run)
            if name in payload:
                return f"sent explicitly as `{_cell(payload[name])}`"
            if legacy_key in run:
                return f"sent explicitly as `{_cell(run[legacy_key])}`"
            return "sent explicitly"
        return "omitted; documented default relied upon"
    if legacy_key in run:
        return f"`{_cell(_display(run[legacy_key]))}` (explicit)"
    return "not recorded"


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
    delays = _delay_metrics(q8)
    q8_text = (
        f"Measured {q8['timestamped_events']} timestamped events; "
        f"regressions={q8['source_timestamp_regressions']}, future-source="
        f"{q8['source_after_local_receive']}, local-clock-dependent p50 delay="
        f"{delays.get('p50')} ms."
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
        f"| Initial dump subscription field | {_subscription_setting(run, 'initial_dump')} |",
        f"| WebSocket level subscription field | {_subscription_setting(run, 'level')} |",
        f"| Subscription control | {_cell(_display(run.get('subscription_control')))} |",
        f"| Subscription fields | `{_cell(_json(_subscription_fields(run)))}` |",
        f"| Subscription payload | `{_cell(_json(_subscription_payload(run)))}` |",
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
    if run.get("baseline_run_id") is not None:
        lines.extend(
            [
                "",
                "Corrective-run baseline provenance:",
                "",
                f"- Baseline run ID: `{_cell(run['baseline_run_id'])}`",
                f"- Baseline summary path: `{_cell(_display(run.get('baseline_summary_path')))}`",
                f"- Baseline summary SHA-256: `{_cell(_display(run.get('baseline_summary_sha256')))}`",
            ]
        )
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
            _display(
                q2.get("scope_note"),
                default=(
                    "Reconnect evidence is bounded to the sampled sessions and tokens; "
                    "it is not a universal server guarantee."
                ),
            ),
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
            f"| Best bid/ask mismatches | {q3.get('genuine_mismatches', q3.get('mismatches', 0))} |",
            f"| Direct single-entry mismatches | {q3['direct_single-entry_mismatches']} |",
            f"| Non-zero updates | {q3['nonzero_replacements']} |",
            f"| Zero-size updates | {q3['zero_size_updates']} |",
            f"| Discriminating non-zero updates | {q3['discriminating_nonzero_updates']} |",
            f"| Independently validated non-zero replacements | {q3['independently_validated_nonzero_replacements']} |",
            f"| Discriminating zero deletions | {q3['discriminating_zero_deletes']} |",
            f"| Independently validated zero deletions | {q3['independently_validated_zero_deletes']} |",
            f"| Idempotent replacements/deletions | {q3['idempotent_replacements']} |",
            f"| Superseded before independent validation | {q3.get('superseded_before_validation', 0)} |",
            "",
            q3["validation_note"],
            "",
            f"Side counts: `{_json(q3['side_updates'])}`. Component comparisons: "
            f"`{_json(q3['components'])}`.",
            "",
        ]
    )
    component_statuses = q3.get("component_statuses")
    if isinstance(component_statuses, Mapping):
        lines.extend(
            [
                f"Component conclusions: `{_json(component_statuses)}`.",
                "",
            ]
        )
    empty_side = q3.get("empty_side_boundary_interpretation")
    if isinstance(empty_side, Mapping):
        lines.extend(
            [
                "#### Empty-side boundary observations",
                "",
                f"Bounded interpretation status: **{_display(empty_side.get('status'), default='UNRESOLVED')}**.",
                "",
                f"Observed candidates: **{_display(empty_side.get('observed_candidates'), default='0')}**; "
                f"confirmed candidates: **{_display(empty_side.get('confirmed_candidates'), default='0')}**; "
                f"unresolved candidates: **{_display(empty_side.get('unresolved_candidates'), default='0')}**; "
                f"targeted REST requests lacking alignment: **{_display(empty_side.get('targeted_rest_unaligned'), default='0')}**.",
                "",
                f"Observed candidate kinds: `{_json(empty_side.get('observed_kinds', {}))}`; "
                f"confirmed kinds: `{_json(empty_side.get('confirmed_kinds', {}))}`.",
                "",
                f"Targeted REST diagnostics: `{_json(empty_side.get('targeted_rest_diagnostics', {}))}`.",
                "",
                _display(
                    empty_side.get("scope"),
                    default=(
                        "Any interpretation applies only to aligned observations in this "
                        "bounded run. Numeric 0/1 values are not asserted to be universal "
                        "empty-side protocol sentinels."
                    ),
                ),
                "",
                "Numeric `best_bid=0` or `best_ask=1` observations are not claimed as "
                "universal empty-side protocol semantics.",
                "",
            ]
        )
        candidates = empty_side.get("candidates", [])
        if isinstance(candidates, list) and candidates:
            lines.append("Traceable candidates:")
            lines.append("")
            lines.extend(f"- `{_json(candidate)}`" for candidate in candidates)
            lines.append("")
        if empty_side.get("confirmation_rule") is not None:
            lines.extend(
                [
                    f"Confirmation rule: {_display(empty_side.get('confirmation_rule'))}",
                    "",
                ]
            )
    component_statuses = q3.get("component_statuses")
    if isinstance(component_statuses, Mapping):
        lines.extend(
            [
                f"Q3 component statuses: `{_json(dict(component_statuses))}`.",
                "",
            ]
        )
    superseded_examples = q3.get("superseded_validation_examples", [])
    if isinstance(superseded_examples, list) and superseded_examples:
        lines.append("Updates excluded because they were superseded before validation:")
        lines.append("")
        lines.extend(f"- `{_json(example)}`" for example in superseded_examples)
        lines.append("")

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
    delays = _delay_metrics(q8)
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
            f"| Delay samples | {delays.get('count')} |",
            f"| Minimum delay (ms) | {delays.get('min')} |",
            f"| P50 delay (ms) | {delays.get('p50')} |",
            f"| P95 delay (ms) | {delays.get('p95')} |",
            f"| Maximum delay (ms) | {delays.get('max')} |",
            "",
            f"Quantile method: {_display(delays.get('quantile_method'))}.",
            "",
            Q8_DELAY_LIMITATION,
            "",
            "Negative values are retained, not corrected, and may indicate clock skew. "
            "These measurements do not prove gap-free delivery.",
            "",
        ]
    )
    processing_order = q8.get("processing_order")
    if isinstance(processing_order, Mapping):
        lines.extend(
            [
                f"Recorded processing-order diagnostics: `{_json(processing_order)}`.",
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
            f"- {Q8_DELAY_LIMITATION}",
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
            "- https://github.com/Polymarket/ts-sdk/blob/main/packages/bindings/src/subscriptions/clob.ts",
            "- https://docs.polymarket.com/api-reference/market-data/get-order-book",
            "- https://docs.polymarket.com/v2-migration",
            "- https://docs.polymarket.com/market-data/overview",
            "- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py",
            "",
        ]
    )
    return "\n".join(lines)


def _comparative_run_value(run: Mapping[str, Any], property_name: str) -> str:
    if property_name == "initial_dump":
        return _subscription_setting(run, "initial_dump")
    if property_name == "level":
        return _subscription_setting(run, "level")
    if property_name == "token_ids":
        return f"`{_cell(_json(run.get('token_ids', [])))}`"
    if property_name == "software":
        return f"`{_cell(_json(run.get('software', {})))}`"
    if property_name == "subscription_fields":
        return f"`{_cell(_json(_subscription_fields(run)))}`"
    if property_name == "subscription_payload":
        return f"`{_cell(_json(_subscription_payload(run)))}`"
    value = run.get(property_name)
    if property_name in {
        "run_id",
        "started_at",
        "ended_at",
        "raw_evidence_path",
        "baseline_run_id",
        "baseline_summary_path",
        "baseline_summary_sha256",
        "offline_test_command",
    }:
        return f"`{_cell(_display(value))}`"
    return _cell(_display(value))


def _append_comparative_manifest(
    lines: list[str], label: str, summary: Mapping[str, Any]
) -> None:
    evidence = summary.get("evidence", {})
    if not isinstance(evidence, Mapping):
        evidence = {}
    manifest = evidence.get("manifest", {})
    if not isinstance(manifest, Mapping):
        manifest = {}
    files = manifest.get("files", {})
    if not isinstance(files, Mapping):
        files = {}
    records = evidence.get("records", {})
    if not isinstance(records, Mapping):
        records = {}

    lines.extend(
        [
            f"### {label}",
            "",
            f"Run directory: `{_cell(_display(evidence.get('run_directory'), default=_display(summary.get('run', {}).get('raw_evidence_path') if isinstance(summary.get('run'), Mapping) else None)))}`.",
            "",
            f"Record counts: `{_json(dict(records))}`.",
            "",
            f"Manifest algorithm: `{_cell(_display(manifest.get('algorithm'), default='not recorded'))}`.",
            "",
            "| File | Bytes | SHA-256 |",
            "| --- | ---: | --- |",
        ]
    )
    if not files:
        lines.append("| _No manifest files recorded_ | n/a | n/a |")
    else:
        for name in sorted(files, key=str):
            detail = files[name]
            if not isinstance(detail, Mapping):
                detail = {}
            lines.append(
                f"| `{_cell(name)}` | {_display(detail.get('bytes'), default='n/a')} | "
                f"`{_cell(_display(detail.get('sha256'), default='not recorded'))}` |"
            )
    lines.append("")


def _append_significant_run_evidence(
    lines: list[str], label: str, summary: Mapping[str, Any]
) -> None:
    lines.extend([f"### {label}", ""])
    emitted = False
    evidence = summary.get("evidence", {})
    if isinstance(evidence, Mapping):
        duplicate_count = evidence.get("duplicate_raw_frames", 0)
        if duplicate_count:
            emitted = True
            lines.append(
                f"- Repeated raw-payload hashes retained: **{duplicate_count}**. This "
                "all-frame count alone does not establish duplicate data events."
            )

    q3 = summary.get("q3", {})
    if isinstance(q3, Mapping):
        excluded = q3.get("excluded_change_entries", 0)
        observed = q3.get("observed_change_entries", "not recorded")
        applied = q3.get("applied_change_entries", "not recorded")
        emitted = True
        lines.append(
            f"- Q3 exclusions preserved: **{excluded}** of **{observed}** observed "
            f"price-change entries were excluded; **{applied}** were applied. Conclusions "
            "must not be generalized across the excluded evidence."
        )
        superseded = q3.get("superseded_before_validation")
        if superseded is not None:
            lines.append(
                f"- Q3 updates superseded before independent validation: **{superseded}**."
            )
        superseded_examples = q3.get("superseded_validation_examples", [])
        if isinstance(superseded_examples, list):
            for example in superseded_examples:
                lines.append(f"- Q3 superseded-validation exclusion: `{_json(example)}`")
        interpretation = q3.get("empty_side_boundary_interpretation")
        if isinstance(interpretation, Mapping):
            lines.append(
                "- Q3 bounded empty-side interpretation: "
                f"status **{_display(interpretation.get('status'), default='UNRESOLVED')}**, "
                f"observed candidates **{_display(interpretation.get('observed_candidates'), default='0')}**, "
                f"confirmed candidates **{_display(interpretation.get('confirmed_candidates'), default='0')}**, "
                f"unresolved candidates **{_display(interpretation.get('unresolved_candidates'), default='0')}**."
            )
            diagnostics = interpretation.get("targeted_rest_diagnostics", {})
            if isinstance(diagnostics, Mapping) and diagnostics:
                lines.append(
                    "- Q3 targeted REST diagnostics: "
                    f"`{_json(dict(diagnostics))}`."
                )
            candidates = interpretation.get("candidates", [])
            if isinstance(candidates, list):
                for candidate in candidates:
                    lines.append(f"- Q3 empty-side candidate: `{_json(candidate)}`")

    for index in range(1, 9):
        section = summary.get(f"q{index}", {})
        if not isinstance(section, Mapping):
            continue
        counterexamples = section.get("counterexamples", [])
        if not isinstance(counterexamples, list):
            continue
        for example in counterexamples:
            emitted = True
            lines.append(f"- Q{index} counterexample: `{_json(example)}`")

    errors = summary.get("errors", {})
    if isinstance(errors, Mapping):
        counts = errors.get("counts", {})
        if isinstance(counts, Mapping):
            for code in sorted(counts, key=str):
                emitted = True
                lines.append(
                    f"- Parser/operational error `{_cell(code)}`: **{counts[code]}** occurrence(s)."
                )
        examples = errors.get("examples", [])
        if isinstance(examples, list):
            examples_by_code: dict[str, list[Any]] = {}
            for example in examples:
                code = str(example.get("code", "uncategorized")) if isinstance(example, Mapping) else "uncategorized"
                examples_by_code.setdefault(code, []).append(example)
            for code in sorted(examples_by_code):
                code_examples = examples_by_code[code]
                for example in code_examples[:3]:
                    lines.append(f"- Representative `{_cell(code)}` error: `{_json(example)}`")
                if len(code_examples) > 3:
                    lines.append(
                        f"- `{_cell(code)}`: {len(code_examples) - 3} additional "
                        "traceable instance(s) remain in the evidence summary."
                    )

    if not emitted:
        lines.append("No significant counterexample or exclusion was recorded.")
    lines.append("")


QUESTION_NAMES = (
    "Current event schema",
    "Initial dump/reconnect",
    "Price-level update semantics",
    "Multiple updates per frame",
    "Hash semantics",
    "REST /book comparison",
    "Optional WebSocket book metadata",
    "Timestamp and processing-order behavior",
)

QUESTION_LIMITS = (
    "Field absence is bounded to the observed sample and does not prove that a field "
    "cannot appear.",
    "The result applies only to the named tokens, sessions, subscription payload, and "
    "clean reconnect; it is not a universal reconnect guarantee.",
    "Only applied, non-superseded, eligible updates and explicitly aligned REST evidence "
    "support the label; candidate numeric boundaries are not universal protocol rules.",
    "A zero count is non-observation, not proof that a frame shape cannot occur.",
    "Only the cited first-party algorithm and payloads containing all its required inputs "
    "were eligible; no alternative hash meaning was inferred.",
    "REST and WebSocket are asynchronous; mismatches remain diagnostics unless the "
    "declared stable request window exists.",
    "Presence frequencies apply only to observed full-book frames; absence is not universal.",
    "Receive-order counters describe local probe processing only, do not prove gap-free "
    "delivery, and the delay distribution is not a venue/network latency estimate.",
)

QUESTION_METHOD_DELTAS = (
    "The schema inventory method is unchanged; the corrective run supplies an independent "
    "current-production sample.",
    "The historical run explicitly sent optional controls. The corrective run sends only "
    "`assets_ids` and `type`, relying on currently documented defaults.",
    "The corrective analyzer separates locally empty-side numeric 0/1 candidates from "
    "genuine mismatches, requires targeted aligned REST confirmation, and prevents "
    "superseded updates from receiving duplicate validation credit.",
    "The corrective analyzer distinguishes multiple entries for different assets from "
    "same-asset ordering ambiguity.",
    "The same official hash algorithm and no-fabricated-input rule apply to both runs.",
    "The stable-window rule is retained; corrective candidate-targeted GETs add diagnostics "
    "without turning asynchronous mismatches into WebSocket failures.",
    "The frequency method is unchanged; differences are sample observations only.",
    "The numeric provenance is retained, but the corrective report labels it as contaminated "
    "source-to-processing delay and adds explicit ingest/monotonic processing-order checks.",
)


def _question_evidence(summary: Mapping[str, Any], index: int) -> str:
    section = summary.get(f"q{index}", {})
    if not isinstance(section, Mapping):
        return "Question evidence was not recorded in a mapping-valued summary section."
    if index == 1:
        schema = section.get("event_schema", {})
        if not isinstance(schema, Mapping):
            schema = {}
        logical_events = sum(
            int(detail.get("events", 0))
            for detail in schema.values()
            if isinstance(detail, Mapping)
        )
        ordering = section.get("candidate_ordering_fields", {})
        ordering_count = (
            sum(int(value) for value in ordering.values())
            if isinstance(ordering, Mapping)
            else 0
        )
        return (
            f"{logical_events} logical events across {len(schema)} event types; "
            f"{ordering_count} candidate ordering-field presences."
        )
    if index == 2:
        sessions = section.get("sessions", {})
        if not isinstance(sessions, Mapping):
            sessions = {}
        token_sessions = 0
        book_first = 0
        later_delta = 0
        for session in sessions.values():
            if not isinstance(session, Mapping):
                continue
            tokens = session.get("tokens", {})
            if not isinstance(tokens, Mapping):
                continue
            for result in tokens.values():
                if not isinstance(result, Mapping):
                    continue
                token_sessions += 1
                book_first += result.get("first_state_event") == "book"
                later_delta += int(result.get("price_change_entries", 0)) > 0
        return (
            f"{len(sessions)} sessions / {token_sessions} token-session observations; "
            f"book first in {book_first}; later delta exercised in {later_delta}; "
            f"clean controlled reconnect={_display(section.get('clean_controlled_reconnect'))}."
        )
    if index == 3:
        interpretation = section.get("empty_side_boundary_interpretation", {})
        if not isinstance(interpretation, Mapping):
            interpretation = {}
        return (
            f"observed/applied/excluded changes="
            f"{section.get('observed_change_entries', 0)}/"
            f"{section.get('applied_change_entries', 0)}/"
            f"{section.get('excluded_change_entries', 0)}; BBO exact/genuine mismatch="
            f"{section.get('exact_matches', 0)}/"
            f"{section.get('genuine_mismatches', section.get('mismatches', 0))}; "
            f"validated non-zero/zero="
            f"{section.get('independently_validated_nonzero_replacements', 0)}/"
            f"{section.get('independently_validated_zero_deletes', 0)}; empty-side "
            f"candidates confirmed/observed="
            f"{interpretation.get('confirmed_candidates', 0)}/"
            f"{interpretation.get('observed_candidates', 0)}; superseded before "
            f"validation={section.get('superseded_before_validation', 0)}."
        )
    if index == 4:
        return (
            f"frames={section.get('price_change_frames', 0)}; multi-entry="
            f"{section.get('multi_entry_frames', 0)}; same-asset="
            f"{section.get('multi_same_asset_frames', 0)}; repeated key="
            f"{section.get('duplicate_asset_side_price_frames', 0)}; order-sensitive="
            f"{section.get('order_sensitive_frames', 0)}."
        )
    if index == 5:
        book = section.get("websocket_book", {})
        change = section.get("price_change_post_update", {})
        rest = section.get("rest_book_diagnostic", {})
        book = book if isinstance(book, Mapping) else {}
        change = change if isinstance(change, Mapping) else {}
        rest = rest if isinstance(rest, Mapping) else {}
        return (
            f"WS book matches/attempts={book.get('matches', 0)}/"
            f"{book.get('attempts', 0)}; post-change={change.get('matches', 0)}/"
            f"{change.get('attempts', 0)}; REST diagnostic={rest.get('matches', 0)}/"
            f"{rest.get('attempts', 0)}."
        )
    if index == 6:
        return (
            f"successful/total REST responses={section.get('successful_responses', 0)}/"
            f"{section.get('requests', 0)}; stable aligned exact/mismatch="
            f"{section.get('aligned_matches', 0)}/"
            f"{section.get('aligned_mismatches', 0)}; unaligned diagnostics="
            f"{section.get('unaligned_diagnostics', 0)}."
        )
    if index == 7:
        return (
            f"{section.get('websocket_book_events', 0)} full-book events; optional "
            f"field presence={_json(section.get('optional_field_presence', {}))}."
        )
    processing = section.get("processing_order", {})
    if not isinstance(processing, Mapping):
        processing = {}
    delays = _delay_metrics(section)
    return (
        f"timestamped events={section.get('timestamped_events', 0)}; source regressions="
        f"{section.get('source_timestamp_regressions', 0)}; same-timestamp groups="
        f"{len(section.get('same_timestamp_groups', []))}; future-source="
        f"{section.get('source_after_local_receive', 0)}; ingest/monotonic receive-order "
        f"regressions={processing.get('ingest_sequence_regressions', 'not recorded')}/"
        f"{processing.get('received_monotonic_ns_regressions', 'not recorded')}; "
        f"delay samples={delays.get('count', 0)}."
    )


def render_comparative_report(
    original: Mapping[str, Any], corrective: Mapping[str, Any]
) -> str:
    """Render a deterministic report that preserves two independently captured runs."""

    original_run = original.get("run", {})
    corrective_run = corrective.get("run", {})
    if not isinstance(original_run, Mapping) or not isinstance(corrective_run, Mapping):
        raise TypeError("both summaries must contain a mapping-valued run section")

    lines: list[str] = [
        "# Market Feed Contract Probe: Historical and Corrective Evidence",
        "",
        "Status: **comparative bounded read-only research report**",
        "",
        "The historical run remains immutable evidence. The corrective run is additional "
        "evidence collected under a revised probe; it does not overwrite the historical "
        "run, its exclusions, counterexamples, status labels, or manifest.",
        "",
        "## Run identities and provenance",
        "",
        "| Property | Historical/original run | Corrective run |",
        "| --- | --- | --- |",
    ]
    run_properties = [
        ("Run ID", "run_id"),
        ("UTC start", "started_at"),
        ("UTC end", "ended_at"),
        ("Requested duration (seconds)", "requested_duration_seconds"),
        ("Actual duration (seconds)", "actual_duration_seconds"),
        ("REST interval (seconds)", "rest_interval_seconds"),
        ("Controlled reconnects", "controlled_reconnects"),
        ("Sample/token IDs", "token_ids"),
        ("Sample provenance", "sample_note"),
        ("Initial dump subscription field", "initial_dump"),
        ("WebSocket level subscription field", "level"),
        ("Subscription control", "subscription_control"),
        ("Subscription fields", "subscription_fields"),
        ("Subscription payload", "subscription_payload"),
        ("Raw evidence path", "raw_evidence_path"),
        ("Raw capture boundary", "raw_boundary"),
        ("Software provenance", "software"),
        ("Offline test command", "offline_test_command"),
        ("Offline test result", "offline_test_result"),
        ("Baseline run ID", "baseline_run_id"),
        ("Baseline summary path", "baseline_summary_path"),
        ("Baseline summary SHA-256", "baseline_summary_sha256"),
    ]
    for label, key in run_properties:
        lines.append(
            f"| {label} | {_comparative_run_value(original_run, key)} | "
            f"{_comparative_run_value(corrective_run, key)} |"
        )

    lines.extend(
        [
            "",
            "Both summaries are reported with their own provenance. A baseline pointer in "
            "the corrective run is a lineage reference, not permission to merge or replace "
            "the historical evidence.",
            "",
            "## Raw evidence manifests",
            "",
        ]
    )
    _append_comparative_manifest(lines, "Historical/original run", original)
    _append_comparative_manifest(lines, "Corrective run", corrective)

    lines.extend(
        [
            "## Q1-Q8 status delta",
            "",
            "Statuses are bounded to each run's eligible evidence. A change in status is "
            "not a retroactive rewrite of the historical result.",
            "",
            "| Question | Historical/original status | Corrective status | Delta |",
            "| --- | --- | --- | --- |",
        ]
    )
    for index in range(1, 9):
        original_section = original.get(f"q{index}", {})
        corrective_section = corrective.get(f"q{index}", {})
        original_status = (
            _display(original_section.get("status"), default="UNRESOLVED")
            if isinstance(original_section, Mapping)
            else "UNRESOLVED"
        )
        corrective_status = (
            _display(corrective_section.get("status"), default="UNRESOLVED")
            if isinstance(corrective_section, Mapping)
            else "UNRESOLVED"
        )
        delta = (
            "UNCHANGED"
            if original_status == corrective_status
            else f"{original_status} -> {corrective_status}"
        )
        lines.append(
            f"| Q{index} | **{_cell(original_status)}** | "
            f"**{_cell(corrective_status)}** | `{_cell(delta)}` |"
        )

    lines.extend(
        [
            "",
            "## Per-question evidence, differences, and applicability",
            "",
            "Counterexamples and exclusions are reproduced in the later significant-"
            "evidence section with raw ingest-sequence lineage where available.",
            "",
        ]
    )
    for index, question_name in enumerate(QUESTION_NAMES, start=1):
        original_section = original.get(f"q{index}", {})
        corrective_section = corrective.get(f"q{index}", {})
        original_status = (
            _display(original_section.get("status"), default="UNRESOLVED")
            if isinstance(original_section, Mapping)
            else "UNRESOLVED"
        )
        corrective_status = (
            _display(corrective_section.get("status"), default="UNRESOLVED")
            if isinstance(corrective_section, Mapping)
            else "UNRESOLVED"
        )
        lines.extend(
            [
                f"### Q{index} — {question_name}",
                "",
                f"- Historical/original: **{original_status}** — "
                f"{_question_evidence(original, index)}",
                f"- Corrective: **{corrective_status}** — "
                f"{_question_evidence(corrective, index)}",
                f"- Difference/method: {QUESTION_METHOD_DELTAS[index - 1]}",
                f"- Applicability limit: {QUESTION_LIMITS[index - 1]}",
                "",
            ]
        )

    lines.extend(
        [
            "",
            "## Evidence classification",
            "",
            "### Established",
            "",
            "- First-party material establishes that the public Market WebSocket carries "
            "full `book` snapshots and `price_change` level updates, and explicitly "
            "describes zero-size changes as level removals and non-zero `size` as the new "
            "aggregate size.",
            "- The current first-party schema documents `initial_dump` and `level` as "
            "optional subscription fields with defaults. Current official TypeScript "
            "bindings separately document an empty string as the raw absent-value form "
            "for optional best bid/ask decimals.",
            "- Each run's identity, software provenance, raw-evidence path, record counts, "
            "and manifest digests establish what these named artifacts contain; they do "
            "not establish universal venue behavior.",
            "",
            "### Suggested",
            "",
            "- Numeric `best_ask=1` or `best_bid=0` coinciding with a locally empty side is "
            "suggested by first-party examples and bounded live observations, but no "
            "first-party source found explicitly defines those numbers as empty-side "
            "sentinels.",
            "- A high best-price match rate is supportive but cannot independently establish "
            "aggregate replacement semantics or completeness.",
            "",
            "### Project-derived",
            "",
            "- Stable-window REST alignment, discriminating-update eligibility, exclusion "
            "rules, and the historical/corrective comparison are project-defined methods.",
            "- Any interpretation of observed `best_ask=1` or `best_bid=0` as an empty side "
            "is limited to directly aligned candidates in the named run. This report does "
            "not claim that numeric 0/1 values are universal empty-side protocol sentinels.",
            "",
            "## Current documentation discrepancy and subscription control",
            "",
            "The current first-party Market WebSocket documentation describes `initial_dump` "
            "and `level` as optional subscription fields with documented defaults. The "
            "historical run sent both fields explicitly; the corrective configuration records "
            "whether it omitted them and relied on those defaults. This differs from the "
            "historical report's premise that these controls were undocumented. It is a "
            "documentation-version discrepancy and experimental control, not evidence that "
            "the same omission semantics applied universally or at every historical point.",
            "",
            "Current reference: https://docs.polymarket.com/api-reference/wss/market",
            "",
            "## Q8 measurement boundary",
            "",
            Q8_DELAY_LIMITATION,
            "",
            "The two runs may still be compared for source-timestamp ordering diagnostics "
            "and for their separately recorded contaminated processing-delay distributions; "
            "neither distribution estimates venue or network latency.",
            "",
            "| Metric | Historical/original run | Corrective run |",
            "| --- | ---: | ---: |",
        ]
    )
    original_delays = _delay_metrics(
        original.get("q8", {}) if isinstance(original.get("q8"), Mapping) else {}
    )
    corrective_delays = _delay_metrics(
        corrective.get("q8", {}) if isinstance(corrective.get("q8"), Mapping) else {}
    )
    for label, key in [
        ("Samples", "count"),
        ("Minimum (ms)", "min"),
        ("P50 (ms)", "p50"),
        ("P95 (ms)", "p95"),
        ("Maximum (ms)", "max"),
    ]:
        lines.append(
            f"| {label} | {_cell(_display(original_delays.get(key)))} | "
            f"{_cell(_display(corrective_delays.get(key)))} |"
        )

    lines.extend(["", "## Significant counterexamples, exclusions, and errors", ""])
    _append_significant_run_evidence(lines, "Historical/original run", original)
    _append_significant_run_evidence(lines, "Corrective run", corrective)

    lines.extend(
        [
            "## Scope limitations",
            "",
            "- Both runs are bounded samples; non-observation does not establish impossibility.",
            "- Neither a short successful connection nor matching top-of-book values proves "
            "gap-free delivery or full-depth correctness.",
            "- Historical exclusions remain exclusions and cannot be converted into eligible "
            "evidence by the corrective run.",
            "- No result here is a trading-performance result.",
            "",
            "## Safety boundary",
            "",
            "Both runs used only the fixed public Market WebSocket, `PING` heartbeats, "
            "and public `GET /book`. No authenticated endpoint, user WebSocket, API "
            "credential, wallet, signing key, order construction, simulation, placement, "
            "cancellation, live trading, or paper trading was used.",
            "",
            "## Deviations from IP-001",
            "",
            "None. The corrective work remains a bounded probe using the packet-authorized "
            "explicit-token mode. It does not implement a production collector, replay "
            "engine, or any code under `src/`.",
            "",
            "## First-party references applied",
            "",
            "- https://docs.polymarket.com/api-reference/wss/market",
            "- https://github.com/Polymarket/agent-skills/blob/main/websocket.md",
            "- https://github.com/Polymarket/ts-sdk/blob/main/packages/bindings/src/subscriptions/clob.ts",
            "- https://docs.polymarket.com/api-reference/market-data/get-order-book",
            "- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(path: Path, report: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8", newline="\n")
