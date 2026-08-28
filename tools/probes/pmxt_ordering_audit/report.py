"""Deterministic Markdown report rendering for IP-002."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _pct(value: Any) -> str:
    if value is None:
        return "not defined"
    return f"{float(value) * 100:.6f}%"


def _value(value: Any) -> str:
    return "not observed" if value is None else str(value)


def _dist(value: Mapping[str, Any], unit: str = "") -> str:
    suffix = f" {unit}" if unit else ""
    return (
        f"n={value.get('count', 0)}, min={_value(value.get('min'))}{suffix}, "
        f"p50={_value(value.get('p50'))}{suffix}, "
        f"p90={_value(value.get('p90'))}{suffix}, "
        f"p95={_value(value.get('p95'))}{suffix}, "
        f"p99={_value(value.get('p99'))}{suffix}, "
        f"max={_value(value.get('max'))}{suffix}"
    )


def _sample_label(sample: Mapping[str, Any]) -> str:
    return str(sample["actual_hour"])


def _counterexample_lines(
    samples: Sequence[Mapping[str, Any]], question: str, key: str
) -> list[str]:
    lines: list[str] = []
    for sample in samples:
        examples = sample[question].get(key, [])
        for example in examples:
            lines.append(f"- `{_sample_label(sample)}`: `{_json(example)}`")
    if not lines:
        lines.append("- None observed in the audited samples.")
    return lines


def render_report(summary: Mapping[str, Any], *, summary_sha256: str) -> str:
    samples = summary["samples"]
    pooled = summary["pooled"]
    run = summary["run"]
    lines: list[str] = [
        "# pmxt Ordering Ambiguity Audit",
        "",
        f"Status: **{summary['status']}**",
        "",
        "This is the bounded offline research audit authorized by IP-002. It is not a "
        "production replay engine, and its candidate fail-closed policy is not an accepted "
        "architecture decision.",
        "",
        "## Method boundary",
        "",
        "Archive-availability groups are exactly `(market, asset_id, timestamp_received)`. "
        "Members are classified symbolically as an unordered set. Parquet row order and "
        "Polymarket source timestamp are used only for A6 diagnostics and never resolve "
        "A3, A4, or A5 ambiguity.",
        "",
        "Each audited hour begins `UNINITIALIZED`; state becomes `VALID` only at an accepted "
        "in-file full book, becomes `INVALID` after a proven ambiguous/unresolved L2 group, "
        "and recovers only at the next accepted in-file full book. Tick and last-trade "
        "ordering do not invalidate L2.",
        "",
        "## Run provenance",
        "",
        f"- Audit start/end: `{run['started_at']}` / `{run['ended_at']}`.",
        f"- Audit code commit: `{run['runtime']['git']['commit']}` on branch "
        f"`{run['runtime']['git']['branch']}`; dirty at analysis: "
        f"`{str(run['runtime']['git']['dirty']).lower()}`.",
        f"- Runtime: Python `{run['runtime']['python']}`, DuckDB "
        f"`{run['runtime']['duckdb']}`.",
        f"- Offline test command: `{run['offline_test_command']}`.",
        f"- Offline test result before real-data analysis: **{run['offline_test_result']}**.",
        f"- Ignored machine summary: `{run['summary_path']}`; SHA-256 "
        f"`{summary_sha256}`.",
        "",
        "## Sample selection and immutable file provenance",
        "",
        "The predeclared hours were `2026-05-01T12`, `2026-06-15T12`, and "
        "`2026-08-01T12`. Fallback, if needed, was restricted mechanically to the first "
        "later valid hour on the same UTC date.",
        "",
        "| Requested | Actual object | Fallback | Bytes | Rows | Row groups | SHA-256 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    recovery = summary.get("recovery")
    if recovery:
        original = recovery["original_ip_002"]
        pilot = recovery["ip_002r_pilot"]
        august = recovery["august_recovery"]
        validation = recovery["june_equivalence"]
        insertion = lines.index("## Run provenance")
        lines[insertion:insertion] = [
            "## Execution history and recovery provenance",
            "",
            "The evidence was produced in distinct execution stages; later recovery "
            "does not rewrite the failed first attempt.",
            "",
            f"- Original IP-002 implementation commit: "
            f"`{original['implementation_commit']}`. Its preserved atomic result is "
            f"**{original['status']}**, SHA-256 `{original['result_sha256']}`.",
            "- The original engine completed June `2026-06-15T12`; May and August "
            "failed with the preserved 25 GiB DuckDB out-of-memory errors. Those "
            "failures are execution-feasibility evidence, not ordering conclusions.",
            f"- IP-002R 32-shard feasibility pilot: **{pilot['proceed_gate']['status']}**; "
            f"mechanically largest shard `{pilot['largest_shard_id']}` had "
            f"{pilot['largest_shard_row_count']} rows and its checkpoint file SHA-256 "
            f"is `{pilot['checkpoint_file_sha256']}`.",
            f"- August recovery: **{august['status']}** from "
            f"{august['checkpoint_count']} immutable checkpoints; integrity "
            f"**{august['checkpoint_integrity']}** and reducer-order independence "
            f"**{august['reducer_order_independence']}**.",
            f"- August active partition-plus-analysis runtime: "
            f"**{float(august['active_total_august_seconds']):.3f} seconds**; peak RSS "
            f"**{august['peak_rss_bytes']} bytes**; final ignored recovery storage "
            f"**{august['final_recovery_storage_bytes']} bytes**; maximum measured "
            f"DuckDB-temp growth **{august['peak_duckdb_temp_growth_bytes']} bytes**.",
            f"- June cross-engine validation: **{validation['status']}** on deterministic "
            f"shard `{validation['validation_manifest']['selected_shard_id']}` of "
            f"{validation['validation_manifest']['shard_count']} with "
            f"{validation['validation_manifest']['row_count']} rows. Reference and "
            f"recovery A1-A8 SHA-256 are both "
            f"`{validation['reference_a1_a8_sha256']}`; the full June hour was not rerun.",
            f"- Recovery checkpoint code commits: "
            f"`{_json(august['recovery_code_git_shas'])}`. Their recovery-module Git "
            f"blob identities are `{_json(august['recovery_module_blob_shas'])}`.",
            "",
            "The next eight sections present raw per-sample A1-A8 observations before "
            "the descriptive A9 interpretation.",
            "",
        ]
    for sample in samples:
        provenance = sample["provenance"]
        lines.append(
            f"| `{sample['requested_hour']}` | `{sample['actual_object_key']}` | "
            f"`{str(sample['fallback_applied']).lower()}` | {provenance['byte_length']} | "
            f"{provenance['row_count']} | {provenance['row_group_count']} | "
            f"`{provenance['sha256']}` |"
        )
    lines.extend(["", "### Selection substitutions and failures", ""])
    selection_failures = 0
    for sample in summary.get("download", {}).get("samples", []):
        for attempt in sample.get("attempts", []):
            if attempt.get("parquet_validation") == "VALID":
                continue
            selection_failures += 1
            reason = attempt.get(
                "parquet_validation_reason",
                attempt.get("reason", attempt.get("outcome", "unknown failure")),
            )
            lines.append(
                f"- Requested `{sample['requested_hour']}` candidate "
                f"`{attempt['object_key']}`: `{attempt.get('outcome')}`; "
                f"HTTP `{attempt.get('http_status')}`; reason `{reason}`."
            )
    for failure in summary.get("failures", []):
        lines.append(f"- Analysis/selection failure: `{_json(failure)}`")
    if selection_failures == 0 and not summary.get("failures"):
        lines.append("- None. All three requested objects were valid; no fallback was used.")
    lines.extend(
        [
            "",
            "Every download attempt, HTTP status, ETag/Last-Modified/Content-Length when "
            "available, UTC start/end, URL, and validation outcome is preserved in the "
            "ignored `download-provenance.json` and repeated in the machine summary.",
            "",
            "## A1 — Actual schema and timestamp structure",
            "",
            "| Hour | Schema | Rows | Distinct receive timestamps | Rows/receive timestamp | Receive range | Source range |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for sample in samples:
        a1 = sample["a1"]
        lines.append(
            f"| `{_sample_label(sample)}` | **{a1['status']}** | {a1['row_count']} | "
            f"{a1['distinct_timestamp_received']} | {_dist(a1['rows_per_timestamp_received'])} | "
            f"`{a1['timestamp_received_min']}` to `{a1['timestamp_received_max']}` | "
            f"`{a1['source_timestamp_min']}` to `{a1['source_timestamp_max']}` |"
        )
    for sample in samples:
        a1 = sample["a1"]
        lines.extend(
            [
                "",
                f"### {_sample_label(sample)} schema/plateau detail",
                "",
                f"- Event counts: `{_json(a1['event_type_counts'])}`.",
                f"- Availability-group size buckets: "
                f"`{_json(a1['availability_group_size_buckets'])}`.",
                f"- Null required fields: `{_json(a1['null_required_fields'])}`; receive/source "
                f"timestamps outside object hour: "
                f"`{a1['timestamp_received_outside_object_hour']}` / "
                f"`{a1['source_timestamp_outside_object_hour']}`.",
                f"- Columns: `{_json(a1['schema']['columns'])}`.",
                f"- Diagnostic interpretation: {a1['plateau_diagnostic']}",
            ]
        )
    lines.extend(
        [
            "",
            "## A2 — Tied state-changing groups",
            "",
            "| Hour | All groups | Groups with >1 L2 row | Rate | L2 rows in those groups | All rows in those groups |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for sample in samples:
        a2 = sample["a2"]
        lines.append(
            f"| `{_sample_label(sample)}` | {a2['total_archive_availability_groups']} | "
            f"{a2['groups_with_multiple_l2_rows']} | {_pct(a2['multiple_l2_group_rate'])} | "
            f"{a2['l2_rows_in_multiple_l2_groups']} | {a2['all_rows_in_multiple_l2_groups']} |"
        )
        lines.append(
            f"\nComposition distribution for `{_sample_label(sample)}`: "
            f"`{_json(a2['composition_distribution'])}`."
        )
    lines.extend(
        [
            "",
            "## A3 — Pure price-change ambiguity",
            "",
            "| Hour | Pure tied groups | Distinct keys | Repeated/idempotent | Repeated/conflicting | Ambiguity rate | Duplicate rows | Malformed |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for sample in samples:
        a3 = sample["a3"]
        lines.append(
            f"| `{_sample_label(sample)}` | {a3['pure_price_change_tied_groups']} | "
            f"{a3['all_distinct_price_level_keys']} | "
            f"{a3['repeated_keys_one_unique_size']} | "
            f"{a3['repeated_keys_multiple_sizes']} | {_pct(a3['order_ambiguous_rate'])} | "
            f"{a3['exact_duplicate_rows_beyond_first']} | {a3['malformed_groups']} |"
        )
    lines.extend(
        [
            "",
            "Representative order-sensitive repeated-key counterexamples:",
            "",
            *_counterexample_lines(samples, "a3", "representative_counterexamples"),
            "",
            "## A4 — Snapshot interactions",
            "",
            "| Hour | Book groups | Multi-book | Multi-book identical | Multi-book differing/unresolved | Book+delta | Invariant | Ambiguous | Unresolved |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for sample in samples:
        a4 = sample["a4"]
        lines.append(
            f"| `{_sample_label(sample)}` | {a4['groups_containing_book']} | "
            f"{a4['groups_with_multiple_books']} | {a4['multiple_book_identical']} | "
            f"{a4['multiple_book_differing_or_unresolved']} | "
            f"{a4['book_plus_price_change_groups']} | "
            f"{a4['book_plus_price_change_invariant']} | "
            f"{a4['book_plus_price_change_ambiguous']} | "
            f"{a4['book_plus_price_change_unresolved']} |"
        )
    lines.extend(
        [
            "",
            "Representative snapshot counterexamples:",
            "",
            *_counterexample_lines(samples, "a4", "representative_counterexamples"),
            "",
            "## A5 — Tick metadata",
            "",
            "| Hour | Tied tick groups | Single deterministic | Duplicate deterministic | Distinct ambiguous | Unresolved | H2 metadata blockers |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for sample in samples:
        a5 = sample["a5"]
        lines.append(
            f"| `{_sample_label(sample)}` | {a5['tied_groups_containing_tick_transition']} | "
            f"{a5['single_transition_deterministic']} | "
            f"{a5['duplicate_identical_transitions_deterministic']} | "
            f"{a5['distinct_transitions_ambiguous']} | {a5['malformed_or_unresolved']} | "
            f"{a5['independent_h2_metadata_blocking_groups']} |"
        )
    lines.extend(
        [
            "",
            "Tick ambiguity is an independent execution-metadata diagnostic and was not "
            "used to invalidate L2.",
            "",
            "## A6 — Source-time diagnostics inside availability ties",
            "",
            "| Hour | Tied groups | All source times equal | Source times differ | Span min/max ms | Published-order regression rows/groups |",
            "| --- | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for sample in samples:
        a6 = sample["a6"]
        span = a6["source_time_span_ms"]
        lines.append(
            f"| `{_sample_label(sample)}` | {a6['tied_groups']} | "
            f"{a6['all_source_timestamps_equal']} | {a6['different_source_timestamps']} | "
            f"{_value(span['min'])} / {_value(span['max'])} | "
            f"{a6['published_row_order_source_regression_rows']} / "
            f"{a6['published_row_order_source_regression_groups']} |"
        )
    lines.extend(
        [
            "",
            "These values are diagnostic only. A different source timestamp or published "
            "row position did not make an otherwise order-sensitive group invariant.",
            "",
            "## A7 — Candidate fail-closed coverage cost",
            "",
            "| Hour | Invalidations | Recovered | Right-censored | Recovery time | Initialized VALID share | Processed rows | Excluded after ambiguity | Excluded share |",
            "| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for sample in samples:
        a7 = sample["a7"]
        coverage = a7["state_changing_row_coverage"]
        lines.append(
            f"| `{_sample_label(sample)}` | {a7['ambiguity_triggered_invalidations']} | "
            f"{a7['recovered_invalidations']} | {a7['right_censored_invalidations']} | "
            f"{_dist(a7['recovery_time_ms'], 'ms')} | "
            f"{_pct(a7['aggregate_asset_time']['initialized_valid_share'])} | "
            f"{coverage['processed']} | {coverage['excluded_after_ambiguity']} | "
            f"{_pct(coverage['excluded_after_ambiguity_share'])} |"
        )
        worst = sorted(
            a7["per_asset_wall_clock"],
            key=lambda item: (-item["invalid_ms"], item["asset_id"]),
        )[:20]
        lines.extend(
            [
                "",
                f"`{_sample_label(sample)}` subsequent groups to recovery: "
                f"{_dist(a7['subsequent_archive_groups_to_recovery'])}.",
                f"Complete per-asset wall-clock rows: "
                f"**{len(a7['per_asset_wall_clock'])}**, preserved in the ignored machine "
                f"summary. Twenty largest invalid-duration rows: `{_json(worst)}`.",
            ]
        )
    lines.extend(
        [
            "",
            "Representative invalidations/recoveries:",
            "",
            *_counterexample_lines(samples, "a7", "representative_invalidations"),
            "",
            "## A8 — Fixed-cadence diagnostics",
            "",
            "Only UTC-aligned points at or after each asset's first accepted in-file book "
            "are eligible. No strategy, PnL, fill, or cadence choice was simulated.",
            "",
            "| Hour | 1-minute valid/eligible | 1-minute fraction | 5-minute valid/eligible | 5-minute fraction | 30-minute valid/eligible | 30-minute fraction |",
            "| --- | --- | ---: | --- | ---: | --- | ---: |",
        ]
    )
    for sample in samples:
        cadence = sample["a8"]["utc_aligned_cadences_minutes"]
        cells = []
        for key in ("1", "5", "30"):
            item = cadence[key]
            cells.extend(
                [
                    f"{item['valid_decision_points']}/{item['decision_points_after_first_book']}",
                    _pct(item["valid_fraction"]),
                ]
            )
        lines.append(f"| `{_sample_label(sample)}` | " + " | ".join(cells) + " |")
    lines.extend(
        [
            "",
            "## Pooled metrics",
            "",
            f"- Rows / archive groups: **{pooled['a1']['row_count']}** / "
            f"**{pooled['a2']['total_archive_availability_groups']}**.",
            f"- Rows per receive timestamp: "
            f"{_dist(pooled['a1']['rows_per_timestamp_received'])}.",
            f"- Availability-group size buckets: "
            f"`{_json(pooled['a1']['availability_group_size_buckets'])}`.",
            f"- Event counts: `{_json(pooled['a1']['event_type_counts'])}`; schema "
            f"statuses: `{_json(pooled['a1']['schema_statuses'])}`.",
            f"- Groups with multiple L2 rows: "
            f"**{pooled['a2']['groups_with_multiple_l2_rows']}** "
            f"({_pct(pooled['a2']['multiple_l2_group_rate'])}).",
            f"- L2 rows in multiple-L2 groups: "
            f"**{pooled['a2']['l2_rows_in_multiple_l2_groups']}**; composition: "
            f"`{_json(pooled['a2']['composition_distribution'])}`.",
            f"- Pure price-change tied groups with conflicting repeated replacements: "
            f"**{pooled['a3']['repeated_keys_multiple_sizes']}** / "
            f"**{pooled['a3']['pure_price_change_tied_groups']}** "
            f"({_pct(pooled['a3']['order_ambiguous_rate'])}).",
            f"- Remaining pure price-change classifications: distinct keys "
            f"**{pooled['a3']['all_distinct_price_level_keys']}**, repeated/idempotent "
            f"**{pooled['a3']['repeated_keys_one_unique_size']}**, exact duplicates beyond "
            f"first **{pooled['a3']['exact_duplicate_rows_beyond_first']}**, malformed "
            f"**{pooled['a3']['malformed_groups']}**.",
            f"- Snapshot interaction metrics: `{_json(pooled['a4'])}`.",
            f"- Tick-metadata metrics: `{_json(pooled['a5'])}`.",
            f"- Source-time diagnostics: tied groups "
            f"**{pooled['a6']['tied_groups']}**, equal/different source timestamps "
            f"**{pooled['a6']['all_source_timestamps_equal']}** / "
            f"**{pooled['a6']['different_source_timestamps']}**, source-span "
            f"min/max **{_value(pooled['a6']['source_time_span_ms']['min'])}** / "
            f"**{_value(pooled['a6']['source_time_span_ms']['max'])}** ms, published-row "
            f"regression rows/groups "
            f"**{pooled['a6']['published_row_order_source_regression_rows']}** / "
            f"**{pooled['a6']['published_row_order_source_regression_groups']}**; "
            f"distinct source timestamps/group "
            f"{_dist(pooled['a6']['distinct_source_timestamps_per_group'])}.",
            f"- Invalidations / recovered / right-censored: "
            f"**{pooled['a7']['ambiguity_triggered_invalidations']}** / "
            f"**{pooled['a7']['recovered_invalidations']}** / "
            f"**{pooled['a7']['right_censored_invalidations']}**.",
            f"- Recovery time: {_dist(pooled['a7']['recovery_time_ms'], 'ms')}.",
            f"- Subsequent groups to recovery: "
            f"{_dist(pooled['a7']['subsequent_archive_groups_to_recovery'])}.",
            f"- Initialized asset-time VALID: "
            f"**{_pct(pooled['a7']['aggregate_asset_time']['initialized_valid_share'])}**.",
            f"- Rows excluded after ambiguity: "
            f"**{pooled['a7']['state_changing_row_coverage']['excluded_after_ambiguity']}** "
            f"({_pct(pooled['a7']['state_changing_row_coverage']['excluded_after_ambiguity_share'])}).",
            f"- Rows excluded while UNINITIALIZED: "
            f"**{pooled['a7']['state_changing_row_coverage']['excluded_while_uninitialized']}**.",
            "",
            "Pooled cadence diagnostics: "
            f"`{_json(pooled['a8']['utc_aligned_cadences_minutes'])}`.",
            "",
            "## Schema and timestamp-period differences",
            "",
        ]
    )
    for item in pooled["a1"]["timestamp_received_ranges"]:
        lines.append(
            f"- `{item['actual_hour']}`: receive range `{item['min']}` to `{item['max']}`; "
            f"rows per receive timestamp {_dist(item['rows_per_timestamp_received'])}."
        )
    lines.extend(
        [
            "",
            f"Schema statuses in early/middle/later order: "
            f"`{_json(pooled['a1']['schema_statuses'])}`. Differences are observations "
            "of the named files, not inferred collector-version history.",
            "",
            "## A9 — Feasibility conclusion",
            "",
            f"Classification: **{pooled['a9']['classification']}**.",
            "",
            pooled["a9"]["rationale"],
            "",
            pooled["a9"]["interpretation"],
            "",
            "## Evidence classification",
            "",
            "### Established by external evidence",
            "",
            "- pmxt publishes hourly Parquet with millisecond receive/source timestamps and "
            "no Polymarket receive-sequence/frame-index tie-breaker in the documented schema.",
            "- The reviewed pmxt collector explodes multi-change frames, and pmxt tooling has "
            "observed repeated same-key changes inside an original message.",
            "- Arbitrary simultaneous-event ordering can change discrete-event simulation state.",
            "",
            "### Audit observations",
            "",
            "- File hashes, schemas, timestamp plateaus, ambiguity rates, recovery distributions, "
            "coverage loss, and cadence availability above apply to the named samples only.",
            "",
            "### Project-derived interpretation",
            "",
            "- The archive-availability key, symbolic invariance rules, fail-closed audit state "
            "machine, and A9 qualitative label are project methods under research review.",
            "- No architecture or production replay contract is frozen by this report.",
            "",
            "## Remaining unresolved questions",
            "",
            "- Whether the candidate conservative grouping should become the final historical "
            "tie policy remains a research/ADR decision.",
            "- Historical pipeline-version attribution cannot be inferred from timestamp "
            "plateaus alone.",
            "- Point-in-time execution-metadata completeness and whether book/metadata validity "
            "should be separate remain unresolved.",
            "- The bounded hours cannot establish archive-wide ambiguity or gap rates.",
            "",
            "## Safety, scope, and deviations",
            "",
            "Only the public pmxt archive was accessed to download the three predeclared sample "
            "dates under the mechanical fallback rule. No Polymarket endpoint, authentication, "
            "wallet, credential, user WebSocket, order API, trading, fill simulation, forecast, "
            "position, PnL, or strategy operation occurred.",
            "",
            f"Deviation from IP-002: **{summary['deviation_from_ip_002']}**.",
            f"Deviation from IP-002R: "
            f"**{summary.get('deviation_from_ip_002r', 'Not applicable')}**.",
            "",
            "ADR-001, `src/`, strategy/risk/execution/portfolio code, and "
            "`docs/research/architecture-interview-notes.md` were not modified.",
            "",
            "## References",
            "",
            "- https://archive.pmxt.dev/docs/v2-data-overview",
            "- https://archive.pmxt.dev/Polymarket/v2",
            "- https://github.com/pmxt-dev/polymarket-orderbook-collector/tree/cb0f6631556bf460d03594fe20f9bbd020b47d19",
            "- https://github.com/pinglucid/pmxt_v2_adapter/blob/main/VALIDATION.md",
            "- Kim, Kang, Sagong & Park (1997), DOI `10.1016/S0928-4869(96)00009-2`.",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(path: Path, report: str) -> None:
    path.write_text(report, encoding="utf-8", newline="\n")
