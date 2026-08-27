# pmxt Ordering Ambiguity Audit

This directory contains the bounded offline research audit authorized by
`docs/implementation-packets/IP-002-pmxt-ordering-ambiguity-audit.md`. It is not
the production downloader or replay engine.

## Safety and methodology boundary

- The only network operation is unauthenticated HTTPS `GET` from the fixed public
  host `r2v2.pmxt.dev` for the predeclared sample and same-date fallback keys.
- Raw Parquet and machine summaries remain below the gitignored
  `outputs/pmxt-ordering-audit/` directory.
- Availability groups use exactly `(market, asset_id, timestamp_received)`.
- Group classification is independent of input row order and source timestamp.
- Published row order and source timestamp are used only for A6 diagnostics.
- No fill, strategy, forecast, PnL, position, order, or production replay path
  exists in this package.

## Research-only dependency

```text
python -m pip install -r tools/probes/pmxt_ordering_audit/requirements-probe.txt
```

DuckDB is a local analytical-engine choice for IP-002 only.

## Full offline suite

Run this before downloading or analyzing real samples:

```text
python -m unittest discover -s tests/probes -p test_*.py -v
```

## Predeclared acquisition

```text
python -m tools.probes.pmxt_ordering_audit download
```

The downloader requests `2026-05-01T12`, `2026-06-15T12`, and
`2026-08-01T12`. An unavailable or invalid object may be replaced only by the
first later valid hour on the same UTC date.

## Offline analysis

The A9 label is a descriptive research interpretation, not a numerical policy
threshold or architecture decision. Supply it only after inspecting the raw
A1-A8 results.

```text
python -m tools.probes.pmxt_ordering_audit analyze \
  --offline-test-result "N tests passed in S seconds on Python X"
```

This writes ignored A1-A8 evidence without requiring an A9 interpretation.
Inspect those raw metrics, then finalize the descriptive conclusion and report:

```text
python -m tools.probes.pmxt_ordering_audit finalize \
  --feasibility-label UNRESOLVED \
  --feasibility-rationale "Bounded evidence does not support a stronger label."
```

The command writes ignored `summary.json` evidence and deterministically updates
`docs/experiments/pmxt-ordering-ambiguity-audit.md`.
