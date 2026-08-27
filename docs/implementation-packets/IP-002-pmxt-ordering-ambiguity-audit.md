# IP-002: pmxt Historical Ordering Ambiguity Audit

## Goal

Build a **bounded offline research audit**, not the production replay engine, that measures how much ordering information is unavailable in published pmxt V2 Polymarket Parquet and quantifies the replay-coverage cost of the conservative candidate policy in `docs/data/pmxt-historical-ordering-risk.md`.

The audit must answer whether an atomic archive-availability-group / fail-closed-on-order-ambiguity policy is practical for V0 historical replay.

The audit must not implement production replay, strategy logic, forecasting, position sizing, execution fills, portfolio accounting, or trading.

## Research/design documents to read first

- `AGENTS.md`
- `docs/README.md`
- `docs/research/principles.md`
- `docs/research/assumptions.md`
- `docs/data/market-data-and-replay.md`
- `docs/data/pmxt-historical-ordering-risk.md`
- `docs/design/decisions/ADR-001-fail-closed-event-sourced-replay.md`
- `docs/experiments/market-feed-contract-probe.md`
- `docs/implementation-packets/README.md`

Primary external references:

- https://archive.pmxt.dev/docs/v2-data-overview
- https://archive.pmxt.dev/Polymarket/v2
- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/shared/rust/polymarket-orderbook-rust/src/events.rs
- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/services/r2-archive/exporter/run.py
- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/shared/rust/polymarket-orderbook-rust/src/sink.rs
- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/services/polymarket/orderbook-compare/README.md
- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/services/polymarket/orderbook-compare/compare_price_changes.py
- https://github.com/pinglucid/pmxt_v2_adapter/blob/main/VALIDATION.md
- Kim, Kang, Sagong & Park (1997), DOI `10.1016/S0928-4869(96)00009-2`

## Evidence basis

### Established by external evidence

- Current published pmxt V2 Polymarket files expose a 16-column schema with millisecond `timestamp_received` and source `timestamp`, sorted by `(market, asset_id, timestamp_received)`.
- Published Polymarket rows do not expose a receive-sequence/frame-index tie-breaker.
- The reviewed current pmxt collector explodes one wire `price_change.price_changes[]` array into individual stored events.
- pmxt's current comparison tooling reports repeated `(asset_id, source_timestamp_ms, side, price)` keys inside a single original WebSocket message in real data.
- General discrete-event simulation research establishes that arbitrary ordering of simultaneous events can affect simulation results.

### Suggested by external evidence, requiring archive audit

- The current pmxt pipeline suggests `timestamp_received` may behave as a coarse/batch availability timestamp rather than a precise network-receive timestamp, but historical files may span collector implementations.
- Same-millisecond event multiplicity is substantial in an independent v1/v2 validation sample, but that statistic is keyed by source time and is not itself the archive-availability ambiguity rate needed by this project.

### Project-derived choices under test

- Group potential simultaneous historical observations by `(market, asset_id, timestamp_received)`.
- Do not expose an intermediate strategy decision between rows in one such group.
- Classify a group as order-invariant only when final relevant state is provably independent of unknown row ordering.
- Treat an order-ambiguous state-changing group as a candidate invalidation until the next accepted full `book` snapshot.
- Use source timestamps only as diagnostics in this audit, not as an invented sequence number.

These choices are **not yet accepted production rules**. IP-002 measures their consequences; it does not freeze them.

## Allowed scope

Codex may create/modify only:

- `tools/probes/pmxt_ordering_audit/`
- `tests/probes/pmxt_ordering_audit/`
- `docs/experiments/pmxt-ordering-ambiguity-audit.md`
- a clearly probe-only dependency file under `tools/probes/pmxt_ordering_audit/` if required
- `docs/data/pmxt-historical-ordering-risk.md` **only to append measured audit results; do not promote the candidate policy to accepted architecture**

Do not modify:

- `src/`
- ADR-001 or any other ADR
- `docs/research/architecture-interview-notes.md`
- strategy/risk/execution/portfolio code

If the results imply a material architecture change, report it for research review instead of implementing it.

## Tooling boundary

Use Python 3.11+ and a mature local analytical engine such as DuckDB and/or PyArrow/Polars. Dependencies are **research-tool-only** and must not establish the production runtime stack.

The audit may access only the public pmxt archive needed to obtain the predeclared Parquet samples. No Polymarket authenticated endpoint, wallet, credential, user WebSocket, order API, or trading operation is permitted.

Downloaded Parquet and generated machine summaries must remain under an already gitignored research/output path such as:

```text
outputs/pmxt-ordering-audit/
```

Do not commit raw Parquet files.

## Predeclared sample

Audit these three post-CLOB-V2 UTC hours, chosen before seeing ambiguity outcomes to cover early, middle, and later archive periods:

1. `2026-05-01T12`
2. `2026-06-15T12`
3. `2026-08-01T12`

Canonical object naming follows pmxt documentation:

```text
polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

### Missing-file rule

Do not silently choose a more convenient hour.

If a predeclared object is unavailable or invalid:

1. record the failed requested object and reason;
2. choose the **first later available hour on the same UTC date** mechanically;
3. record the substitution explicitly.

If no later hour exists that date, leave that sample missing. If fewer than two valid hourly samples remain, stop and classify the audit `UNRESOLVED` rather than expanding the date range ad hoc.

## Inputs / provenance to preserve

For every sampled object record at least:

- requested sample hour;
- actual object key/hour used;
- download URL;
- download start/end time;
- HTTP status and useful immutable/object metadata where available (e.g. ETag, Last-Modified, Content-Length);
- exact file byte length;
- SHA-256 of downloaded bytes;
- Parquet schema;
- row count and row-group count;
- min/max `timestamp_received`;
- min/max source `timestamp`;
- audit code git commit;
- Python and analytical dependency versions.

The report must make historical-file provenance sufficient to rerun the same audit later.

## Canonical audit definitions

### Archive-availability group

For this audit only:

```text
G = rows with equal (market, asset_id, timestamp_received)
```

This is a **candidate conservative availability grouping**, not a claim that all members came from the same original WebSocket frame.

### State-changing rows

For L2 book validity:

- `book`
- `price_change`

For point-in-time tick metadata validity:

- `tick_size_change`

`last_trade_price` is auxiliary and must be counted separately; its ordering does not invalidate L2 book state in this audit.

### Price-level key

```text
(side, price)
```

Use exact decimal/fixed-point comparisons.

### L2 order-invariant group

A group is L2 order-invariant only when the final L2 book state is provably identical under every possible ordering of its L2-changing rows.

At minimum implement the following conservative symbolic rules:

1. **Pure `price_change` group:** invariant iff every repeated `(side, price)` key has one unique replacement size. Distinct keys commute; exact/idempotent replacements commute.
2. **Only `book` rows:** invariant iff all full-book snapshots in the group are exactly equivalent after canonical decimal parsing and canonical level ordering.
3. **`book` + `price_change`:** invariant only if all full-book snapshots are equivalent **and** every price-change replacement is already idempotent with respect to that snapshot state. Otherwise ordering can affect the final state.
4. If a payload cannot be parsed exactly enough to apply those rules, classify the group as unresolved/ambiguous rather than using file row order.

Do not enumerate factorial permutations for large groups when the symbolic test proves invariance/ambiguity directly.

### Tick-metadata order-invariant group

A group with zero or one tick-size transition is deterministic for tick state. Multiple tick transitions are invariant only when they are exact duplicates under canonical parsing. Multiple distinct transitions are order-ambiguous for this audit.

### Overall state ambiguity

Report L2 ambiguity and tick-metadata ambiguity separately. Do not make an L2-valid book invalid solely because auxiliary `last_trade_price` ordering is unknown.

## Questions / metrics the audit must answer

### A1 — Actual schema and timestamp structure

For each file:

- verify expected columns/types;
- detect unexpected schema drift;
- count distinct `timestamp_received` values;
- report rows per `timestamp_received` distribution (min/p50/p90/p95/p99/max);
- report number/share of `(market, asset_id, timestamp_received)` groups of size 1, 2, 3, 4, 5-9, 10-99, 100+;
- inspect whether equal `timestamp_received` values occur in large plateaus consistent with coarse/batched availability.

Do not infer implementation history from this pattern alone; label it diagnostic.

### A2 — Tied state-changing groups

Measure:

- total archive-availability groups;
- groups containing >1 L2-changing row;
- rows contained in those groups;
- group-size distribution by event-type composition.

### A3 — Pure price-change ambiguity

Measure:

- pure price-change tied groups;
- groups with all distinct `(side, price)` keys;
- groups with repeated keys but one unique replacement size per key;
- groups with a repeated key and multiple replacement sizes;
- exact duplicate rows;
- representative counterexamples with anonymization **not** required: retain real market/asset IDs and timestamps because they are public archive identifiers.

### A4 — Snapshot interactions

Measure:

- multiple `book` rows in one group;
- identical versus differing snapshots;
- `book` + `price_change` groups;
- those proven invariant by the rule above;
- those order-ambiguous;
- representative ambiguity examples.

### A5 — Tick metadata

Measure single/duplicate/distinct tick transitions in tied groups and whether tick ambiguity would independently block H2 execution metadata.

### A6 — Source-time diagnostics inside availability ties

For tied groups report:

- number of distinct source timestamps;
- groups where source timestamps are all equal versus different;
- min/max source-time span inside the group;
- any source-time regressions in **published row order**, explicitly labeled diagnostic because published row order is not accepted as causal order.

Do **not** use source timestamp to resolve A3/A4/A5 ambiguity.

### A7 — Candidate fail-closed coverage cost

Replay only the minimum state machine required for this audit:

```text
UNINITIALIZED
  -> accepted book -> VALID
VALID
  -> order-ambiguous L2 group -> INVALID
INVALID
  -> next accepted full book -> VALID
```

This is audit instrumentation, not production replay.

For each file report:

- number of ambiguity-triggered invalidations;
- time and number of subsequent archive groups until next accepted full book;
- p50/p90/p95/p99/max recovery time among recovered invalidations;
- right-censored invalidations that do not recover before the audited hour ends;
- per-asset valid/invalid/uninitialized wall-clock duration inside the hour;
- aggregate time-weighted share of initialized asset-time that remains VALID;
- share of state-changing rows processed while state is valid versus excluded after ambiguity.

Do not carry state from an un-audited previous hour. Each audited hour begins `UNINITIALIZED` and becomes valid only from an in-file full book. This intentionally measures a conservative within-hour lower bound.

### A8 — Fixed cadence diagnostics

Without simulating a strategy or choosing a winner, report validity availability at predeclared UTC-aligned cadence grids:

- 1 minute;
- 5 minutes;
- 30 minutes.

For each grid, report the fraction of asset/time decision points **after that asset's first in-file accepted book** at which L2 state is `VALID` under the candidate policy.

These are sensitivity diagnostics only. Do not tune replay policy or choose a trading cadence from these results.

### A9 — Feasibility conclusion

Classify the candidate policy as one of:

- `PRACTICALLY_LOW_COST`
- `MATERIAL_COVERAGE_COST`
- `SEVERE_COVERAGE_COST`
- `UNRESOLVED`

Do **not** invent numerical thresholds after seeing results. The report must present raw rates/distributions first. The qualitative label should be explained and treated as descriptive, not as a pre-registered statistical test. No architecture is frozen by the label itself.

## Required invariants

1. Raw downloaded Parquet is immutable during analysis and stays outside Git.
2. File SHA-256 and schema are recorded before conclusions are produced.
3. No Parquet row order is treated as the true causal tie-break for equal archive availability.
4. No source timestamp is treated as a sequence number or used to resolve final-state ambiguity.
5. Exact decimal/fixed-point comparison is used for prices and sizes.
6. Unknown event/schema shapes are counted and preserved as unresolved; they are not silently discarded.
7. Order-invariant classification must be justified by state-transition semantics, not by whichever order appears first in the file.
8. The audit does not simulate fills, PnL, forecasts, positions, or orders.
9. Results are reported per sample and pooled; a pooled statistic must not hide a materially different sample.
10. Negative findings are valid. Do not relax ambiguity rules merely to increase coverage.

## Failure cases

Record and handle:

- sample object missing;
- HTTP/download interruption;
- hash mismatch across repeated download when revalidation is attempted;
- invalid/corrupt Parquet;
- schema drift;
- null required grouping fields;
- decimal parse failure;
- malformed book JSON;
- unknown event type;
- timestamp outside declared object hour;
- memory pressure / analytical-engine failure;
- output write failure.

A failure that prevents complete analysis of one predeclared file must be visible in the final report; do not silently omit the file from pooled statistics.

## Required tests

Offline tests must use synthetic/small local Parquet fixtures and require no network.

At minimum test:

1. exact sample/fallback selection rule;
2. provenance/hash recording;
3. grouping key is exactly `(market, asset_id, timestamp_received)`;
4. distinct price-level replacements are order-invariant;
5. repeated same-key/same-size replacements are idempotent;
6. repeated same-key/different-size replacements are order-ambiguous;
7. identical book snapshots are invariant;
8. different book snapshots are ambiguous;
9. snapshot + delta matching the snapshot state is invariant;
10. snapshot + delta that changes snapshot state is ambiguous;
11. a single tick transition is deterministic;
12. duplicate identical tick transitions are deterministic;
13. distinct tied tick transitions are ambiguous;
14. auxiliary last-trade ties do not invalidate L2 state;
15. source timestamp differences do not resolve an otherwise ambiguous group;
16. file/row ordering changes do not change the audit classification/results for the same logical group;
17. `UNINITIALIZED -> VALID -> INVALID -> VALID` recovery accounting;
18. right-censoring at the audited hour boundary;
19. deterministic report generation;
20. no import/call path to authenticated/trading Polymarket clients.

Include at least one regression fixture containing the same `(side, price)` key multiple times with conflicting replacement sizes.

## Derived outputs

Write a deterministic machine-readable run summary under the ignored output directory and generate/update:

```text
docs/experiments/pmxt-ordering-ambiguity-audit.md
```

The committed report must include:

- exact predeclared/requested and actual sample files;
- provenance/hashes/schema;
- tooling versions and git commit;
- A1-A9 metrics per file and pooled;
- representative order-ambiguous examples;
- exclusions/failures/right-censoring;
- an explicit distinction between established evidence, audit observations, and project-derived interpretation;
- whether the candidate policy appears practically viable, without editing ADR-001.

After evidence exists, append a short results section to `docs/data/pmxt-historical-ordering-risk.md`. Preserve the pre-audit reasoning rather than rewriting it as though the result was known in advance.

## Acceptance criteria

IP-002 is complete only when:

- the offline test suite passes;
- at least two of the three predeclared sample dates are successfully audited, with any mechanical substitutions documented;
- every analyzed Parquet file has recorded SHA-256/schema/provenance;
- A1-A9 are reported for each valid sample and pooled;
- ambiguity classification is deterministic under fixture row reordering;
- fail-closed coverage cost is quantified rather than assumed;
- raw Parquet remains outside Git;
- no production replay code or `src/` changes were made;
- no trading/authenticated operation occurred;
- unresolved implications remain unresolved rather than being encoded into an ADR or production code.

## Explicitly out of scope

- production pmxt downloader;
- production collector/reconstructor;
- implementation of `MarketSnapshot(t)`;
- execution/fill simulator;
- forecasting/H1 evaluation;
- Brier sizing or kappa selection;
- risk engine;
- portfolio accounting;
- fee/min-order historical recovery beyond reporting whether relevant fields are absent;
- AWS/cloud/database architecture;
- on-chain trade ingestion;
- live Polymarket feed probing;
- authenticated endpoints or trading;
- choosing final historical tie policy;
- changing ADR-001;
- updating architecture interview notes with an unaccepted policy.

## Open questions

1. What fraction of actual published `timestamp_received` groups is state-order-ambiguous?
2. How much replay coverage would be lost by invalidating until the next full snapshot?
3. Does `timestamp_received` behavior materially differ across early/mid/late sampled archive periods?
4. Is the candidate conservative grouping too coarse for H2, or cheap enough to adopt?
5. After this audit, should `BookState` and `ExecutionMetadataState` be formalized as separate validity dimensions?

Codex must not answer these by assumption before the audit results exist.

## Traceability check

```text
pmxt public archive schema
+ pmxt current collector/exporter implementation
+ pmxt current same-key multi-update diagnostics
+ IP-001 aggregate replacement/deletion evidence
+ simultaneous-event simulation literature
        ↓
docs/data/pmxt-historical-ordering-risk.md
        ↓
IP-002 bounded offline ambiguity audit
        ↓
raw sampled Parquet + hashes
+ deterministic audit code/tests
        ↓
docs/experiments/pmxt-ordering-ambiguity-audit.md
        ↓
research review
        ↓
ONLY THEN: amend/freeze historical replay contract + ADR
        ↓
production replay implementation packet
```
