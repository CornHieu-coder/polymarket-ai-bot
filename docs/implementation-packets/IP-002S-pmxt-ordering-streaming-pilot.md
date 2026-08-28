# IP-002S: Streaming Feasibility Pilot for the pmxt Ordering Audit

## Goal

Test one final execution strategy for completing the scientifically frozen IP-002 pmxt ordering audit: a **sequential streaming analyzer over the original sorted Parquet**, exploiting the archive's documented `(market, asset_id, timestamp_received)` sort order without using row order as a causal tie-break.

This packet authorizes **only implementation, exact-equivalence validation, and a bounded resource pilot**. It does **not** authorize a full August run, A9, final reporting, PR opening, production replay, or any scientific-policy change.

If the pilot fails its exactness or resource gate, stop. Do not invent a fourth execution engine.

## Research/design documents to read first

- `AGENTS.md`
- `docs/README.md`
- `docs/research/principles.md`
- `docs/data/pmxt-historical-ordering-risk.md`
- `docs/data/pmxt-ordering-audit-execution-feasibility.md`
- `docs/implementation-packets/README.md`
- `docs/implementation-packets/IP-002-pmxt-ordering-ambiguity-audit.md`
- `docs/implementation-packets/IP-002R-pmxt-ordering-audit-recovery.md`

Primary external technical references:

- pmxt V2 data overview / sort contract: https://archive.pmxt.dev/docs/v2-data-overview
- PyArrow Parquet streaming batches: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html
- Arrow larger-than-memory / iterative dataset reads: https://arrow.apache.org/docs/python/dataset.html
- DuckDB blocking-operator guidance: https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads
- DuckDB OOM guidance: https://duckdb.org/docs/current/guides/performance/oom

## Evidence basis

### Established by prior project evidence

- The original IP-002 run preserved all three predeclared Parquet files and hashes; June completed, May/August OOM'd under a 25 GiB DuckDB limit.
- IP-002R's 32-shard pilot passed on shard 18, but shard 2 required 591.329 s; the conservative projection for the then-missing shards was 275.954 min, so IP-002R correctly stopped as `RECOVERY_NOT_FEASIBLE` under its 90-minute gate.
- Four verified recovery checkpoints exist for shards 0, 1, 2, and 18.
- The scientific A1-A9 definitions remain unresolved and unchanged.

### Established by external source-format evidence

pmxt documents each V2 Polymarket hourly Parquet as sorted ascending by:

```text
(market, asset_id, timestamp_received)
```

This is exactly IP-002's archive-availability grouping key. The `(market, asset_id)` prefix is also exactly the unit of A7/A8 state trajectory.

PyArrow supports iterative Parquet `RecordBatch` reads and column projection, allowing a sequential scan without loading the full file into RAM.

DuckDB documents `GROUP BY`, JOIN, sorting and windowing as blocking/memory-intensive operators. IP-002S therefore tests an algorithm that avoids those operations for the core grouping/state path rather than trying to force a larger hash/shuffle plan through the same machine.

### Project-derived execution choice

Use the archive's sorted layout only to discover contiguous equal-key groups and complete asset trajectories.

**Do not treat published row order inside one equal `(market, asset_id, timestamp_received)` group as causal order.** Members of one group remain an unordered simultaneous-availability set under the frozen IP-002 methodology. Row order may affect only A6's explicitly diagnostic published-order statistic.

## Allowed scope

Codex may modify/create only:

- `tools/probes/pmxt_ordering_audit/`
- `tests/probes/pmxt_ordering_audit/`
- ignored pilot outputs under `outputs/pmxt-ordering-audit/`

Codex must not modify during this pilot:

- `src/`
- ADR-001 or any ADR
- `docs/research/architecture-interview-notes.md`
- `docs/experiments/pmxt-ordering-ambiguity-audit.md`
- `docs/data/pmxt-historical-ordering-risk.md`
- `docs/data/pmxt-ordering-audit-execution-feasibility.md`
- strategy/risk/execution/portfolio modules

Do not commit raw or derived pilot datasets.

## Frozen scientific invariants

All original IP-002 A1-A8 definitions remain authoritative. At minimum:

1. Grouping key is exactly `(market, asset_id, timestamp_received)`.
2. Source timestamp never resolves group ambiguity.
3. Published row order never resolves group ambiguity.
4. Exact decimal/fixed-point price/size semantics remain unchanged.
5. Price-change, book/snapshot, tick and last-trade ambiguity rules remain unchanged.
6. A7 starts each audited hour `UNINITIALIZED`, becomes `VALID` only from an accepted in-file book, invalidates only under the frozen rules, and recovers only on the next accepted book.
7. A8 cadence grids remain 1m/5m/30m UTC-aligned.
8. The predeclared scientific samples remain May 1, June 15 and August 1; the pilot does not choose replacement science samples.
9. Prior OOM/runtime failures remain part of provenance and are never rewritten as successful results.

## Streaming execution design to test

### Sequential scan

Read the original preserved August Parquet directly in bounded record batches. Prefer `pyarrow.parquet.ParquetFile.iter_batches` or an equivalently auditable Arrow path.

Read only columns needed for A1-A8. Do not materialize the full file or a full partition in a DataFrame/Table.

### Sort-contract verification

Maintain the previous composite key and assert that the observed sequence never decreases under lexicographic:

```text
(market, asset_id, timestamp_received)
```

If a tested input violates this contract, fail visibly. Do not sort it in memory as a recovery convenience.

### Group handling

Rows with equal `(market, asset_id, timestamp_received)` are accumulated only until the group ends, then passed to the existing/frozen group classifier as an **unordered logical group**.

A batch boundary may split a group. The implementation must carry the incomplete group across batches exactly.

### Asset trajectory handling

Because `(market, asset_id)` is the sort-key prefix, one asset's rows should be contiguous. Maintain only the current asset's A7/A8 state and finalize it when the asset changes.

A batch/row-group boundary may split an asset trajectory; state must carry across that boundary.

### Exact bounded aggregates

Do not introduce a full-file hash `GROUP BY` merely for A1.

For in-hour millisecond `timestamp_received` counts, an exact fixed-size array indexed by millisecond offset from the UTC hour is acceptable because the source field has millisecond precision and one hour has exactly 3,600,000 possible millisecond slots. Any out-of-hour timestamp must remain visible under IP-002's failure/diagnostic rules.

Exact histograms/sufficient statistics may similarly replace lists when they reproduce the same A1/A7 quantiles exactly. No approximate sketch, sampled percentile, averaged shard percentile, or floating-point approximation is allowed.

### Book parsing

Parse/canonicalize `bids` and `asks` only for `book` events and only as needed by the frozen symbolic rules. Do not repeatedly parse book JSON for unrelated rows.

### No full-file blocking plan

The pilot must not use a full-file `GROUP BY`, JOIN, `ORDER BY`, or window operation for the core A1-A8 reconstruction path. Small reference-fixture SQL is allowed if useful for tests.

## Exact-equivalence requirements

Before any resource conclusion:

1. preserve all existing IP-002/IP-002R scientific tests unchanged;
2. add streaming-specific regression fixtures for group split across Arrow batches and asset split across batches;
3. prove the streaming result is invariant to batch size on the same logical fixture;
4. compare streaming A1-A8 exactly against the existing unsharded reference classifier on deterministic small fixtures;
5. compare against at least one preserved IP-002R real-data shard checkpoint **only if that shard file itself passes the streaming sort-contract assertion**; do not sort/rewrite a shard merely to make the comparison possible;
6. if a real-shard comparison cannot be made because the recovery partition is not sorted, say so explicitly; the synthetic/reference exactness tests remain mandatory.

No difference is acceptable merely because it is small.

## Feasibility and resource budget

This packet is a **pilot only**. Do not launch a complete August streaming scan.

### Pilot windows on the original August file

Select two deterministic, non-outcome-based, complete-asset windows:

#### W1 — beginning window

Starting from physical row 0, include complete `(market, asset_id)` trajectories until at least **2,000,000 rows** have been included. If the threshold falls inside an asset, continue through that asset's final row.

#### W2 — midpoint window

Locate the physical midpoint from Parquet row-count metadata. Start reading at the row group containing the midpoint, discard the partial asset containing the midpoint, then start W2 at the **next `(market, asset_id)` boundary**. Include complete asset trajectories until at least **2,000,000 rows** have been included.

Selection uses only physical position and asset boundaries, never ambiguity outcomes or scientific metrics.

W1/W2 are resource-profiling windows only. Their scientific results must not be used to tune IP-002 methodology or represent August.

### Resource measurements

For each window record:

- physical row range / row groups read;
- complete asset count;
- rows analyzed;
- wall-clock time;
- process-tree peak RSS;
- Arrow batch size;
- bytes of temporary storage created;
- number of batches;
- maximum buffered logical group size;
- maximum single-asset row count within the window;
- output/checkpoint size if any.

### Proceed / stop formula

Let:

```text
r1 = W1 seconds / W1 rows
r2 = W2 seconds / W2 rows
r = max(r1, r2)
projected_full_august = r * 82,705,648 * 1.5
```

The 1.5 factor is an operational safety factor, not a scientific parameter.

Pilot status is `STREAMING_PILOT_PASS` only if all are true:

- exact-equivalence tests pass;
- tested original-file keys obey the documented monotonic sort contract;
- W1 + W2 combined analysis time <= **10 minutes**;
- peak process-tree RSS <= **4 GiB**;
- temporary storage growth <= **2 GiB**;
- neither window has an unrecoverable parser/engine failure;
- conservative `projected_full_august` <= **30 minutes**.

If any condition fails, status is `STREAMING_NOT_FEASIBLE`.

Do not lower the 1.5 safety factor, shrink the windows, select easier windows, add more hardware, or loosen the 30-minute limit after seeing pilot performance.

## Final-stop rule

If this pilot is `STREAMING_NOT_FEASIBLE`, stop the exact local IP-002 recovery effort. Do not create a fourth execution engine or another sharding scheme.

The research architect will then decide between forward sequence-preserving collection and evaluating a separate historical data provider.

## Failure cases

Handle explicitly:

- preserved August hash mismatch;
- Arrow/Parquet schema mismatch;
- observed sort-key regression;
- batch boundary splits one logical group incorrectly;
- batch boundary splits one asset state incorrectly;
- exact-decimal conversion mismatch;
- streaming/reference result mismatch;
- out-of-hour `timestamp_received`;
- parser failure for required book payload;
- RSS/storage/runtime budget breach;
- inability to obtain two mechanically selected complete-asset pilot windows.

A resource failure is not scientific evidence about pmxt ambiguity.

## Required tests

Preserve all prior tests and add at least:

1. composite sort-contract monotonicity acceptance;
2. sort regression fails visibly rather than auto-sorting;
3. group split across batches is reconstructed exactly once;
4. asset trajectory split across batches preserves A7 state;
5. changing batch size does not change A1-A8 results;
6. exact millisecond counter matches reference `timestamp_received` distribution;
7. exact recovery-time histogram/statistics match reference A7 percentiles;
8. A8 cadence counts match the reference classifier;
9. A6 published-order diagnostics are preserved without affecting ambiguity classification;
10. streaming output matches the unsharded reference classifier on synthetic fixtures;
11. prior IP-002/IP-002R scientific tests still pass unchanged;
12. pilot gate computes the conservative 1.5x projection correctly and blocks unauthorized full execution.

## Acceptance criteria

IP-002S pilot is complete when:

- original evidence is unchanged and hash-verified;
- streaming implementation/tests are complete;
- full offline suite passes;
- W1 and W2 are selected mechanically and executed once under the frozen pilot rule;
- exactness/resource measurements are written under ignored output;
- status is exactly `STREAMING_PILOT_PASS` or `STREAMING_NOT_FEASIBLE`;
- **no full August streaming audit is launched**;
- no A9/report/ADR/production-replay work occurs.

## Explicitly out of scope

- full August streaming execution;
- May recovery;
- rerunning the full June hour;
- changing A1-A9;
- reducing the number of scientific metrics after seeing resource results;
- selecting different scientific dates/hours;
- production replay;
- forecasting, sizing, risk, fills, PnL;
- cloud/hardware scaling;
- purchasing or integrating a commercial data source.

## Open questions

- Can a sequential algorithm exploiting the archive's documented sort order reproduce A1-A8 exactly?
- Does its conservative projected full-August runtime fit within 30 minutes on the current machine?

These are answered by the bounded pilot only.

## Traceability check

```text
IP-002 scientific definitions (frozen)
        +
monolithic OOM evidence
        +
IP-002R runtime-gate failure
        +
pmxt documented physical sort order
        +
Arrow iterative Parquet reads
        ↓
docs/data/pmxt-ordering-audit-execution-feasibility.md
        ↓
IP-002S streaming feasibility pilot
        ↓
exactness tests + W1/W2 resource measurements
        ↓
STREAMING_PILOT_PASS
or
STREAMING_NOT_FEASIBLE
```
