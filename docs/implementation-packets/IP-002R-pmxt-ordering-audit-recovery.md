# IP-002R: Memory-Bounded Recovery for the pmxt Ordering Audit

## Goal

Recover the incomplete `IP-002-pmxt-ordering-ambiguity-audit.md` experiment **without changing its scientific definitions, sample-selection rule, ambiguity classifications, or A1-A9 questions**.

The first IP-002 execution plan proved computationally impractical on the available machine: the preserved atomic result completed June but recorded DuckDB out-of-memory failures for May and August at the 25 GiB limit. The raw Parquet files and hashes remain intact. This packet authorizes only a **memory-bounded execution redesign** and the minimum additional analysis needed to obtain a second successful sample, preferably August, so the original IP-002 acceptance condition can be evaluated.

Do not restart production replay work, forecasting, sizing, execution simulation, or any other out-of-scope task.

## Research/design documents to read first

- `AGENTS.md`
- `docs/README.md`
- `docs/research/principles.md`
- `docs/data/pmxt-historical-ordering-risk.md`
- `docs/implementation-packets/README.md`
- `docs/implementation-packets/IP-002-pmxt-ordering-ambiguity-audit.md`

Relevant external execution references:

- DuckDB workload tuning: https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads
- DuckDB out-of-memory guidance: https://duckdb.org/docs/current/guides/performance/oom
- DuckDB partitioned writes: https://duckdb.org/docs/lts/data/partitioning/partitioned_writes

## Evidence basis

### Established by the failed first execution

- The original three predeclared Parquet files were downloaded and preserved immutably.
- The original analyzer produced one atomic `analysis-a1-a8.json` with overall status `UNRESOLVED`.
- June completed successfully.
- May and August failed with DuckDB out-of-memory errors at the 25 GiB analytical-memory limit.
- The prior design retained expensive per-file results until the final combined artifact, giving poor crash/recovery granularity.

The recovery run must preserve the exact failure record; it must not rewrite the first attempt as though it had succeeded.

### Established by external technical guidance

DuckDB documents `GROUP BY`, `JOIN`, `ORDER BY`, and window operations as blocking/memory-intensive operators and supports disk spilling, while also documenting that some workloads can still fail with OOM. Its OOM guidance recommends reducing thread count and using a lower memory limit, and its partitioned-write facility can physically split a dataset by a derived partition key.

### Project-derived execution choices

The scientific grouping key remains exactly:

```text
(market, asset_id, timestamp_received)
```

For recovery, rows may be physically partitioned by a deterministic hash of `(market, asset_id)` because every archive-availability group and every per-asset state trajectory then belongs wholly to one partition. This is an **execution transformation only**: it must not alter group membership, ambiguity classification, timestamp semantics, or A7/A8 state accounting.

The recovery should use independent, atomic checkpoints per completed partition/sample so later failures do not destroy valid earlier work.

## Allowed scope

Codex may modify/create only:

- `tools/probes/pmxt_ordering_audit/`
- `tests/probes/pmxt_ordering_audit/`
- `docs/experiments/pmxt-ordering-ambiguity-audit.md`
- `docs/data/pmxt-historical-ordering-risk.md` only to append the first-attempt failure and final measured result
- ignored recovery outputs under `outputs/pmxt-ordering-audit/`

Do not modify:

- `src/`
- ADR-001 or any other ADR
- `docs/research/architecture-interview-notes.md`
- strategy/risk/execution/portfolio code

Do not delete or overwrite the original raw sample files, original manifest/provenance, or the first atomic `analysis-a1-a8.json`.

## Scientific invariants that are frozen

All definitions and A1-A9 requirements from IP-002 remain authoritative. In particular:

1. Grouping remains exactly `(market, asset_id, timestamp_received)`.
2. Parquet row order is never used as a causal tie-break.
3. Source timestamp is diagnostic only and never resolves ambiguity.
4. Exact decimal/fixed-point semantics remain unchanged.
5. The symbolic rules for price-change, snapshot, tick, and auxiliary last-trade groups remain unchanged.
6. A7 still begins each audited hour `UNINITIALIZED` and recovers from `INVALID` only on the next accepted full `book`.
7. A8 cadence grids remain 1m/5m/30m UTC-aligned.
8. The predeclared files remain May 1, June 15, and August 1; this packet does not authorize choosing easier scientific samples.
9. The first execution failures remain part of the final report.

If the memory-bounded engine cannot reproduce these definitions exactly, stop rather than weakening the audit.

## Memory-bounded execution design

### 1. Reuse preserved evidence

Before any computation:

- verify the three preserved Parquet SHA-256 values against the original provenance;
- verify the first atomic result SHA-256 and record its status;
- retain the successful June metrics from that artifact;
- do **not** redownload any sample unless the preserved file fails its recorded hash.

### 2. Partition by complete asset trajectory

Create a deterministic physical partition key from `(market, asset_id)` only. Use a stable implementation-defined hash whose exact algorithm and shard count are recorded in recovery provenance.

Start with **32 shards** for August.

Every row for one `(market, asset_id)` must map to one shard. This guarantees:

- no archive-availability group is split;
- no A7 state trajectory is split;
- no A8 per-asset cadence trajectory is split.

Partitioning must preserve all columns needed by A1-A8. It may discard columns proven irrelevant only if IP-002 itself does not require them.

A6 published-row-order diagnostics must not be silently changed by repartitioning. Either:

- compute the published-row-order diagnostic in a separate low-memory sequential pass over the original file; or
- preserve a physical source-row ordinal sufficient to reconstruct that diagnostic exactly.

Do not treat that ordinal as causal order.

### 3. Independent shard analysis and checkpointing

Analyze one shard at a time. A completed shard must be written atomically to a deterministic machine-readable checkpoint containing at least:

- raw sample SHA-256;
- recovery-code git SHA;
- shard count and shard ID;
- row count;
- exact A1-A8 additive/intermediate metrics needed for final reduction;
- bounded representative counterexamples;
- checkpoint SHA-256.

A shard checkpoint is immutable once accepted. A later retry may verify/reuse it but must not silently replace it.

The final August result is reduced only from successfully verified shard checkpoints plus the separately verified A6 diagnostic. Reduction must be deterministic and must not depend on shard-processing order.

### 4. Exact reducibility requirements

Before the real recovery run, tests must demonstrate that all reported A1-A8 quantities are exactly reducible across shards.

Examples:

- group-count and ambiguity-count metrics are additive;
- group-size distributions are reducible from exact bucket counts;
- global `timestamp_received` row-count distribution must be merged by timestamp, not approximated by summing per-shard quantiles;
- A7 recovery distributions/right-censoring must be reduced from exact event records or exact sufficient statistics;
- A8 numerator/denominator counts are additive only after per-asset trajectories are complete inside one shard.

No pooled percentile may be computed by averaging shard percentiles.

## Feasibility and resource budget

This section is binding. Do **not** start a full August recovery until the pilot passes.

### Environment target

The previous run failed at a 25 GiB DuckDB limit. The recovery design should target:

- DuckDB/configured analytical memory: **8 GiB or less** where DuckDB is used;
- observed process peak RSS: **12 GiB or less**;
- analytical threads: **2 by default**, increase only if the pilot shows the RSS budget remains satisfied;
- temporary recovery storage: **50 GiB or less** in the authorized ignored directory;
- no single non-checkpointed computation expected to run longer than **30 minutes**.

These are project operational budgets, not research-backed scientific thresholds.

### Pilot — resource profiling only

The pilot must not inspect or report ambiguity outcomes for decision-making.

1. Build/verify the 32-shard August partitioning.
2. Determine shard row counts only.
3. Select the **largest shard mechanically by row count** (lowest shard ID breaks ties).
4. Run the full exact shard analyzer on that largest shard.
5. Record wall-clock time, peak RSS, DuckDB peak/configured memory where available, temp-storage growth, and output size.
6. Run the same shard through the small/reference classifier where computationally practical, or otherwise compare against deterministic synthetic/reference fixtures sufficient to prove semantic equivalence.

### Proceed / stop rule

Proceed to the remaining August shards only if all are true:

- largest-shard peak RSS <= 12 GiB;
- no OOM or spill/storage failure;
- partitioning + pilot elapsed time <= 30 minutes;
- measured largest-shard time implies a conservative projected remaining August wall-clock <= **90 minutes** when processed sequentially with the chosen settings;
- projected temporary storage <= 50 GiB;
- semantic-equivalence tests pass exactly.

If memory exceeds the budget but partitioning itself succeeded, Codex may mechanically retry once with **64 shards**. Re-run the same largest-shard pilot and the same proceed/stop rule.

If the 64-shard pilot still fails any proceed condition, stop and report `RECOVERY_NOT_FEASIBLE` rather than trying 128+ shards, increasing memory, changing scientific samples, or running for many additional hours.

If projected runtime exceeds 90 minutes, stop even if memory is safe. Runtime is now an explicit feasibility criterion.

## June reuse and cross-engine validation

Do not rerun the full June hour merely to obtain a second copy of an already successful result.

Before combining June with recovered August:

- verify the original June result is internally complete under IP-002;
- run the new recovery implementation on a small mechanically selected validation unit (for example the lowest non-empty recovery shard or a smaller deterministic hash sub-shard from preserved June data) and compare it against the reference classifier/tests;
- confirm the recovery implementation does not alter scientific classification semantics.

The final report must clearly state that June came from the first engine and August from the memory-bounded recovery engine, with the equivalence evidence linking them.

## Failure cases

In addition to IP-002 failures, handle explicitly:

- preserved raw hash mismatch;
- preserved first-result hash mismatch;
- partitioning OOM/failure;
- skewed shard exceeding resource budget;
- shard checkpoint write/hash failure;
- reducer mismatch or non-determinism;
- source-order diagnostic not reproducible after partitioning;
- pilot projected runtime above budget;
- process RSS above 12 GiB;
- temp storage above 50 GiB;
- recovery-code semantic mismatch against reference fixtures.

A resource failure is a valid result. Do not convert it into a scientific conclusion about pmxt ordering.

## Required tests

Preserve all IP-002 tests and add at least:

1. same `(market, asset_id)` always maps to the same shard;
2. no logical archive-availability group is split across shards;
3. repartitioning/shard-processing order does not change final A1-A8 metrics;
4. global timestamp-received distribution is merged exactly, not by averaging per-shard quantiles;
5. A7 recovery percentiles from sharded reduction match an unsharded reference fixture exactly;
6. A8 cadence counts from sharded reduction match an unsharded reference fixture exactly;
7. A6 published-order diagnostics match the unpartitioned reference fixture;
8. accepted shard checkpoint is atomic and hash-verified;
9. interrupted run reuses completed immutable shard checkpoints;
10. final reducer rejects missing/duplicate/wrong-sample shard checkpoints;
11. pilot resource/projection gate prevents full-run launch when thresholds are exceeded;
12. existing IP-002 scientific fixtures continue to pass unchanged.

## Acceptance criteria

IP-002R is complete only when one of these outcomes is reached:

### `RECOVERED`

- the original evidence/hashes are preserved and verified;
- the feasibility pilot passes;
- August completes under the resource/time budget using exact frozen A1-A8 semantics;
- all shard checkpoints and final August result are deterministic and hash-verified;
- the original successful June result is retained and validated for combination;
- at least two predeclared samples are now scientifically complete;
- A9 is evaluated from the valid samples under the original IP-002 rules;
- `docs/experiments/pmxt-ordering-ambiguity-audit.md` records both the failed first execution and recovery methodology/results;
- `docs/data/pmxt-historical-ordering-risk.md` receives a short append-only results section;
- all relevant offline tests pass;
- changes are committed/pushed and a PR is opened against `main`.

### `RECOVERY_NOT_FEASIBLE`

- the bounded pilot or one allowed 64-shard retry fails the declared memory/time/storage gate;
- no brute-force full run is launched;
- the exact resource measurements and blocker are recorded;
- IP-002 remains `UNRESOLVED` and no production replay rule is frozen.

## Explicitly out of scope

- retrying May merely to increase sample count after June + August are sufficient;
- choosing new dates/hours because they are smaller;
- changing A1-A9 definitions or candidate ambiguity policy;
- production replay implementation;
- strategy/fill/PnL simulation;
- forecasting/sizing/risk work;
- infrastructure/cloud scaling to brute-force the audit;
- raising memory limits above the recovery budget to force completion.

## Open questions

- Whether 32-shard execution meets the declared feasibility budget.
- Whether the recovered August scientific metrics, combined with June, are sufficient to classify A9 under the original descriptive labels.

These must be answered by the bounded recovery, not guessed.

## Traceability check

```text
IP-002 scientific definitions (frozen)
        +
first execution: June success / May+August OOM
        +
DuckDB resource guidance
        ↓
docs/research/principles.md feasibility/resource rules
        ↓
IP-002R memory-bounded execution contract
        ↓
resource-only largest-shard pilot
        ↓
verified shard checkpoints
        ↓
recovered August A1-A8 + preserved June A1-A8
        ↓
original IP-002 A9 conclusion or RECOVERY_NOT_FEASIBLE
```
