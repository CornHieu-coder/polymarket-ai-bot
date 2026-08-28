# pmxt Ordering Audit — Execution Feasibility

Status: **execution research; the scientific replay policy remains unresolved**  
Verified/reviewed: **2026-08-28**

## Main idea

Two execution plans for the exact IP-002 audit have now failed their declared feasibility gates. The next attempt must change the computational algorithm rather than increase resources, add more shards, relax the runtime gate, or alter the scientific definitions.

The original IP-002 scientific question remains valid: quantify how much state-order ambiguity in published pmxt V2 Polymarket data would cost a conservative fail-closed historical replay. No A9 conclusion has been authorized yet.

---

## 1. First execution attempt — monolithic DuckDB

The original analyzer processed the three predeclared hourly files under a 25 GiB DuckDB memory limit.

Preserved atomic result SHA-256:

```text
1fa6594ce2f46158af127d5e19966b6af5c69f1e38d22b3be1a6661cb83c2513
```

Outcome:

- June completed;
- May failed with DuckDB out-of-memory;
- August failed with DuckDB out-of-memory;
- IP-002 remained `UNRESOLVED` because at least two samples were required.

This was an execution-feasibility failure, not a scientific conclusion about pmxt.

---

## 2. IP-002R — deterministic hash sharding

IP-002R kept the scientific definitions frozen and partitioned August by a stable hash of `(market, asset_id)` so a complete asset trajectory stayed inside one shard.

### Resource pilot

The 32-shard pilot passed comfortably:

- partitioning: 343.202 s;
- largest shard by rows: shard 18;
- shard 18 rows: 3,115,850 / 82,705,648 (3.7674%);
- shard 18 analysis: 41.297 s;
- peak process-tree RSS: 3,388,092,416 bytes (3.155 GiB);
- DuckDB memory setting: 8 GiB, 2 threads;
- projected full-August runtime from that pilot: 27m 44.7s;
- semantic-equivalence regression: PASS;
- complete offline suite at pilot boundary: 103/103 passed.

### Full recovery gate failure

The pilot's row-count projection did not generalize to other shards. Shard 2 required:

```text
591.329 s
```

Using that observed rate conservatively across the 28 then-missing shards projected:

```text
275.954 min
```

which exceeded IP-002R's binding 90-minute full-recovery limit. Codex correctly stopped during shard 3 and classified the recovery `RECOVERY_NOT_FEASIBLE`.

Preserved recovery state:

- verified completed shard checkpoints: 0, 1, 2, and pilot shard 18;
- peak RSS remained 3.155 GiB;
- recovery storage at stop: 796,776,376 bytes;
- original raw Parquet and first atomic result remained unchanged;
- complete offline suite at stop: 105/105 passed;
- completion-infrastructure commit: `6993a180826ac7fb2f3b593256056cc489f54ffa`.

### Research interpretation

The sharded plan solved the memory problem but not the runtime problem. Row count was an inadequate predictor of shard cost: a shard with fewer rows could be much slower because the expensive work depends on event/group/asset-state structure, not only bytes or rows.

Therefore **more hash shards are not the next answer**. Increasing to 64/128+ shards may reduce memory further but does not remove the underlying per-group/per-asset analytical work and would violate the already-declared IP-002R stop rule.

---

## 3. New technical evidence: the archive is already sorted for our exact grouping problem

The pmxt V2 data overview documents each Polymarket hourly Parquet file as sorted by:

```text
(market, asset_id, timestamp_received)
```

which is exactly IP-002's archive-availability grouping key, with `(market, asset_id)` also being the complete A7/A8 state-trajectory prefix.

Source, verified 2026-08-28:

- https://archive.pmxt.dev/docs/v2-data-overview

pmxt explicitly says the row order is preserved at write time and readers that want to exploit it should avoid re-sorting on load.

This creates a materially different execution option:

```text
sorted Parquet
    ↓ sequential batches
one contiguous availability group at a time
    ↓
one contiguous asset trajectory at a time
    ↓
A1-A8 counters/state
```

No global hash `GROUP BY`, full-file `ORDER BY`, JOIN, or window operation is required merely to discover groups or state trajectories.

Apache Arrow's Parquet APIs support iterative `RecordBatch` reads and column projection so a file can be scanned without materializing the entire dataset in memory.

Sources:

- https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html
- https://arrow.apache.org/docs/python/dataset.html

DuckDB's own performance documentation identifies grouping, joining, sorting, and windowing as blocking/memory-intensive operators. That does not make DuckDB unsuitable generally; it explains why a sequential algorithm aligned to the archive's existing sort order is a better candidate for this particular audit.

Sources:

- https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads
- https://duckdb.org/docs/current/guides/performance/oom

---

## 4. Important methodological distinction

Using the documented sort order to **find group boundaries** is not the same as treating row order inside a tied group as causal order.

The scientific rule remains:

```text
G = equal (market, asset_id, timestamp_received)
```

Members of `G` remain an unordered simultaneous-availability set for A2-A5 classification. Published row order may still be inspected only for the already-defined A6 diagnostic. It must never resolve an otherwise ambiguous group.

Thus a streaming implementation can be an execution-only change if it reproduces the frozen IP-002 metrics exactly.

---

## 5. Candidate streaming algorithm — not yet authorized for a full sample

A final bounded feasibility pilot should test a direct sequential scanner over the **original preserved August Parquet**, not another repartitioned full run.

Candidate design:

1. Read only required columns in bounded Arrow record batches.
2. Assert the observed composite key never decreases while scanning; fail if the documented sort contract is violated.
3. Keep only the current archive-availability group in memory for A2-A6 classification.
4. Keep only the current `(market, asset_id)` state trajectory in memory for A7/A8.
5. Treat price and size as exact fixed-point/scaled integers or exact Arrow decimals; do not convert to binary floating point.
6. Parse `bids`/`asks` JSON only for `book` rows; pmxt documents `book` as a tiny fraction of rows.
7. Compute the global one-hour `timestamp_received` row-count distribution with an exact bounded millisecond counter (at most 3,600,000 in-hour millisecond slots), rather than a full hash aggregation.
8. Keep representative counterexamples bounded and deterministic.
9. Do not perform a full-file `GROUP BY`, `ORDER BY`, JOIN, or window solely to reconstruct grouping/state order.

The implementation may use vectorized Arrow/NumPy operations to locate run boundaries. Performance optimizations are allowed only when exact equivalence is demonstrated.

---

## 6. Final feasibility boundary

This project should not enter an endless sequence of increasingly elaborate recovery engines.

One **streaming feasibility pilot** is justified because it exploits a newly recognized property of the source format that directly matches the scientific grouping/state keys. It is algorithmically different from both failed plans.

If that pilot cannot demonstrate exact equivalence and a conservative projected full-August runtime within its declared budget, then the exact IP-002 pmxt audit should stop as computationally unresolved on the current local environment.

At that point the project should choose between:

- using pmxt for questions that do not require exact intra-timestamp ordering while collecting its own forward sequence-preserving feed for H2; or
- evaluating a separate historical provider with explicit receive ordering / snapshots, with provenance, licensing, depth limits, and reproducibility reviewed before adoption.

A commercial provider such as TickFoundry currently advertises nanosecond receive timestamps, capture sequence numbers, and normalized L2 snapshots, but this is vendor-provided evidence and a paid/proprietary dependency. It is a fallback to evaluate, not an accepted source-of-truth decision.

Sources reviewed 2026-08-28:

- https://tickfoundry.com/docs
- https://tickfoundry.com/pricing

No ADR or production replay rule should change from execution-feasibility results alone.
