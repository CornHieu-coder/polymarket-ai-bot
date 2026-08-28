# pmxt Ordering Audit — Execution Feasibility

Status: **local exact audit stopped; scientific replay policy remains unresolved**  
Verified/reviewed: **2026-08-28**

## Main idea

Three execution plans for the exact IP-002 audit have now failed their predeclared feasibility gates. The project should **stop local exact recovery over the same pmxt hourly files** rather than create a fourth engine, increase resources, or relax the scientific definitions.

The original IP-002 scientific question remains valid: quantify how much state-order ambiguity in published pmxt V2 Polymarket data would cost a conservative fail-closed historical replay. No A9 conclusion has been authorized.

The execution result is instead:

> Recovering that exact answer from this archive representation is not worth further local computation under the project's declared time/resource budget.

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

The 32-shard pilot initially looked strong:

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

The row-count projection did not generalize. Shard 2 required:

```text
591.329 s
```

Using that observed rate conservatively across the 28 then-missing shards projected:

```text
275.954 min
```

which exceeded IP-002R's binding 90-minute limit. The run stopped during shard 3 as `RECOVERY_NOT_FEASIBLE`.

Preserved recovery state:

- verified completed shard checkpoints: 0, 1, 2, and pilot shard 18;
- peak RSS remained 3.155 GiB;
- recovery storage at stop: 796,776,376 bytes;
- original raw Parquet and first atomic result remained unchanged;
- complete offline suite at stop: 105/105 passed;
- completion-infrastructure commit: `6993a180826ac7fb2f3b593256056cc489f54ffa`.

### Research interpretation

Sharding solved the memory problem but not the runtime problem. Row count was an inadequate predictor of shard cost; event/group/asset-state structure materially affected runtime.

That finding is why more hash shards were rejected rather than tried indefinitely.

---

## 3. IP-002S — sequential streaming over the source sort order

### Why this attempt was justified

pmxt documents each V2 Polymarket hourly Parquet as sorted by:

```text
(market, asset_id, timestamp_received)
```

which is exactly IP-002's archive-availability grouping key, with `(market, asset_id)` also being the complete A7/A8 state-trajectory prefix.

Source reviewed 2026-08-28:

- https://archive.pmxt.dev/docs/v2-data-overview

This enabled a materially different algorithm:

```text
sorted Parquet
    ↓ bounded Arrow batches
one contiguous availability group at a time
    ↓
one contiguous asset trajectory at a time
    ↓
exact A1-A8 counters/state
```

Physical row order was used only to locate group/asset boundaries. Rows sharing one equal `(market, asset_id, timestamp_received)` key remained an unordered logical set for ambiguity classification.

### Exactness / test result

The streaming-specific suite passed:

```text
9/9 passed in 5.496 s
```

The complete offline suite passed:

```text
114/114 passed in 30.921 s
```

A direct preserved-recovery-shard comparison was not permitted because the preserved hash-sharded files exhibited physical sort regressions. Rewriting/sorting them solely to manufacture a comparison would have violated the test boundary. The original August W1/W2 source windows themselves passed the monotonic sort assertion.

### Deterministic resource windows

#### W1 — beginning window

- physical range: `[0, 2,000,028)`;
- rows: 2,000,028;
- complete assets: 3,885;
- row groups: 0–1;
- runtime: 307.311145 s;
- Arrow batch size: 65,536;
- batches read/analyzed: 31 / 31;
- maximum logical group: 205 rows;
- maximum asset: 103,246 rows;
- peak process-tree RSS: 428,978,176 bytes;
- temporary-storage growth: 0 bytes.

#### W2 — midpoint window

- physical range: `[41,353,141, 43,431,984)`;
- rows: 2,078,843;
- complete assets: 3,306;
- row groups: 39–41;
- runtime: 208.728061 s;
- Arrow batch size: 65,536;
- batches read/analyzed: 39 / 33;
- maximum logical group: 255 rows;
- maximum asset: 166,416 rows;
- peak process-tree RSS: 422,998,016 bytes;
- temporary-storage growth: 0 bytes.

The broader sort checks covered:

- W1: `[0, 2,000,029)` — monotonic;
- W2: `[40,894,464, 43,431,985)` including the discarded midpoint asset — monotonic.

Maximum observed RSS across the pilot was about 409 MiB.

Combined W1+W2 analysis time:

```text
516.039206 s
```

which passed the 600-second combined-window gate.

### Binding runtime gate failure

Measured rates:

```text
r1 = 0.00015365342150212177 s/row
r2 = 0.00010040588000152012 s/row
```

Using the predeclared conservative formula:

```text
max(r1, r2) * 82,705,648 * 1.5
```

gave:

```text
19,062.008689 s
= 317.700 min
≈ 5.295 h
```

The packet required projected full-August runtime <= 30 minutes. Therefore final status was:

```text
STREAMING_NOT_FEASIBLE
```

No full-August streaming run, A9, final experiment report, ADR change, push/PR, or production replay followed from this result.

Local streaming implementation commit reported by Codex:

```text
871a73be4994dbb61ed4aa96c2c03503c97a3286
```

---

## 4. What the three attempts teach us

### Memory was not the final bottleneck

The sequence was:

```text
monolithic  -> memory failure
sharding    -> memory safe, runtime failure
streaming   -> very low memory, CPU/runtime failure
```

The streaming algorithm reduced peak memory from multi-GiB/25-GiB failure territory to roughly 0.4 GiB and eliminated temporary spill, yet the exact audit still projected to ~5.3 hours.

That means the remaining cost is largely the actual per-row/per-group/per-asset scientific work required by A1-A8, not merely avoidable global sorting or hashing.

### Scientific methodology and execution strategy stayed separate

Across all three attempts the project did **not** change:

- archive-availability grouping key;
- source-time treatment;
- tied-group ambiguity rules;
- A7 validity/recovery semantics;
- A8 cadence grids;
- predeclared scientific sample dates.

Only the computational method changed. That separation is what lets the negative feasibility result be interpreted cleanly.

### Stop rules prevented sunk-cost drift

The correct response to the streaming failure is not to raise the 30-minute gate after seeing the result. The project explicitly declared the streaming pilot to be the final local exact-recovery engine.

---

## 5. Final local decision boundary

Do **not**:

- create a fourth exact pmxt audit engine;
- add 64/128+ hash shards to evade the previous stop rule;
- raise RAM/CPU simply to force completion;
- loosen the scientific ambiguity rules;
- reinterpret pmxt row/source time as a hidden sequence;
- claim the candidate fail-closed policy has low or high coverage cost from these execution failures.

Instead, move the source decision forward:

1. qualify a historical source that explicitly preserves provider-observed receive ordering and usable L2 snapshots; and
2. design our own forward sequence-preserving raw collector so future evidence is not dependent on recovering ordering from a lossy archive.

A candidate third-party source is evaluated separately in `historical-data-source-strategy.md`; no provider is accepted by this feasibility note.

No ADR or production replay rule changes from execution-feasibility evidence alone.
