# Market-Data Audit Case Study — Interview Recall

Purpose: a short, memorable story for interviews. This is **not** the authoritative replay specification. For exact methodology and evidence, use the linked data/research documents.

## The problem

We wanted to backtest against historical Polymarket order books without quietly inventing an event order.

The historical pmxt archive is useful, but multiple rows can share the same observation timestamp and it does not publish a sequence number that proves which tied update happened first. If two tied updates touch the same price level differently, choosing an arbitrary order can change the reconstructed book.

So the research question was simple:

> How often does missing ordering information actually matter, and how much historical replay would we lose if we fail closed whenever the order cannot be proved?

The scientific rules were fixed before the expensive runs. What changed across attempts was only **how we computed the answer**.

---

## Attempt 1 — one large analytical query

### What we tried

Analyze each full hourly Parquet file with DuckDB using large grouping/state queries.

### What happened

- June completed.
- May and August exceeded a 25 GiB DuckDB memory limit.
- The audit therefore remained unresolved.

### What we learned

A method can be scientifically correct and still be a bad experiment if it is not practical on the machine that must run it.

We also learned that long-running research jobs need independent checkpoints. Completed work should not depend on one huge process surviving until the very end.

**Memory hook:** **Correct but not runnable is not finished research.**

---

## Attempt 2 — hash sharding

### What we changed

We split complete `(market, asset)` trajectories into 32 deterministic shards. This kept every asset's state history together while reducing memory pressure.

### What improved

Memory dropped dramatically: the pilot peaked around 3.2 GiB instead of exhausting 25 GiB.

### What failed

The pilot shard looked fast, but another shard took roughly ten times longer. The projected remaining runtime exceeded the predeclared 90-minute budget, so the run stopped.

The important mistake was assuming **row count was a good proxy for computational cost**. It was not. The cost also depended on the structure of assets, groups, snapshots, and state transitions inside a shard.

### What we learned

A pilot only helps if it tests the resource risk that can actually dominate the full run.

And when a predeclared feasibility gate fails, do not move the goalposts just because a lot of work has already been invested.

**Memory hook:** **Fixing memory does not automatically fix the algorithm.**

---

## Attempt 3 — sequential streaming

### What we noticed

pmxt already documents the Parquet files as physically sorted by:

```text
(market, asset_id, timestamp_received)
```

That is also the audit's grouping key, and `(market, asset_id)` is the unit of book-state tracking.

Instead of globally grouping or shuffling tens of millions of rows, we could scan the file in order and retain only:

- the current tied event group;
- the current asset's state;
- bounded exact counters.

Physical order was used only to locate group boundaries. Rows inside one tied group were still treated as **unordered**, so the scientific rule did not change.

### What improved

The streaming pilot used only about 0.4 GiB of RAM and no temporary disk spill.

### What still failed

Two mechanically selected ~2-million-row windows took about 307 s and 209 s. The conservative projection for the complete August hour was about **5.3 hours**, above the predeclared 30-minute limit.

So streaming solved the memory problem, but not the total CPU cost of performing the exact audit over ~83 million rows.

The correct decision was to stop the local exact audit rather than invent a fourth engine.

**Memory hook:** **The best data-layout optimization still does not justify an experiment whose value is lower than its cost.**

---

## The main engineering lessons

1. **Research rigor includes feasibility.** Estimate runtime, RAM, storage, and recovery cost before the full experiment.
2. **Separate methodology from execution.** We changed DuckDB → sharding → streaming without changing what counted as an ambiguous event.
3. **Use predeclared stop rules.** They prevent sunk-cost reasoning from turning a bounded experiment into an open-ended optimization project.
4. **Understand the physical data layout.** The biggest algorithmic improvement came from noticing that the file was already sorted by the key we needed.
5. **A pilot must test representative cost, not just convenient size.** Row count alone did not predict shard runtime.
6. **Checkpoint expensive independent units.** A failure late in a run should not erase valid earlier work.
7. **Negative results are useful.** We learned the historical source/analysis path was not worth further local optimization under our budget.

---

## 60-second interview answer

> "I had to validate a historical order-book source before trusting it for a trading backtest. The hard part was that tied updates did not have a reliable sequence number, so I designed a conservative audit that marked state invalid whenever the final book depended on unknown ordering. My first implementation used large DuckDB aggregations and ran out of memory. I redesigned it with deterministic sharding, which fixed memory but exposed a runtime problem. Then I noticed the Parquet was already sorted by the exact grouping key, so I rewrote the computation as a bounded-memory stream. That reduced RAM to roughly 400 MB, but the exact full audit still projected to about five hours, above a limit I had declared in advance. I stopped rather than moving the goalposts. The main lesson was that research correctness includes computational feasibility: you should separate the scientific question from the execution algorithm, measure resource risk early, and be willing to stop when the cost of proving something exceeds its value."

---

## Short prompts to remember the story

```text
Ordering ambiguity
    -> exact audit
    -> monolithic: OOM
    -> sharding: memory fixed, runtime bad
    -> streaming: memory excellent, CPU still too expensive
    -> stop rule
    -> choose a better historical source / forward collection
```

If asked what you would improve next:

> "I would not keep optimizing the same local audit. I would either qualify a historical feed that preserves receive ordering explicitly or collect my own forward raw feed with sequence-preserving provenance, then spend engineering effort on the actual research hypotheses rather than on recovering information the archive discarded."

## Authoritative sources

- `docs/data/pmxt-historical-ordering-risk.md`
- `docs/data/pmxt-ordering-audit-execution-feasibility.md`
- `docs/implementation-packets/IP-002-pmxt-ordering-ambiguity-audit.md`
- `docs/implementation-packets/IP-002R-pmxt-ordering-audit-recovery.md`
- `docs/implementation-packets/IP-002S-pmxt-ordering-streaming-pilot.md`
