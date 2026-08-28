# pmxt Historical Ordering Risk

Status: **audit in progress; no production replay rule is frozen by this document**  
Verified/reviewed: **2026-08-28**

## Main idea

The public pmxt V2 archive is useful historical L2 evidence, but the currently published Polymarket files must **not** be assumed to be an exact arrival-ordered copy of the original WebSocket event stream.

The remaining question is no longer whether ordering ambiguity can exist. Current archive/collector evidence shows that it can. The next research question is how often the published archive loses order information that matters to reconstructed state, and how much usable replay coverage a conservative policy would sacrifice.

This note separates external evidence from the candidate V0 policy being tested by `IP-002-pmxt-ordering-ambiguity-audit.md`. The first full execution attempt was computationally infeasible on two of the three samples; `IP-002R-pmxt-ordering-audit-recovery.md` now governs a bounded memory-safe recovery without changing the scientific definitions.

---

## 1. Evidence established outside this project

### 1.1 Published pmxt V2 archive shape

The pmxt V2 data overview currently documents:

- one Polymarket Parquet object per UTC hour;
- a 16-column typed schema;
- millisecond `timestamp_received` and millisecond Polymarket source `timestamp`;
- sort order `(market, asset_id, timestamp_received)`;
- no published `receive_sequence`, frame identifier, or row-index tie-breaker for Polymarket rows.

Source:

- https://archive.pmxt.dev/docs/v2-data-overview

The documentation describes `timestamp_received` as when the exporter ingested the event. That description is useful provenance, but current collector implementation details below mean this field must not automatically be interpreted as a precise per-frame network receive timestamp for every historical file.

### 1.2 Current pmxt collector explodes WebSocket frames

In the current open-source collector, the wire parser represents a `price_change` as one WebSocket message containing `price_changes[]`. The collector then explicitly explodes that array into one stored `PriceChange` event per entry.

Therefore the published row stream does not preserve the original `price_changes[]` frame boundary.

Source, reviewed at collector commit `cb0f6631556bf460d03594fe20f9bbd020b47d19`:

- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/shared/rust/polymarket-orderbook-rust/src/events.rs

### 1.3 Current exporter has no Polymarket receive-sequence tie-breaker

The current R2 exporter orders Polymarket rows by:

```text
market, asset_id, timestamp_received
```

The same exporter explicitly includes `receive_sequence` and `row_index` for some other venue profiles, but not for the Polymarket profile.

Source at the same collector commit:

- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/services/r2-archive/exporter/run.py

This establishes that the published Polymarket schema does not expose an explicit total-order tie-breaker for rows sharing the same archived observation timestamp.

### 1.4 Current pmxt sink makes `timestamp_received` pipeline-version sensitive

The current Rust sink declares:

```text
timestamp_received DateTime64(3) DEFAULT now64(3)
```

and its insert path omits `timestamp_received`, leaving that value to the ClickHouse default. The current pmxt comparison utility itself describes `timestamp_received` as **batch-flush time** and adds slack because it may lag event time by about a second.

Sources at the same collector commit:

- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/shared/rust/polymarket-orderbook-rust/src/sink.rs
- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/services/polymarket/orderbook-compare/compare_price_changes.py

The current default sink batch size is 5,000 and the default flush interval is 500 ms.

Source:

- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/shared/rust/polymarket-orderbook-rust/src/config.rs

Important applicability limit: this is evidence about the reviewed collector version. Historical pmxt files may have been produced by earlier pipeline versions. The audit must inspect the actual sampled Parquet files rather than retroactively assuming every historical `timestamp_received` has current-code semantics.

### 1.5 Repeated same-key updates inside one original message are observed in current pmxt diagnostics

pmxt's current order-book comparison documentation says its external comparison data retains one row per original WebSocket message with arrays, while its ClickHouse store is exploded to one row per change entry. It reports roughly **66,000** cases in a five-minute slice where the same `(asset_id, source_timestamp_ms, side, price)` key repeats within one WebSocket message.

Source:

- https://github.com/pmxt-dev/polymarket-orderbook-collector/blob/cb0f6631556bf460d03594fe20f9bbd020b47d19/services/polymarket/orderbook-compare/README.md

This is direct evidence that repeated-key multi-change frames are not merely theoretical, even though the much smaller IP-001 live sample did not happen to observe one.

### 1.6 Independent v1/v2 validation also finds heavy same-millisecond multiplicity

A third-party pmxt V2 compatibility/validation project reports that, in a 2,000,000-row overlap sample, 500,905 rows belonged to `(market_id, token_id, source_timestamp_ms)` groups containing more than one distinct event in the same source millisecond. It also reports much weaker agreement for delta-like fields than state-like best-bid/ask fields under naïve millisecond joins.

Source:

- https://github.com/pinglucid/pmxt_v2_adapter/blob/main/VALIDATION.md

Applicability limit: this statistic is grouped by **Polymarket source timestamp**, not `timestamp_received`, and comes from a third-party validation project. It supports the existence of sub-millisecond event multiplicity but does not measure the exact ambiguity rate in our intended archive-availability grouping.

### 1.7 Simulation literature warns against arbitrary simultaneous-event ordering

Discrete-event simulation literature explicitly notes that processing order among simultaneous events can change the resulting simulation state; a deterministic but arbitrary tie-break therefore does not make the modeled history correct.

Reference:

- Kim, Kang, Sagong & Park (1997), *Ordering of simultaneous events in distributed DEVS simulation*, Simulation Practice and Theory 5(3), 253-268, DOI `10.1016/S0928-4869(96)00009-2`.

This literature establishes the generic ordering problem. It does **not** prescribe the Polymarket-specific policy below.

---

## 2. Relevant result from our IP-001 probe

`IP-001-market-feed-contract-probe.md` and its merged experiment established, within the stated bounded live sample, that:

- non-zero `price_change.size` behaves as the new aggregate size at a price level;
- zero size deletes the level;
- BUY updates bids and SELL updates asks;
- no reliable sequence field was established;
- same-asset/repeated-key ordering inside one live frame remained unexercised in that small sample.

The current pmxt collector evidence now shows that repeated same-key entries can occur in original frames. That does not retroactively change the IP-001 result; it supplies external evidence for a shape that IP-001 did not observe.

Internal sources:

- `docs/experiments/market-feed-contract-probe.md`
- `docs/data/market-data-and-replay.md`

---

## 3. Research conclusion we can make now

### Established/supported conclusion

The published pmxt V2 Polymarket Parquet should **not be modeled as a proven exact arrival-ordered event tape**.

Reasons:

1. original multi-entry `price_change` frames are exploded;
2. frame identity is not published;
3. there is no published receive-sequence tie-breaker for Polymarket rows;
4. timestamp precision is milliseconds;
5. current pipeline evidence makes the operational meaning of `timestamp_received` coarser than a guaranteed per-frame network receive time;
6. repeated same-key changes within an original message are empirically observed by pmxt's own diagnostics.

This conclusion does **not** mean pmxt is unusable. It means historical replay must quantify and explicitly handle the information lost by the archive representation.

---

## 4. Candidate V0 historical replay policy — not yet accepted

The following is a **project-derived policy to audit**, not a rule established by pmxt, Polymarket, or the simulation literature.

### 4.1 Archive-availability group

Candidate grouping key:

```text
(market, asset_id, timestamp_received)
```

All state-changing rows in one group would be treated as becoming available together for the historical data source. A simulated strategy would not be allowed to observe or trade between rows in the same group.

`timestamp` (Polymarket source time) remains diagnostic. It is not promoted to a hidden sequence number or used to choose the most favorable ordering.

### 4.2 Order-invariant price-change group

Because IP-001 supports aggregate replacement semantics, pure `price_change` operations are final-state order invariant when, for every `(side, price)` key in the group, all replacements set that key to the same final size.

Examples:

```text
BUY 0.40 -> 10
BUY 0.41 -> 20
```

commute because they touch different levels.

```text
BUY 0.40 -> 10
BUY 0.40 -> 10
```

is idempotent.

But:

```text
BUY 0.40 -> 10
BUY 0.40 -> 25
```

is order-sensitive because the final size depends on which replacement is last.

### 4.3 Full snapshots mixed with changes

A group containing one or more full `book` snapshots and deltas is candidate order-invariant only if all snapshots are identical and every change in the group is already idempotent with respect to that snapshot's final state. Otherwise snapshot-vs-delta order can change the final book and the group is order-ambiguous.

### 4.4 Tick-size state

Tick-size metadata is logically separate from L2 price-level state. A single tick change or exact duplicate tick transition is candidate deterministic. Multiple distinct tick transitions sharing one archive-availability group are order-ambiguous unless a stronger ordering rule is independently justified.

### 4.5 Last-trade events

`last_trade_price` is auxiliary and does not mutate L2 depth. Ambiguous ordering among last-trade rows must not invalidate an otherwise identifiable L2 book merely for V0 taker-style execution. Any future experiment that requires exact historical last-trade ordering must define its own validity rule.

### 4.6 Candidate fail-closed recovery

If an archive-availability group is state-order-ambiguous, the simplest candidate V0 behavior is:

```text
VALID -> INVALID -> next accepted full book -> VALID
```

No execution snapshot would be exposed while invalid.

This is intentionally **not frozen yet** because its scientific cost depends on how often ambiguity occurs and how quickly full snapshots recover state.

---

## 5. What the audit must measure before an ADR change

The next bounded offline audit must answer:

1. How many published rows share `(market, asset_id, timestamp_received)` with another state-changing row?
2. What is the group-size distribution?
3. How often do tied groups contain repeated `(side, price)` keys?
4. Of repeated keys, how often are replacements idempotent versus different-size/order-sensitive?
5. How often do full `book` snapshots share an archive-availability group with price changes or other snapshots, and are those groups final-state invariant?
6. How often do multiple tick transitions share a group?
7. If order-ambiguous groups invalidate state until the next full snapshot, what event-time and wall-clock coverage is lost inside the audited windows?
8. Does the actual `timestamp_received` structure of early/mid/late CLOB V2 files look like precise per-event arrival time, coarse/batched availability time, or a mixture consistent with pipeline evolution?

The audit must report source-timestamp structure inside ties as a diagnostic, but it must not use source timestamps or Parquet row order to make an ambiguous group appear ordered.

---

## 6. Separate book validity from execution-metadata completeness

A related architectural refinement is under consideration:

```text
BookState:
UNINITIALIZED / VALID / INVALID

ExecutionMetadataState:
COMPLETE / INCOMPLETE / INVALID
```

Reason: a reconstructable L2 state can remain useful for H1 forecasting evaluation even when point-in-time fee, minimum-order, or other execution metadata is missing. H2 execution claims should require both a valid book and sufficiently complete execution metadata.

This separation is **not accepted by this note**. It will be decided together with the historical replay contract after the ordering audit and metadata-source research.

---

## 7. First IP-002 execution attempt: feasibility failure, not scientific conclusion

The first full IP-002 execution attempt downloaded and hash-preserved all three predeclared hourly samples and ran the frozen A1-A8 analyzer under a 25 GiB DuckDB memory limit.

Its final atomic result was preserved with SHA-256:

```text
1fa6594ce2f46158af127d5e19966b6af5c69f1e38d22b3be1a6661cb83c2513
```

Outcome:

- June completed successfully;
- May failed with a DuckDB out-of-memory error;
- August failed with a DuckDB out-of-memory error;
- overall result: `UNRESOLVED` because IP-002 requires at least two successfully audited samples.

This is an **execution-feasibility failure**, not evidence that the candidate replay policy is good or bad. The raw Parquet evidence remained intact, and no A9 conclusion was authorized.

The run also exposed a recovery weakness: expensive per-file work was not independently checkpointed before the final combined artifact, so completed units were unnecessarily coupled to one long-running process.

`IP-002R-pmxt-ordering-audit-recovery.md` therefore freezes the scientific definitions and changes only the execution strategy: deterministic per-asset sharding, bounded resource profiling before the full recovery, immutable shard checkpoints, and explicit memory/time/storage stop rules. June is reused; recovery targets August only unless preservation checks fail.

---

## 8. Decision boundary

Do **not** yet:

- amend ADR-001 to define pmxt tie handling as accepted;
- implement a production replay engine;
- use Parquet row order as historical causal order;
- use source timestamp as an undocumented sequence number;
- claim `timestamp_received` has one universal per-event meaning across all pmxt historical versions;
- discard pmxt merely because some groups are ambiguous;
- interpret the first IP-002 OOM failures as scientific evidence about ordering quality.

The next step is the bounded recovery in `IP-002R-pmxt-ordering-audit-recovery.md`. Its result will either complete the original IP-002 evidence requirement or establish that the audit remains computationally impractical under the declared budget.
