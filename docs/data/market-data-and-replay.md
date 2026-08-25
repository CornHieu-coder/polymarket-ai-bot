# Market Data and Replay Protocol

Status: **V0 research specification with explicitly unresolved feed-semantics probes**  
Verified against sources: **2026-08-26**

## Main idea

A historical strategy may act only on market state that can be reconstructed from evidence available at that point in time. Unknown or corrupted market state must fail closed rather than being replaced with a convenient price, stale book, or invented liquidity.

This document separates:

- facts established by first-party Polymarket documentation;
- empirical findings from external research/data archives;
- project-derived replay rules;
- open questions that must be measured before the full replay engine is implemented.

---

## 1. Evidence basis

### 1.1 First-party Polymarket facts

The public Market WebSocket exposes full `book` snapshots and incremental `price_change` events, plus `last_trade_price`, `tick_size_change`, and optional best-bid/ask and lifecycle events. The documented `book` payload contains an asset ID, market/condition ID, bids, asks, source timestamp, and hash. A `price_change` contains price-level changes with asset ID, price, size, side, hash, best bid, and best ask. The current documentation describes `price_change` as an order-book price-level delta update. Polymarket's agent documentation explicitly states that a `price_change` with size `"0"` removes that price level.

Sources:

- https://docs.polymarket.com/api-reference/wss/market
- https://github.com/Polymarket/agent-skills/blob/main/websocket.md

The CLOB REST `GET /book` endpoint returns a complete order-book summary including bids, asks, timestamp, hash, minimum order size, tick size, negative-risk flag, and last trade price.

Source:

- https://docs.polymarket.com/api-reference/market-data/get-order-book

The displayed Polymarket probability is not necessarily executable. Buying crosses the ask and selling crosses the bid.

Source:

- https://docs.polymarket.com/concepts/prices-orderbook

CLOB V2 became production on **2026-04-28**. V1 SDKs/orders are not production-compatible after the cutover, and V2 changed contracts, backend behavior, collateral, order fields, and fee handling.

Sources:

- https://docs.polymarket.com/v2-migration
- https://docs.polymarket.com/changelog

Current fees are determined per market at match time. V2 exposes CLOB-level fee parameters through market information; takers may pay price-dependent fees while makers are not charged Polymarket trading fees.

Sources:

- https://docs.polymarket.com/trading/fees
- https://docs.polymarket.com/trading/clients/public

The matching engine may undergo restarts and temporary unavailable/restricted modes. Therefore continuous availability cannot be assumed by a live collector.

Source:

- https://docs.polymarket.com/trading/matching-engine

### 1.2 External empirical evidence

The pmxt V2 archive records the public Polymarket CLOB WebSocket market stream in hourly Parquet files. It stores both Polymarket's source timestamp and the exporter's receive timestamp, and includes `book`, `price_change`, `last_trade_price`, and `tick_size_change`. Its documentation says V2 was created partly because its earlier archive missed roughly half of live markets and that V2 uses redundant exporters to reduce gaps.

Important naming distinction: **pmxt “V2” is the archive format/collector generation, not Polymarket CLOB V2.** pmxt V2 begins on 2026-04-13, while Polymarket CLOB V2 cut over on 2026-04-28.

Source:

- https://archive.pmxt.dev/docs/v2-data-overview

Dubach (2026) studies Polymarket using a continuous tick-level public WebSocket archive joined to authoritative on-chain trade records. It reports sub-50 ms median archive ingestion delay with a multi-second tail and finds that trade direction inferred from the public order-book feed agrees with on-chain ground truth only about 59-62% of the time. Therefore feed-derived trade direction is not authoritative for microstructure measurements that require aggressor direction.

Source:

- https://arxiv.org/abs/2604.24366

Cheng, Yang, and Zou (2026) reconstruct continuous Polymarket order-book states for NBA markets and find that executable opportunity size is often constrained by shallow book depth, reinforcing that execution-aware backtests must use order-book depth rather than only historical prices.

Source:

- https://arxiv.org/abs/2605.00864

### 1.3 Project-derived choices

The following are our V0 methodology, not claims proved by the cited sources:

1. Historical execution replay will initially be restricted to **CLOB V2-era data on or after 2026-04-28** unless a separate V1 regime adapter is explicitly researched and implemented.
2. pmxt may bootstrap historical L2 research, but it is a third-party archive and must retain provenance. Platform semantics come from first-party Polymarket sources, not from pmxt assumptions.
3. Our own future collector will preserve the exact raw WebSocket frame before normalization so unknown/new fields are not lost.
4. A reconstructed book is either `VALID`, `INVALID`, or `UNINITIALIZED`; there is no silent “best effort” state for simulated execution.
5. A simulated strategy may execute only against a `VALID` snapshot.
6. Known data loss, contradictory ordering, malformed state-changing events, or a connection gap invalidates the affected asset until a fresh full `book` snapshot reinitializes it.
7. No execution fill is simulated from a stale last-known book while state is invalid.
8. Prices and sizes are represented with decimal/fixed-point arithmetic, not binary floating-point, in the canonical reconstruction layer.

---

## 2. Source-of-truth hierarchy

Different questions have different authoritative sources.

### Visible order-book state

Primary live source:

1. Polymarket Market WebSocket raw events.
2. Polymarket REST `GET /book` as an independent resynchronization/validation snapshot.

Historical bootstrap:

3. pmxt V2 archive, with archive provenance and coverage validation.

### Market/trading parameters

Primary source:

1. Polymarket CLOB/Gamma market metadata at the relevant observation time, including minimum order size, tick size, negative-risk state, fee configuration, and trading status where available.

Going forward, these responses should be archived point-in-time rather than reconstructed later from today's API state.

### Historical trade direction or authoritative realized fills

When an experiment genuinely requires aggressor direction or authoritative settlement of historical trades, prefer on-chain exchange `OrderFilled`/settlement evidence rather than inferring direction from the public feed. This is not required merely to simulate consuming visible L2 liquidity.

---

## 3. Canonical raw-event contract

### Main idea

Raw observations are evidence. Parsed books are derived state. Raw evidence must remain immutable.

For our own collector, every received frame should retain at least:

- `source_name` — e.g. `polymarket_market_ws`;
- `source_version` or protocol regime — e.g. `clob_v2`;
- `collector_id` and `collector_session_id`;
- `received_at` — local monotonic/wall-clock receive time in UTC with the highest practical precision;
- `ingest_sequence` — monotonically increasing sequence assigned by our collector to preserve local arrival order;
- `source_timestamp` — Polymarket's timestamp when supplied;
- `market_id` / condition ID when supplied;
- `asset_id` when supplied;
- `event_type` when parseable;
- `raw_payload` — exact WebSocket frame bytes/text;
- `raw_payload_hash` — integrity/deduplication aid;
- parser version;
- parse status/error without mutating the raw payload.

For third-party pmxt data, provenance must additionally include:

- archive name/version;
- file/object key;
- row index or equivalent reproducible row locator where practical;
- pmxt `timestamp_received`;
- pmxt source `timestamp`;
- note that pmxt stores a normalized/flattened schema rather than our exact raw WebSocket frame.

Raw records are never deleted merely because the derived layer classifies them as duplicates or malformed.

---

## 4. Two clocks: source time and observation time

### Main idea

The event's exchange/source time and the time a collector could actually observe it are different quantities and must not be collapsed.

Store both:

- `source_timestamp`: timestamp in the Polymarket event;
- `received_at`: timestamp when the collector received the frame.

For our **no-lookahead strategy replay**, the default availability clock is **observation time**: an event may influence a simulated decision only after it has been observed by the replay's data source.

For our own forward collector, this is our actual receive time.

For pmxt historical replay, `timestamp_received` is the pmxt exporter's observation time, not a claim about what our own machine would have received. It is therefore a conservative/provenance-specific proxy and must be labeled as such.

`source_timestamp` remains necessary for diagnostics, event-time analysis, and detection of suspicious backwards/out-of-order source timestamps.

A separate exchange-time replay may later be supported for microstructure analysis, but results from that mode must not be mislabeled as an executable no-lookahead strategy replay.

---

## 5. Order-book reconstruction state machine

Each `asset_id` has independent reconstructed state.

### States

- `UNINITIALIZED` — no accepted full snapshot yet.
- `VALID` — initialized and no known integrity violation since the latest accepted full snapshot.
- `INVALID` — a known integrity problem occurred; execution snapshots are prohibited until a new accepted full snapshot.

### Full `book` snapshot

A full `book` event replaces the entire bid and ask maps for that asset.

Validation before marking `VALID` should include, at minimum:

- market and asset identifiers are nonempty and consistent with the subscription;
- prices and sizes parse exactly as decimals;
- prices are within valid contract bounds;
- sizes are non-negative;
- duplicate price levels in one side are either rejected or deterministically consolidated only if first-party semantics explicitly justify consolidation;
- best bid/ask derived from levels are internally coherent;
- source timestamp is parseable when present.

After acceptance, the snapshot becomes the new integrity boundary and can recover an `INVALID` asset.

### `price_change`

First-party material establishes that this is a price-level update and that size zero removes the level. The exact interpretation of a non-zero `size` as the post-update aggregate level size rather than an arithmetic increment is **expected but must be confirmed by IP-001 before the full replay engine relies on it**.

Planned V0 behavior once confirmed:

- `side == BUY` updates the bid map;
- `side == SELL` updates the ask map;
- size `0` deletes the price level;
- non-zero size replaces the aggregate size at that price level;
- materialized bids are sorted descending by price;
- materialized asks are sorted ascending by price;
- after applying an event, reconstructed best bid/ask are checked against event-provided `best_bid`/`best_ask` where present.

A mismatch between reconstructed and event-provided best bid/ask invalidates the asset unless the mismatch is explained by a documented feed-semantic exception.

### `tick_size_change`

Track tick size as point-in-time market state when known. A change must be applied in observed order. If an event's `old_tick_size` contradicts the current known tick size and the event is not an exact duplicate, mark the asset/market metadata state invalid until authoritative metadata is refreshed.

### `last_trade_price`

Treat as informational market state, not as a substitute for book depth. Do not use its `side` as authoritative historical aggressor direction for microstructure research without stronger evidence.

### `best_bid_ask`

Use as a validation/diagnostic signal and convenient top-of-book feed, not as a replacement for L2 when execution depth is required.

---

## 6. Duplicate and ordering policy

### Duplicate events

Do not delete raw duplicates. In the derived layer:

- exact repeated full snapshots are idempotent replacements;
- exact repeated price-level replacements are idempotent;
- exact repeated tick changes may be treated as no-ops if current state already equals `new_tick_size`;
- duplicate classification must retain provenance and the rule/version that classified it.

### Out-of-order events

The currently documented Polymarket Market WebSocket schema does not expose a required global sequence number. Optional fields observed in third-party datasets must not be assumed to exist universally until IP-001 measures the current live feed.

Therefore V0 does **not** claim that completeness/order can be proven from timestamps alone.

Project rule:

- preserve collector arrival order with `ingest_sequence`;
- do not reorder state-changing events after the fact merely to improve a backtest;
- if a state-changing event arrives with a materially older source timestamp than the last applied state-changing event for the same asset, mark the state invalid unless an empirically validated buffering/reordering rule is later adopted;
- if multiple conflicting updates for the same asset/side/price are observationally indistinguishable in ordering, fail closed until the next full snapshot rather than choosing the favorable interpretation.

---

## 7. Gap and reconnect policy

### Own live collector

Known events that invalidate state include:

- WebSocket disconnect;
- subscription loss;
- local receive queue overflow/drop;
- parser failure for a state-changing message;
- write failure where the raw event cannot be durably persisted;
- integrity-check mismatch.

On reconnect/resubscribe, request/accept an initial full order-book dump. The asset remains `INVALID`/`UNINITIALIZED` until that full `book` is accepted.

No strategy execution may use the last-known book during a known gap.

### Third-party archives

Silence alone is not proof of data loss because a market may genuinely be quiet. Historical archive coverage therefore requires separate diagnostics, including:

- missing/corrupt files;
- archive health/gap metadata when available;
- suspicious coverage drops across many assets;
- consistency failures between reconstructed top-of-book and event-provided best bid/ask;
- full-snapshot disagreements;
- known archive-version limitations.

A detected archive gap invalidates affected intervals until a later full snapshot re-establishes state.

Residual limitation: without an authoritative sequence/gap marker, undetected loss of a deeper-book update that does not affect top-of-book may remain possible. This limitation must be disclosed in experiment reports rather than hidden.

---

## 8. `MarketSnapshot(t)` semantics

### Main idea

`MarketSnapshot(t)` is not “the closest historical price.” It is the latest **valid, observed, reconstructable** L2 state that the simulated strategy was permitted to know by time `t`.

For default no-lookahead replay:

`MarketSnapshot(t)` may include only events satisfying the replay source's observation-time rule and already applied in permitted arrival order by `t`.

It contains at least:

- market/condition ID;
- asset ID;
- snapshot/replay time;
- latest source timestamp represented;
- latest observation timestamp represented;
- validity status and, if invalid, reason;
- sorted L2 bids and asks;
- derived best bid, best ask, spread;
- latest known tick size, minimum order size, negative-risk flag, and fee parameters when point-in-time evidence exists;
- latest known last-trade information as non-authoritative auxiliary data;
- lineage/provenance references to the raw events/metadata used.

A strategy receives **no executable snapshot** when state is invalid. The execution simulator must not silently fall back to midpoint, last trade, current REST data, or a future snapshot.

---

## 9. CLOB regime boundary

pmxt V2 archive coverage begins before Polymarket CLOB V2 production cutover. The two “V2” labels refer to different systems.

V0 execution research therefore uses:

- `2026-04-28` onward for CLOB V2 execution semantics;
- earlier pmxt rows only for explicitly labeled V1-compatible research, which is currently out of scope.

This prevents a single replay adapter from silently mixing two exchange regimes.

---

## 10. Point-in-time market metadata

Order-book events alone are insufficient to reproduce every order-validity and fee rule.

Going forward, archive point-in-time CLOB/Gamma metadata at market discovery/subscription and at strategy decision time where practical, including:

- token/outcome mapping;
- condition/event identifiers;
- minimum order size;
- minimum tick size;
- negative-risk configuration;
- fee parameters/schedule;
- whether orders are accepted / market trading state;
- metadata retrieval timestamp and raw response.

Tick-size WebSocket events remain part of the event stream, but the metadata snapshot supplies an authoritative initialization/reference.

For historical pmxt periods where point-in-time metadata cannot be established, do not silently substitute current values. Mark the missing field/interval and either restrict the experiment to behavior that does not depend on it or perform a separately documented sensitivity/assumption analysis.

---

## 11. Hashes and integrity checks

Polymarket exposes order-book hashes and its official client code contains a server-compatible order-book-summary hash function for REST summaries. However, the current public WebSocket documentation does not fully specify the semantics of every `hash` field on `book` and `price_change` events.

Therefore:

- preserve all hashes verbatim;
- do not treat a `price_change.hash` as a sequence number;
- do not claim hash-based delta validation until empirically confirmed;
- IP-001 must test whether reconstructed state can reproduce current live hashes and under what payload/metadata rules.

If hash semantics are validated, they may become a stronger integrity check in a later ADR.

Relevant first-party code/source:

- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py
- https://github.com/Polymarket/py-sdk

---

## 12. Validation strategy

Validation is layered; no single test proves the archive is complete.

### Schema/contract validation

- sample current raw WebSocket payloads;
- compare observed fields/event types to first-party docs;
- retain unknown fields rather than rejecting forward-compatible additions.

### Reconstruction validation

- initialize from full `book`;
- apply observed price-level changes;
- verify event-provided best bid/ask against reconstructed best bid/ask;
- test zero-size deletion;
- test multiple changes in one message;
- test reconnect behavior and whether a fresh full dump arrives;
- investigate optional sequence fields and hash semantics.

### REST cross-check

Periodically fetch official `GET /book` during live capture. Because REST and WebSocket observations are asynchronous, a mismatch is not automatically proof of corruption. Prefer comparisons where timestamps/hashes or recent-state lineage allow a defensible alignment; otherwise use REST primarily as a resynchronization source and diagnostic.

### Archive coverage validation

For pmxt:

- verify requested hourly objects exist and decode;
- inspect event counts/asset counts for abrupt coverage anomalies;
- record pmxt archive version and file manifest;
- use health/gap diagnostics where historically available;
- exclude known invalid intervals.

### Trade-ground-truth validation

Use on-chain filled-order evidence only when the experiment requires authoritative historical trade direction/fill facts. Do not infer aggressor direction from WebSocket `last_trade_price.side` for research claims where direction matters.

---

## 13. Decimal arithmetic

Canonical prices and sizes must use exact decimal/fixed-point representation. Binary floating-point is not permitted in the state reconstruction/accounting boundary.

This is a project engineering decision motivated by exact tick/size semantics and supported by Polymarket's own Python SDK models/tests, which preserve `Decimal` precision for order-book frames.

Source:

- https://github.com/Polymarket/py-sdk/blob/main/tests/unit/test_frames_order_book.py

Floating-point conversion may be used later for analytics/ML after an explicit boundary, but must not alter canonical replay or accounting values.

---

## 14. Open questions that must not be guessed

These require a bounded empirical feed-contract probe before the full replay engine is authorized:

1. Does the current production Market WebSocket expose any sequence/sequence-id field not present in the public documented schema, and is it reliable enough for gap detection?
2. For non-zero `price_change.size`, does the live feed consistently mean **post-update aggregate size at that level**, as expected?
3. What exactly does each `hash` field mean on current `book` and `price_change` events, and can the official client hash algorithm reproduce it?
4. When `price_changes` contains multiple entries, can there be multiple conflicting updates to the same asset/side/price in one frame, and does order matter?
5. Which optional market metadata fields are actually included in current live `book` frames versus requiring REST/Gamma initialization?
6. After disconnect/resubscribe with `initial_dump` enabled/defaulted, is a fresh full `book` reliably observed for every subscribed asset before deltas?
7. Can point-in-time fee/min-order metadata for the historical pmxt period be recovered with sufficient reliability for H2 execution simulation?
8. What historical gap metadata can be obtained from pmxt beyond the Parquet files themselves?

These questions are the purpose of `IP-001-market-feed-contract-probe.md`.

---

## 15. V0 decisions frozen now

The following do **not** depend on the open probe results and are accepted for V0:

- immutable raw evidence + derived reconstruction;
- source and observation timestamps are separate;
- no-lookahead replay uses observation availability;
- full snapshots are the only recovery boundary after known state corruption/gaps;
- invalid/unknown state cannot generate simulated fills;
- L2 depth, not displayed midpoint/history price, is required for execution simulation;
- pmxt is third-party historical bootstrap, not authoritative platform semantics;
- CLOB V2 replay baseline begins 2026-04-28;
- trade direction is not inferred from the public feed when authoritative direction matters;
- exact decimal arithmetic at the canonical data boundary;
- provenance and validity state propagate into every `MarketSnapshot`.
