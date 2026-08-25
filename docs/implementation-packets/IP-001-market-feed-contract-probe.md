# IP-001: Market Feed Contract Probe

## Goal

Build a **read-only research probe**, not the production collector or replay engine, that samples the current Polymarket CLOB V2 public market-data interfaces and empirically resolves the feed-contract questions listed in `docs/data/market-data-and-replay.md`.

The probe must preserve raw evidence and produce a reproducible report. It must not place, sign, prepare, or simulate live orders.

## Research/design documents to read first

- `docs/research/principles.md`
- `docs/research/assumptions.md`
- `docs/data/market-data-and-replay.md`
- `docs/design/decisions/ADR-001-fail-closed-event-sourced-replay.md`
- `docs/design/execution-model.md`

Primary external references:

- https://docs.polymarket.com/api-reference/wss/market
- https://github.com/Polymarket/agent-skills/blob/main/websocket.md
- https://docs.polymarket.com/api-reference/market-data/get-order-book
- https://docs.polymarket.com/v2-migration
- https://docs.polymarket.com/market-data/overview
- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py

## Evidence basis

### Established by external evidence

- The Market WebSocket is public and supplies full `book` snapshots and `price_change` level updates.
- Zero-size `price_change` removes the affected price level.
- REST `GET /book` supplies a current full order-book summary and execution metadata.
- CLOB V2 is production from 2026-04-28.

### Suggested by external evidence but requiring current-production confirmation

- Non-zero `price_change.size` represents the post-update aggregate size at that level.
- Hashes may permit stronger reconstruction-integrity validation.
- Some current feed payloads may expose optional fields beyond the public minimal schema.

### Project-derived choices

- Use a bounded live probe before implementing the full replay engine.
- Preserve exact raw WebSocket frames and local receive order/timestamps.
- Treat contradictory or unprovable semantics as unresolved rather than choosing the interpretation that makes replay easiest.

## Allowed scope

Codex may create/modify only:

- `tools/probes/market_feed_contract/`
- `tests/probes/market_feed_contract/`
- `docs/experiments/market-feed-contract-probe.md` (generated/updated research report template and results)
- a **probe-only** dependency/config file if required, provided it is clearly labeled as non-production and does not establish the application stack
- `docs/data/market-data-and-replay.md` **only to append observed results to the existing open-questions section after evidence has been captured; do not alter accepted methodology without a separate ADR update**

Do not modify `src/` as part of this packet.

## Tooling boundary

Use Python 3.11+ for the research probe. Capture the WebSocket at the raw JSON/text boundary rather than relying exclusively on a model-normalizing SDK, because unknown fields are themselves part of what is being measured.

A small mature WebSocket/HTTP transport dependency may be used (for example `websockets` and `httpx`). These are **probe-only choices** and do not decide the production bot's eventual networking stack.

All interactions must use public read-only endpoints. No wallet, API key, private key, trading credentials, or authenticated user WebSocket is permitted.

## Inputs/outputs or interfaces

### Inputs

The probe should support either:

1. explicit token IDs supplied by CLI; or
2. a bounded public-market discovery mode using first-party Polymarket market discovery, selecting a small sample of currently active CLOB-enabled binary markets.

The sample size and runtime must be CLI-configurable. Default to a small research run, e.g. 3-5 assets for several minutes, not an all-market collector.

### Raw output

Persist an append-only run directory containing:

- exact received WebSocket frames;
- local UTC receive timestamp with sub-second precision;
- monotonic local `ingest_sequence`;
- connection/session identifier;
- token IDs subscribed;
- raw REST `/book` responses used for comparison;
- run configuration;
- software/git version where practical.

Do not commit captured live datasets to Git by default. Store them under a gitignored probe-output path.

### Derived report

Produce a deterministic machine-readable summary plus `docs/experiments/market-feed-contract-probe.md` documenting observations, counts, counterexamples, and unresolved items.

## Required invariants

1. **Read only.** No endpoint capable of placing/cancelling orders may be called.
2. Raw frames are persisted before/alongside derived parsing; unknown fields must survive capture.
3. `source_timestamp` and local `received_at` are stored separately.
4. Local `ingest_sequence` preserves receive order independent of source timestamp.
5. Use exact decimal parsing for price and size comparisons; do not use binary float equality for contract conclusions.
6. Do not silently discard malformed/unexpected messages. Record them with parse/error status.
7. Do not infer that a field never exists merely because it was absent in a small sample; report observed frequency/sample size.
8. Do not claim hash semantics unless the tested reconstruction reproduces hashes consistently under a documented algorithm.
9. Do not claim gap-free delivery from a short successful run.
10. No result from this probe may itself be treated as a trading-performance result.

## Questions the probe must answer

### Q1 — Current event schema

For each observed event type, enumerate all top-level and nested fields and their observed frequency. Specifically check for undocumented `sequence`, `sequence_id`, `seq_id`, or equivalent ordering fields.

### Q2 — Initial dump/reconnect

On initial subscription, record whether a full `book` arrives before state-changing deltas for each token.

Perform at least one controlled disconnect/reconnect or unsubscribe/resubscribe test using public data only. Determine whether a fresh full `book` is observed before subsequent deltas when initial dump is enabled/defaulted.

Do not manufacture network corruption beyond a clean client-controlled reconnect.

### Q3 — Price-level update semantics

Starting from an accepted full `book`, apply observed `price_change` entries under the hypothesis:

- BUY -> bid level;
- SELL -> ask level;
- size zero -> delete level;
- non-zero size -> replace aggregate size at that price.

After every applicable update, derive best bid/ask and compare with the event-provided `best_bid` and `best_ask` when present.

Report total comparisons, exact matches, mismatches, and representative counterexamples. A high match rate alone is not enough if systematic mismatches remain unexplained.

### Q4 — Multiple updates per frame

Measure:

- number of `price_changes` entries per frame;
- whether a frame contains multiple entries for the same asset;
- whether the same asset/side/price can appear more than once in one frame;
- whether ordering would change final state in any observed case.

### Q5 — Hash semantics

Preserve every `book.hash` and per-change `hash`.

Attempt the documented/official-client server-compatible order-book-summary hash algorithm **only where the required fields are available**. Do not fabricate missing fields.

Determine separately:

- whether full WebSocket `book` hashes are reproducible;
- whether `price_change.hash` corresponds to any reproducible post-update book hash;
- whether hash behavior differs by payload shape.

If not reproducible, report `UNRESOLVED`; do not reverse-engineer speculative semantics as part of this packet.

### Q6 — REST `/book` comparison

Periodically retrieve official REST `GET /book` for sampled tokens.

Store raw REST response and timestamps. Compare REST and reconstructed state only when a defensible temporal alignment exists. Because the book may change during the HTTP request, a mismatch must be reported as a diagnostic, not automatically classified as a WebSocket failure.

Record whether REST responses expose the expected:

- hash;
- tick size;
- minimum order size;
- negative-risk flag;
- last trade price.

### Q7 — Optional metadata in WebSocket book frames

Measure whether live `book` events include optional `min_order_size`, `tick_size`, `neg_risk`, or `last_trade_price` fields beyond the minimal public example.

### Q8 — Timestamp behavior

Measure:

- source timestamp monotonicity per token in receive order;
- same-timestamp event groups;
- source-to-local-receive delay distribution, clearly labeled as local-clock dependent;
- any source timestamps later than local receive time, which may indicate clock skew and must not be silently corrected.

## Failure cases

The probe must handle and record:

- WebSocket disconnect;
- heartbeat timeout;
- invalid JSON/non-JSON frames other than expected heartbeat messages;
- missing required fields;
- decimal parse failure;
- unexpected event type;
- REST timeout/non-2xx response;
- source timestamp regression;
- best-bid/ask reconstruction mismatch;
- duplicate raw frames;
- output write failure.

If durable raw output fails, terminate the research run rather than continuing to generate derived conclusions from unrecorded evidence.

## Required tests

Offline tests must not require network access.

At minimum test:

1. raw frame persistence preserves exact input text/bytes;
2. receive sequence is monotonic;
3. decimal parsing preserves exact values;
4. zero-size level deletion;
5. non-zero replacement hypothesis;
6. independent bid/ask level updates;
7. best-bid/ask derivation;
8. duplicate replacement is idempotent;
9. unexpected fields are retained in raw capture;
10. malformed state-changing frames are recorded as errors rather than silently discarded;
11. report aggregation correctly counts matches/mismatches/field frequencies;
12. no code path imports or invokes authenticated/trading clients/endpoints.

Include fixtures modeled on first-party documented payloads, but label them as fixtures rather than empirical live evidence.

## Acceptance criteria

The packet is complete only when:

- probe code and offline tests pass;
- at least one bounded live read-only run has been executed against current CLOB V2;
- raw run evidence is saved outside Git;
- `docs/experiments/market-feed-contract-probe.md` records run time, sample, commit, methodology, counts, and results for Q1-Q8;
- every conclusion states whether it is confirmed, contradicted, or unresolved;
- counterexamples are included for any non-perfect invariant;
- no trading/authenticated operation occurred;
- no production replay/collector code was added to `src/`;
- unresolved feed semantics remain unresolved in documentation rather than being guessed.

## Explicitly out of scope

- production market-data collector;
- pmxt downloader/replayer;
- complete historical replay engine;
- execution simulator;
- forecasting;
- Brier sizing or kappa selection;
- risk engine;
- portfolio accounting;
- AWS/cloud deployment;
- database selection;
- live or paper order placement;
- user WebSocket/authentication;
- reverse engineering undocumented hash semantics beyond straightforward tests against first-party algorithms.

## Open questions

The research questions Q1-Q8 are intentionally open until the probe is executed. Codex must not close them by assumption.

## Traceability check

```text
Polymarket first-party WS/REST documentation
        +
external microstructure/archive evidence
        ↓
docs/data/market-data-and-replay.md
        ↓
ADR-001 fail-closed event-sourced replay
        ↓
IP-001 market-feed-contract-probe
        ↓
probe code + offline tests + raw evidence
        ↓
experiment report
        ↓
revised/frozen replay contract
```
