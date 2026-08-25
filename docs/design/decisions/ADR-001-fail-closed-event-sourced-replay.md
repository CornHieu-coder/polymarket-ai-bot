# ADR-001: Fail-closed event-sourced L2 replay

## Status

Accepted for V0, with feed-contract details delegated to `IP-001-market-feed-contract-probe.md` before full replay implementation.

## Context

The project needs historical and forward replay that can support execution-aware testing without inventing liquidity or using future information. Polymarket provides a public L2 WebSocket with full `book` snapshots and incremental price-level updates, while its historical price endpoint does not contain historical depth. Third-party pmxt archives the public WebSocket stream but has its own collector/coverage limitations.

## Research / authoritative evidence

### First-party Polymarket

- Market WebSocket exposes full `book` snapshots and incremental `price_change` events: https://docs.polymarket.com/api-reference/wss/market
- Polymarket's agent documentation states a zero-size `price_change` removes a price level: https://github.com/Polymarket/agent-skills/blob/main/websocket.md
- REST `GET /book` exposes full current L2 state plus market execution parameters: https://docs.polymarket.com/api-reference/market-data/get-order-book
- Displayed market probability is not necessarily executable; buys cross asks and sells cross bids: https://docs.polymarket.com/concepts/prices-orderbook
- CLOB V2 production cutover occurred 2026-04-28: https://docs.polymarket.com/v2-migration

### External empirical evidence

- pmxt V2 stores public WebSocket events with source and receive timestamps and documents historical coverage/gap limitations: https://archive.pmxt.dev/docs/v2-data-overview
- Dubach (2026) shows public-feed-derived trade direction is unreliable relative to on-chain ground truth and reports non-zero archive ingestion latency: https://arxiv.org/abs/2604.24366
- Cheng, Yang, and Zou (2026) show Polymarket execution opportunities can be strongly constrained by visible depth: https://arxiv.org/abs/2605.00864

Verified: 2026-08-26.

## Evidence classification

- **Established by external evidence:** Polymarket exposes event-based L2 data; historical midpoint/price data alone lacks depth; public-feed trade direction should not be treated as authoritative where direction matters; CLOB V2 begins 2026-04-28.
- **Suggested by external evidence:** source and receive timestamps should both be preserved; third-party archive gaps and feed latency matter for replay validity.
- **Project-derived choice:** fail closed on known gaps/integrity failures, recover only on a new full snapshot, use observation-time availability for default no-lookahead replay, and represent each asset's reconstruction state explicitly as `UNINITIALIZED`, `VALID`, or `INVALID`.

## Decision

V0 market replay is event sourced from L2 observations.

1. Preserve immutable raw observations plus provenance before producing derived state.
2. Maintain independent reconstructed state per asset ID.
3. Initialize/reinitialize execution-capable state only from an accepted full `book` snapshot.
4. Apply validated price-level updates only while state is valid.
5. Known data loss, malformed state-changing events, contradictory ordering, or integrity mismatch transitions the affected asset to `INVALID`.
6. A strategy may not receive an executable `MarketSnapshot` while state is `INVALID` or `UNINITIALIZED`.
7. Do not fall back to midpoint, last trade, stale book, current REST state, or a future snapshot to create a historical fill.
8. Default strategy replay uses observation-time availability; source timestamps remain separate for diagnostics/event-time analysis.
9. Canonical price/size representation uses exact decimal/fixed-point values.
10. V0 execution replay is restricted to CLOB V2-era observations on or after 2026-04-28 unless a separate regime adapter is researched.

## Reasoning

The project's core research question requires distinguishing forecasting edge from execution artifacts. A permissive replay engine can manufacture profitability by filling at prices/quantities that were not demonstrably available or by carrying stale state through data loss. A fail-closed state machine sacrifices some sample coverage in exchange for stronger validity and auditability.

Using observation-time availability prevents the replay from consuming information before the selected data source observed it. Keeping source time separately allows latency diagnostics without giving the strategy retrospective reordering powers.

## Alternatives considered

1. Historical midpoint/price-series backtest.
2. Carry last-known book through gaps.
3. Automatically reorder events by source timestamp after ingestion.
4. Use pmxt semantics as the authoritative protocol definition.
5. Mix pre- and post-2026-04-28 data in one replay adapter.

## Why alternatives were rejected

1. Price series do not expose executable quantity/depth.
2. Carrying stale books converts known uncertainty into fictional execution.
3. Retrospective reordering can give the historical simulator information/repair ability that a live strategy did not have, and the public documented schema does not provide a required global sequence field.
4. pmxt is valuable historical evidence but is third-party and documents prior collector coverage failures; protocol semantics should come from first-party Polymarket sources.
5. CLOB V2 changed backend/contracts/order/fee semantics, so a single silent regime is not defensible.

## Consequences

- Some historical intervals will be excluded rather than imputed.
- The raw data layer must preserve lifecycle/provenance information, not only parsed prices.
- Replay code requires validity state and explicit recovery boundaries.
- More historical data may not translate into more usable execution samples if metadata or integrity cannot be established.
- Full replay implementation is blocked until IP-001 empirically resolves current feed details that first-party docs do not specify precisely.

## Validation / falsification plan

`IP-001-market-feed-contract-probe.md` will sample the live public feed and test:

- current event schemas and optional fields;
- non-zero price-level size semantics;
- zero-size deletion;
- reconstructed versus event-provided best bid/ask;
- possible sequence fields;
- hash behavior;
- multi-change frames;
- reconnect/initial-dump behavior;
- REST cross-check feasibility.

If live evidence shows that the feed provides a reliable sequence/gap protocol or different update semantics, this ADR will be amended before the full replay engine is implemented.

## Related assumptions/research

- `docs/research/principles.md`
- `docs/research/assumptions.md`
- `docs/data/market-data-and-replay.md`
- `docs/design/execution-model.md`
- `docs/implementation-packets/IP-001-market-feed-contract-probe.md`

## Date

2026-08-26
