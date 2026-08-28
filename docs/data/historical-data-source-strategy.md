# Historical Market-Data Source Strategy

Status: **research direction; no production historical source is accepted yet**  
Verified/reviewed: **2026-08-28**

## Main idea

The exact local pmxt ordering audit is now stopped for feasibility reasons. This does **not** prove pmxt is bad data; it means recovering enough missing ordering information to make strong H2 execution claims is not worth further local computation under the project's declared resource budget.

The next step is to qualify a source that preserves observation ordering explicitly, while separately planning our own forward raw-feed collection so future data is under our control.

---

## 1. What the pmxt work established

### Established about the archive

Published pmxt V2 Polymarket data remains useful historical evidence, but it is not a proven exact arrival-ordered tape. Original multi-change frames are exploded, the public schema does not expose an authoritative receive-sequence/frame index for Polymarket rows, and tied archived timestamps can therefore hide order that matters to final book state.

See `pmxt-historical-ordering-risk.md` for the evidence chain.

### Established about our exact audit

Three execution strategies were tried without changing the scientific ambiguity definitions:

1. **Monolithic DuckDB:** June completed; May/August exceeded a 25 GiB memory limit.
2. **Deterministic hash sharding:** memory became safe, but observed shard cost projected beyond the 90-minute recovery budget.
3. **Sequential streaming over the documented sort order:** memory became very small (~0.4 GiB peak in the pilot) with no temporary spill, but two deterministic ~2M-row windows projected the exact full-August audit to roughly 5.3 hours, above the predeclared 30-minute streaming limit.

The final streaming status is therefore:

```text
STREAMING_NOT_FEASIBLE
```

This is an **execution-feasibility result**, not A9 evidence about whether the candidate fail-closed pmxt policy would have low or high coverage cost.

### Decision

Do not create a fourth local engine, loosen the stop rules, or spend more time trying to complete exact IP-002 over the same hourly files.

pmxt may still be useful for questions that do not require exact historical execution ordering, but it is not yet an accepted H2 execution-replay source.

---

## 2. Separate H1 and H2 data requirements

### H1 — forecasting

H1 asks whether an AI forecast is better calibrated/more accurate than the market at the simulated decision time.

Minimum historical market-data needs are comparatively modest:

- reliable point-in-time observation timestamp;
- market identity and rules/context;
- contemporaneous executable/quoted market probability proxy;
- no future-information leakage.

Full queue/order-event fidelity is not automatically required for every H1 experiment.

### H2 — edge monetization / execution

H2 makes stronger claims. It needs:

- reliable observation order;
- L2 depth at the decision/execution time;
- detectable gaps/invalid state;
- bid/ask-aware execution rather than midpoint fiction;
- enough delivered depth to prove the simulated fill, or an explicit incomplete/partial result when the ladder is truncated.

Therefore a source can be usable for H1 while remaining insufficient for H2.

---

## 3. Candidate historical source: TickFoundry

This is a **candidate third-party source**, not an accepted authority.

Current documentation reviewed 2026-08-28 claims:

- event-level Polymarket capture with nanosecond collector receive time `ts_recv_ns`;
- a capture `msg_seq` described as monotonic per connection and gap-audited;
- two collectors in separate regions;
- deterministic duplicate handling by payload hash;
- normalized L1 and L2 data derived from a raw WebSocket capture layer;
- self-contained L2 snapshots after updates;
- normalized L2 rows expose `bid_ask_reconciles` and true `bid_levels` / `ask_levels`;
- the standard normalized ladder is capped at 25 levels per side on Premium and 10 on Free/Explorer; deeper books are explicitly detectable as truncated;
- raw WebSocket atoms are available in the public free sample, while routine raw delivery is an Enterprise feature;
- live-capture book/tick coverage is advertised from 2026-05-11; older coverage may come from a licensed archive and must not be assumed to share the same capture provenance without checking the specific series/day.

Sources:

- https://tickfoundry.com/docs
- https://tickfoundry.com/samples
- https://tickfoundry.com/pricing

### Important limits

1. `msg_seq` is a **collector/capture sequence**, not a Polymarket exchange sequence. It can establish ordering among messages that the provider observed, but by itself does not prove the exchange emitted nothing missing.
2. Dual-collector gap recovery and normalization correctness are provider claims until independently checked on the available sample/evidence.
3. The 25-level ladder is not literally full depth when `bid_levels`/`ask_levels` exceed 25. A backtest must never interpret missing levels as zero liquidity.
4. Raw atoms are not generally available on self-serve personal tiers, so using normalized historical data creates a third-party reconstruction trust boundary.
5. Pricing/licensing is mutable and must be rechecked before purchase or production use. Current self-serve pricing advertises a free tier with five market-days, $4 à-la-carte full-depth market-day downloads, and a Premium subscription for wider 25-level access; no purchase is authorized by this note.

---

## 4. Conservative use if a capped L2 source is later accepted

A 25-level cap does not automatically make H2 impossible.

For an immediate-taker simulation:

- if the proposed order can be completely filled inside the delivered levels, deeper undisclosed levels do not affect the price actually consumed;
- if the delivered ladder is exhausted while `bid_levels`/`ask_levels` says more levels exist, the simulator must return **incomplete/partial evidence**, not assume no further liquidity;
- order-size/liquidity-participation controls still apply.

This is a project-derived execution rule to be formalized only if the source is accepted.

---

## 5. Forward source: our own sequence-preserving collector

Historical vendors solve the retroactive-data problem; they do not remove the value of owning future evidence.

The forward collector should eventually preserve:

```text
exact raw WS frame
+ local ingest sequence
+ receive timestamp
+ source timestamp
+ connection/session identity
+ raw payload hash
+ parser status
```

The local ingest sequence is not claimed to be an exchange sequence. Its purpose is to preserve the exact order in which **our collector observed** frames so future replay never needs to recover ordering from millisecond archive timestamps.

Known disconnects, local drops, parser/write failures, or integrity mismatches still invalidate state under the existing fail-closed contract.

This forward collector cannot repair old history, but starting it prevents the same evidence problem from recurring for future experiments.

---

## 6. Current decision

Proceed in this order:

1. **Stop exact local IP-002 recovery.** Preserve all negative feasibility evidence.
2. **Run a bounded, no-purchase qualification of TickFoundry's free sample** against the project's H1/H2 requirements.
3. If the candidate passes sufficiently, decide whether a small number of free/à-la-carte historical market-days is enough for the next research experiment before considering any subscription.
4. Independently prepare the design/implementation packet for our own forward sequence-preserving collector.
5. Do not implement production historical replay until the accepted source contract is documented.

The next bounded packet is `IP-003-historical-data-source-qualification.md`.
