# Architecture Interview Notes

Purpose: a concise, living recall sheet for resume drills and technical interviews.

This file is **not** an authoritative design specification. It summarizes major decisions in interview-friendly language and links back to the research/design source of truth. If this note conflicts with an ADR, research document, or implementation packet, the authoritative document wins.

Update this note when a consequential architecture or methodology decision is accepted, materially revised, or invalidated.

## 1. Research questions are separated before architecture

**Decision:** Treat forecasting (H1), monetizing forecast edge (H2), and improved/learned sizing (H3) as separate experiments rather than building one opaque "AI trading bot."

**Why:** If forecasting, sizing, and execution change together, a profitable result cannot tell us which component created the edge. Separating them makes the system experimentally identifiable and easier to debug.

**Trade-off:** More interfaces and staged work up front, but much stronger causal attribution and reproducibility.

**Interview version:** "I structured the system around separate forecasting, sizing, execution, and accounting boundaries so I could test where edge actually came from instead of treating the whole bot as one black box."

**Sources:** `docs/research/hypotheses.md`, `docs/research/principles.md`, `docs/design/architecture.md`.

---

## 2. `Target Position != Order != Fill`

**Decision:** Desired strategy position, submitted order intent, and actual fills are distinct domain objects/states.

**Why:** Real orders can partially fill or not fill at all. Updating the portfolio from desired or submitted quantity would invent positions that never existed.

**Invariant:** Holdings change only because of fills or settlement; cash changes only because of fills, fees, deposits/withdrawals, or settlement.

**Interview version:** "One of my core invariants is that target, order, and fill are different. The portfolio only moves on actual fills, which prevents optimistic backtests from silently assuming execution."

**Source:** `docs/design/execution-model.md`.

---

## 3. Execution simulation uses executable L2 depth, not displayed probability

**Decision:** Historical execution must consume visible bid/ask depth and walk the book. A midpoint, displayed probability, or last trade is not treated as an executable price.

**Why:** Quantity matters. A 20-share order may consume several levels or only partially fill; using a single historical price can materially overstate achievable returns.

**Trade-off:** Requires much richer data and book reconstruction, but eliminates a major source of execution bias.

**Interview version:** "I rejected midpoint backtesting because it answers what the market was quoting, not what my order could actually have filled. I model visible L2 liquidity and partial fills instead."

**Sources:** `docs/research/assumptions.md`, `docs/design/execution-model.md`, `docs/data/market-data-and-replay.md`.

---

## 4. V0 execution is conservative taker-style execution

**Decision:** Initial replay only claims fills against observable executable liquidity. It does not invent resting-maker fills.

**Why:** Historical L2 generally cannot prove a hypothetical maker order's queue position or whether it would have been filled.

**Trade-off:** The baseline may be conservative and miss profitable maker strategies, but its fills are auditable from evidence.

**Interview version:** "I deliberately chose a conservative taker baseline because maker backtests need queue-position assumptions. I preferred an auditable lower-bound model over fabricated fills."

**Source:** `docs/research/assumptions.md` (A-003).

---

## 5. Market replay is event-sourced and fail-closed

**Decision:** Preserve raw market events, reconstruct book state deterministically, and represent each asset as `UNINITIALIZED`, `VALID`, or `INVALID`. Known gaps/integrity failures prohibit simulated execution until a fresh authoritative snapshot restores state.

**Why:** Carrying a stale book across a disconnect converts missing information into fictional liquidity. Raw-event preservation also lets reconstruction be rerun after bugs or semantic discoveries.

**Trade-off:** We lose some historical coverage rather than impute uncertain state.

**Interview version:** "The replay engine is fail-closed: if I know the feed is incomplete, I sacrifice data coverage instead of pretending the last book stayed valid. Raw events are immutable so I can replay them after fixing bugs."

**Sources:** `docs/data/market-data-and-replay.md`, `docs/design/decisions/ADR-001-fail-closed-event-sourced-replay.md`.

---

## 6. Observation time and source time are separate clocks

**Decision:** Preserve both exchange/source timestamp and collector receive timestamp. Default no-lookahead replay gates strategy information by observation time rather than retrospectively reordering events by source timestamp.

**Why:** A strategy cannot use information before its data source observed it. Exchange timestamps remain useful for diagnostics, but using them alone can accidentally grant the historical simulator knowledge a live system did not yet have.

**Interview version:** "I explicitly model two clocks. Event time tells me when the exchange says something happened; observation time tells me when the strategy could actually know it. That distinction is important for avoiding subtle lookahead."

**Source:** `docs/data/market-data-and-replay.md`.

---

## 7. Historical raw data and platform semantics have different authorities

**Decision:** First-party Polymarket documentation/live interfaces define protocol semantics; third-party archives such as pmxt may provide historical observations but retain explicit provenance. On-chain evidence is preferred when an experiment needs authoritative historical trade direction.

**Why:** A third-party archive can be useful without being authoritative about undocumented exchange behavior, and public-feed aggressor-side inference has known reliability limitations.

**Interview version:** "I separated data provenance from protocol authority: a third-party archive can supply observations, but I don't let it redefine exchange semantics."

**Source:** `docs/data/market-data-and-replay.md`.

---

## 8. Do not silently mix Polymarket CLOB regimes

**Decision:** Initial execution replay is restricted to the CLOB V2 regime beginning 2026-04-28 unless a separate V1 adapter is researched.

**Why:** The V2 cutover changed contracts/backend/order/fee behavior. Mixing regimes behind one adapter can make historical results internally inconsistent.

**Interview version:** "I treated the exchange upgrade as a schema/behavior boundary rather than assuming historical data was homogeneous."

**Sources:** `docs/data/market-data-and-replay.md`, `docs/design/decisions/ADR-001-fail-closed-event-sourced-replay.md`.

---

## 9. Proper-betting signal and bankroll risk are separate layers

**Decision:** Preserve the Brier/proper-betting relative signal shape separately from absolute bankroll scaling and deterministic safety controls. Use a distinct scale symbol `kappa` (`Q = kappa(p-q)`) to avoid conflating it with unrelated risk parameters in the literature.

**Why:** Proper-scoring theory motivates direction/relative size but does not uniquely determine dollars at risk. Treating scale as a separate problem prevents a risk policy from silently replacing the strategy being tested.

**Current status:** A simple budget-bounded `kappa` is a V0 baseline concept, not claimed to be theoretically optimal. Fractional-Kelly, risk-constrained Kelly, and robust-sizing ideas are research benchmarks for later once forecast-error/calibration evidence exists.

**Interview version:** "I separated alpha generation from capital allocation. The scoring rule determines relative conviction; bankroll scale and hard risk limits are a different optimization problem."

**Sources:** research discussion reflected by `docs/research/principles.md`; formal sizing/risk ADR still to be written when the policy is frozen.

---

## 10. Hard risk controls must remain deterministic

**Decision:** An AI model may influence forecasts and, in later H3 work, learned sizing within bounds, but it must not override deterministic portfolio safety constraints.

**Why:** Letting the same model that claims high confidence relax its own loss limits creates a dangerous feedback loop under miscalibration.

**Current direction:** Worst-case settlement loss is the primary candidate V0 hard-risk quantity; mark-to-market drawdown is tracked separately. Numerical limits remain unresolved until the risk protocol is formally frozen.

**Interview version:** "I don't let the model grade its own safety. Learned sizing can operate inside deterministic risk limits, but it cannot waive them."

**Source:** project research to be formalized in a future risk/sizing ADR before implementation.

---

## 11. Research ambiguity is probed before production code

**Decision:** When first-party docs leave feed semantics materially ambiguous, build a bounded read-only research probe before writing the production replay engine.

**Why:** Small protocol assumptions such as level-update semantics, reconnect ordering, or empty-book encoding can contaminate an entire backtest. Measuring them first is cheaper than debugging research conclusions later.

**Current example:** `IP-001-market-feed-contract-probe.md` tests the live CLOB feed before IP-002 is allowed to implement production replay. PR #1 is still under methodological review, so its empirical conclusions are not yet frozen here.

**Interview version:** "Instead of guessing undocumented exchange behavior, I built a read-only contract probe and made production implementation depend on the evidence."

**Sources:** `docs/implementation-packets/IP-001-market-feed-contract-probe.md`, `docs/design/decisions/ADR-001-fail-closed-event-sourced-replay.md`.

---

## 12. Implementation is bounded by research-backed contracts

**Decision:** Every meaningful Codex task uses an implementation packet specifying authoritative docs, allowed files, invariants, failure cases, tests, acceptance criteria, and explicit exclusions.

**Why:** The coding agent should implement decisions, not invent research or silently expand scope. This also makes code review tractable as the repository grows.

**Trade-off:** More documentation overhead for each task, but less architecture drift and easier traceability from evidence to tests.

**Interview version:** "I used implementation contracts to keep an AI coding agent from making architecture decisions implicitly. Each task is scoped, testable, and traceable back to research assumptions."

**Sources:** `AGENTS.md`, `docs/implementation-packets/README.md`, `docs/research/principles.md`.

---

# Interview drill template

For any decision above, practice answering in this order:

1. **Problem:** What could go wrong with the obvious/simple design?
2. **Decision:** What boundary/invariant did I introduce?
3. **Why:** What evidence or engineering principle supported it?
4. **Trade-off:** What did the design cost or make harder?
5. **Validation:** How did/will I test that the choice is sound?
6. **What I would change later:** What becomes worthwhile at larger scale or with better data?

Keep interview claims narrower than the authoritative evidence. Do not claim a research paper proved a project-specific synthesis when it only motivated it.
