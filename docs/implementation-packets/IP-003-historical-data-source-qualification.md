# IP-003: Historical Data Source Qualification

## Goal

Run a **bounded, no-purchase qualification** of TickFoundry's free Polymarket sample as a candidate historical source for H1 forecasting evaluation and conservative H2 execution replay.

This packet does **not** accept TickFoundry, purchase data, implement production replay, or start strategy/backtest work. Its job is to test whether the provider's documented ordering/provenance and normalized L2 representation are credible enough to justify a later source decision.

If the free sample bundle is not locally available, stop after implementing/tests and report the exact download needed. Do not create an account, accept terms, or purchase anything on the user's behalf.

## Research/design documents to read first

- `AGENTS.md`
- `docs/README.md`
- `docs/research/principles.md`
- `docs/data/market-data-and-replay.md`
- `docs/data/pmxt-historical-ordering-risk.md`
- `docs/data/pmxt-ordering-audit-execution-feasibility.md`
- `docs/data/historical-data-source-strategy.md`
- `docs/design/decisions/ADR-001-fail-closed-event-sourced-replay.md`
- `docs/implementation-packets/README.md`

External documentation, re-check before running:

- https://tickfoundry.com/docs
- https://tickfoundry.com/samples
- https://tickfoundry.com/pricing
- current first-party Polymarket Market WebSocket documentation referenced by the project

## Evidence basis

### Established by prior project work

- pmxt remains useful historical evidence but its public Polymarket archive does not preserve enough explicit ordering information for this project to claim exact arrival-ordered H2 replay without additional assumptions.
- IP-002, IP-002R and IP-002S exhausted the project's bounded local recovery path; the exact pmxt audit remains scientifically unresolved and further local execution engines are out of scope.
- H1 and H2 have different data requirements: H2 needs stronger execution-state/order/depth evidence.

### Established by current provider documentation — to be independently checked where the free sample permits

TickFoundry currently documents:

- `ts_recv_ns` as collector receive time used for ordering;
- `msg_seq` as a capture sequence, monotonic per connection and gap-audited;
- dual collectors;
- raw inbound WebSocket messages as the system of record;
- normalized event-level L1/L2/trades derived from raw capture;
- `bid_ask_reconciles` for reconstructed-vs-payload top-of-book checks;
- detectable L2 truncation using `bid_levels` / `ask_levels` versus the delivered ladder cap;
- a free sample containing raw + normalized layers.

These are third-party claims, not Polymarket protocol guarantees.

### Project-derived qualification policy

- Treat `msg_seq` as **provider observation-order provenance**, never as an exchange sequence.
- A source may qualify separately for H1 and H2.
- H2 qualification may allow a capped ladder only if truncation is explicit and the simulator later refuses to invent liquidity beyond delivered levels.
- A free-sample pass is evidence for provider plausibility, not proof that every historical day is gap-free or normalized identically.

## Allowed scope

Codex may create/modify only:

- `tools/probes/historical_source_qualification/`
- `tests/probes/historical_source_qualification/`
- `docs/experiments/historical-data-source-qualification.md`
- `docs/data/historical-data-source-strategy.md` only to append measured qualification results
- a probe-only dependency file under the tool directory if necessary
- ignored evidence/output paths under `outputs/historical-source-qualification/`

Do not modify:

- `src/`
- ADR-001 or any ADR
- strategy/risk/execution/portfolio modules
- existing pmxt raw/output evidence
- `docs/research/architecture-interview-notes.md` during this bounded qualification

## Inputs

Preferred input is the official free sample bundle described at:

```text
https://tickfoundry.com/samples
```

As of 2026-08-28, the public page describes a one-hour BTC Up or Down 4h sample for 2026-05-19 14:00–15:00 UTC containing raw WebSocket messages plus normalized L1/L2/trades/reference files. The download requires a free sign-up.

Codex must **not** sign up or purchase anything. The user should place the downloaded archive or extracted bundle under an ignored path such as:

```text
outputs/historical-source-qualification/tickfoundry-free-sample/
```

If multiple candidate bundles exist, use only the one whose README/provenance matches the public sample description; otherwise stop and ask for an explicit path.

## Feasibility and resource budget

This is deliberately small.

Before analysis:

- record bundle/file sizes and hashes;
- estimate rows/files from metadata;
- use streaming/chunked reads where practical.

Operational budgets:

- expected complete qualification wall-clock: <= 30 minutes on the local machine;
- peak process-tree RSS: <= 4 GiB;
- temporary generated storage: <= 5 GiB excluding the user-provided sample itself;
- no single non-checkpointed analysis step > 10 minutes.

If these limits are projected or observed to be exceeded, stop and report `SOURCE_QUALIFICATION_NOT_FEASIBLE`; do not build another large-data engine.

## Questions to answer

### Q1 — Provenance and schema

For every relevant file record:

- exact path/name;
- byte length;
- SHA-256;
- schema/column names/types;
- row count;
- timestamp range;
- sample README/version/provider metadata if present.

Compare observed schema with the provider's current public documentation and report drift explicitly.

### Q2 — Raw capture envelope

Inspect raw capture without assuming provider claims are true.

Measure/check at least:

- event types;
- `collector_id`, `run_id`, `conn_id` presence where documented;
- `msg_seq` presence and monotonicity **within the documented sequence scope**;
- duplicate `(conn_id,msg_seq)` or equivalent sequence identities;
- receive timestamp regressions;
- payload hash duplicates where available;
- `parse_ok=false` / parse errors where available;
- raw payloads are parseable enough to recover the Polymarket event type and token/asset identifiers.

Do not interpret a numerical `msg_seq` gap as an exchange-data gap unless the provider's exact sequence scope makes that inference valid. Report observed gaps separately from unexplained capture-integrity failures.

### Q3 — Observation order

Determine whether the sample provides an unambiguous total order for the provider-observed raw messages using documented receive timestamp / sequence fields.

Report:

- tied `ts_recv_ns` counts;
- whether `msg_seq` resolves provider-observation ties under its documented scope;
- any collisions or contradictions where two distinct raw messages cannot be ordered by the claimed provenance;
- reconnect/session boundaries.

Qualification means **provider-observed order is explicit**, not that Polymarket itself emitted an exchange sequence.

### Q4 — Raw-to-normalized traceability

Test that normalized L1/L2 rows can be linked to their triggering raw event using available identifiers/timestamps/sequence fields.

At minimum:

- quantify rows with missing/unmatched trigger provenance;
- inspect exact matches for a deterministic sample of early/middle/late events and all exceptional/mismatch cases;
- verify source event types are consistent with raw payload event types.

Do not silently fuzzy-match by timestamp when exact sequence/provenance linkage is available.

### Q5 — Independent book reconstruction spot-check

Using the already project-validated Polymarket replacement semantics, independently reconstruct book state from the raw sample for a **mechanically selected bounded subset** of tokens/events.

Selection must be predeclared from identifiers/physical position, not chosen by outcome quality. For example:

- lowest 4 token IDs lexicographically with both book and price-change evidence;
- plus any token associated with a normalized reconciliation failure.

Compare independent reconstructed best bid/ask and available depth with the provider normalized L2 at matched event provenance.

Report exact mismatch counts and representative counterexamples. Do not modify scientific semantics to make the provider match.

### Q6 — Provider reconciliation field

Measure `bid_ask_reconciles` (or current documented equivalent):

- true / false / null counts;
- false-rate overall and by source event type;
- every false case in the bounded sample should be retained or summarized with enough provenance to inspect.

A false flag is not automatically disqualifying; unexplained systematic reconstruction disagreement is.

### Q7 — L2 truncation

Measure:

- delivered level cap;
- share of L2 rows where `bid_levels` exceeds delivered bid columns;
- share where `ask_levels` exceeds delivered ask columns;
- both-side truncation;
- maximum true depth observed;
- whether truncation is mechanically detectable on every affected row.

Do not call a capped ladder 'full depth' when the provider's own depth counters indicate more levels existed.

### Q8 — H1 suitability

Assess whether the sample format can support point-in-time H1 market baselines without future leakage:

- observation timestamp available;
- quoted/executable best bid/ask available;
- deterministic latest-state-at-or-before-t lookup possible;
- ordering ties resolvable in provider observation provenance;
- any material caveats.

Classification:

- `H1_QUALIFIED_FOR_PILOT`
- `H1_QUALIFIED_WITH_LIMITATIONS`
- `H1_NOT_QUALIFIED`
- `H1_UNRESOLVED`

### Q9 — H2 suitability

Assess conservative immediate-taker replay suitability:

- explicit provider observation order;
- normalized L2/raw traceability;
- reconstruction/reconciliation quality;
- explicit gap/error provenance;
- detectable ladder truncation;
- enough information to refuse fills beyond delivered evidence.

Classification:

- `H2_QUALIFIED_FOR_PILOT`
- `H2_QUALIFIED_WITH_LIMITATIONS`
- `H2_NOT_QUALIFIED`
- `H2_UNRESOLVED`

A `QUALIFIED` label applies only to a later **research pilot**, not production or final backtesting.

### Q10 — Cost / coverage / trust boundary

Using current public documentation only, record:

- earliest advertised live-capture coverage relevant to normalized L2/raw;
- free-tier/sample availability;
- current self-serve L2 depth limits;
- current à-la-carte/subscription options relevant to research;
- raw-data availability tier;
- personal/commercial license distinction;
- third-party trust assumptions that remain even if the sample passes.

Do not purchase anything and do not treat mutable pricing as permanent.

## Required invariants

1. No paid action/account creation.
2. Raw sample bytes are immutable and stay outside Git.
3. Every analyzed input receives a SHA-256 before conclusions.
4. Provider sequence is never described as a Polymarket exchange sequence.
5. No current REST snapshot is used to validate historical state at a past timestamp.
6. Exact decimal/fixed-point parsing is used where needed for price/size reconstruction.
7. Normalized data is not trusted merely because it exists; raw-to-normalized evidence must be checked.
8. Truncated ladders never imply absent liquidity beyond the cap.
9. H1 and H2 classifications remain separate.
10. Negative results are valid and do not authorize changing the sample or methodology.

## Required tests

Use synthetic/local fixtures; no network required for the offline test suite.

At minimum test:

1. input hash/provenance recording;
2. per-connection sequence monotonicity and duplicate detection;
3. receive-time ties with deterministic sequence resolution;
4. reconnect/session boundary handling;
5. raw payload parse failure remains visible;
6. exact raw-to-normalized trigger matching;
7. independent replacement-style book reconstruction;
8. zero-size deletion;
9. normalized BBO mismatch detection;
10. truncation detection when true depth exceeds delivered columns;
11. no false truncation when depth <= delivered cap;
12. H1/H2 classification logic remains independent;
13. no authenticated/trading/purchase client import path;
14. deterministic report generation.

## Derived outputs

Write machine evidence under the ignored output path and generate:

```text
docs/experiments/historical-data-source-qualification.md
```

The report must separate:

- provider-documented claims;
- directly observed sample evidence;
- project-derived interpretation;
- unresolved trust/coverage questions.

Append only a short results section to `docs/data/historical-data-source-strategy.md` after evidence exists.

## Acceptance criteria

The packet is complete when either:

### `QUALIFICATION_COMPLETE`

- sample provenance/hashes recorded;
- Q1-Q10 answered;
- raw-to-normalized/reconstruction checks completed;
- H1 and H2 labels assigned separately;
- complete offline suite passes;
- resource budget respected;
- report generated;
- no purchase or production integration performed.

### `SAMPLE_REQUIRED`

- implementation/tests may be completed;
- the official sample is not locally available;
- exact expected local input path/download description is reported;
- no account creation or substitute dataset is used.

### `SOURCE_QUALIFICATION_NOT_FEASIBLE`

- the bounded sample itself cannot be analyzed within the declared local resource budget;
- blocker and resource measurements are recorded;
- no further execution-engine redesign is attempted.

## Explicitly out of scope

- buying/subscribing to TickFoundry;
- production data-source integration;
- full historical backtest;
- changing pmxt IP-002 conclusions;
- implementing our forward collector;
- strategy/forecasting/sizing/risk/execution experiments;
- ADR changes.

## Open questions

- Whether the free sample independently supports the provider's ordering/reconstruction claims strongly enough for an H1/H2 pilot.
- Whether normalized 25-level data is sufficient for the project's conservative size/liquidity regime without routine raw access.
- Whether the useful historical coverage is long/diverse enough for the eventual H1/H2 study; this qualification does not answer that by itself.

## Traceability check

```text
pmxt ordering ambiguity + local audit feasibility stop
        ↓
historical-data-source-strategy.md
        ↓
provider documentation + official free sample
        ↓
IP-003 bounded qualification
        ↓
raw/provenance/reconstruction evidence
        ↓
separate H1/H2 source decision
        ↓
later replay/experiment packet only if accepted
```
