# Data Documentation

This area owns source-of-truth policy, raw-data contracts, replay semantics, provenance, validation, and data-quality rules.

Current documents:

- [`market-data-and-replay.md`](market-data-and-replay.md) — Polymarket market-data sources, canonical raw-event contract, order-book reconstruction, replay validity, and validation policy.
- [`pmxt-historical-ordering-risk.md`](pmxt-historical-ordering-risk.md) — evidence that published pmxt V2 is not a proven exact arrival-ordered tape, the unresolved historical-ordering risks, and the candidate policy to be measured before production replay.

Data documents must follow the evidence-first rule in `docs/research/principles.md`: distinguish first-party platform facts, external empirical evidence, and project-derived methodology.
