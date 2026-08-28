# Data Documentation

This area owns source-of-truth policy, raw-data contracts, replay semantics, provenance, validation, and data-quality rules.

Current documents:

- [`market-data-and-replay.md`](market-data-and-replay.md) — Polymarket market-data sources, canonical raw-event contract, order-book reconstruction, replay validity, and validation policy.
- [`pmxt-historical-ordering-risk.md`](pmxt-historical-ordering-risk.md) — evidence that published pmxt V2 is not a proven exact arrival-ordered tape, the unresolved historical-ordering risks, and the candidate policy investigated before production replay.
- [`pmxt-ordering-audit-execution-feasibility.md`](pmxt-ordering-audit-execution-feasibility.md) — records why the monolithic, hash-sharded, and sorted-streaming exact-audit execution paths were stopped under predeclared resource gates.
- [`historical-data-source-strategy.md`](historical-data-source-strategy.md) — current post-audit direction for H1/H2 historical sources, third-party qualification, capped-L2 limits, and future sequence-preserving collection.

Data documents must follow the evidence-first rule in `docs/research/principles.md`: distinguish first-party platform facts, external empirical evidence, and project-derived methodology.
