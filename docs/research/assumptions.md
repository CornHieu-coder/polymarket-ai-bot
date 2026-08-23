# Assumptions and Methodological Positions

This register distinguishes uncertain research assumptions from supported conclusions and adopted methodological decisions. A status of `active — unverified` is not a claim of fact. Dates use ISO 8601.

## A-001

- **Classification:** Research assumption
- **Statement:** For initial simulation research, hypothetical strategy orders should be sufficiently small relative to visible market liquidity that first-order market impact can initially be approximated as negligible.
- **Status:** Active — unverified
- **Justification:** This bounds the first simulation problem while retaining observable liquidity constraints. It is an assumption, not a verified fact.
- **Risk if wrong:** Simulated fills and returns may be materially overstated because the strategy's own orders would move prices or exhaust displayed liquidity.
- **How it may eventually be tested:** Compare hypothetical order sizes with contemporaneous depth, replay fills with impact and slippage sensitivity models, and validate against paper or controlled live observations when separately authorized.
- **Date introduced:** 2026-08-23

## A-002

- **Classification:** Supported methodological conclusion
- **Statement:** Historical price series alone are insufficient for realistic execution simulation because they do not describe available quantity across order-book levels.
- **Status:** Adopted
- **Justification:** A price observation identifies neither how much could execute nor the prices reached while consuming multiple order-book levels.
- **Risk if wrong:** The project may collect and process more granular market data than a valid baseline requires.
- **How it may eventually be tested:** Compare simulations based only on price series with simulations using contemporaneous order-book depth, measuring differences in fill feasibility, fill price, and quantity.
- **Date introduced:** 2026-08-23

## A-003

- **Classification:** Methodological decision
- **Statement:** The initial execution baseline should claim fills only against observable executable liquidity rather than inventing maker-order fills.
- **Status:** Adopted
- **Justification:** Historical data generally cannot establish that a hypothetical resting maker order would have received queue priority and executed. Restricting the baseline to observable executable liquidity is conservative and auditable.
- **Risk if wrong:** The baseline may omit economically relevant maker strategies or model execution too conservatively.
- **How it may eventually be tested:** Compare the baseline with a separately documented maker-fill model using order-level or queue-position evidence when suitable data becomes available.
- **Date introduced:** 2026-08-23
