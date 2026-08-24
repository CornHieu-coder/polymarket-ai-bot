# Research Principles

1. Prefer factual accuracy and verified sources over speed.
2. Clearly distinguish verified facts, research assumptions, hypotheses, and implementation decisions.
3. Do not silently introduce assumptions into code.
4. Every important research assumption must be documented and later testable where possible.
5. Forecasting, position sizing and risk, execution, portfolio accounting, market data, and infrastructure must remain conceptually separable.
6. Do not introduce unnecessary architectural complexity before the underlying research requires it.
7. Every experiment should be reproducible.
8. Historical experiments must not use information that was unavailable at the simulated decision timestamp.
9. Preserve raw data wherever practical so derived results can later be reconstructed after bugs or methodological changes are discovered.
10. A negative experimental result is valid. Do not modify methodology merely to force a profitable result.
11. Engineering fault tolerance and research or methodological fault tolerance are both first-class requirements.
12. Implementation should be auditable: important system behaviour must be explainable from documentation, code, and tests.

## Evidence-first decision rule

13. Before adopting any material research, methodological, mathematical, market-microstructure, risk, data, backtesting, forecasting, or architecture decision, first check the relevant credible literature or authoritative technical documentation when such evidence exists.
14. Prefer primary sources where practical: peer-reviewed papers or original working papers for research claims, and first-party documentation/specifications for platform behaviour. Secondary sources may supplement but should not silently replace stronger available evidence.
15. Every consequential decision must distinguish among:
   - **Established by external evidence:** directly supported by a cited source within the source's actual assumptions and scope.
   - **Suggested by external evidence:** motivated by literature or documentation but not established for this project's exact setting.
   - **Project-derived choice:** a design, derivation, simplification, parameterization, or synthesis introduced by this project.
16. Never present a project-derived synthesis as if a cited paper proved it. When combining ideas from multiple sources, identify which part each source supports and which part is the project's own extension.
17. When no sufficiently relevant research exists, say so explicitly, document the gap, derive the simplest defensible choice from first principles, and define how that choice will later be validated or falsified.
18. Numerical parameters must not be described as research-backed merely because the surrounding framework is research-backed. Document whether each value is derived, externally supported, operationally constrained, predeclared for sensitivity analysis, or still unresolved.
19. Record important applicability limits: assumptions of the cited work, differences between its environment and Polymarket, data requirements, and reasons the result may fail to transfer.
20. For mutable platform behaviour such as APIs, fees, order semantics, geographic restrictions, or market rules, record the authoritative source and verification date when the fact becomes implementation-relevant, and re-check it before relying on it in live or forward operation.
21. Architectural and methodological decisions should be traceable from evidence -> assumptions -> decision -> implementation packet -> code/tests -> experiment result.
22. If new evidence materially undermines an existing decision, do not preserve the decision for consistency alone. Re-open it through an ADR or research update and document the consequences for prior experiments and implementations.
