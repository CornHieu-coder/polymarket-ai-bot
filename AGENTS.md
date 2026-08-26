# Instructions for Codex

This repository is research-first. Future Codex work must follow these rules:

1. Read `docs/README.md` first.
2. Read the relevant implementation packet before making changes.
3. Read every research and design document referenced by that packet.
4. Do not introduce undocumented research or architecture assumptions.
5. Do not modify modules outside the packet's allowed scope. If expansion is necessary, stop and explain why before changing anything outside that scope.
6. Do not implement unresolved ideas by guessing.
7. Preserve all documented invariants.
8. Add tests for important behavioural invariants.
9. Prefer simple, auditable implementations over premature abstractions.
10. Do not add live-trading functionality unless an implementation packet explicitly authorizes it.
11. Do not add infrastructure or technologies merely because they may be useful later.
12. Keep documentation synchronized whenever implementation changes an established design decision.
13. Treat research traceability as part of correctness. For consequential behaviour, verify that the implementation packet points to the governing research/design evidence and that the code does not claim more than that evidence supports.
14. Do not convert a literature-inspired idea into an implementation rule unless the project documentation explicitly identifies the adaptation or project-derived step.
15. If implementation reveals that an authoritative source is ambiguous, stale, incompatible with current platform behaviour, or insufficient for the requested behaviour, stop and report the evidence gap rather than selecting an interpretation silently.
16. Do not tune unresolved numerical parameters from outcome data unless a documented experimental protocol explicitly authorizes that procedure.
17. For mutable external behaviour such as Polymarket APIs, fees, order semantics, or restrictions, rely on the project-designated authoritative documentation and verification date. If implementation requires fresher verification than the docs provide, flag it before proceeding.
18. When a consequential architecture or methodology decision becomes accepted, materially changes, or is invalidated, keep `docs/research/architecture-interview-notes.md` synchronized with a concise interview-oriented summary and links to the authoritative sources. That note is a recall aid only; never treat it as authority when it conflicts with an ADR, research document, or implementation packet.
