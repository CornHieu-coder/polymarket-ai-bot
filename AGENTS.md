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
