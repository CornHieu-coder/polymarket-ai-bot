# Documentation Map

This file is the authoritative map of project documentation. Research should lead design, and bounded implementation packets should lead code changes.

## Areas

- [`research/`](research/) owns scientific questions, evidence, assumptions, and hypotheses.
  - [Principles](research/principles.md)
  - [Hypotheses](research/hypotheses.md)
  - [Assumptions and methodological positions](research/assumptions.md)
- [`data/`](data/) owns source-of-truth policy, raw-data contracts, replay semantics, provenance, and data-quality rules.
  - [Data documentation map](data/README.md)
  - [Market data and replay protocol](data/market-data-and-replay.md)
- [`design/`](design/) owns system architecture and engineering decisions.
  - [Architecture](design/architecture.md)
  - [Execution model](design/execution-model.md)
  - [Architecture Decision Records](design/decisions/README.md)
  - [ADR-001: Fail-closed event-sourced L2 replay](design/decisions/ADR-001-fail-closed-event-sourced-replay.md)
- [`implementation-packets/`](implementation-packets/) owns bounded implementation contracts for Codex tasks.
  - [Packet format and workflow](implementation-packets/README.md)
  - [IP-000: Repository bootstrap](implementation-packets/IP-000-repository-bootstrap.md)
  - [IP-001: Market feed contract probe](implementation-packets/IP-001-market-feed-contract-probe.md)
- [`experiments/`](experiments/) owns experimental methodology, runs, and results.
  - [Experiment log](experiments/experiment-log.md)

## Extensibility

The documentation structure is intentionally extensible. New sections should be added only when a genuine new concern appears rather than pre-created speculatively. The `data/` area was added when point-in-time market-state integrity became an implementation dependency. Future concerns such as `operations/`, `security/`, or a dedicated `forecasting/` area should follow the same rule.
