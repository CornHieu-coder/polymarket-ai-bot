# Documentation Map

This file is the authoritative map of project documentation. Research should lead design, and bounded implementation packets should lead code changes.

## Areas

- [`research/`](research/) owns scientific questions, evidence, assumptions, and hypotheses.
  - [Principles](research/principles.md)
  - [Hypotheses](research/hypotheses.md)
  - [Assumptions and methodological positions](research/assumptions.md)
- [`design/`](design/) owns system architecture and engineering decisions.
  - [Architecture](design/architecture.md)
  - [Execution model](design/execution-model.md)
  - [Architecture Decision Records](design/decisions/README.md)
- [`implementation-packets/`](implementation-packets/) owns bounded implementation contracts for Codex tasks.
  - [Packet format and workflow](implementation-packets/README.md)
  - [IP-000: Repository bootstrap](implementation-packets/IP-000-repository-bootstrap.md)
- [`experiments/`](experiments/) owns experimental methodology, runs, and results.
  - [Experiment log](experiments/experiment-log.md)

## Extensibility

The documentation structure is intentionally extensible. Sections such as `data/`, `operations/`, `security/`, or `forecasting/` should be added only when a genuine new concern appears. They must not be pre-created speculatively.
