# Implementation Packets

An implementation packet is mandatory for every meaningful Codex coding task in this repository. A packet is a bounded implementation contract: it identifies the authoritative context, constrains the allowed changes, and makes completion objectively testable. It is intended to prevent undocumented research or architecture decisions from entering the codebase.

Implementation packets must not become a place where new methodology is invented. Any material research, mathematical, market-microstructure, risk, data, backtesting, forecasting, or architectural choice needed by a packet must already be supported by the referenced research/design documentation or explicitly marked unresolved. If Codex discovers that implementation requires a new consequential assumption or decision, it must stop rather than guess.

Name packets approximately:

```text
IP-001-short-task-name.md
IP-002-short-task-name.md
```

Each implementation packet must contain all of the following sections.

## Goal

State exactly what should be implemented.

## Research/design documents to read first

List the authoritative documentation relevant to the task. For consequential behaviour, these documents should expose the evidence chain from source/research to project decision.

## Evidence basis

Summarize, without re-deriving the research, which behaviours in this packet are:

- **Established by external evidence**;
- **Suggested by external evidence but adapted here**;
- **Project-derived choices**.

Link the governing ADRs, assumptions, or research documents. If no external evidence exists for a project-derived choice, say so explicitly rather than presenting it as research-backed.

## Allowed scope

List the files or modules that may be created or modified.

## Required invariants

State the behaviours that must remain true. Where an invariant exists because of a research or methodological decision, link its authoritative document.

## Inputs/outputs or interfaces

Define them when relevant; otherwise state that none apply.

## Failure cases

Identify important edge cases and fault conditions, including failures that could invalidate research conclusions rather than only software failures.

## Required tests

List tests Codex must create or preserve. Tests should encode documented invariants and, where practical, guard against methodological shortcuts such as invented liquidity, future-information leakage, or portfolio updates from unfilled orders.

## Acceptance criteria

State how completion is objectively determined.

## Explicitly out of scope

List what Codex must not implement during this task.

## Open questions

Record anything unresolved that must not be guessed. State `None` when there are no open questions.

## Traceability check

Before the packet is considered complete, verify that every consequential implemented behaviour can be traced through:

```text
evidence / authoritative source
        ↓
research assumption or finding
        ↓
ADR / design decision
        ↓
implementation packet requirement
        ↓
code and tests
```

If that chain is broken for a consequential behaviour, the packet should not authorize implementation until the gap is resolved.
