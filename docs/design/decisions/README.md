# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for consequential engineering decisions. Important methodological decisions may also use ADRs when they materially affect implementation. Do not create speculative ADRs for decisions the project has not needed to make.

Every consequential ADR should be evidence-traceable. Before recording a decision, check relevant credible research or authoritative technical documentation when available. The ADR must distinguish what external evidence establishes from what this project is choosing, adapting, or deriving.

Name each record:

```text
ADR-XXX-short-title.md
```

Use this structure:

```text
# ADR-XXX: Short title

## Status

## Context

## Research / authoritative evidence
- Cite the strongest relevant primary sources or first-party technical documentation.
- State exactly what each source supports.
- Record important applicability assumptions or environment differences.
- For mutable platform behaviour, include the verification date.
- If no sufficiently relevant evidence exists, state that explicitly.

## Evidence classification
- **Established by external evidence:**
- **Suggested by external evidence:**
- **Project-derived choice:**

## Decision

## Reasoning
Explain how the evidence and project constraints lead to the decision. Do not imply that a cited source proves a project-specific synthesis unless it actually does.

## Alternatives considered

## Why alternatives were rejected
Include contrary or competing evidence where relevant.

## Consequences
Include effects on implementation, experiments, validity, failure modes, and future review.

## Validation / falsification plan
State how the project will test whether the project-derived portion of this decision is sound and what evidence would cause the decision to be reopened.

## Related assumptions/research
Link assumptions, hypotheses, other ADRs, and implementation packets where applicable.

## Date
```

A decision should be reopened when new research, new platform behaviour, or experimental evidence materially undermines its basis. When that happens, document the effect on prior experiments and implementations instead of silently replacing the old rationale.
