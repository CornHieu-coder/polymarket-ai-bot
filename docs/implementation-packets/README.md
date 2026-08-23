# Implementation Packets

An implementation packet is mandatory for every meaningful Codex coding task in this repository. A packet is a bounded implementation contract: it identifies the authoritative context, constrains the allowed changes, and makes completion objectively testable. It is intended to prevent undocumented research or architecture decisions from entering the codebase.

Name packets approximately:

```text
IP-001-short-task-name.md
IP-002-short-task-name.md
```

Each packet must contain all of the following sections.

## Goal

State exactly what should be implemented.

## Research/design documents to read first

List the authoritative documentation relevant to the task.

## Allowed scope

List the files or modules that may be created or modified.

## Required invariants

State the behaviours that must remain true.

## Inputs/outputs or interfaces

Define them when relevant; otherwise state that none apply.

## Failure cases

Identify important edge cases and fault conditions.

## Required tests

List tests Codex must create or preserve.

## Acceptance criteria

State how completion is objectively determined.

## Explicitly out of scope

List what Codex must not implement during the task.

## Open questions

Record anything unresolved that must not be guessed. State `None` when there are no open questions.
