# IP-000: Repository Bootstrap

## Goal

Create the initial private GitHub repository and establish the research-first documentation architecture and empty project scaffold. Do not implement a trading system.

## Research/design documents to read first

- `docs/research/principles.md`
- `docs/research/hypotheses.md`
- `docs/research/assumptions.md`
- `docs/design/architecture.md`
- `docs/design/execution-model.md`

## Allowed scope

- Repository-level Git and GitHub configuration needed for the initial commit and private remote.
- `AGENTS.md`, `README.md`, and `.gitignore`.
- Documentation under `docs/`.
- Empty `src/` and `tests/` directory placeholders.

## Required invariants

- Research questions, assumptions, methodological positions, and implementation decisions remain distinguishable.
- The documented architecture components remain conceptually separable.
- `Target Position != Order != Fill`.
- No live-trading or trading-system behaviour is introduced.
- No unnecessary infrastructure or framework is introduced.

## Inputs/outputs or interfaces

- **Input:** The repository-bootstrap specification.
- **Output:** A private GitHub repository on `main` containing only the requested documentation and scaffold.
- **Runtime interfaces:** None.

## Failure cases

- GitHub authentication is absent or lacks permission to create a private repository.
- The requested repository name already exists or cannot be created.
- Required files or intentionally empty directories are omitted from Git.
- Documentation links do not resolve.
- Prohibited application or infrastructure code is introduced.
- The initial commit cannot be pushed to `main`.

## Required tests

- Verify every required path exists and is tracked by Git.
- Verify relative links in `README.md` and `docs/README.md` resolve.
- Verify the repository is private and its default branch is `main`.
- Verify the committed scaffold contains no trading implementation.

## Acceptance criteria

- The GitHub repository exists.
- The repository is private.
- The required directory structure exists.
- All required documentation files exist.
- The documentation index links correctly.
- `AGENTS.md` exists.
- No trading implementation has been added.
- The initial commit has been pushed to `main`.

## Explicitly out of scope

- Polymarket API integration.
- Forecasting implementation.
- Position-sizing implementation.
- Execution simulation.
- AWS or other cloud configuration.
- Docker, Kafka, Redis, Terraform, or databases.
- Live trading.
- Additional strategy assumptions.

## Open questions

None.
