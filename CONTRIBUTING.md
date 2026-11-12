# Contributing

## Prerequisites

- .NET SDK 10.0.400 or later (`global.json` rolls forward to the latest feature band)
- Docker. The PostgreSQL tests start `postgres:17-alpine` through Testcontainers; the Oracle tests start
  `gvenzl/oracle-free:23-slim-faststart` (6.5 GB image, about 2.5 GB of memory) and run only with
  `OUTBOX_ORACLE_TESTS=1`; `docker compose up` runs the demo.

## Workflow

1. Open an issue first for anything larger than a small fix.
2. Branch from `main`; one concern per pull request.
3. Before pushing:

   ```bash
   dotnet build                                   # warnings are errors
   dotnet test                                    # unit, property, golden and PostgreSQL tests
   OUTBOX_ORACLE_TESTS=1 dotnet test --project tests/OutboxLab.OracleTests
   dotnet format --verify-no-changes
   ```

## Adding a strategy

- Implement `IOutboxStrategy` (reset, producer, relay) in the engine's project. Producers must write the business row
  and the message in one round trip plus COMMIT, like the existing ones, or the comparison stops being fair
  (ADR 0001).
- Relays deliver before they acknowledge (ADR 0005) and must survive cancellation.
- Add semantic integration tests (what is delivered, when, in which order, after a crash), add the strategy to
  `LabEnvironment` and `LabPlans`, re-run the benchmark and update `docs/benchmarks.md` with measured numbers only.

## Benchmarks

Numbers in the README and `docs/benchmarks.md` come from `benchmark-results/*.json`. Never edit a number by hand;
re-run `outbox-lab bench` and state the host load.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/) keep the history readable and make the hand-written
CHANGELOG easy to assemble at release time
(`feat:`, `fix:`, `perf:` are user-visible; `docs:`, `test:`, `build:`, `ci:`, `refactor:`, `chore:` are not).

## Architecture decisions

A change to a decision in `docs/adr/` needs a new ADR that supersedes the old one (MADR format).
