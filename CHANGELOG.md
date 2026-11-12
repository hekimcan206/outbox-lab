# Changelog

All notable changes to this project are documented here. The file is maintained by hand, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-11-12

### Added

- Engine-neutral outbox model: one binary payload layout for every engine, per-aggregate sequences, a delivery
  recorder with a single global delivery order, and analysis of loss, duplicates and per-aggregate inversions.
- Scenario engine with four scenarios run identically for every strategy: drain (pre-filled backlog), open-loop
  commit-to-delivery latency, ordering under commit reordering, and closed-loop publish cost.
- PostgreSQL strategies: `FOR UPDATE SKIP LOCKED` competing consumers, the same with LISTEN/NOTIFY wake-up, a
  commit-horizon (`pg_snapshot_xmin`) ordered reader, a naive id cursor (kept to measure its message loss), and a
  WAL-only outbox with transactional `pg_logical_emit_message` streamed through pgoutput.
- Oracle strategies: a `FOR UPDATE SKIP LOCKED` cursor relay with a batch-sized fetch, and classic Advanced Queuing
  with transactional enqueue in the business PL/SQL block and `DequeueArray` relays.
- Least-privilege roles and users per engine, created by init scripts that docker compose and the integration tests
  share.
- `outbox-lab` CLI: `demo` (every strategy at demo scale with pass/fail checks) and `bench` (the measured matrix, three
  runs per configuration, raw runs and medians as JSON and Markdown), OpenTelemetry traces and metrics over OTLP.
- `docker compose up` demo with PostgreSQL 17 and Oracle Database 23ai Free, both limited to 2 CPUs.
- Measured results in `docs/benchmarks.md`, five ADRs and a threat model.

[Unreleased]: https://github.com/hekimcan206/outbox-lab/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/hekimcan206/outbox-lab/releases/tag/v1.0.0
