# 0001: One measurement harness for every strategy, with an in-process sink and one clock

- Status: accepted
- Date: 2026-11-12

## Context

The question this repository answers is comparative: which outbox relay should a team pick on PostgreSQL or on Oracle,
and what does each choice cost in throughput, latency and ordering? A comparison is only worth something if the
strategies are measured the same way. Published comparisons usually fail on one of these:

- each strategy has its own harness, so the harness becomes a hidden variable;
- latency is computed from timestamps taken on two machines, or two clocks, and includes their skew;
- the broker is included, so the result measures Kafka or RabbitMQ rather than the outbox;
- one database gets more hardware than the other (Oracle Database Free limits itself to 2 CPU threads and 2 GB);
- a closed-loop load generator slows down with the system under test and hides queueing delay
  (coordinated omission).

The brief also says "measure, don't write a new library": the relays should be the smallest correct implementation of
each pattern, not a framework.

## Options considered

1. **Benchmark existing libraries** (MassTransit, NServiceBus, Wolverine, Debezium). Realistic, but every library adds
   its own batching, serialisation and threading, and none of them covers both engines with all the patterns
   (logical decoding messages, AQ). The result would compare libraries, not patterns.
2. **One harness, one small relay per pattern** (chosen). Strategies implement a three-member interface
   (`IOutboxStrategy`: reset, create producer, create relay); the scenario engine (`ScenarioRunner`) is the same code
   for all of them.
3. **External load generator (NBomber, k6) with a broker sink.** Good for service capacity tests, but adds a broker
   and a network hop to every delivery and makes ordering analysis depend on the broker.

## Decision

- **Sink.** Relays hand messages to an in-process `IDeliverySink`. The lab's sink (`DeliveryRecorder`) records each
  delivery under one lock, which yields a single global delivery order for the ordering analysis. Broker publish cost
  is excluded on purpose and is the same constant for every strategy in production.
- **Clock.** Producers and relays run in the same process. Each message carries the `Stopwatch` timestamp taken when
  its producing transaction started, inside the payload, so latency is one subtraction on one monotonic clock.
  It therefore includes the business transaction itself (insert, outbox write, commit) plus the relay's pickup.
- **Payload.** One binary layout (`EventCodec`: magic, aggregate, sequence, timestamp, filler), stored as `bytea`,
  `RAW(2000)`, an AQ RAW payload or a logical decoding message. Default 256 bytes everywhere.
- **Workload.** Synthetic. Producer *p* of *P* owns aggregates with `id % P == p` and writes them one transaction at a
  time, so per-aggregate commit order equals sequence order, which is what a relay must preserve. Sequences are
  gapless, so loss is countable.
- **Scenarios.** `drain` (pre-filled backlog, first-to-last delivery), `latency` (open loop: a ticker schedules sends
  at a fixed rate independent of the system's response, four producers take the ticks), `ordering` (concurrent
  producers hold their transactions open 0 to *n* ms so commits finish out of id order) and `publish` (closed-loop
  producers, no relay: the cost each strategy adds to the business write).
- **Fairness.** docker compose gives PostgreSQL the same 2-CPU budget Oracle Database Free enforces on itself
  ([Oracle Database Free FAQ](https://www.oracle.com/database/free/faq/),
  [licensing restrictions](https://docs.oracle.com/en/database/oracle/oracle-database/23/xeinl/licensing-restrictions.html)).
  Both producers write one business row plus the outbox message in **one round trip plus COMMIT** (PostgreSQL: one
  `NpgsqlBatch` with `unnest` arrays; Oracle: one array-bound anonymous PL/SQL block).
- **Repetition.** Every configuration runs three times; the run with the median key value (throughput, p50, or
  inversions + missing) is reported, and all raw runs are kept in `benchmark-results/*.json`.

## Consequences

- Differences in the results come from the strategy and the engine, not from the harness. The same harness runs in
  the unit tests against an in-memory strategy, so its own logic (timeouts, loss detection, relay failure) is tested.
- Absolute numbers are a laptop's numbers through Docker Desktop port forwarding, not server capacity. The ratios and
  the qualitative findings (who loses messages, who reorders, who is bounded by the poll interval) are the point.
- Two engines on one host still compete for the same disk and memory bus; docs/benchmarks.md states the host load.
