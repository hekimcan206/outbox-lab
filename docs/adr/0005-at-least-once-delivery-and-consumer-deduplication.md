# 0005: At-least-once delivery everywhere; consumers deduplicate by (aggregate, sequence)

- Status: accepted
- Date: 2026-11-12

## Context

The outbox pattern exists to avoid a distributed transaction between the database and the broker
([microservices.io: transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)). The
relay therefore has two separate effects: hand the message to the broker, and record in the database that it was
handed over. Whichever comes first, a crash between them breaks exactly-once delivery.

## Options considered

1. **Acknowledge first, then deliver** (at-most-once): a crash loses the message. Unacceptable for an outbox.
2. **Deliver first, then acknowledge** (at-least-once, chosen): a crash redelivers the in-flight batch.
3. **Exactly-once by a shared transaction** with the broker (XA, or Oracle AQ consumed by an in-database consumer):
   only possible when the consumer lives in the same database, which defeats the purpose of publishing events.

## Decision

Every relay delivers inside its claiming transaction and acknowledges afterwards:

| Strategy | Acknowledgement after delivery |
|---|---|
| `pg-skip-locked`, `pg-notify`, `pg-xmin` | `DELETE ... RETURNING` in the claim transaction, then COMMIT |
| `pg-logical` | `SetReplicationStatus(commit LSN)`, sent to the server within 1 s |
| `ora-skip-locked` | array `DELETE` in the claim transaction, then COMMIT |
| `ora-aq` | `DequeueArray` (REMOVE, ON_COMMIT) then COMMIT |
| `pg-id-cursor` | none durable (in-memory cursor); it is in the lab to show loss, not as a design |

Messages carry the aggregate id and a per-aggregate sequence. Consumers keep the highest sequence applied per
aggregate and ignore anything at or below it, which turns redelivery into a no-op. The lab's `OrderingAnalyzer`
counts duplicates separately from inversions for the same reason: a redelivered old message is harmless to such a
consumer, a first delivery that arrives late is not.

## Consequences

- Integration tests crash a relay before it acknowledges (a sink that throws on the 10th message) on
  `pg-skip-locked`, `ora-skip-locked` and `ora-aq`, and restart a `pg-logical` relay. In every case all messages are
  delivered afterwards and the early deliveries become detectable duplicates.
- Duplicates are bounded by the in-flight batch (the claim size), or for `pg-logical` by the transactions delivered
  since the last status update.
- Consumers that cannot deduplicate (for example, a side effect without an idempotency key) need an inbox table or an
  idempotency key on the downstream call. That is outside this lab.
