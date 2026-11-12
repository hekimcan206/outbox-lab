# 0002: PostgreSQL polling relays: SKIP LOCKED for throughput, the commit horizon for order, never an id cursor

- Status: accepted
- Date: 2026-11-12

## Context

A table-based outbox is read by a relay. The relay must not lose a committed message, should not deliver one twice in
the normal case, and, for most event consumers, must keep the order of messages that belong to the same aggregate.
Three facts of PostgreSQL shape the options:

1. **Identity and sequence values are assigned at INSERT, not at COMMIT.** Transaction A can take id 41, transaction
   B id 42, and B can commit first. A reader that saw 42 and remembers "everything up to 42 is done" never sees 41.
   This exact bug has been reported in event stores that use a high-water mark
   ([Marten discussion #4953](https://github.com/JasperFx/marten/discussions/4953),
   [Precision Nutrition: fun with MVCC](https://tech.precisionnutrition.com/postgres/databases/2020/03/06/fun-with-mvcc)).
2. **`FOR UPDATE SKIP LOCKED`** lets many workers claim disjoint rows without blocking each other
   ([PostgreSQL 17: The Locking Clause](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE)).
   It requires the UPDATE privilege on the table even though no row is updated (the lab's first integration run
   failed with `42501` on a SELECT-and-DELETE-only relay role; the init script now grants UPDATE with a comment).
3. **Snapshots expose the oldest running transaction.** `pg_snapshot_xmin(pg_current_snapshot())` is the lowest
   transaction id that may still be in progress; every transaction below it has finished
   ([PostgreSQL 17: snapshot functions](https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-PG-SNAPSHOT)).
   Oskar Dudycz describes using this for ordered outbox reads
   ([event-driven.io](https://event-driven.io/en/ordering_in_postgres_outbox/)).

## Options considered

1. **Id cursor** (`id > last ORDER BY id`): simple, ordered, and wrong (fact 1). Kept in the lab as `pg-id-cursor`
   only to measure how often it loses messages under realistic commit reordering.
2. **Competing consumers with SKIP LOCKED** (`pg-skip-locked`): claim a batch, deliver, delete, commit. Scales with
   workers. Breaks per-aggregate order as soon as there are two workers: worker 1 holds sequence 7 of an aggregate
   while worker 2 delivers sequence 8.
3. **SKIP LOCKED + LISTEN/NOTIFY wake-up** (`pg-notify`): same claim, but idle workers wait on `LISTEN` instead of
   sleeping, and producers `NOTIFY` inside the business transaction. Cuts idle latency from about half the poll
   interval to a few milliseconds. The cost: committing a transaction that issued `NOTIFY` takes a global lock that
   serialises such commits ([Recall.ai](https://recall.ai/blog/postgres-listen-notify-does-not-scale); a counterpoint
   with batching: [DBOS](https://dbos.dev/blog/postgres-listen-notify-scalability)). The lab's `publish` scenario
   measures that cost.
4. **Commit-horizon reader** (`pg-xmin`): one reader takes only rows whose `txid` (an `xid8` column defaulting to
   `pg_current_xact_id()`) is below the snapshot xmin, in `(txid, id)` order, and deletes them. Rows below the
   horizon can no longer change, so nothing can later appear behind what was delivered.
5. **Per-aggregate partitioned workers** (hash the aggregate to a worker, each worker ordered). Keeps order and
   scales, but needs a coordinator for partition ownership. Out of scope for a measurement lab; noted as a next step.

## Decision

Implement 1 to 4 with identical claim shapes (one `DELETE ... USING (SELECT ... LIMIT n FOR UPDATE SKIP LOCKED)
RETURNING` statement per batch, deliver, then COMMIT) and let the numbers decide the trade-off per use case:

- unordered consumers, high volume: `pg-skip-locked` with several workers, or `pg-notify` when idle latency matters
  and the commit-time lock is acceptable at the expected write rate;
- ordered consumers on a table outbox: `pg-xmin` with exactly one active reader (behind an advisory lock or leader
  election in production);
- never an id or timestamp high-water mark.

The notify relay keeps a wake flag set by the `Notification` event, because Npgsql raises a notification that arrives
while the connection is busy claiming at that moment, not during the next `WaitAsync`. Without the flag a worker can
sleep through its wake-up until the poll timeout.

## Consequences

- `pg-xmin` stalls while any transaction in the cluster stays open: the horizon cannot pass it. A forgotten `BEGIN`
  in a psql session stops delivery. Monitoring must alert on old `backend_xmin` in `pg_stat_activity`.
- `pg-xmin`'s single reader bounds throughput; the drain benchmark shows by how much.
- Every strategy is at-least-once (ADR 0005): a crash between delivery and COMMIT redelivers the batch. An integration
  test crashes a relay mid-batch and checks that every message is still there.
- Deleted outbox rows leave dead tuples. Autovacuum handles the steady state; the lab truncates between runs.
