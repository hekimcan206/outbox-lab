# 0004: Oracle: a SKIP LOCKED cursor relay and classic Advanced Queuing with transactional enqueue

- Status: accepted
- Date: 2026-11-12

## Context

Oracle offers two native ways to run an outbox. A table polled with `FOR UPDATE SKIP LOCKED`, as on PostgreSQL, and
Advanced Queuing (AQ), a queue stored in tables whose enqueue joins the caller's transaction. The brief asks for both.

Oracle's SKIP LOCKED differs from PostgreSQL's in a way that matters for batching:

- `FETCH FIRST n ROWS ONLY` cannot be combined with `FOR UPDATE`, and an ordered inline view with `ROWNUM` raises
  ORA-02014 or returns the wrong rows.
- `WHERE ROWNUM <= n FOR UPDATE SKIP LOCKED` applies `ROWNUM` *before* skipping, so a second worker whose first n
  candidate rows are locked gets zero rows instead of the next n
  ([OraFAQ forum](https://www.orafaq.com/forum/t/46034); Franck Pachot on multi-threaded dequeuing:
  [dbi services](https://www.dbi-services.com/blog/oracle-write-consistency-bug-and-multi-thread-de-queuing/)).
  An Oracle integration test reproduces this.
- With SKIP LOCKED, rows are locked as they are fetched, so an unbounded cursor from which the worker fetches only n
  rows is the working pattern.

For AQ, current ODP.NET Core (23ai) exposes `OracleAQQueue` with `EnqueueArray`/`DequeueArray`, which earlier
managed drivers did not ([ODP.NET: Advanced Queuing and Transactional Event Queues](https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/featAQ.html)).
Queue administration is not available from ODP.NET, so the queue is created in the init script with `DBMS_AQADM`.

## Options considered

1. **SKIP LOCKED with ROWNUM**: rejected, demonstrably starves workers.
2. **SKIP LOCKED cursor, fetch n rows** (chosen for `ora-skip-locked`): `SELECT id, payload FROM outbox ORDER BY id
   FOR UPDATE SKIP LOCKED`, read n rows, array-bound `DELETE`, COMMIT. The reader's `FetchSize` is set to exactly one
   batch (`RowSize * n`); with ODP.NET's default 128 KB prefetch a worker would lock several batches it never
   delivers.
3. **Classic AQ, single-consumer RAW queue** (chosen for `ora-aq`): producers call `DBMS_AQ.ENQUEUE` with default
   (ON_COMMIT) visibility inside the business PL/SQL block; relays block in `DequeueArray` (REMOVE mode) and commit
   after delivery. `FIRST_MESSAGE` navigation, so a message that committed late with an earlier enqueue time is never
   behind the dequeue position.
4. **Transactional Event Queues (TxEventQ)**, AQ's successor built for high throughput with partitioned
   (sharded) queues. The same ODP.NET API works with it. Not measured: its benefits come from multiple event streams
   and more CPUs than Database Free's two, and its ordering is per stream. Listed as a next step.

## Decision

Measure options 2 and 3 with the same producer shape as PostgreSQL (one array-bound PL/SQL block per batch: business
insert plus outbox insert or enqueue, then COMMIT) and the same harness. The AQ dequeue call is synchronous, so each AQ
worker owns a dedicated thread (`TaskCreationOptions.LongRunning`); its wait (whole seconds) only bounds how fast it
notices cancellation, because a commit wakes a blocked dequeuer immediately (tested: under 1 s with a 1 s wait).

## Consequences

- Both Oracle strategies are competing-consumer designs: per-aggregate order holds with one worker and breaks with
  several, like `pg-skip-locked`. AQ message grouping (transactional grouping) or one queue per partition would be
  needed to keep order with parallel dequeuers.
- `ora-aq` needs no polling and no index on an outbox table, and gives push latency without a separate notification
  channel. It costs an AQ queue table, its index-organised internals and DBMS_AQ privileges.
- Everything measured is on Oracle Database Free (2 CPU threads, 2 GB). Enterprise Edition numbers would differ;
  docs/benchmarks.md says so.
