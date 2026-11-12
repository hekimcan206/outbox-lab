# 0003: Logical decoding outbox with transactional pg_logical_emit_message, consumed through pgoutput

- Status: accepted
- Date: 2026-11-12

## Context

Polling relays pay for every message twice: an INSERT into the outbox and a DELETE (plus vacuum) when it is relayed,
and they discover new messages either late (poll interval) or through NOTIFY (global commit lock). Logical decoding
streams committed changes out of the WAL in commit order, which is both push-based and ordered. The common way to use
it is Debezium's outbox event router on an outbox table
([Debezium outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)).

## Options considered

1. **Debezium Server + broker.** Production-grade, but it measures Debezium and the broker, and it is already
   covered by the cdc-pipeline project of this portfolio.
2. **Outbox table in a publication, streamed with pgoutput.** The relay sees `InsertMessage`s. Rows must still be
   deleted (insert-then-delete in the same transaction is a known trick, at the cost of a second statement) or
   cleaned up later.
3. **`pg_logical_emit_message(true, prefix, content)`** (chosen). Writes a message straight into the WAL as part of
   the current transaction; with `transactional = true` it is decoded only if and when the transaction commits
   ([PostgreSQL 17: replication management functions](https://www.postgresql.org/docs/17/functions-admin.html#FUNCTIONS-REPLICATION)).
   pgoutput forwards such messages when the client asks for `messages = true`, independent of publication tables.
   Oskar Dudycz describes the push-based outbox on logical replication
   ([event-driven.io](https://event-driven.io/en/push_based_outbox_pattern_with_postgres_logical_replication/)).

## Decision

- Producers run `SELECT count(pg_logical_emit_message(true, 'outbox', p)) FROM unnest($1::bytea[])` in the same batch
  as the business insert. No outbox table, no index, no delete, no vacuum.
- The relay (`PgLogicalRelay`) opens a `LogicalReplicationConnection` as `outbox_relay` (a role with the REPLICATION
  attribute and no table privileges beyond the outbox), starts pgoutput on slot `outbox_lab_slot` with an empty
  publication and `messages: true`, and handles `LogicalDecodingMessage` with prefix `outbox`
  ([Npgsql logical replication](https://www.npgsql.org/doc/replication.html)).
- Progress is acknowledged per transaction after delivery (`SetReplicationStatus(commit.TransactionEndLsn)`), with a
  1-second status interval instead of Npgsql's 10-second default, so the slot's confirmed position follows closely.

## Consequences

- Order is commit order across all aggregates, from a single stream. One relay process per slot; throughput is bounded
  by one decoder and one consumer (the drain benchmark shows the bound on this machine).
- **The slot pins WAL while the relay is down.** A dead relay fills the disk; `max_slot_wal_keep_size` and alerting on
  `pg_replication_slots` lag are mandatory in production. The lab drops the slot on every non-logical reset for the
  same reason.
- `wal_level = logical` is required, which some managed PostgreSQL offerings expose only as a parameter-group change
  and a restart.
- A restart resumes from the confirmed position and can redeliver the last unacknowledged transactions (tested).
- Messages exist only in the WAL: there is no table to query for "what is pending", which makes operational inspection
  harder than with a table outbox (`pg_logical_slot_peek_binary_changes` is the tool).
- Failover: a logical slot on a primary is lost on promotion unless failover slots are configured (PostgreSQL 17
  supports slot synchronisation to standbys). Not exercised here; listed in the README limitations.
