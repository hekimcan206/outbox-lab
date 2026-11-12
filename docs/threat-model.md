# Threat model

outbox-lab is a measurement lab, not a service: it has no network listener, no end users and no real data. The
threats that matter are the ones a team inherits when it copies one of these relay designs into production, plus the
lab's own handling of credentials. STRIDE categories in brackets.

## Assets

| Asset | Why it matters |
|---|---|
| Outbox messages | They become events other systems act on. Loss, duplication without detection, reordering or forgery is a business incident. |
| Business rows (`orders`) | Written in the same transaction as the message; the relay must never need to read them. |
| Replication slot | Pins WAL. A stuck slot fills the database disk (availability of the whole database, not just the outbox). |
| Database credentials | Three roles per engine; the relay role on PostgreSQL has the REPLICATION attribute. |

## Actors

- **Business service** (`outbox_app`): writes orders and outbox messages.
- **Relay** (`outbox_relay`): reads and removes messages, publishes them.
- **Operator / DBA** with superuser or owner rights (resets, slot management).
- **Attacker** with access to one compromised component (the business service or the relay host).

## Threats and mitigations

| # | Threat | Mitigation in this repository | Residual risk / production advice |
|---|---|---|---|
| T1 | [Spoofing / Tampering] A compromised relay forges or alters messages. | The relay role cannot INSERT into the outbox or the queue (PostgreSQL: SELECT/UPDATE/DELETE only; Oracle: SELECT/DELETE, DEQUEUE). Tested in `Roles_are_least_privilege` / `Users_are_least_privilege`. | A relay can still drop or alter a message *after* reading it. Sign events at the producer if consumers must verify origin. |
| T2 | [Information disclosure] The relay reads business data. | The relay has no privilege on `orders` (PostgreSQL `42501`, Oracle `ORA-00942`, tested). | PostgreSQL's REPLICATION attribute defeats this for the logical strategy: a role with it can create its own slot with `test_decoding` and read every row change in the database, `orders` included. The lab's slot decodes only messages (empty publication), but the credential is as sensitive as read access to the whole database. Use a separate relay role without REPLICATION for the polling strategies. |
| T3 | [Tampering] The business service reads or deletes pending messages to hide them. | `outbox_app` has INSERT only on PostgreSQL and INSERT plus ENQUEUE on Oracle; it cannot SELECT the outbox (PostgreSQL `42501`, Oracle `ORA-41900`) or dequeue (tested). | Superusers and schema owners can do anything; audit them separately. |
| T4 | [Repudiation / Integrity] Messages are lost silently (id high-water mark relay). | `pg-id-cursor` is kept only to measure the loss; the README and ADR 0002 say never to use it. Every other relay is at-least-once and tested for zero loss under commit reordering and relay crashes. | Monitor outbox age (oldest pending row, slot lag, AQ queue depth). |
| T5 | [Integrity] Duplicates after a relay crash are applied twice by consumers. | Messages carry (aggregate, sequence); ADR 0005 prescribes consumer deduplication. Crash tests show duplicates are bounded by the in-flight batch. | Consumers without deduplication will double-apply. |
| T6 | [Denial of service] A stopped logical relay pins WAL until the disk is full. | The lab drops the slot whenever a non-logical strategy resets. ADR 0003 documents the risk. | Set `max_slot_wal_keep_size`, alert on `pg_replication_slots` lag, run the relay with a restart policy. |
| T7 | [Denial of service] One long transaction stops `pg-xmin` delivery cluster-wide. | Documented (ADR 0002); the ordering test shows the hold-back. | Alert on old `backend_xmin`; set `idle_in_transaction_session_timeout`. |
| T8 | [Denial of service] NOTIFY's global commit lock throttles every writer that notifies. | Measured in the `publish` scenario. | Batch notifications, or notify from the relay side only when idle. |
| T9 | [Information disclosure] Credentials leak through the repository or logs. | Only development defaults (suffix `-dev-only` / `Dev1`) in compose and `.env.example`; tests generate throwaway containers with test-only passwords; connection strings are never logged. Host ports bind to 127.0.0.1. | Production deployments must inject secrets from a vault and use TLS to the database. |
| T10 | [Tampering] Malformed payloads crash the relay. | `EventCodec.Decode` validates length and magic and throws `FormatException`, which aborts the claim transaction, so the message stays for inspection instead of being deleted. Property-tested round trip. | A poison message blocks a single ordered reader (`pg-xmin`, `pg-logical`) until it is removed: production relays need a dead-letter path. |

## Out of scope

TLS between the lab and the databases, secret rotation, multi-tenant isolation, and broker-side security: the lab has
no broker by design (ADR 0001).

## Data

All data is synthetic: integer aggregate ids, sequence numbers, timestamps and filler bytes. There is no personal data,
so KVKK (the Turkish personal data protection law) and GDPR obligations do not arise for the lab itself.
