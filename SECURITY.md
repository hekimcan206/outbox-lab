# Security policy

## Supported versions

Only the latest release receives fixes.

## Reporting a vulnerability

Please **do not open a public issue**. Use GitHub's private vulnerability reporting
(**Security → Report a vulnerability**). Expect an acknowledgement within 5 working days and a fix or
mitigation plan within 30 days of confirmation.

## Data

This project contains **synthetic data only** (integer aggregate ids, sequence numbers, timestamps, filler bytes).
Never submit real business or personal data in issues, pull requests or benchmark results. See
`docs/threat-model.md` for the threats a production adoption of these relay designs inherits.

## Security-relevant defaults

| Area | Default |
|---|---|
| PostgreSQL roles | `outbox_app`: INSERT on `orders` and `outbox` only. `outbox_relay`: SELECT, UPDATE (needed by `FOR UPDATE`) and DELETE on `outbox`, REPLICATION for logical decoding; no access to `orders`. Superuser only for resets |
| Oracle users | `outbox_app`: INSERT on both tables, ENQUEUE. `outbox_relay`: SELECT and DELETE on `outbox`, DEQUEUE; no access to `orders`. `outbox_owner` only for resets |
| Privilege tests | The init scripts used by docker compose are the ones the integration tests run; denied access is asserted per role |
| SQL | All values are bind parameters; no SQL text is built from input |
| Container | Chiseled runtime image, non-root UID 1654, no shell; CI verifies the image user |
| Network | Database ports bind to 127.0.0.1 only |
| Supply chain | Central package versions, updated by hand; vulnerable transitive `System.Security.Cryptography.Pkcs` pinned to a patched version; CI fails on known-vulnerable direct or transitive packages (`dotnet list package --vulnerable --include-transitive`); CodeQL `security-and-quality` |
| Secrets | None in the repository; compose defaults are development-only and overridable via `.env` |

Not included: TLS to the databases, secret rotation, a broker (the lab measures the outbox, not the broker).
