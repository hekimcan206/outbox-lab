#!/bin/sh
# Runs once, when the PostgreSQL data directory is first initialised (docker-entrypoint-initdb.d).
# The Testcontainers integration tests run this exact script, so the privilege model below is tested, not assumed.
#
# Least privilege:
#   outbox_app    is the business service: INSERT on the business table and the outbox, nothing else. It may call
#                 pg_notify and pg_logical_emit_message, which are open to every role.
#   outbox_relay  is the relay: SELECT, UPDATE (needed for FOR UPDATE row locks) and DELETE on the outbox, plus
#                 the REPLICATION attribute for the logical decoding strategy. It cannot read or change business data.
#   The superuser (POSTGRES_USER) owns the schema; the lab uses it only to reset state between runs.
set -eu

: "${APP_PASSWORD:?APP_PASSWORD must be set}"
: "${RELAY_PASSWORD:?RELAY_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
     -v app_password="$APP_PASSWORD" \
     -v relay_password="$RELAY_PASSWORD" \
     -v db="$POSTGRES_DB" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE outbox_app LOGIN PASSWORD :'app_password';
CREATE ROLE outbox_relay LOGIN REPLICATION PASSWORD :'relay_password';

REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO outbox_app, outbox_relay;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA outbox_lab;
GRANT USAGE ON SCHEMA outbox_lab TO outbox_app, outbox_relay;

-- The business table: each outbox message is written in the same transaction as one row here.
CREATE TABLE outbox_lab.orders (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id  integer       NOT NULL,
    aggregate_seq bigint        NOT NULL,
    amount        numeric(12,2) NOT NULL,
    created_at    timestamptz   NOT NULL DEFAULT now()
);

-- The outbox for the polling strategies. txid records the writing transaction so the commit-horizon relay
-- (pg-xmin) can read only rows whose transaction can no longer be in flight.
CREATE TABLE outbox_lab.outbox (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id  integer NOT NULL,
    aggregate_seq bigint  NOT NULL,
    payload       bytea   NOT NULL,
    txid          xid8    NOT NULL DEFAULT pg_current_xact_id()
);
CREATE INDEX outbox_txid_id ON outbox_lab.outbox (txid, id);

-- pgoutput needs a publication name. Logical decoding messages (pg_logical_emit_message) are not filtered by
-- publication, so the WAL-only strategy uses an empty one and no table at all.
CREATE PUBLICATION outbox_lab_pub;

GRANT INSERT ON outbox_lab.orders, outbox_lab.outbox TO outbox_app;
-- UPDATE is required by SELECT ... FOR UPDATE (row locks), even though the relay never updates a row.
GRANT SELECT, UPDATE, DELETE ON outbox_lab.outbox TO outbox_relay;
SQL
