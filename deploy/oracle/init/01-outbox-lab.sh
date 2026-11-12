#!/bin/bash
# Runs once, on the first start of the gvenzl/oracle-free container, as the oracle OS user.
# The Testcontainers integration tests run this exact script, so the privilege model below is tested, not assumed.
#
# Least privilege:
#   outbox_owner  owns the business table, the outbox table and the Advanced Queuing queue (DDL, queue admin).
#                 The lab uses it only to reset state between runs.
#   outbox_app    is the business service: INSERT on orders and outbox, ENQUEUE on the queue.
#   outbox_relay  is the relay: SELECT and DELETE on the outbox (SELECT also allows FOR UPDATE), DEQUEUE on the
#                 queue. It cannot read or change business data.
set -euo pipefail

: "${OWNER_PASSWORD:?OWNER_PASSWORD must be set}"
: "${APP_PASSWORD:?APP_PASSWORD must be set}"
: "${RELAY_PASSWORD:?RELAY_PASSWORD must be set}"

sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE USER outbox_owner IDENTIFIED BY "${OWNER_PASSWORD}" DEFAULT TABLESPACE users QUOTA 2G ON users;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO outbox_owner;
GRANT EXECUTE ON SYS.DBMS_AQADM TO outbox_owner;
GRANT EXECUTE ON SYS.DBMS_AQ TO outbox_owner;

CREATE USER outbox_app IDENTIFIED BY "${APP_PASSWORD}";
GRANT CREATE SESSION TO outbox_app;
GRANT EXECUTE ON SYS.DBMS_AQ TO outbox_app;

CREATE USER outbox_relay IDENTIFIED BY "${RELAY_PASSWORD}";
GRANT CREATE SESSION TO outbox_relay;
GRANT EXECUTE ON SYS.DBMS_AQ TO outbox_relay;
EXIT
SQL

sqlplus -s "outbox_owner/\"${OWNER_PASSWORD}\"@//localhost:1521/FREEPDB1" <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE

-- The business table: each outbox message is written in the same transaction as one row here.
CREATE TABLE orders (
    id            NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id  NUMBER(10)   NOT NULL,
    aggregate_seq NUMBER(19)   NOT NULL,
    amount        NUMBER(12,2) NOT NULL,
    created_at    TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
);

-- The outbox for the polling strategy. RAW(2000) holds the same binary payload PostgreSQL stores in bytea.
CREATE TABLE outbox (
    id            NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id  NUMBER(10) NOT NULL,
    aggregate_seq NUMBER(19) NOT NULL,
    payload       RAW(2000)  NOT NULL
);

-- Classic Advanced Queuing, single consumer, RAW payload, FIFO by enqueue time. Enqueue with the default
-- ON_COMMIT visibility joins the producer's transaction, which is what makes AQ an outbox.
BEGIN
    DBMS_AQADM.CREATE_QUEUE_TABLE(
        queue_table        => 'outbox_qt',
        queue_payload_type => 'RAW',
        sort_list          => 'ENQ_TIME',
        multiple_consumers => FALSE);
    DBMS_AQADM.CREATE_QUEUE(queue_name => 'outbox_q', queue_table => 'outbox_qt');
    DBMS_AQADM.START_QUEUE(queue_name => 'outbox_q');
    DBMS_AQADM.GRANT_QUEUE_PRIVILEGE(privilege => 'ENQUEUE', queue_name => 'outbox_q', grantee => 'OUTBOX_APP');
    DBMS_AQADM.GRANT_QUEUE_PRIVILEGE(privilege => 'DEQUEUE', queue_name => 'outbox_q', grantee => 'OUTBOX_RELAY');
END;
/

GRANT INSERT ON orders TO outbox_app;
GRANT INSERT ON outbox TO outbox_app;
GRANT SELECT, DELETE ON outbox TO outbox_relay;
EXIT
SQL
