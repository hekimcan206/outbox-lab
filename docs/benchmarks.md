# Benchmarks

Every number here was measured on the machine below and comes from `benchmark-results/*.json` (all raw runs plus the
median run of each configuration) and the matching `*.md` tables. Each configuration ran three times; the run with
the median key value is reported (throughput, p50 latency, or inversions + missing), and the spread of the three runs
is given where it matters. Nothing is extrapolated.

## Environment

| | |
|---|---|
| CPU | 13th Gen Intel Core i7-13620H (10 cores, 16 threads), laptop |
| RAM | 15.7 GB host; Docker Desktop (WSL2) VM limited to 7.6 GB |
| OS | Windows 11 Home 25H2 (build 26200), Docker Desktop, engine 29.9.1 |
| .NET | SDK 10.0.401, runtime 10.0.12, Release build of `outbox-lab` on the host |
| PostgreSQL | 17.11 (`postgres:17-alpine`), `wal_level=logical`, otherwise defaults (`fsync` and `synchronous_commit` on); container limited to **2 CPUs**, 1 GB |
| Oracle | Oracle AI Database Free 23.26.3 (`gvenzl/oracle-free:23-slim-faststart`), which limits itself to **2 CPU threads** and 2 GB SGA+PGA; container limited to 2 CPUs, 3 GB |
| Network | The lab runs on the Windows host and reaches both containers through Docker Desktop port forwarding (127.0.0.1) |
| Host load | **Not idle.** Another agent's builds and tests ran in parallel, plus a set of idle Supabase containers. Sampled host CPU load was 42% before the publish runs, 49-54% before the others (`Win32_Processor.LoadPercentage`) |

Run-to-run spread is large on this machine (often ±30%, occasionally 2x on a single run), which is why medians of
three are reported and why small differences below should not be read as significant.

How to reproduce (Docker running):

```bash
docker compose up -d postgres oracle
dotnet run -c Release --project src/OutboxLab.Runner -- bench all --runs 3 --out benchmark-results
# or one scenario / a subset:
dotnet run -c Release --project src/OutboxLab.Runner -- bench latency --runs 3 --strategies pg-notify,pg-logical
```

Common settings (`ScenarioSettings`): payload 256 bytes, relay batch 100, poll interval / wait timeout 100 ms,
64 aggregates, fixed seed. Every producer writes one business row plus the message in one round trip plus COMMIT.

## 1. Publish cost: what each strategy adds to the business transaction

**Method.** Closed-loop producers (1, 8, 32 concurrent connections), each committing single-message business
transactions as fast as it can for 8 s, with no relay running. One warm-up transaction per connection first.
Latency is one whole business transaction (INSERT order + outbox write + COMMIT), measured by the producer.

| Strategy | 1 producer | 8 producers | 32 producers | p50 / p99 at 32 |
|---|---:|---:|---:|---:|
| `pg-skip-locked` (outbox row) | 133 /s | 974 /s | 1,458 /s | 16.3 / 71.4 ms |
| `pg-notify` (outbox row + `pg_notify`) | 133 /s | **718 /s** | **519 /s** | **50.2 / 165.9 ms** |
| `pg-logical` (`pg_logical_emit_message`, no table) | 138 /s | 1,141 /s | 1,842 /s | 12.8 / 64.4 ms |
| `ora-skip-locked` (outbox row) | 238 /s | 940 /s | 1,746 /s | 12.3 / 75.4 ms |
| `ora-aq` (`DBMS_AQ.ENQUEUE`) | 191 /s | 898 /s | 1,350 /s | 14.3 / 74.5 ms |

Spread of the three runs at 32 producers: `pg-skip-locked` 1,146-1,717, `pg-notify` 375-629, `pg-logical`
1,600-2,004, `ora-skip-locked` 1,552-1,824, `ora-aq` 1,227-1,462 transactions/s.

Findings:

- **NOTIFY stops scaling.** With one producer it costs nothing measurable; at 8 producers it delivers 74% of the
  plain table's throughput, and at 32 producers **36%**, with *fewer* commits per second than at 8. This is the
  global lock PostgreSQL takes when committing a transaction that issued `NOTIFY` (ADR 0002): commits that notify are
  serialised. Notifying from every business transaction is the wrong default above a handful of writers.
- **The WAL-only outbox is the cheapest write.** `pg_logical_emit_message` writes one WAL record and no heap tuple or
  index entry, and is 26% faster than an outbox row at 32 producers (median), consistent across runs.
- **AQ enqueue costs about 23% against a plain outbox row on Oracle at 32 producers** (1,350 vs 1,746 /s).
- With one producer every strategy is bound by commit latency (about 5 ms on PostgreSQL, 3.4 ms on Oracle through
  Docker Desktop on this laptop), not by the strategy.

## 2. Drain throughput: how fast a relay empties a backlog

**Method.** 20,000 committed messages (64 aggregates) are written first, then the relay starts and drains them into
the in-process sink. Throughput is messages divided by the time from the first to the last delivery (relay start-up
excluded). Per-aggregate inversions are counted on the way.

| Strategy | Workers | Messages/s (median) | Spread of 3 runs | Per-aggregate inversions |
|---|---:|---:|---:|---:|
| `pg-skip-locked` | 1 | 10,330 | 10,043-13,668 | 0 |
| `pg-skip-locked` | 4 | 45,699 | 29,446-54,418 | 1,260 |
| `pg-xmin` | 1 | 9,852 | 5,820-17,067 | 0 |
| `pg-logical` | 1 | **146,969** | 143,654-175,493 | 0 |
| `ora-skip-locked` | 1 | 7,566 | 6,511-8,276 | 0 |
| `ora-skip-locked` | 4 | 13,755 | 12,175-14,445 | 3,761 |
| `ora-aq` | 1 | 5,856 | 4,078-7,105 | 0 |
| `ora-aq` | 4 | 5,029 | 1,382-6,168 | 5,727 |

Findings:

- **Logical decoding drains an order of magnitude faster** than any polling relay: the WAL is read sequentially and
  there is nothing to delete. Its ceiling is decoding plus one consumer, not the outbox table.
- **Competing workers buy throughput with order.** Four SKIP LOCKED workers drain 4.4x faster on PostgreSQL and 1.8x
  on Oracle (two CPU threads), and per-aggregate order breaks immediately: 1,260 and 3,761 late messages out of 20,000.
- **`pg-xmin` costs nothing against a single SKIP LOCKED worker** (same order of magnitude, overlapping spreads): the
  horizon filter is an index range on `(txid, id)`.
- **More AQ dequeuers did not help on Database Free.** Four `DequeueArray` workers were no faster than one (and one
  run collapsed to 1,382 /s). With `FIRST_MESSAGE` navigation every dequeue restarts at the head of the queue and
  skips messages locked by the other workers; on two CPU threads that contention outweighs the parallelism.

## 3. Ordering, loss and duplicates under commit reordering

**Method.** 8 producers commit 4,000 single-message transactions; each holds its transaction open for a random
0-5 ms after writing, so transactions commit out of id order. The relay runs concurrently. A run ends when all
messages arrived or deliveries have been quiet for 3 s.

| Strategy | Workers | Missing (3 runs) | Duplicates | Inversions (3 runs) |
|---|---:|---|---:|---|
| `pg-id-cursor` | 1 | **60, 87, 63** | 0 | 0, 0, 0 |
| `pg-skip-locked` | 1 | 0, 0, 0 | 0 | 0, 0, 0 |
| `pg-skip-locked` | 4 | 0, 0, 0 | 0 | 40, 143, 18 |
| `pg-xmin` | 1 | 0, 0, 0 | 0 | 0, 0, 0 |
| `pg-logical` | 1 | 0, 0, 0 | 0 | 0, 0, 0 |
| `ora-skip-locked` | 1 | 0, 0, 0 | 0 | 0, 0, 0 |
| `ora-skip-locked` | 4 | 0, 0, 0 | 0 | 18, 55, 49 |
| `ora-aq` | 1 | 0, 0, 0 | 0 | 0, 0, 0 |
| `ora-aq` | 4 | 0, 0, 0 | 0 | 0, 1, 0 |

Findings:

- **The id high-water mark loses about 1.5-2.2% of messages** (60-87 of 4,000) with only 0-5 ms of commit jitter.
  Those messages are committed and sit in the table forever; nothing errors.
- **Every other strategy lost nothing** in any run. No duplicates appear without crashes; the crash tests in the
  integration suites cover redelivery.
- **Single readers keep per-aggregate order** (`pg-xmin`, `pg-logical`, and one worker of any kind). Four competing
  workers reorder messages of the same aggregate on both engines. AQ with four workers reordered only once in three
  runs at this rate (the producers, at about 700 messages/s, rarely let two dequeuers hold consecutive messages of
  one aggregate); under backlog (section 2) it reorders heavily.

## 4. Commit-to-delivery latency

**Method.** A ticker schedules single-message business transactions at a fixed rate (100 and 500 /s) for 15 s after
a 2 s warm-up, independent of how the system responds (open loop); four producer connections take the ticks. One
relay worker. Latency runs from the start of the producing transaction to the sink, so it includes the business
transaction's own commit. Polling strategies sleep 100 ms when a claim comes back short; push strategies wait up to
100 ms (AQ: 1 s, the minimum `Wait`) and are woken by the commit.

| Strategy | Rate | Achieved | p50 | p95 | p99 | max | p50 of the 3 runs |
|---|---:|---:|---:|---:|---:|---:|---|
| `pg-skip-locked` | 100 /s | 100 /s | 68.8 ms | 126.7 ms | 157.6 ms | 245 ms | 64.0, 68.8, 70.3 |
| `pg-notify` | 100 /s | 100 /s | **6.0 ms** | 13.1 ms | 25.6 ms | 91 ms | 5.2, 6.0, 6.9 |
| `pg-xmin` | 100 /s | 100 /s | 63.4 ms | 109.9 ms | 122.2 ms | 154 ms | 63.4, 63.4, 63.7 |
| `pg-logical` | 100 /s | 100 /s | **4.2 ms** | 7.5 ms | 17.7 ms | 36 ms | 4.2, 4.2, 4.9 |
| `ora-skip-locked` | 100 /s | 100 /s | 64.7 ms | 124.2 ms | 135.4 ms | 187 ms | 64.5, 64.7, 64.8 |
| `ora-aq` | 100 /s | 100 /s | **4.4 ms** | 12.9 ms | 33.6 ms | 52 ms | 4.2, 4.4, 5.4 |
| `pg-skip-locked` | 500 /s | 499 /s | 63.9 ms | 112.9 ms | 125.8 ms | 142 ms | 63.0, 63.9, 64.1 |
| `pg-notify` | 500 /s | **328 /s** | 10.8 ms | 34.0 ms | 52.1 ms | 1,856 ms | 9.5, 10.8, 12.8 |
| `pg-xmin` | 500 /s | 500 /s | 63.4 ms | 113.5 ms | 128.1 ms | 528 ms | 63.2, 63.4, 63.8 |
| `pg-logical` | 500 /s | 499 /s | **6.0 ms** | 14.0 ms | 30.2 ms | 377 ms | 5.3, 6.0, 6.2 |
| `ora-skip-locked` | 500 /s | 494 /s | 68.0 ms | 125.1 ms | 181.8 ms | 301 ms | 66.1, 68.0, 68.0 |
| `ora-aq` | 500 /s | 499 /s | **7.1 ms** | 16.6 ms | 28.9 ms | 233 ms | 6.7, 7.1, 8.7 |

Findings:

- **Polling latency is the poll interval, whatever the engine.** All three pollers sit at 63-69 ms p50 with a
  100 ms interval: half the interval, plus the claim, the producer's commit and Windows timer granularity (`Task.Delay` resolves to
  the 15.6 ms system tick; not separately measured). The three runs agree within a few ms, so this is
  the most reproducible number in the report. Shorter intervals trade latency for idle queries.
- **Push strategies are 11-16x faster at p50.** `pg-logical` (4.2 ms), `ora-aq` (4.4 ms) and `pg-notify` (6.0 ms)
  at 100 /s; most of what remains is the producer's own commit (about 3.4-5 ms here, section 1).
- **NOTIFY could not sustain 500 /s with four producers** (328 /s achieved in the median run, 363-473 in the others):
  the open-loop ticks queued up behind the producers, which matches the commit-lock ceiling in section 1. Its max
  of 1.9 s is a stall in that queue.
- **`pg-xmin` adds no latency of its own** compared with `pg-skip-locked` at the same interval: the horizon only
  holds rows back while an older transaction is open, and these transactions are short.
- Tails (max) of a few hundred ms on otherwise fast strategies appear in single runs and move between strategies from
  run to run; on this shared laptop they are host scheduling noise rather than a property of a strategy.

## What the numbers say

| If you need | PostgreSQL | Oracle |
|---|---|---|
| Lowest latency and cheapest writes | `pg-logical` (4-6 ms p50, fastest publish, ordered); pay with slot operations (ADR 0003) | `ora-aq` (4-7 ms p50); costs about 23% of write throughput against a table at 32 writers |
| Throughput with unordered consumers | `pg-skip-locked`, several workers (45.7k/s drain with 4); add `NOTIFY` only with few writers | `ora-skip-locked`, several workers (13.8k/s with 4 on 2 CPU threads) |
| Per-aggregate order on a table outbox | `pg-xmin`, one reader (no loss, no inversions, same cost as one SKIP LOCKED worker) | one worker of either strategy |
| Never | `pg-id-cursor`: lost 1.5-2.2% of messages with 0-5 ms commit jitter | `ROWNUM <= n ... SKIP LOCKED` (starves workers, see ADR 0004) |
