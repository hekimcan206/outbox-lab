### Drain throughput (pre-filled backlog)

| Strategy | Engine | Workers | Messages | Seconds | Messages/s | Inversions | Duplicates | Missing |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| pg-skip-locked | PostgreSQL 17 | 4 | 20,000 | 1.25 | 16,000 | 312 | 0 | 0 |
| pg-logical | PostgreSQL 17 | 1 | 20,000 | - | - | - | - | - |

### Commit-to-delivery latency (open loop)

| Strategy | Engine | Workers | Offered/s | Achieved/s | Samples | p50 ms | p95 ms | p99 ms | max ms | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ora-aq | Oracle 23ai Free | 1 | 250 | 273 | 2,750 | 3.2 | 7.5 | 12.0 | 30.9 | poll/wait 100 ms |

### Ordering, loss and duplicates under commit reordering

| Strategy | Engine | Workers | Messages | Delivered | Missing | Duplicates | Inversions | Aggregates hit | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| pg-id-cursor | PostgreSQL 17 | 1 | 2,000 | 1,987 | 13 | 0 | 0 | 0 | hold 0-5 ms |

### Publish cost (closed loop, no relay)

| Strategy | Engine | Producers | Transactions | Transactions/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|
| pg-notify | PostgreSQL 17 | 32 | 41,234 | 4,123 | 7.46 | 21.33 |
