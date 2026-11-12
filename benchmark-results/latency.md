### Commit-to-delivery latency (open loop)

| Strategy | Engine | Workers | Offered/s | Achieved/s | Samples | p50 ms | p95 ms | p99 ms | max ms | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| pg-skip-locked | PostgreSQL | 1 | 100 | 100 | 1,502 | 68.8 | 126.7 | 157.6 | 245.1 | poll/wait 100 ms |
| pg-notify | PostgreSQL | 1 | 100 | 100 | 1,501 | 6.0 | 13.1 | 25.6 | 91.1 | poll/wait 100 ms |
| pg-xmin | PostgreSQL | 1 | 100 | 100 | 1,501 | 63.4 | 109.9 | 122.2 | 153.8 | poll/wait 100 ms |
| pg-logical | PostgreSQL | 1 | 100 | 100 | 1,501 | 4.2 | 7.5 | 17.7 | 35.7 | poll/wait 100 ms |
| ora-skip-locked | Oracle | 1 | 100 | 100 | 1,502 | 64.7 | 124.2 | 135.4 | 187.1 | poll/wait 100 ms |
| ora-aq | Oracle | 1 | 100 | 100 | 1,501 | 4.4 | 12.9 | 33.6 | 52.0 | poll/wait 100 ms |
| pg-skip-locked | PostgreSQL | 1 | 500 | 499 | 7,503 | 63.9 | 112.9 | 125.8 | 141.6 | poll/wait 100 ms |
| pg-notify | PostgreSQL | 1 | 500 | 328 | 8,227 | 10.8 | 34.0 | 52.1 | 1,856.0 | poll/wait 100 ms |
| pg-xmin | PostgreSQL | 1 | 500 | 500 | 7,968 | 63.4 | 113.5 | 128.1 | 527.7 | poll/wait 100 ms |
| pg-logical | PostgreSQL | 1 | 500 | 499 | 7,636 | 6.0 | 14.0 | 30.2 | 376.9 | poll/wait 100 ms |
| ora-skip-locked | Oracle | 1 | 500 | 494 | 7,504 | 68.0 | 125.1 | 181.8 | 300.7 | poll/wait 100 ms |
| ora-aq | Oracle | 1 | 500 | 499 | 7,508 | 7.1 | 16.6 | 28.9 | 233.4 | poll/wait 100 ms |
