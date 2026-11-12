### Publish cost (closed loop, no relay)

| Strategy | Engine | Producers | Transactions | Transactions/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|
| pg-skip-locked | PostgreSQL | 1 | 1,065 | 133 | 5.02 | 49.11 |
| pg-notify | PostgreSQL | 1 | 1,063 | 133 | 6.33 | 25.45 |
| pg-logical | PostgreSQL | 1 | 1,104 | 138 | 5.46 | 25.89 |
| ora-skip-locked | Oracle | 1 | 1,906 | 238 | 3.42 | 16.41 |
| ora-aq | Oracle | 1 | 1,535 | 191 | 3.87 | 34.46 |
| pg-skip-locked | PostgreSQL | 8 | 7,807 | 974 | 7.35 | 21.99 |
| pg-notify | PostgreSQL | 8 | 5,759 | 718 | 10.26 | 24.22 |
| pg-logical | PostgreSQL | 8 | 9,152 | 1,141 | 5.51 | 33.54 |
| ora-skip-locked | Oracle | 8 | 7,526 | 940 | 6.44 | 38.34 |
| ora-aq | Oracle | 8 | 7,220 | 898 | 6.47 | 45.23 |
| pg-skip-locked | PostgreSQL | 32 | 11,714 | 1,458 | 16.29 | 71.36 |
| pg-notify | PostgreSQL | 32 | 4,197 | 519 | 50.22 | 165.92 |
| pg-logical | PostgreSQL | 32 | 14,770 | 1,842 | 12.76 | 64.39 |
| ora-skip-locked | Oracle | 32 | 13,994 | 1,746 | 12.30 | 75.38 |
| ora-aq | Oracle | 32 | 10,888 | 1,350 | 14.33 | 74.47 |
