### Drain throughput (pre-filled backlog)

| Strategy | Engine | Workers | Messages | Seconds | Messages/s | Inversions | Duplicates | Missing |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| pg-skip-locked | PostgreSQL | 1 | 20,000 | 1.94 | 10,330 | 0 | 0 | 0 |
| pg-skip-locked | PostgreSQL | 4 | 20,000 | 0.44 | 45,699 | 1,260 | 0 | 0 |
| pg-xmin | PostgreSQL | 1 | 20,000 | 2.03 | 9,852 | 0 | 0 | 0 |
| pg-logical | PostgreSQL | 1 | 20,000 | 0.14 | 146,969 | 0 | 0 | 0 |
| ora-skip-locked | Oracle | 1 | 20,000 | 2.64 | 7,566 | 0 | 0 | 0 |
| ora-skip-locked | Oracle | 4 | 20,000 | 1.45 | 13,755 | 3,761 | 0 | 0 |
| ora-aq | Oracle | 1 | 20,000 | 3.42 | 5,856 | 0 | 0 | 0 |
| ora-aq | Oracle | 4 | 20,000 | 3.98 | 5,029 | 5,727 | 0 | 0 |
