### Ordering, loss and duplicates under commit reordering

| Strategy | Engine | Workers | Messages | Delivered | Missing | Duplicates | Inversions | Aggregates hit | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| pg-id-cursor | PostgreSQL | 1 | 4,000 | 3,937 | 63 | 0 | 0 | 0 | hold 0-5 ms |
| pg-skip-locked | PostgreSQL | 1 | 4,000 | 4,000 | 0 | 0 | 0 | 0 | hold 0-5 ms |
| pg-skip-locked | PostgreSQL | 4 | 4,000 | 4,000 | 0 | 0 | 40 | 14 | hold 0-5 ms |
| pg-xmin | PostgreSQL | 1 | 4,000 | 4,000 | 0 | 0 | 0 | 0 | hold 0-5 ms |
| pg-logical | PostgreSQL | 1 | 4,000 | 4,000 | 0 | 0 | 0 | 0 | hold 0-5 ms |
| ora-skip-locked | Oracle | 1 | 4,000 | 4,000 | 0 | 0 | 0 | 0 | hold 0-5 ms |
| ora-skip-locked | Oracle | 4 | 4,000 | 4,000 | 0 | 0 | 49 | 32 | hold 0-5 ms |
| ora-aq | Oracle | 1 | 4,000 | 4,000 | 0 | 0 | 0 | 0 | hold 0-5 ms |
| ora-aq | Oracle | 4 | 4,000 | 4,000 | 0 | 0 | 0 | 0 | hold 0-5 ms |
