# Benchmark Results

This benchmark compares Floe, Pandas, and Spark (local mode) on a single-node CSV ingestion workload with consistent schema enforcement and checks. Runs were executed on the same datasets and configuration.

## Machine Specs

- CPU: AMD Ryzen AI 7 350
- RAM: 32 GB
- OS: Windows (WSL)

## Results

All times are wall-clock seconds. `peak_rss_mb` is best-effort and may under-report for Spark on WSL.

| Tool  | Dataset | Rows     | Wall Time (s) | Peak RSS (MB) | Accepted | Rejected | Notes     |
|-------|---------|----------|---------------|---------------|----------|----------|-----------|
| floe  | uber    | 100,000  | 0.0997        | 46.75         | 100,000  | 0        |           |
| floe  | uber    | 1,000,000| 0.9706        | 131.84        | 1,000,000| 0        |           |
| floe  | uber    | 5,000,000| 5.1067        | 495.80        | 5,000,000| 0        |           |
| pandas| uber    | 100,000  | 0.2550        | 92.16         | 100,000  | 0        |           |
| pandas| uber    | 1,000,000| 2.0062        | 220.89        | 1,000,000| 0        |           |
| pandas| uber    | 5,000,000| 9.7555        | 707.65        | 5,000,000| 0        |           |
| spark | uber    | 100,000  | 6.4687        | 36.64         | 100,000  | 0        | local[*]  |
| spark | uber    | 1,000,000| 2.4636        | 36.64         | 1,000,000| 0        | local[*]  |
| spark | uber    | 5,000,000| 8.0092        | 36.64         | 5,000,000| 0        | local[*]  |

## Notes

- Spark startup overhead dominates the small dataset run (100k rows).
- Peak RSS for Spark is likely under-reported in WSL due to JVM process accounting; treat it as indicative only.

