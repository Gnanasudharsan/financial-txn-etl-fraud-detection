# Progress

| Phase | Status |
|---|---|
| 0 – Scaffold | ✅ done |
| 1 – Data generator | ✅ done |
| 2 – Bronze | ⏳ next |
| 3 – Silver | ☐ |
| 4 – Gold + features | ☐ |
| 5 – Model | ☐ |
| 6 – Reconciliation | ☐ |
| 7 – Security & audit | ☐ |
| 8 – Orchestration & deploy | ☐ |
| 9 – Dashboards | ☐ |
| 10 – CI & polish | ☐ |

## Next phase
Phase 2 – bronze ingestion (Spark, Delta, processed-file tracking, corrupt-record capture).

## Open issues
- MinIO images/binaries are no longer distributed; local S3 API is SeaweedFS (see DECISIONS.md, D-001).

## Key results
- Toolchain: Python 3.11.18, PySpark 3.5.9, delta-spark 3.3.3, Java 17.0.21, Terraform 1.17.1.
- Generator @ 5M (seed 42, 2026-01-01 → 2026-06-30): 4,997,510 clean transactions, 40,515 fraud
  (0.811%), 5,044,977 raw feed rows after defect injection, 4,916,427 ledger rows, 1,116 files,
  2.39 GB raw; 96.9 s wall clock, 0.85 GB peak RSS on an M-series laptop.
