# Progress

| Phase | Status |
|---|---|
| 0 – Scaffold | ✅ done |
| 1 – Data generator | ✅ done |
| 2 – Bronze | ✅ done |
| 3 – Silver | ⏳ next |
| 4 – Gold + features | ☐ |
| 5 – Model | ☐ |
| 6 – Reconciliation | ☐ |
| 7 – Security & audit | ☐ |
| 8 – Orchestration & deploy | ☐ |
| 9 – Dashboards | ☐ |
| 10 – CI & polish | ☐ |

## Next phase
Phase 3 – silver (cleaning, dedup, GX validation, quarantine, CHECK constraints, PII encryption).

## Open issues
- MinIO images/binaries are no longer distributed; local S3 API is SeaweedFS (see DECISIONS.md, D-001).

## Key results
- Toolchain: Python 3.11.18, PySpark 3.5.9, delta-spark 3.3.3, Java 17.0.21, Terraform 1.17.1.
- Generator @ 5M (seed 42, 2026-01-01 → 2026-06-30): 4,997,510 clean transactions, 40,515 fraud
  (0.811%), 5,037,479 raw feed rows after defect injection, 4,916,427 ledger rows, 1,116 files,
  2.39 GB raw; 96.9 s wall clock, 0.85 GB peak RSS on an M-series laptop.
- Bronze @ 5M on SeaweedFS (s3a): 466 transaction files -> 5,037,479 rows (= raw line count),
  4,916,427 ledger, 4,903,765 settlement, 40,515 label rows; 50k/100k/20k dimension rows MERGEd.
  407.6 s wall clock (`make bronze`, local[*], 10 cores). PANs tokenized on land.
