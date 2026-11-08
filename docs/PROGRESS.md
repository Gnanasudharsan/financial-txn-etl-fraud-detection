# Progress

| Phase | Status |
|---|---|
| 0 – Scaffold | ✅ done |
| 1 – Data generator | ✅ done |
| 2 – Bronze | ✅ done |
| 3 – Silver | ✅ done |
| 4 – Gold + features | ⏳ next |
| 5 – Model | ☐ |
| 6 – Reconciliation | ☐ |
| 7 – Security & audit | ☐ |
| 8 – Orchestration & deploy | ☐ |
| 9 – Dashboards | ☐ |
| 10 – CI & polish | ☐ |

## Next phase
Phase 4 – gold point-in-time features and DQ marts.

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
- Silver @ 5M: 5,037,479 bronze rows -> 4,916,368 valid (97.60%) + 121,111 quarantined (2.40%).
  Quarantine by primary reason: DUPLICATE_EXACT 24,860 · AMOUNT_MALFORMED 17,506 ·
  TS_UNPARSEABLE 15,069 · DUPLICATE_NEAR 13,940 · CURRENCY_INVALID 9,811 · NULL_AMOUNT 6,051 ·
  NULL_CARD 6,025 · NULL_EVENT_TS 6,003 · NULL_CURRENCY 5,981 · NULL_MERCHANT 5,953 ·
  TS_FUTURE 4,904 · AMOUNT_NON_POSITIVE 2,519 · CORRUPT_RECORD 2,489 (= injected corrupt lines).
  GX suite: 23/23 expectations passed. 869.2 s wall clock (incl. GX scans over s3a).
