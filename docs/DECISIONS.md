# Architecture Decision Log

Short, dated records of decisions and the reason behind each one.

## D-001 · SeaweedFS instead of MinIO for the local S3 API (2026-11-07)
**Context.** The plan called for MinIO in Docker. During setup, `minio/minio` and `minio/mc` no
longer resolve on Docker Hub, `quay.io/minio/*` returns 401, `dl.min.io` archive binaries
return HTTP 410, and the `minio/minio` GitHub repository is archived. Building an archived,
unmaintained server from source is not a production-grade choice.
**Decision.** Use [SeaweedFS](https://github.com/seaweedfs/seaweedfs) (Apache-2.0, actively
maintained) `chrislusf/seaweedfs:4.48`, which exposes an S3-compatible API on port 8333 and
supports per-identity, per-bucket permissions (`Read:<bucket>`, `Write:<bucket>`, `List:<bucket>`).
**Consequences.** Application code is unchanged: it only speaks the S3 API (boto3 + Hadoop
`s3a://`), which is the point of the storage abstraction. "MinIO policies" in the original
plan become SeaweedFS identity policies in `infra/seaweedfs/s3.json.tmpl`.

## D-002 · Bucket per medallion layer (2026-11-07)
`fintxn-raw`, `fintxn-bronze`, `fintxn-silver`, `fintxn-gold`, `fintxn-audit`. Bucket-level
boundaries map one-to-one onto RBAC in every target: SeaweedFS identities, AWS IAM policies,
and Unity Catalog schemas (`fintxn_bronze`, `fintxn_silver`, ...). Analysts get the gold bucket
only, so no prefix-level policy tricks are needed.

## D-003 · Pinned toolchain (2026-11-07)
PySpark 3.5.9 + delta-spark 3.3.3 (Delta 3.x is the Spark 3.5 line), `hadoop-aws:3.3.4`
(matches Spark 3.5's bundled Hadoop 3.3.4). `great-expectations==1.4.3` was the first pick but
caps pandas < 2.2, conflicting with Streamlit/MLflow; resolved to GX Core 1.24.1 with pandas
2.2.3 and numpy 2.2.6. MLflow 2.22 with a SQLite backend (the model registry needs a database
backend; the plain file store is deprecated for the registry).

## D-004 · Tokenize card numbers at bronze landing, not in silver (2026-11-07)
Raw PANs would otherwise sit in bronze Delta files (and their time-travel history) forever.
Bronze replaces `card_number` with `card_token = HMAC-SHA256(key, PAN)` and `card_last4`, and
redacts PAN-like digit runs inside `_corrupt_record`. HMAC is computed with native Spark
`sha2`/`unhex`/`concat` (no Python UDF), verified bit-for-bit against Python's `hmac`.
Customer PII (name, email, phone, DOB) stays in bronze (engineer/auditor-only bucket) and is
encrypted with AES-256-GCM in silver, as the brief requires.

## D-005 · Idempotent bronze appends via Delta `txnAppId`/`txnVersion` (2026-11-07)
Processed-file tracking alone leaves a gap: if a job dies after the data commit but before the
`_ingest_log` commit, a retry would double-load. Each append carries
`txnAppId=fintxn_bronze_<table>` and `txnVersion=<batch_id>`; Delta skips a write whose
(appId, version) was already committed, so the retry only writes the log.

## D-006 · Row rules decide quarantine; Great Expectations is the published contract (2026-11-07)
Quarantine needs per-row reason codes, which GX does not produce at scale. Silver therefore
applies explicit Spark rules (19 reason codes + exact/near duplicate windows) and then runs a
23-expectation GX suite over what was *published*. A GX failure means a bug in cleaning logic.

## D-007 · UTC everywhere (2026-11-07)
Spark session timezone, JVM `user.timezone`, and the Python driver (`TZ=UTC`) are all UTC.
PySpark returns `TimestampType` as naive *local* datetimes, which silently shifted values on a
machine in America/New_York until the driver was pinned. Local hour features are derived
explicitly from the customer's home UTC offset.

## D-008 · `spark.sql.legacy.timeParserPolicy=CORRECTED` + `try_to_timestamp` (2026-11-07)
Malformed timestamps such as `2026-13-45 25:61:00` would otherwise raise
`SparkUpgradeException` (or fail under ANSI mode on Databricks serverless) instead of being
quarantined as `TS_UNPARSEABLE`.

## D-009 · Merchant name and MCC come from the merchant dimension (2026-11-07)
Raw feeds carry free-text merchant names with casing/whitespace noise. Silver keeps the
conformed master-data values from `silver.merchants`; the raw text is not needed downstream.

## D-010 · SQLAlchemy pinned to 2.0.x (2026-11-08)
The resolver picked SQLAlchemy 2.1.8, which removed `FallbackAsyncAdaptedQueuePool`; MLflow
2.22's SQLite tracking store fails on import. Pinned `sqlalchemy==2.0.57`.

## D-011 · Simulation v1 was too easy; confounders added (2026-11-08)
The first 5M-row model scored PR-AUC 0.9994 / ROC-AUC 1.0000 on the June test month with 12
false positives (kept in `docs/results/model_metrics_sim_v1.json`). Leakage was ruled out
(features are strictly prior; MCC risk is fit on the training window only). The cause was the
simulator: every fraud originated from a far-away city, so `implied_speed_kmh` alone carried
55% of the gain and no legitimate transaction ever showed impossible travel. Simulation v2 adds
realistic confounders and re-runs the full pipeline:
- 3% of legitimate online sessions geo-locate far from home (VPN, roaming, proxies);
- 40% of remote fraud comes from domestic cities / residential proxies;
- 25% of account takeovers reuse the victim's known device (stolen session);
- velocity-burst card clones use chip/contactless 40% of the time, not only magstripe.
The reported metrics are from v2. Both are real measurements; neither was tuned to a target.

## D-012 · Local encryption at rest with SeaweedFS SSE-S3 (2026-11-08)
SeaweedFS accepts `PutBucketEncryption` but then fails every write until a key-encryption key is
configured. The KEK comes from `.env` (`S3_SSE_KEK`, 256-bit hex) via `WEED_S3_SSE_KEK`;
`bucket-init` sets AES256 default encryption on every bucket. Verified: `HeadObject` reports
`ServerSideEncryption: AES256`.
