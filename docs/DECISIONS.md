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
