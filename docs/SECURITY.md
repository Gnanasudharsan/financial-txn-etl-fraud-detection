# Security, privacy and auditability

## RBAC: one model, three enforcement points

| Persona | raw | bronze | silver | gold | audit | Decrypt PII |
|---|---|---|---|---|---|---|
| `data_engineer` (pipeline identity) | RW | RW | RW | RW | RW | yes (audited) |
| `analyst` | – | – | masked view only | R | – | **no** |
| `auditor` | – | R | R | R | R | yes (audited) |

Raw is engineer-only because it is the only layer that ever holds a PAN, and only transiently.

| Target | Mechanism | File |
|---|---|---|
| Local (SeaweedFS S3 API) | Per-identity `Read:/Write:/List:<bucket>` actions | `infra/seaweedfs/s3.json.tmpl` |
| AWS S3 | One least-privilege IAM role per persona; bucket-per-layer ARNs; KMS `Decrypt` only for readers | `infra/terraform/iam.tf` |
| Databricks | Unity Catalog `GRANT`s per schema; dynamic view for PII | `databricks/sql/01_rbac_grants.sql`, `02_pii_secure_views.sql` |

The local policy is verified, not just written down: with the analyst credentials, listing
`fintxn-gold` succeeds, while listing `fintxn-bronze` or writing to `fintxn-gold` returns
`AccessDenied`. (This check was run during Phase 0.)

## Encryption

| Layer | What | How |
|---|---|---|
| At rest (object store) | Every object | SeaweedFS SSE-S3 (bucket default encryption, KEK from `.env` `S3_SSE_KEK`); on AWS, SSE-KMS with a rotating CMK plus a bucket policy that denies non-TLS access and the wrong SSE header |
| Card numbers | PAN | Replaced at bronze landing by `HMAC-SHA256(key, PAN)` (deterministic, joinable, irreversible) + last4. Implemented as native Spark SQL, verified against Python `hmac`. PAN-like digit runs in `_corrupt_record` are redacted |
| Customer PII | first/last name, email, phone, DOB | AES-256-GCM per value, random 96-bit nonce, column name as associated data, `v1:` key-version prefix. Masked columns (`email_masked`, `phone_masked`, `birth_year`) are stored for analysts |
| In transit | S3 API | `configs/aws.yaml` / `r2.yaml` set `ssl: true` (`fs.s3a.connection.ssl.enabled`). AWS bucket policy denies `aws:SecureTransport=false`. Locally, plain HTTP on the loopback interface |

The AES-GCM layout (`nonce || ciphertext || tag`) is exactly what Spark's built-in
`aes_decrypt(input, key, 'GCM', 'DEFAULT', aad)` expects, so auditors can decrypt inside
Databricks SQL with a key from a secret scope (`tests/unit/test_crypto.py` proves it).

### Role-gated decrypt utility

```bash
FINTXN_ROLE=auditor uv run fintxn pii-decrypt C0000042 --reason "chargeback dispute #123"
```

`analyst` (or any unknown role) is refused with `AccessDeniedError`. Every successful call writes
a `pii_access` row (customer, role, reason) to the audit trail.

## Secrets

| Where | Source |
|---|---|
| Laptop | `.env` (git-ignored, `chmod 600`), generated with random values by `make setup` / `make keys` |
| CI | GitHub Actions secrets (tests use throwaway keys generated in `tests/conftest.py`) |
| Databricks | Secret scope `fintxn` (`token_key`, `encryption_key`), read via `dbutils.secrets` |

Pre-commit blocks `.env` files and private keys; `.gitignore` excludes `.env*`, `*.tfvars`,
rendered `s3.json`, state files, and all generated data.

## Audit trail

`audit.pipeline_runs` (Delta, append-only) gets one row per job run, success **or** failure:

| Column | Meaning |
|---|---|
| `run_id`, `job_name`, `env`, `user` | who/what ran |
| `git_sha` | exact code version |
| `started_at`, `ended_at`, `duration_s`, `status`, `error` | execution |
| `rows_in`, `rows_out`, `rows_quarantined`, `dq_pass_rate` | data movement and quality |
| `tables_written` | JSON map `layer.table -> Delta version` written by this run |
| `metrics` | job-specific JSON (counts by reason code, model metrics, recon categories, ...) |

### Reproducing any past run with Delta time travel

```bash
uv run fintxn audit-log                 # find the run_id
uv run fintxn time-travel <run_id>      # versions written + row counts read back at those versions
```

In code (local or Databricks):

```python
lake.read("silver", "transactions", version=12)          # DataFrame as of that run
spark.sql("SELECT * FROM workspace.fintxn_silver.transactions VERSION AS OF 12")
```

The MLflow run for each model also records `data.gold.features_transactions.version` and
`data.silver.transactions.version`, so a model can be retrained on exactly the data it saw.
Delta's default retention (`delta.logRetentionDuration` 30 days, `VACUUM` 7 days) bounds how far
back this works; raise both table properties for regulatory retention periods.
