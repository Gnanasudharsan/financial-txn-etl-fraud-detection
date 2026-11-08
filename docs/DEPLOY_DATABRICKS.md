# Deploying to Databricks Free Edition

The same wheel that runs locally runs on Databricks serverless compute. The only differences
come from `configs/databricks.yaml`: Delta tables become Unity Catalog managed tables
(`workspace.fintxn_<layer>.<table>`), raw files land in a UC Volume, MLflow uses the
workspace tracking server with the Unity Catalog model registry, and keys come from a secret
scope instead of `.env`.

> Nothing here has been run against a workspace by the author of this repo's automation: it
> needs **your** Databricks credentials. Each step below is a command you run yourself.

## 0. Prerequisites

- A Databricks **Free Edition** workspace (sign up at databricks.com/learn/free-edition).
- This repo set up locally (`make setup`), so `.env` holds `FINTXN_TOKEN_KEY` and
  `FINTXN_ENCRYPTION_KEY`.
- `uv` (the bundle builds the wheel with `uv build --wheel`).

## 1. Install and authenticate the Databricks CLI

```bash
brew tap databricks/tap
```

```bash
brew install databricks
```

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

```bash
databricks current-user me
```

## 2. Create the secret scope with the pipeline keys

The keys must be the same as in your local `.env` if you want local and cloud tokens to match.

```bash
databricks secrets create-scope fintxn
```

```bash
databricks secrets put-secret fintxn token_key --string-value "$(grep '^FINTXN_TOKEN_KEY=' .env | cut -d= -f2-)"
```

```bash
databricks secrets put-secret fintxn encryption_key --string-value "$(grep '^FINTXN_ENCRYPTION_KEY=' .env | cut -d= -f2-)"
```

The code reads them with `dbutils.secrets.get("fintxn", ...)` when the environment variables
are absent (`src/fintxn/secrets.py`).

## 3. Validate, deploy, run

```bash
databricks bundle validate -t dev
```

```bash
databricks bundle deploy -t dev
```

```bash
databricks bundle run fintxn_pipeline -t dev
```

The job (`databricks/resources/fintxn_pipeline.job.yml`) runs:

```
setup_uc -> generate -> bronze -> silver -> gold -> train -> score -> export
                                     \-> recon ------------------/
```

`setup_uc` creates the schemas `fintxn_raw|bronze|silver|gold|audit` and the volume
`fintxn_raw.landing`. The default dataset is **500,000** transactions (`n_transactions`
bundle variable) to stay inside Free Edition serverless quotas. Override it:

```bash
databricks bundle deploy -t dev --var="n_transactions=200000"
```

## 4. (Optional) Upload a locally generated sample instead of generating in the workspace

```bash
uv run fintxn --env ci generate --rows 10000
```

```bash
databricks fs cp -r data/ci/raw dbfs:/Volumes/workspace/fintxn_raw/landing/raw --overwrite
```

Then run the downstream tasks only (task selection needs a recent CLI):

```bash
databricks bundle run fintxn_pipeline -t dev --only bronze,silver,gold,train,score,recon,export
```

## 5. Apply RBAC and PII views

Create the account groups `data_engineers`, `analysts`, `auditors` (workspace **Settings →
Identity and access → Groups**), then run in the SQL editor:

1. `databricks/sql/02_pii_secure_views.sql` – `customers_secure` decrypts PII **inside SQL**
   with `aes_decrypt(..., 'GCM', 'DEFAULT', <column name>)` for members of `auditors` only;
   everyone else gets masked values. (The ciphertext format is verified against Spark's
   built-in `aes_decrypt` in `tests/unit/test_crypto.py`.)
2. `databricks/sql/01_rbac_grants.sql` – schema-level grants per group.

## 6. Inspect

- **Jobs & Pipelines → fintxn-pipeline-dev** for the task DAG and run history.
- **Catalog → workspace → fintxn_gold** for the marts; `fintxn_audit.pipeline_runs` for the
  audit trail.
- **Experiments → /Shared/fintxn-fraud** for MLflow runs; the model is registered as
  `workspace.fintxn_gold.fintxn_fraud_lgbm` with alias `champion`.

## Free Edition notes

- Serverless only: the code never touches `sparkContext`, and skips `persist()` on Databricks.
- One job run at a time (`max_concurrent_runs: 1`); a 500k-row run is the comfortable size.
- Tear down: `databricks bundle destroy -t dev` (removes the job; drop the schemas manually).
