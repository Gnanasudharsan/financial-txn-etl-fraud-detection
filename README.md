# Financial Transaction ETL & Fraud Detection

Cloud-native lakehouse pipeline (PySpark, Delta Lake, S3 API, Databricks) that ingests 5M+
simulated banking transactions, enforces data quality, engineers point-in-time fraud features,
trains a LightGBM fraud model tracked in MLflow, reconciles ledger vs settlement, and serves BI
dashboards, with RBAC, encryption, and a full audit trail.

> Work in progress. See [docs/PROGRESS.md](docs/PROGRESS.md).

## Quickstart

```bash
make setup && make up && make pipeline
```

## License

MIT
