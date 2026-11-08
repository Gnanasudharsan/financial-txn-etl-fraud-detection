# Optional AWS target (Terraform)

Creates one S3 bucket per medallion layer (`<prefix>-raw|bronze|silver|gold|audit`) with
versioning, SSE-KMS (rotating CMK, or SSE-S3 with `use_kms=false`), public-access block,
BucketOwnerEnforced ownership, a TLS-only + encrypted-uploads-only bucket policy, a lifecycle
rule for noncurrent versions, three least-privilege IAM roles (`data_engineer`, `analyst`,
`auditor`) and a monthly AWS Budget with e-mail alerts.

Cost: S3 storage for a 5M-row run is ~5 GB (free tier covers 5 GB for 12 months); the KMS key is
~$1/month (set `use_kms = false` for $0). Budgets are free.

```bash
cp terraform.tfvars.example terraform.tfvars   # edit bucket_prefix + email
terraform init
terraform plan -out tfplan
# review the plan, then and only then:
terraform apply tfplan
```

Then point the pipeline at AWS: `FINTXN_ENV=aws FINTXN_BUCKET_PREFIX=<prefix>
FINTXN_KMS_KEY_ARN=$(terraform output -raw kms_key_arn) make pipeline ROWS=500000`.

CI only runs `terraform fmt -check` and `terraform validate`; nothing is ever applied
automatically.
