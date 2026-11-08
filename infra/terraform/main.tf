# Optional AWS target for the lakehouse. Free-tier friendly: S3 + one KMS key + IAM + a budget.
# NEVER `terraform apply` without reviewing `terraform plan` - see infra/terraform/README.md.

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project     = "financial-txn-etl-fraud-detection"
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  layers  = toset(["raw", "bronze", "silver", "gold", "audit"])
  buckets = { for l in local.layers : l => "${var.bucket_prefix}-${l}" }
}

# ------------------------------------------------------------------------------------------
# Encryption key (SSE-KMS). One customer-managed key ~ $1/month; set use_kms=false for SSE-S3.
# ------------------------------------------------------------------------------------------
resource "aws_kms_key" "lake" {
  count                   = var.use_kms ? 1 : 0
  description             = "fintxn lakehouse SSE-KMS key"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "lake" {
  count         = var.use_kms ? 1 : 0
  name          = "alias/${var.bucket_prefix}-lake"
  target_key_id = aws_kms_key.lake[0].key_id
}

# ------------------------------------------------------------------------------------------
# Buckets: one per medallion layer
# ------------------------------------------------------------------------------------------
resource "aws_s3_bucket" "layer" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.use_kms ? "aws:kms" : "AES256"
      kms_master_key_id = var.use_kms ? aws_kms_key.lake[0].arn : null
    }
    bucket_key_enabled = var.use_kms
  }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each                = aws_s3_bucket.layer
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# Deny plaintext transport and unencrypted uploads on every bucket.
data "aws_iam_policy_document" "bucket" {
  for_each = aws_s3_bucket.layer

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [each.value.arn, "${each.value.arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid     = "DenyUnencryptedObjectUploads"
    effect  = "Deny"
    actions = ["s3:PutObject"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = ["${each.value.arn}/*"]
    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption"
      values   = [var.use_kms ? "aws:kms" : "AES256"]
    }
    condition {
      test     = "Null"
      variable = "s3:x-amz-server-side-encryption"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.bucket[each.key].json
}

# ------------------------------------------------------------------------------------------
# Cost guardrail
# ------------------------------------------------------------------------------------------
resource "aws_budgets_budget" "monthly" {
  name         = "${var.bucket_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
