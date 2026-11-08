# Least-privilege roles mirroring the RBAC model used everywhere else:
#   data_engineer - read/write all layers (pipeline identity)
#   analyst       - read gold only (aggregated, tokenized data)
#   auditor       - read everything except raw (no PAN ever lands past raw), incl. audit trail

locals {
  arn = { for k, b in aws_s3_bucket.layer : k => b.arn }

  role_access = {
    data_engineer = { read = ["raw", "bronze", "silver", "gold", "audit"], write = ["raw", "bronze", "silver", "gold", "audit"] }
    analyst       = { read = ["gold"], write = [] }
    auditor       = { read = ["bronze", "silver", "gold", "audit"], write = [] }
  }
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = length(var.trusted_principal_arns) > 0 ? var.trusted_principal_arns : ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

data "aws_iam_policy_document" "role" {
  for_each = local.role_access

  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for l in distinct(concat(each.value.read, each.value.write)) : local.arn[l]]
  }

  statement {
    sid       = "ReadObjects"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [for l in each.value.read : "${local.arn[l]}/*"]
  }

  dynamic "statement" {
    for_each = length(each.value.write) > 0 ? [1] : []
    content {
      sid       = "WriteObjects"
      actions   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
      resources = [for l in each.value.write : "${local.arn[l]}/*"]
    }
  }

  dynamic "statement" {
    for_each = var.use_kms ? [1] : []
    content {
      sid       = "UseLakeKey"
      actions   = length(each.value.write) > 0 ? ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"] : ["kms:Decrypt", "kms:DescribeKey"]
      resources = [aws_kms_key.lake[0].arn]
    }
  }
}

resource "aws_iam_role" "role" {
  for_each             = local.role_access
  name                 = "${var.bucket_prefix}-${replace(each.key, "_", "-")}"
  assume_role_policy   = data.aws_iam_policy_document.assume.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "role" {
  for_each = local.role_access
  name     = "${var.bucket_prefix}-${each.key}-s3"
  role     = aws_iam_role.role[each.key].id
  policy   = data.aws_iam_policy_document.role[each.key].json
}
