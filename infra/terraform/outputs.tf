output "buckets" {
  description = "Bucket name per medallion layer"
  value       = { for k, b in aws_s3_bucket.layer : k => b.bucket }
}

output "kms_key_arn" {
  description = "KMS key used for SSE-KMS (set FINTXN_KMS_KEY_ARN to this)"
  value       = var.use_kms ? aws_kms_key.lake[0].arn : null
}

output "role_arns" {
  description = "IAM role per RBAC persona"
  value       = { for k, r in aws_iam_role.role : k => r.arn }
}
