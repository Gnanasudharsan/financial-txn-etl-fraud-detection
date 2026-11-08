variable "region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "dev"
}

variable "bucket_prefix" {
  description = "Globally unique prefix; buckets become <prefix>-raw, <prefix>-bronze, ..."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,40}$", var.bucket_prefix))
    error_message = "bucket_prefix must be 3-41 chars of lowercase letters, digits and hyphens."
  }
}

variable "use_kms" {
  description = "SSE-KMS with a customer-managed key (~$1/month) instead of SSE-S3 (free)"
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "Allow `terraform destroy` to delete non-empty buckets (demo environments only)"
  type        = bool
  default     = false
}

variable "noncurrent_version_days" {
  description = "Days to keep noncurrent object versions (Delta time travel uses its own log)"
  type        = number
  default     = 30
}

variable "monthly_budget_usd" {
  description = "Monthly AWS budget that triggers e-mail alerts"
  type        = string
  default     = "5"
}

variable "budget_alert_email" {
  description = "E-mail address for budget alerts"
  type        = string
}

variable "trusted_principal_arns" {
  description = "Principals allowed to assume the RBAC roles (defaults to this account root)"
  type        = list(string)
  default     = []
}
