variable "bucket" {
  description = "S3-compatible bucket holding artifacts and snapshots."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{2,62}$", var.bucket))
    error_message = "bucket must be a valid S3 bucket name."
  }
}

variable "region" {
  description = "Region the bucket lives in."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]{2,32}$", var.region))
    error_message = "region must be a provider region identifier."
  }
}

variable "endpoint" {
  description = "Optional S3 endpoint override, for MinIO or another S3-compatible store."
  type        = string
  default     = ""

  validation {
    condition     = var.endpoint == "" || can(regex("^https?://", var.endpoint))
    error_message = "endpoint must be an http or https URL."
  }
}

variable "credentials_source" {
  description = "How the control service obtains store credentials: instance-role, env, or external-secret."
  type        = string
  default     = "instance-role"

  # There is deliberately no "static" option and no access-key variable. A
  # long-lived key pasted into a tfvars file becomes a key in state, in the
  # plan artifact, and in whatever CI log printed the plan.
  validation {
    condition     = contains(["instance-role", "env", "external-secret"], var.credentials_source)
    error_message = "credentials_source must be instance-role, env, or external-secret. Static keys are not accepted."
  }
}

variable "credentials_env_prefix" {
  description = "NAME prefix of the environment variables carrying store credentials when credentials_source is env."
  type        = string
  default     = "AWS"

  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{1,31}$", var.credentials_env_prefix))
    error_message = "credentials_env_prefix is a NAME prefix (upper snake case), not a credential value."
  }
}

variable "versioning" {
  description = "Whether object versioning is enabled."
  type        = bool
  default     = true
}

variable "retention_days" {
  description = "Days before a noncurrent artifact version is expired. 0 keeps every version."
  type        = number
  default     = 30

  validation {
    condition     = var.retention_days >= 0 && var.retention_days <= 3650
    error_message = "retention_days must be between 0 and 3650."
  }
}

variable "tags" {
  description = "Provider tags applied to the bucket."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for k, v in var.tags : !can(regex("(?i)(secret|token|password|passwd|credential|private[_-]?key|api[_-]?key)$", k))
    ])
    error_message = "Tag keys must not name a credential. Provider tags are readable by anyone with describe rights."
  }

  validation {
    condition = alltrue([
      for k, v in var.tags : !can(regex("(sk-[A-Za-z0-9_-]{16,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|ghp_[A-Za-z0-9]{20,})", v))
    ])
    error_message = "A tag value looks like a reusable credential. Secrets reach Remount through the broker, never through provider metadata."
  }
}
