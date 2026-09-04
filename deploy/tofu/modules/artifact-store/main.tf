locals {
  # Snapshots and session-log tiers are content addressed, so the store is a
  # dumb bucket by design: no lifecycle rule here may delete an object the
  # control plane still references, which is why retention expires noncurrent
  # versions only.
  store = {
    bucket             = var.bucket
    region             = var.region
    endpoint           = var.endpoint
    credentials_source = var.credentials_source
    credentials_env    = var.credentials_source == "env" ? ["${var.credentials_env_prefix}_ACCESS_KEY_ID", "${var.credentials_env_prefix}_SECRET_ACCESS_KEY"] : []
    versioning         = var.versioning
    retention_days     = var.retention_days
    tags               = var.tags
  }
}

# The provider seam: replace with aws_s3_bucket, google_storage_bucket, or the
# MinIO bucket resource. The contract consumers depend on is the output below.
resource "terraform_data" "bucket" {
  input = local.store
}
