output "store" {
  description = "The artifact store descriptor the control service is configured with."
  value       = terraform_data.bucket.output
}

output "credentials_env" {
  description = "Names of the environment variables carrying store credentials, empty unless credentials_source is env."
  value       = local.store.credentials_env
}
