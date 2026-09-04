output "service" {
  description = "The control service descriptor a provider resource consumes."
  value       = terraform_data.control.output
}

output "endpoint" {
  description = "In-network endpoint nodes and clients dial."
  value       = "http://${var.name}:${var.listen_port}"
}

output "environment_names" {
  description = "Names of the environment variables the control service requires. Names only; no values cross this boundary."
  value       = local.environment_names
}
