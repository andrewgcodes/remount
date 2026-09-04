output "control_endpoint" {
  description = "Where nodes and clients dial the control service."
  value       = module.control.endpoint
}

output "artifact_store" {
  description = "The artifact store the control service is configured with."
  value       = module.artifacts.store
}

output "node_pools" {
  description = "The node pools, by name."
  value = {
    general  = module.nodes_general.pool
    isolated = module.nodes_isolated.pool
  }
}

output "required_environment_names" {
  description = "Every environment variable name the deployment needs. Names only; no secret value is produced by this configuration."
  value       = module.control.environment_names
}
