output "pool" {
  description = "The node-pool descriptor a provider resource consumes."
  value       = terraform_data.pool.output
}

output "capability_labels" {
  description = "Capability labels every node in this pool advertises."
  value       = var.capability_labels
}

output "runtime_owner" {
  description = "Who owns workspace placement. Constant, and read by the composability policy tests."
  value       = "remount-control-plane"
}
