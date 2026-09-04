locals {
  # A node is started with the pool's capability labels and nothing about any
  # particular workspace. Which workspaces land here is decided after the node
  # is online, by the control plane, from claims this module never sees.
  label_args = [for k, v in var.capability_labels : "--label=${k}=${v}"]

  command = concat(
    [
      "up",
      "--server=${var.control_endpoint}",
      "--data=/var/lib/remount-node",
      "--backend=${var.backend}",
    ],
    sort(local.label_args),
  )
}

# The provider seam: replace with an autoscaling group, managed instance group,
# or a Kubernetes DaemonSet (deploy/helm ships the last of those).
resource "terraform_data" "pool" {
  input = {
    name              = var.name
    image             = var.image
    backend           = var.backend
    desired_nodes     = var.desired_nodes
    max_nodes         = var.max_nodes
    command           = local.command
    environment_names = [var.enrollment_secret_env]
    subnet_ids        = var.network.subnet_ids
    reachability      = var.network.reachability
    tags              = var.tags
  }
}
