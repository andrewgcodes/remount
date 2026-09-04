locals {
  # The argv the control service is started with. Keeping it here, rather than
  # in whatever resource a provider needs, is what lets the same module body
  # serve a VM, an ECS task or a Kubernetes Deployment: the contract is the
  # command and the environment, not the compute primitive.
  command = concat(
    ["server", "--listen=0.0.0.0:${var.listen_port}", "--data=/var/lib/remount"],
    var.event_export_url == "" ? [] : ["--event-export=${var.event_export_url}"],
  )

  # Names only. A value here would be a value in the plan file.
  environment_names = compact([var.admin_token_env, var.master_key_env])
}

# The provider seam. Replace terraform_data with the compute resource the target
# platform uses; nothing outside this module depends on which one it is, because
# every consumer reads the outputs below.
resource "terraform_data" "control" {
  input = {
    name              = var.name
    image             = var.image
    replicas          = var.replicas
    command           = local.command
    environment_names = local.environment_names
    data_volume_gb    = var.data_volume_gb
    subnet_ids        = var.network.subnet_ids
    ingress_cidrs     = var.network.control_ingress_cidrs
  }
}
