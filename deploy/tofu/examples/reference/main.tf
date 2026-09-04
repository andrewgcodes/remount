# The reference wiring: network inputs, one control service, one artifact store,
# two node pools. This is the root module CI validates, so it is also the
# example that cannot silently rot.
#
# It creates nothing. Every module's provider seam is `terraform_data`, so
# `tofu validate` here needs no provider, no registry and no cloud account.
# See ../../README.md for what to substitute at each seam.

locals {
  # This repository publishes no image: Plan B §18 cuts public release, image
  # push and package publish. These are syntactically valid digests that resolve
  # to nothing, present so the example exercises the digest validation the way a
  # real deployment would. Replace them with the digests `docker build` printed.
  placeholder_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

  control_image = "ghcr.io/andrewgcodes/remount@${local.placeholder_digest}"

  # Nodes run a different image from the control plane on purpose: the release
  # image is FROM scratch, which a process-backend node cannot use because its
  # workspaces would have no userland. See deploy/compose/node.Dockerfile.
  node_image = "ghcr.io/andrewgcodes/remount-node@${local.placeholder_digest}"
}

module "network" {
  source = "../../modules/network"

  vpc_id                = "vpc-reference"
  subnet_ids            = ["subnet-a", "subnet-b"]
  control_ingress_cidrs = ["10.0.0.0/16"]
  reachability          = "relay"
}

module "control" {
  source = "../../modules/control"

  name           = "remount-control"
  network        = module.network.network
  image          = local.control_image
  data_volume_gb = 50

  # Names only. The values live in the deployment platform's secret store.
  admin_token_env = "REMOUNT_TOKEN"
  master_key_env  = "REMOUNT_MASTER_KEY"

  event_export_url = "http://events.internal:9200/remount"
}

module "artifacts" {
  source = "../../modules/artifact-store"

  bucket             = "remount-reference-artifacts"
  region             = "us-east-1"
  credentials_source = "instance-role"
  retention_days     = 30

  tags = {
    owner     = "platform"
    component = "remount-artifacts"
  }
}

# Two pools, because capability is a property of the pool and a fleet normally
# has more than one kind. Placement between them is the control plane's job:
# a workspace asking for enforced egress lands on the isolated pool because the
# node advertises it, not because Terraform put it there.
module "nodes_general" {
  source = "../../modules/node-pool"

  name             = "remount-nodes-general"
  network          = module.network.network
  control_endpoint = module.control.endpoint
  image            = local.node_image
  backend          = "process"
  desired_nodes    = 2
  max_nodes        = 10

  capability_labels = {
    zone    = "a"
    tier    = "general"
    backend = "process"
  }

  tags = {
    owner     = "platform"
    component = "remount-nodes"
  }
}

module "nodes_isolated" {
  source = "../../modules/node-pool"

  name             = "remount-nodes-isolated"
  network          = module.network.network
  control_endpoint = module.control.endpoint
  image            = local.node_image
  backend          = "gvisor"
  desired_nodes    = 0
  max_nodes        = 4

  capability_labels = {
    zone    = "b"
    tier    = "isolated"
    backend = "gvisor"
  }
}
