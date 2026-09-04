# Rejected: a reusable credential in provider metadata. Node labels are copied
# into consoles, inventories and billing exports, and none of those expire it.
module "network" {
  source = "./modules/network"

  vpc_id                = "vpc-fixture"
  subnet_ids            = ["subnet-a"]
  control_ingress_cidrs = ["10.0.0.0/16"]
}

module "nodes" {
  source = "./modules/node-pool"

  name             = "fixture-nodes"
  network          = module.network.network
  control_endpoint = "http://control:7443"
  image            = "ghcr.io/example/remount-node@sha256:0000000000000000000000000000000000000000000000000000000000000000"

  capability_labels = {
    vendor = "sk-live-abcdefghijklmnopqrstuvwxyz012345"
  }
}
