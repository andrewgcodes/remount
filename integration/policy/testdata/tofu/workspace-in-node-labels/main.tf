# Rejected: a capability label naming a runtime fact. Labels say what a node can
# do, not what is running on it.
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
    workspace = "ws_01HZQ"
  }
}
