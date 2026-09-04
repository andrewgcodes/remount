# Rejected: Terraform pinning a live workspace to a node. The operator's
# question is reasonable; the tool is not, because the control plane revokes
# and regrants that placement without consulting any plan.
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

  node_assignments = {
    ws_01HZQ = "n_a1"
  }
}
