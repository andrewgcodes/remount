# Rejected: a floating tag. The image that was reviewed and the image that runs
# would be two artifacts wearing one name.
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
  image            = "ghcr.io/example/remount-node:latest"
}
