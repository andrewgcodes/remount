# Rejected: scaling the control plane. Two replicas are two single-writer
# SQLite databases, each answering coherently about a different fleet.
module "network" {
  source = "./modules/network"

  vpc_id                = "vpc-fixture"
  subnet_ids            = ["subnet-a"]
  control_ingress_cidrs = ["10.0.0.0/16"]
}

module "control" {
  source = "./modules/control"

  name     = "fixture-control"
  network  = module.network.network
  image    = "ghcr.io/example/remount@sha256:0000000000000000000000000000000000000000000000000000000000000000"
  replicas = 2
}
