# Rejected: a literal token where the module wants the name of an environment
# variable. Accepting it would put the token in state and in plan output.
module "network" {
  source = "./modules/network"

  vpc_id                = "vpc-fixture"
  subnet_ids            = ["subnet-a"]
  control_ingress_cidrs = ["10.0.0.0/16"]
}

module "control" {
  source = "./modules/control"

  name            = "fixture-control"
  network         = module.network.network
  image           = "ghcr.io/example/remount@sha256:0000000000000000000000000000000000000000000000000000000000000000"
  admin_token_env = "sk-live-abcdefghijklmnopqrstuvwxyz012345"
}
