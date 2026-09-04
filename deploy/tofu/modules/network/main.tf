locals {
  # Normalizing here rather than in each consumer keeps one description of the
  # network, so the control service and the node pool cannot disagree about it.
  network = {
    vpc_id                = var.vpc_id
    subnet_ids            = var.subnet_ids
    control_ingress_cidrs = var.control_ingress_cidrs
    reachability          = var.reachability
  }
}
