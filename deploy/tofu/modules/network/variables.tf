variable "vpc_id" {
  description = "Identifier of the network Remount is deployed into. Opaque to Remount; only its shape is checked."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._-]{1,127}$", var.vpc_id))
    error_message = "vpc_id must be a non-empty provider identifier."
  }
}

variable "subnet_ids" {
  description = "Subnets the control service and nodes may be placed in."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) > 0
    error_message = "At least one subnet is required."
  }
}

variable "control_ingress_cidrs" {
  description = "CIDRs allowed to reach the control service."
  type        = list(string)

  validation {
    condition     = alltrue([for c in var.control_ingress_cidrs : can(regex("/[0-9]{1,3}$", c))])
    error_message = "Every control_ingress_cidrs entry must be a CIDR."
  }

  # The control plane holds every grant, lease and generation in the fleet. A
  # world-open ingress rule is not a configuration choice this module will
  # normalize silently; the operator has to write it somewhere it is visible.
  validation {
    condition     = !contains(var.control_ingress_cidrs, "0.0.0.0/0")
    error_message = "0.0.0.0/0 must not reach the control service; put a load balancer or relay in front of it."
  }
}

variable "reachability" {
  description = "How clients reach nodes: vpc-only, relay, or tailscale-sidecar."
  type        = string
  default     = "relay"

  validation {
    condition     = contains(["vpc-only", "relay", "tailscale-sidecar"], var.reachability)
    error_message = "reachability must be one of vpc-only, relay, tailscale-sidecar."
  }
}
