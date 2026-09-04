variable "name" {
  description = "Name of the node pool."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,62}$", var.name))
    error_message = "name must be a lowercase DNS-style label."
  }
}

variable "network" {
  description = "Output of the network module."
  type = object({
    vpc_id                = string
    subnet_ids            = list(string)
    control_ingress_cidrs = list(string)
    reachability          = string
  })
}

variable "control_endpoint" {
  description = "Control service endpoint nodes dial, from the control module."
  type        = string

  validation {
    condition     = can(regex("^https?://", var.control_endpoint))
    error_message = "control_endpoint must be an http or https URL."
  }
}

variable "image" {
  description = "Node image, pinned by digest."
  type        = string

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image))
    error_message = "image must be pinned by digest (repository@sha256:<64 hex>), never by a tag."
  }
}

variable "backend" {
  description = "Workspace backend the nodes run: process, docker, gvisor, or firecracker."
  type        = string
  default     = "process"

  validation {
    condition     = contains(["process", "docker", "gvisor", "firecracker"], var.backend)
    error_message = "backend must be process, docker, gvisor, or firecracker."
  }
}

variable "desired_nodes" {
  description = "How many nodes this pool runs."
  type        = number
  default     = 2

  validation {
    condition     = var.desired_nodes >= 0 && var.desired_nodes <= 1000
    error_message = "desired_nodes must be between 0 and 1000."
  }
}

variable "max_nodes" {
  description = "Ceiling the pool may scale to."
  type        = number
  default     = 10

  validation {
    condition     = var.max_nodes >= 0 && var.max_nodes <= 1000
    error_message = "max_nodes must be between 0 and 1000."
  }
}

variable "capability_labels" {
  description = "Capability labels advertised by every node in this pool. Capacity and capability only."
  type        = map(string)
  default     = {}

  validation {
    condition     = alltrue([for k, v in var.capability_labels : can(regex("^[a-z][a-z0-9_.-]{0,62}$", k))])
    error_message = "Label keys must be lowercase identifiers."
  }

  # A label naming a workspace, session, claim, lease, generation or checkpoint
  # is Terraform asserting a runtime fact. The control plane already owns that
  # fact and will not consult Terraform before changing it, so the two would
  # disagree the first time a workspace moved.
  validation {
    condition = alltrue([
      for k, v in var.capability_labels : !contains(
        ["workspace", "workspace_id", "session", "session_id", "claim", "lease", "generation", "checkpoint", "snapshot"],
        k,
      )
    ])
    error_message = "Labels describe what a node can do, not what is running on it. Workspace, session, claim, lease, generation and checkpoint are the control plane's facts."
  }

  validation {
    condition = alltrue([
      for k, v in var.capability_labels : !can(regex("(?i)(secret|token|password|passwd|credential|private[_-]?key|api[_-]?key)$", k))
    ])
    error_message = "Label keys must not name a credential. Node labels are provider metadata and are visible to every reader of the pool."
  }

  validation {
    condition = alltrue([
      for k, v in var.capability_labels : !can(regex("(sk-[A-Za-z0-9_-]{16,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|ghp_[A-Za-z0-9]{20,})", v))
    ])
    error_message = "A label value looks like a reusable credential. Workspaces are trusted with nothing; secrets reach the network edge through the broker."
  }
}

variable "node_assignments" {
  description = "Must be empty. Present so that setting it fails validation with a reason."
  type        = map(string)
  default     = {}

  # This is the two-owners case Plan B phase B5 names. An operator reaching for
  # this is asking a reasonable question -- "how do I pin this workspace to that
  # node?" -- with the wrong tool. Placement is a lease the control plane grants
  # and revokes on node loss; Terraform reconciles minutes later against a fact
  # that has already changed, and would move a live workspace to satisfy a plan.
  validation {
    condition     = length(var.node_assignments) == 0
    error_message = "Terraform does not place, move, or pin live workspaces. Placement, claims and moves belong to the Remount control plane; use the Remount workspace move command or a placement constraint on the workspace itself."
  }
}

variable "tags" {
  description = "Provider tags applied to pool inventory."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for k, v in var.tags : !can(regex("(?i)(secret|token|password|passwd|credential|private[_-]?key|api[_-]?key)$", k))
    ])
    error_message = "Tag keys must not name a credential."
  }

  validation {
    condition = alltrue([
      for k, v in var.tags : !can(regex("(sk-[A-Za-z0-9_-]{16,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|ghp_[A-Za-z0-9]{20,})", v))
    ])
    error_message = "A tag value looks like a reusable credential."
  }
}

variable "enrollment_secret_env" {
  description = "NAME of the environment variable holding the one-time node enrollment secret."
  type        = string
  default     = "REMOUNT_ENROLL_SECRET"

  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{2,63}$", var.enrollment_secret_env))
    error_message = "enrollment_secret_env is the NAME of an environment variable, not a secret value."
  }
}
