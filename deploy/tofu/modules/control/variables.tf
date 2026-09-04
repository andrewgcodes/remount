variable "name" {
  description = "Name of the control service deployment."
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

variable "image" {
  description = "Control-plane image, pinned by digest."
  type        = string

  # A floating tag means the image that was validated and the image that runs
  # are two different facts wearing one name. Digest or nothing.
  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image))
    error_message = "image must be pinned by digest (repository@sha256:<64 hex>), never by a tag."
  }
}

variable "replicas" {
  description = "Control service replicas. Must be 1."
  type        = number
  default     = 1

  # The control plane is a single writer over SQLite. Two replicas are two
  # control planes with two databases, and the failure is silent: each one
  # answers coherently about a different fleet.
  validation {
    condition     = var.replicas == 1
    error_message = "The control plane is a single writer; replicas must be 1. Scale nodes, not the control service."
  }
}

variable "admin_token_env" {
  description = "NAME of the environment variable holding the admin token. Never the token itself."
  type        = string
  default     = "REMOUNT_TOKEN"

  # This variable takes a name, not a value, so a literal token cannot travel
  # through this interface into Terraform state, a plan file, or a CI log.
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{2,63}$", var.admin_token_env))
    error_message = "admin_token_env is the NAME of an environment variable (upper snake case), not a token value."
  }
}

variable "master_key_env" {
  description = "NAME of the environment variable holding the artifact master key, or empty for none."
  type        = string
  default     = ""

  validation {
    condition     = var.master_key_env == "" || can(regex("^[A-Z][A-Z0-9_]{2,63}$", var.master_key_env))
    error_message = "master_key_env is the NAME of an environment variable, not a key value."
  }
}

variable "data_volume_gb" {
  description = "Size of the control service's durable volume."
  type        = number
  default     = 20

  validation {
    condition     = var.data_volume_gb >= 1 && var.data_volume_gb <= 16384
    error_message = "data_volume_gb must be between 1 and 16384."
  }
}

variable "listen_port" {
  description = "Port the control service listens on."
  type        = number
  default     = 7443

  validation {
    condition     = var.listen_port > 0 && var.listen_port < 65536
    error_message = "listen_port must be a TCP port."
  }
}

variable "event_export_url" {
  description = "Optional event-export receiver URL. Empty disables export."
  type        = string
  default     = ""

  validation {
    condition     = var.event_export_url == "" || can(regex("^https?://", var.event_export_url))
    error_message = "event_export_url must be an http or https URL."
  }
}
