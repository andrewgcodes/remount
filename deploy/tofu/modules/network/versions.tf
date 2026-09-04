terraform {
  # No required_providers block: this module declares no provider, so
  # `tofu init -backend=false` needs neither a registry nor a credential.
  required_version = ">= 1.6.0"
}
