# Planted violations. Nothing here is a real module; it exists so the ownership
# and credential scanners are watched finding something. A scan nobody has seen
# match is not yet a scan (AGENTS.md: "prove the scan works by planting a canary
# the same scan must find").

module "canary" {
  source = "../../../../deploy/tofu/modules/node-pool"

  node_assignments = { ws_01HZ = "n_a" }
  workspace_ids    = ["ws_01HZ"]
}

resource "null_resource" "canary" {
  provisioner "local-exec" {
    command = "remount ws move ws_01HZ --node n_b"
  }
}

resource "terraform_data" "canary_api" {
  input = "https://control.internal/v1/ws/ws_01HZ/move"
}
