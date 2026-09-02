"""Remount on Modal: the control plane plus one node, in a Modal container.

The container runs `remount server` on port 7443 and, next to it, `remount up`
so the same container is also a node. Everything else in the world dials in
outbound over HTTPS, which is the only thing Remount ever needs.
"""
import os
import subprocess
import time

import modal

TOKEN = os.environ.get("REMOUNT_TOKEN", "modal-demo-token")

image = (
    modal.Image.debian_slim()
    .apt_install("curl", "procps")
    .add_local_file("remount-linux-amd64", "/usr/local/bin/remount", copy=True)
    .run_commands("chmod +x /usr/local/bin/remount")
    .env({"REMOUNT_TOKEN": TOKEN})
)

app = modal.App("remount-demo-claude", image=image)

# The control plane is a single stateful process, so it must be exactly one
# container with a durable volume. A web endpoint that scales out would give
# you N independent control planes behind one URL, each with its own database.
volume = modal.Volume.from_name("remount-demo-claude-data", create_if_missing=True)


@app.function(
    timeout=3600,
    min_containers=1,
    max_containers=1,
    volumes={"/data": volume},
)
@modal.concurrent(max_inputs=200)
@modal.web_server(7443, startup_timeout=180)
def control():
    os.makedirs("/data/server", exist_ok=True)
    os.makedirs("/data/node", exist_ok=True)
    server = subprocess.Popen(
        ["/usr/local/bin/remount", "server",
         "--listen", "0.0.0.0:7443",
         "--data", "/data/server",
         "--token", TOKEN,
         "--lease", "20"],
        stdout=open("/data/server.log", "w"), stderr=subprocess.STDOUT,
    )
    # Wait for the control plane to answer before enrolling the local node.
    for _ in range(60):
        try:
            subprocess.check_output(["curl", "-sf", "http://127.0.0.1:7443/healthz"])
            break
        except Exception:
            time.sleep(0.5)
    subprocess.Popen(
        ["/usr/local/bin/remount", "up",
         "--server", "http://127.0.0.1:7443",
         "--token", TOKEN,
         "--data", "/data/node",
         "--label", "vendor=modal",
         "--label", "zone=cloud",
         "--allow", "api.openai.com",
         "--allow", "registry.npmjs.org"],
        stdout=open("/data/node.log", "w"), stderr=subprocess.STDOUT,
    )
    assert server.poll() is None, "control plane died at startup"
