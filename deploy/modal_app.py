"""Remount demo on Modal: one control plane and node in one container.

The container runs `remount server` on port 7443 and, next to it, `remount up`
so the same container is also a node. Everything else in the world dials in
outbound over HTTPS, which is the only thing Remount ever needs.

Build with ``make modal-binary`` and deploy with ``make modal-deploy``. The
deployment requires a Modal secret (``remount-control`` by default) containing
``REMOUNT_TOKEN``; there is deliberately no fallback credential.
"""
import atexit
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

import modal

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "dist" / "remount-linux-amd64"
APP_NAME = os.environ.get("REMOUNT_MODAL_APP", "remount-demo-claude")
VOLUME_NAME = os.environ.get("REMOUNT_MODAL_VOLUME", APP_NAME + "-data")
SECRET_NAME = os.environ.get("REMOUNT_MODAL_SECRET", "remount-control")

if not BINARY.is_file():
    raise RuntimeError(f"missing {BINARY}; run `make modal-binary` first")

image = (
    # Pin the runtime used by the checked-in harness. add_python supplies the
    # Python runtime Modal functions require without changing Node/npm.
    modal.Image.from_registry("node:22.14.0-bookworm-slim", add_python="3.12")
    .apt_install("ca-certificates", "curl", "procps")
    .add_local_file(str(BINARY), "/usr/local/bin/remount", copy=True)
    .run_commands("chmod +x /usr/local/bin/remount")
)

app = modal.App(APP_NAME, image=image)
control_secret = modal.Secret.from_name(SECRET_NAME, required_keys=["REMOUNT_TOKEN"])

# The control plane is a single stateful process, so it must be exactly one
# container with a durable volume. A web endpoint that scales out would give
# you N independent control planes behind one URL, each with its own database.
volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)

_children: list[subprocess.Popen] = []


def _spawn(argv: list[str], log_path: str) -> subprocess.Popen:
    # The parent closes its descriptor immediately; the child owns the dup.
    # Append preserves the previous boot's evidence on the durable volume.
    with open(log_path, "ab", buffering=0) as log:
        child = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            close_fds=True,
            start_new_session=True,
        )
    _children.append(child)
    return child


def _shutdown() -> None:
    for child in reversed(_children):
        if child.poll() is None:
            child.terminate()
    deadline = time.monotonic() + 10
    for child in reversed(_children):
        if child.poll() is not None:
            continue
        try:
            child.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
    try:
        volume.commit()
    except Exception:
        # Modal also commits the volume periodically and at container exit.
        pass


atexit.register(_shutdown)


@app.function(
    timeout=3600,
    min_containers=1,
    max_containers=1,
    volumes={"/data": volume},
    secrets=[control_secret],
)
@modal.concurrent(max_inputs=200)
@modal.web_server(7443, startup_timeout=180)
def control():
    token = os.environ.get("REMOUNT_TOKEN", "")
    if not token:
        raise RuntimeError("REMOUNT_TOKEN is required")
    # A harness failure must be loud before a public endpoint is advertised.
    subprocess.run(["node", "--version"], check=True)
    subprocess.run(["npm", "--version"], check=True)
    subprocess.run(["/usr/local/bin/remount", "help"], check=True, stdout=subprocess.DEVNULL)
    os.makedirs("/data/server", exist_ok=True)
    os.makedirs("/data/node", exist_ok=True)
    server = _spawn(
        ["/usr/local/bin/remount", "server",
         "--listen", "0.0.0.0:7443",
         "--data", "/data/server",
         "--lease", "20"],
        "/data/server.log",
    )
    # Wait for the control plane to answer before enrolling the local node.
    for _ in range(60):
        if server.poll() is not None:
            raise RuntimeError(f"control plane exited during startup ({server.returncode})")
        try:
            subprocess.check_output(["curl", "-sf", "http://127.0.0.1:7443/healthz"])
            break
        except subprocess.CalledProcessError:
            time.sleep(0.5)
    else:
        raise RuntimeError("control plane did not become healthy within 30 seconds")
    node = _spawn(
        ["/usr/local/bin/remount", "up",
         "--server", "http://127.0.0.1:7443",
         "--data", "/data/node",
         "--label", "vendor=modal",
         "--label", "zone=cloud",
         "--allow", "api.openai.com",
         "--allow", "registry.npmjs.org"],
        "/data/node.log",
    )
    # Do not claim the deployment is ready merely because the HTTP listener is
    # up. Prove the colocated node enrolled and the CLI can authenticate.
    for _ in range(60):
        if node.poll() is not None:
            raise RuntimeError(f"node exited during startup ({node.returncode})")
        try:
            raw = subprocess.check_output([
                "/usr/local/bin/remount", "nodes",
                "--server", "http://127.0.0.1:7443", "--json",
            ])
            if json.loads(raw).get("nodes"):
                return
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        time.sleep(0.5)
    raise RuntimeError("node did not enroll within 30 seconds")


@app.function(image=image, secrets=[control_secret], timeout=120)
def smoke() -> dict[str, object]:
    """Exercise the deployed HTTPS endpoint and authenticated CLI surface."""
    url = control.get_web_url()
    if not url:
        raise RuntimeError("control web URL is unavailable; deploy the app first")
    with urllib.request.urlopen(url + "/healthz", timeout=30) as response:
        health = json.load(response)
    raw = subprocess.check_output([
        "/usr/local/bin/remount", "nodes", "--server", url, "--json",
    ], timeout=30)
    nodes = json.loads(raw).get("nodes", [])
    if not nodes or not any(node.get("online") for node in nodes):
        raise RuntimeError("deployed control plane has no online node")
    return {"url": url, "health": health, "online_nodes": len(nodes)}
