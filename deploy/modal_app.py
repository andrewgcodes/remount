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
APP_NAME = os.environ.get("REMOUNT_MODAL_APP", "remount-demo")
VOLUME_NAME = os.environ.get("REMOUNT_MODAL_VOLUME", APP_NAME + "-data")
SECRET_NAME = os.environ.get("REMOUNT_MODAL_SECRET", "remount-control")

if modal.is_local() and not BINARY.is_file():
    raise RuntimeError(f"missing {BINARY}; run `make modal-binary` first")

image = (
    # Pin the runtime used by the checked-in harness. add_python supplies the
    # Python runtime Modal functions require without changing Node/npm.
    modal.Image.from_registry("node:22.14.0-bookworm-slim", add_python="3.12")
    .apt_install("ca-certificates", "curl", "procps")
    # Function source is imported again inside a container, where the local
    # deployment environment is absent. Bake only the non-secret deployed app
    # name so smoke can resolve the persistent endpoint instead of its
    # ephemeral `modal run` app.
    .env({"REMOUNT_DEPLOYED_APP": APP_NAME})
    .add_local_file(str(BINARY), "/usr/local/bin/remount", copy=True)
    .run_commands("chmod +x /usr/local/bin/remount")
)

app = modal.App(APP_NAME, image=image)
control_secret = modal.Secret.from_name(SECRET_NAME, required_keys=["REMOUNT_TOKEN"])

# Optional. With it the deployment can broker a model credential instead of
# merely allowing egress to the provider: the workspace holds `ref:b_openai`
# and the node substitutes the real value at the network edge. Without it the
# demo can reach api.openai.com and has nothing to send, so no harness can
# actually run there.
MODEL_SECRET_NAME = os.environ.get("REMOUNT_MODAL_MODEL_SECRET", "remount-openai")


def _optional_model_secret() -> list[modal.Secret]:
    """The model secret is optional, so its absence must not fail a deploy.

    from_name is lazy, so a missing secret surfaces at hydration rather than
    here; resolving it now is what makes "optional" true rather than merely
    documented. A deployment without it still runs — it simply has no binding
    to broker, which the control function reports at startup.
    """
    try:
        secret = modal.Secret.from_name(MODEL_SECRET_NAME, required_keys=["OPENAI_API_KEY"])
        secret.hydrate()
        return [secret]
    except Exception as err:  # noqa: BLE001 - any resolution failure means absent
        print(f"remount: model secret {MODEL_SECRET_NAME!r} unavailable ({err.__class__.__name__}); "
              "the deployment will allow egress to the provider but broker no credential")
        return []


model_secrets = _optional_model_secret() if modal.is_local() else []

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


def _decode_nodes(raw: bytes) -> list[dict[str, object]]:
    """Accept the CLI's JSON array and tolerate an empty/null startup read."""
    decoded = json.loads(raw)
    if isinstance(decoded, list):
        return [node for node in decoded if isinstance(node, dict)]
    # Compatibility with an older experimental CLI shape.
    if isinstance(decoded, dict) and isinstance(decoded.get("nodes"), list):
        return [node for node in decoded["nodes"] if isinstance(node, dict)]
    return []


atexit.register(_shutdown)


@app.function(
    timeout=3600,
    min_containers=1,
    max_containers=1,
    volumes={"/data": volume},
    secrets=[control_secret, *model_secrets],
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
    argv = ["/usr/local/bin/remount", "server",
            "--listen", "0.0.0.0:7443",
            "--data", "/data/server",
            "--lease", "20"]
    # The secret's value never enters the bindings file: it names the
    # environment variable the node resolves at lease time, the same contract
    # docs/operations.md documents for a production deployment.
    if os.environ.get("OPENAI_API_KEY"):
        bindings = Path("/data/bindings.json")
        bindings.write_text(json.dumps([{
            "id": "b_openai",
            "secret": "$OPENAI_API_KEY",
            "destinations": ["api.openai.com"],
            "placeholder": "sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY",
            "ttl_sec": 900,
        }]))
        argv += ["--bindings", str(bindings)]
        print("remount: brokering b_openai for api.openai.com", flush=True)
    else:
        print("remount: no OPENAI_API_KEY; no binding configured", flush=True)
    server = _spawn(argv, "/data/server.log")
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
            if _decode_nodes(raw):
                return
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        time.sleep(0.5)
    raise RuntimeError("node did not enroll within 30 seconds")


@app.function(image=image, secrets=[control_secret], timeout=120)
def smoke() -> dict[str, object]:
    """Exercise the deployed HTTPS endpoint and authenticated CLI surface."""
    # `modal run` creates an ephemeral copy of this source app. Resolve the
    # named deployment explicitly so smoke cannot accidentally test that copy.
    deployed_app = os.environ.get("REMOUNT_DEPLOYED_APP", APP_NAME)
    deployed_control = modal.Function.from_name(deployed_app, "control")
    url = deployed_control.get_web_url()
    if not url:
        raise RuntimeError("control web URL is unavailable; deploy the app first")
    with urllib.request.urlopen(url + "/healthz", timeout=30) as response:
        health = json.load(response)
    raw = subprocess.check_output([
        "/usr/local/bin/remount", "nodes", "--server", url, "--json",
    ], timeout=30)
    nodes = _decode_nodes(raw)
    if not nodes or not any(node.get("online") for node in nodes):
        raise RuntimeError("deployed control plane has no online node")
    return {"url": url, "health": health, "online_nodes": len(nodes)}
