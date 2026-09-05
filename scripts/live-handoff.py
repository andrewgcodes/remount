"""Opt-in Claude/Codex handoff evidence driver; no cloud work at import time.

Use the isolated venv in remount-data/handoff-venv. Modes:
  seed --run-dir DIR --recipe claude|codex --execute
  prepare --run-dir DIR --execute
  test --run-dir DIR --binary MAC_BINARY --linux-binary LINUX_BINARY --execute
  cleanup --run-dir DIR --execute

prepare deliberately leaves its single, 30-minute-TTL VM for test to consume.
It never recreates a VM: a second attempt requires a new run and authorization.
All state/evidence is private and local; only synthetic checkout/home trees are
handed off. A test terminates the VM in finally unless --keep-on-failure was
explicitly chosen for bounded debugging; cleanup is then mandatory before TTL.
The model timeout/request settings are NOT a hard dollar billing cap. Claude's
own max-budget is additional defense; Codex records token usage only.
"""
from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_KEYS = {"MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET", "MODAL_ENVIRONMENT",
                "ANTHROPIC_API_KEY", "OPENAI_API_KEY"}
PINS = {"claude": "2.1.260", "codex": "0.152.1"}
MODELS = {"claude": "claude-haiku-4-5-20251001", "codex": "gpt-5-mini"}
BASE_IMAGE = "node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5"
MAX_SCAN_BYTES = 256 * 1024 * 1024


class Failure(RuntimeError):
    pass


def private_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8", opener=lambda p, f: os.open(p, f, 0o600)) as f:
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_keys() -> dict[str, str]:
    from dotenv.parser import parse_stream
    with (ROOT / ".env").open() as stream:
        return {item.key: item.value for item in parse_stream(stream)
                if item.key in ALLOWED_KEYS and item.value and not item.error}


def patterns(keys: dict[str, str]) -> list[bytes]:
    out = []
    for name, value in keys.items():
        if name == "MODAL_ENVIRONMENT" or len(value) < 8:
            continue
        raw = value.encode()
        out.extend([raw, base64.b64encode(raw)])
    return out


def redact(text: str, keys: dict[str, str]) -> str:
    for needle in patterns(keys):
        text = text.replace(needle.decode(), "[REDACTED]")
    return text


def scan(root: Path, needles: list[bytes]) -> dict:
    files, size, hits = 0, 0, 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise Failure("fixture contains a symlink; refusing an unbounded scan")
        if not path.is_file():
            continue
        size += path.stat().st_size
        if size > MAX_SCAN_BYTES:
            raise Failure("fixture scan exceeded 256 MiB")
        data = path.read_bytes()
        files += 1
        hits += int(any(n in data for n in needles))
    return {"files": files, "bytes": size, "leaking_files": hits}


def prove_scanner(run: Path) -> None:
    canary = secrets.token_bytes(32)
    directory = run / ("scan-canary-" + secrets.token_hex(6))
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / "canary"
    path.write_bytes(canary)
    try:
        if scan(directory, [canary])["leaking_files"] != 1:
            raise Failure("secret scanner canary failed")
    finally:
        path.unlink()
        directory.rmdir()


def clean_env(home: Path) -> dict[str, str]:
    return {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(home), "LANG": "en_US.UTF-8", "TERM": "dumb",
            "CLAUDE_CONFIG_DIR": str(home / ".claude"), "CODEX_HOME": str(home / ".codex"),
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_AUTOUPDATER": "1",
            "DISABLE_TELEMETRY": "1", "DISABLE_ERROR_REPORTING": "1"}


def command(argv, *, env, cwd=None, timeout=180, stdin=None):
    p = subprocess.Popen(argv, env=env, cwd=cwd, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        stdout, stderr = p.communicate(stdin, timeout=timeout)
    except BaseException:
        os.killpg(p.pid, signal.SIGTERM)
        try:
            p.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.communicate()
        raise
    return p.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


def progress(step, **fields):
    print(json.dumps({"step": step, **fields}), flush=True)


def save_command(run: Path, label: str, result, keys):
    code, out, err = result
    progress(label, exit=code)
    private_json(run / "evidence" / (label + ".json"),
                 {"exit": code, "stdout": redact(out, keys), "stderr": redact(err, keys)})
    if any(n in (out + err).encode() for n in patterns(keys)):
        raise Failure(f"credential detected in {label} output; evidence redacted")
    return code, out, err


def seed(args, keys):
    recipe = args.recipe
    fixture = args.run_dir / ("fixture-" + recipe)
    if fixture.exists():
        raise Failure("fixture already exists; refusing an implicit model retry")
    home, checkout = fixture / "home", fixture / "checkout"
    home.mkdir(parents=True, mode=0o700)
    checkout.mkdir(mode=0o700)
    env = clean_env(home)
    key_name = "ANTHROPIC_API_KEY" if recipe == "claude" else "OPENAI_API_KEY"
    if not keys.get(key_name):
        raise Failure("missing " + key_name)
    binary = Path.home() / ".local/bin" / recipe
    version = command([str(binary), "--version"], env=env, timeout=20)
    save_command(args.run_dir, recipe + "-version", version, keys)
    if version[0] or PINS[recipe] not in version[1]:
        raise Failure("unexpected installed harness version")
    result = command(["git", "init", "-q", str(checkout)], env=env)
    if result[0]:
        raise Failure("git fixture initialization failed")
    (checkout / "README.md").write_text("Synthetic Remount handoff fixture. No credentials.\n")
    nonce = "handoff-" + secrets.token_hex(16)
    prompt = ("Remember this exact continuity code for our next turn: " + nonce +
              ". Do not use tools, create files, or repeat the code. Reply only READY.")
    model = args.model or MODELS[recipe]
    env[key_name] = keys[key_name]
    if recipe == "claude":
        argv = [str(binary), "--bare", "--model", model, "--max-budget-usd", "1",
                "--tools", "", "--output-format", "json", "-p", prompt]
    else:
        config = ('model = ' + json.dumps(model) + '\nmodel_provider = "handoff"\n'
                  'model_reasoning_effort = "low"\n'
                  '[model_providers.handoff]\nname = "OpenAI API"\n'
                  'base_url = "https://api.openai.com/v1"\nenv_key = "OPENAI_API_KEY"\n'
                  'wire_api = "responses"\nrequest_max_retries = 0\n'
                  'stream_max_retries = 0\n')
        (home / ".codex").mkdir(mode=0o700, exist_ok=True)
        (home / ".codex/config.toml").write_text(config)
        argv = [str(binary), "exec", "--sandbox", "read-only", "--skip-git-repo-check",
                "--ignore-rules", "--json", prompt]
    started = time.time()
    result = command(argv, env=env, cwd=checkout, timeout=180)
    save_command(args.run_dir, recipe + "-seed", result, keys)
    removed = []
    for rel in (".codex/auth.json", ".claude/.credentials.json"):
        path = home / rel
        if path.exists():
            path.unlink()
            removed.append(rel)
    prove_scanner(args.run_dir)
    scanned = scan(fixture, patterns(keys))
    metadata = {"recipe": recipe, "model": model, "version": PINS[recipe],
                "home": str(home), "checkout": str(checkout), "nonce": nonce,
                "scan": scanned, "auth_files_removed": removed,
                "duration_sec": round(time.time() - started, 3), "exit": result[0],
                "model_cost_hard_cap": False}
    private_json(args.run_dir / ("seed-" + recipe + ".json"), metadata)
    if scanned["leaking_files"]:
        raise Failure("seed fixture contains credentials: do not upload")
    if result[0] != 0:
        raise Failure(recipe + " seed failed; sanitized evidence recorded; no automatic retry")
    if "READY" not in result[1]:
        raise Failure(recipe + " seed did not acknowledge the conversation")
    print(json.dumps({k: v for k, v in metadata.items() if k != "nonce"}), flush=True)


def modal_client(keys):
    import modal
    for name in ("MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET"):
        if not keys.get(name):
            raise Failure("missing " + name)
        os.environ[name] = keys[name]
    os.environ["MODAL_ENVIRONMENT"] = keys.get("MODAL_ENVIRONMENT", "dev")
    return modal


def remote(sb, argv, timeout=180):
    p = sb.exec(*argv, timeout=timeout)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        out = pool.submit(p.stdout.read)
        err = pool.submit(p.stderr.read)
        p.wait()
        return p.returncode, out.result(), err.result()


def state_load(run):
    return json.loads((run / "provider.json").read_text())


def node_inventory_command(binary):
    return [str(binary), "nodes", "--json"]


def stop_app(args, keys, state):
    env = clean_env(args.run_dir)
    env.update({k: v for k, v in keys.items() if k.startswith("MODAL_")})
    env.setdefault("MODAL_ENVIRONMENT", "dev")
    result = command([sys.executable, "-m", "modal", "app", "stop", state["app_id"], "--yes"],
                     env=env, timeout=60)
    label = "app-stop-retry" if (args.run_dir / "evidence/app-stop.json").exists() else "app-stop"
    save_command(args.run_dir, label, result, keys)
    state["app_stop_exit"] = result[0]
    private_json(args.run_dir / "provider.json", state)
    if result[0]:
        raise Failure("owned app stop failed")
    return result[0]


def cleanup_workspaces(args, keys, state):
    candidate = args.run_dir / "candidate.json"
    runtime = args.run_dir / "runtime.json"
    if not candidate.exists() or not runtime.exists() or not state.get("server"):
        return
    manifest = json.loads(candidate.read_text())
    binary = manifest["binaries"]["native"]
    if digest(binary["path"]) != binary["sha256"]:
        raise Failure("cleanup candidate changed; refusing to execute it")
    token = json.loads(runtime.read_text())["REMOUNT_TOKEN"]
    scan_keys = {**keys, "REMOUNT_TOKEN": token}
    env = clean_env(args.run_dir / "client-home")
    env.update({"REMOUNT_SERVER": state["server"], "REMOUNT_TOKEN": token})
    result = command([binary["path"], "ws", "ls", "--json"], env=env, timeout=30)
    save_command(args.run_dir, "final-workspace-inventory", result, scan_keys)
    if result[0]:
        state["workspace_cleanup"] = "unavailable; provider termination still required"
        return
    for workspace in json.loads(result[1]) or []:
        result = command([binary["path"], "ws", "destroy", workspace["id"]], env=env, timeout=60)
        save_command(args.run_dir, "final-destroy-" + workspace["id"], result, scan_keys)
    result = command([binary["path"], "ws", "ls", "--json"], env=env, timeout=30)
    save_command(args.run_dir, "final-workspace-absence", result, scan_keys)
    state["workspace_cleanup"] = "verified" if result[0] == 0 and not json.loads(result[1]) else "failed"


def cleanup(args, keys):
    modal = modal_client(keys)
    state = state_load(args.run_dir)
    if state.get("cleaned"):
        if state.get("app_stop_exit") != 0:
            stop_app(args, keys, state)
        return
    try:
        cleanup_workspaces(args, keys, state)
    except Exception as exc:
        state["workspace_cleanup"] = "unavailable: " + type(exc).__name__
    sb_id = state.get("sandbox_id")
    sandboxes = list(modal.Sandbox.list(app_id=state["app_id"]))
    owned = [s for s in sandboxes if s.object_id == sb_id or
             s.get_tags().get("remount.handoff") == state["run_id"]]
    if sb_id and not any(s.object_id == sb_id for s in owned):
        try:
            owned.append(modal.Sandbox.from_id(sb_id))
        except modal.exception.NotFoundError:
            pass
    for sb in owned:
        sb.terminate()
        sb.wait(raise_on_termination=False)
    deadline = time.monotonic() + 60
    while True:
        remaining = [s for s in modal.Sandbox.list(app_id=state["app_id"])
                     if s.object_id == sb_id or s.get_tags().get("remount.handoff") == state["run_id"]]
        if not remaining:
            break
        if time.monotonic() > deadline:
            raise Failure("sandbox still present after terminate")
        time.sleep(1)
    tunnel_closed = None
    if state.get("server"):
        try:
            with urllib.request.urlopen(state["server"] + "/healthz", timeout=10) as response:
                tunnel_closed = response.status != 200
        except OSError:
            tunnel_closed = True
        if not tunnel_closed:
            raise Failure("deleted sandbox tunnel still reports healthy")
    state["cleaned"] = True
    state["cleanup_time"] = time.time()
    state["remaining_owned_sandboxes"] = 0
    state["tunnel_closed"] = tunnel_closed
    private_json(args.run_dir / "provider.json", state)
    app_stop_exit = stop_app(args, keys, state)
    (args.run_dir / "runtime.json").unlink(missing_ok=True)
    print(json.dumps({"cleanup": "verified", "sandbox_id": sb_id,
                      "remaining_owned_sandboxes": 0, "tunnel_closed": tunnel_closed,
                      "app_stop_exit": app_stop_exit}), flush=True)


def docker_profile_argv(argv, image, security_options):
    if not argv or argv[0] != "run" or image not in argv:
        return list(argv)
    label = next((argv[i + 1] for i, value in enumerate(argv[:-1])
                  if value == "--label" and argv[i + 1].startswith("remount.workspace=ws_")), None)
    name = next((argv[i + 1] for i, value in enumerate(argv[:-1]) if value == "--name"), None)
    if not label or name != "remount-" + label.split("=", 1)[1].replace("_", "-"):
        return list(argv)
    approved = {"seccomp=unconfined", "apparmor=unconfined"}
    if not security_options or not set(security_options).issubset(approved):
        raise Failure("unsupported nested sandbox test security option")
    extra = [item for option in security_options for item in ("--security-opt", option)]
    return ["run", *extra, *argv[1:]]


def docker_wrapper_source(image, security_options):
    import inspect
    source = inspect.getsource(docker_profile_argv)
    return (chr(35) + "!/usr/bin/python3\nimport os, sys\nFailure = RuntimeError\n" + source +
            "\nargs = docker_profile_argv(sys.argv[1:], " + repr(image) + ", " + repr(security_options) + ")\n"
            "os.execv('/usr/bin/docker', ['/usr/bin/docker', *args])\n")


def prepare_nested_profile(args, keys, sb, state):
    profile = {"enabled": True, "scope": "test-only labeled handoff containers in this disposable VM",
               "production_defaults_changed": False, "codex_sandbox_disabled": False}
    state["nested_sandbox_test_profile"] = profile
    image = state["image"]
    for options in (["seccomp=unconfined"], ["seccomp=unconfined", "apparmor=unconfined"]):
        source = docker_wrapper_source(image, options)
        if remote(sb, ["mkdir", "-p", "/opt/remount-handoff/bin"])[0]:
            raise Failure("cannot create test-only wrapper directory")
        sb.filesystem.write_text(source, "/opt/remount-handoff/bin/docker")
        if remote(sb, ["chmod", "755", "/opt/remount-handoff/bin/docker"])[0]:
            raise Failure("cannot activate test-only wrapper")
        code = ("set -eu; printf codex-sandbox-write > NESTED-PROBE.txt; "
                "test \"$(cat NESTED-PROBE.txt)\" = codex-sandbox-write; "
                "if printf forbidden > /etc/remount-handoff-denied 2>/dev/null; then "
                "echo outside-write-unexpectedly-allowed; exit 31; fi; "
                "echo inside-write-ok; echo outside-write-denied")
        argv = ["/opt/remount-handoff/bin/docker", "run", "--rm", "--name", "remount-ws-handoff-probe",
                "--label", "remount.workspace=ws_handoff_probe",
                "-v", "/tmp/handoff-probe:/Users/handoff/probe", "-w", "/Users/handoff/probe",
                "-e", "HOME=/Users/handoff/probe", "-e", "CODEX_HOME=/Users/handoff/probe/.codex",
                image, "sh", "-c", 'mkdir -p "$CODEX_HOME"; exec "$@"', "sh",
                "codex", "sandbox", "-c", 'sandbox_mode="workspace-write"',
                "--", "sh", "-c", code]
        progress("nested-codex-sandbox-probe-start", security_options=options)
        result = remote(sb, argv, 60)
        suffix = "seccomp" if len(options) == 1 else "seccomp-apparmor"
        save_command(args.run_dir, "nested-sandbox-probe-" + suffix, result, keys)
        permission_failure = any(word in result[2].lower() for word in
                                 ("namespace", "operation not permitted", "permission denied", "apparmor"))
        if result[0] and not permission_failure:
            profile.update({"probe_passed": False, "failure_kind": "probe_setup", "security_options": options})
            private_json(args.run_dir / "provider.json", state)
            raise Failure("nested sandbox probe setup failed before compatibility could be judged; see sanitized probe evidence")
        if result[0] == 0 and "inside-write-ok" in result[1] and "outside-write-denied" in result[1]:
            profile.update({"security_options": options, "probe_passed": True,
                            "wrapper_sha256": hashlib.sha256(source.encode()).hexdigest()})
            private_json(args.run_dir / "provider.json", state)
            progress("nested-codex-sandbox-compatible", **profile)
            return
    profile["probe_passed"] = False
    private_json(args.run_dir / "provider.json", state)
    raise Failure("Codex sandbox remains incompatible after approved test-only seccomp/AppArmor options; no additional permission changes made")


def prepare(args, keys):
    modal = modal_client(keys)
    path = args.run_dir / "provider.json"
    if path.exists():
        raise Failure("provider state exists; refusing a second VM creation")
    run_id = "remount-handoff-" + time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(3)
    image = (modal.Image.from_registry(BASE_IMAGE)
             .apt_install("docker.io", "ca-certificates", "curl", "git", "python3"))
    app = modal.App.lookup(run_id, create_if_missing=True)
    state = {"run_id": run_id, "app_id": app.app_id, "started_at": time.time(),
             "ttl_sec": 1800, "cpu_request": 1, "cpu_limit": 2, "memory_mib": 4096,
             "runtime": "vm_runtime", "prepared": False, "cleaned": False}
    private_json(path, state)
    runtime_secrets = {k: keys[k] for k in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY") if keys.get(k)}
    runtime_secrets["REMOUNT_TOKEN"] = secrets.token_urlsafe(36)
    private_json(args.run_dir / "runtime.json", {"REMOUNT_TOKEN": runtime_secrets["REMOUNT_TOKEN"]})
    success = False
    try:
        sb = modal.Sandbox.create(
            "/usr/sbin/dockerd", app=app, image=image, name=run_id,
            tags={"remount.handoff": run_id}, timeout=1800, cpu=(1.0, 2.0), memory=4096,
            encrypted_ports=[7443], experimental_options={"vm_runtime": True},
            secrets=[modal.Secret.from_dict(runtime_secrets)])
        state["sandbox_id"] = sb.object_id
        state["created_at"] = time.time()
        private_json(path, state)
        print(json.dumps({"sandbox_id": sb.object_id, "run_id": run_id, "ttl_sec": 1800}), flush=True)
        result = remote(sb, ["sh", "-c", "for i in $(seq 1 90); do docker info >/dev/null 2>&1 && break; sleep 1; done; docker info --format '{{.ServerVersion}}'; uname -srm"], 120)
        if save_command(args.run_dir, "docker-ready", result, keys)[0]:
            raise Failure("Docker VM readiness failed")
        dockerfile = ("FROM " + BASE_IMAGE + "\nRUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates python3 && rm -rf /var/lib/apt/lists/*\n"
                      "RUN npm install -g @anthropic-ai/claude-code@" + PINS["claude"] +
                      " @openai/codex@" + PINS["codex"] + "\n")
        sb.filesystem.write_text(dockerfile, "/tmp/handoff.Dockerfile")
        image_name = "remount-handoff:" + run_id
        result = remote(sb, ["docker", "build", "-f", "/tmp/handoff.Dockerfile", "-t", image_name, "/tmp"], 600)
        if save_command(args.run_dir, "workspace-image", result, keys)[0]:
            raise Failure("pinned workspace image build failed")
        probe = "mkdir -p /tmp/handoff-probe; printf shared > /tmp/handoff-probe/proof; docker run --rm -v /tmp/handoff-probe:/Users/handoff/probe -w /Users/handoff/probe " + image_name + " sh -c 'test \"$(cat proof)\" = shared && pwd && claude --version && codex --version'"
        if save_command(args.run_dir, "mount-and-version-probe", remote(sb, ["sh", "-c", probe]), keys)[0]:
            raise Failure("mount/version probe failed")
        state["image"] = image_name
        if args.nested_sandbox_test_profile:
            prepare_nested_profile(args, keys, sb, state)
        tunnel = sb.tunnels()[7443]
        state.update({"prepared": True,
                      "server": "https://" + tunnel.host + ":" + str(tunnel.port)})
        private_json(path, state)
        success = True
        print(json.dumps({"prepared": True, "run_dir": str(args.run_dir),
                          "sandbox_id": sb.object_id, "expires_by": state["created_at"] + 1800,
                          "waiting_for": "frozen candidate binaries"}), flush=True)
    finally:
        if not success:
            if args.keep_on_failure and state.get("sandbox_id"):
                progress("preparation-held-for-bounded-debugging", sandbox_id=state["sandbox_id"],
                         expires_by=state["created_at"] + 1800, cleanup_required=True)
            else:
                cleanup(args, keys)


def digest(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load_seed(args, recipe, keys):
    seed_run = (args.seed_run_dir or args.run_dir).resolve()
    if not seed_run.is_relative_to((ROOT / "remount-data").resolve()):
        raise Failure("seed run must be inside ignored remount-data")
    data = json.loads((seed_run / ("seed-" + recipe + ".json")).read_text())
    fixture = seed_run / ("fixture-" + recipe)
    if Path(data["home"]).resolve() != fixture / "home" or Path(data["checkout"]).resolve() != fixture / "checkout":
        raise Failure("seed metadata does not identify the original synthetic fixture")
    if data["exit"] or scan(fixture, patterns(keys))["leaking_files"]:
        raise Failure("seed is not safe to hand off")
    return data


def candidate_manifest(args):
    if not args.binary or not args.linux_binary or not args.candidate:
        raise Failure("test requires frozen --binary, --linux-binary and --candidate")
    manifest = {"candidate": args.candidate, "binaries": {}}
    for name, path in (("native", args.binary), ("linux", args.linux_binary)):
        path = path.resolve(strict=True)
        manifest["binaries"][name] = {"path": str(path), "sha256": digest(path),
                                       "bytes": path.stat().st_size}
    return manifest


def reconnect_probe(args, env, ws, session, keys, label):
    argv = [str(args.binary), "attach", ws, session]
    p = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        out, err = p.communicate(timeout=0.5)
        detached = False
    except subprocess.TimeoutExpired:
        p.send_signal(signal.SIGINT)
        try:
            out, err = p.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            out, err = p.communicate()
        detached = True
    save_command(args.run_dir, label + "-first-attach",
                 (p.returncode, out.decode(errors="replace"), err.decode(errors="replace")), keys)
    result = command(argv, env=env, timeout=240)
    save_command(args.run_dir, label + "-reconnect", result, keys)
    if result[0]:
        raise Failure("remote harness failed after reconnect")
    replay = command(argv, env=env, timeout=60)
    save_command(args.run_dir, label + "-replay", replay, keys)
    if replay[0] or replay[1] != result[1]:
        raise Failure("session replay differs after closing/reconnecting the client")
    private_json(args.run_dir / "evidence" / (label + "-reconnect-proof.json"),
                 {"interrupted_live_attachment": detached, "replay_equal": True,
                  "replay_bytes": len(replay[1].encode())})


def test(args, keys):
    manifest = candidate_manifest(args)
    private_json(args.run_dir / "candidate.json", manifest)
    state = state_load(args.run_dir)
    if not state.get("prepared") or state.get("cleaned"):
        raise Failure("no prepared live VM; never create a replacement implicitly")
    profile = state.get("nested_sandbox_test_profile", {})
    if bool(profile.get("enabled")) != args.nested_sandbox_test_profile:
        raise Failure("test profile differs from preparation; explicit --nested-sandbox-test-profile is required")
    if profile.get("enabled") and not profile.get("probe_passed"):
        raise Failure("nested Codex sandbox profile was not proven compatible")
    if time.time() > state["created_at"] + 1800 - 600:
        raise Failure("less than ten minutes of VM TTL remain; ask before another VM")
    if (args.run_dir / "test-started.json").exists():
        raise Failure("test already attempted; no implicit billable retry")
    private_json(args.run_dir / "test-started.json", {"at": time.time()})
    token = json.loads((args.run_dir / "runtime.json").read_text())["REMOUNT_TOKEN"]
    keys = {**keys, "REMOUNT_TOKEN": token}
    env = clean_env(args.run_dir / "client-home")
    Path(env["HOME"]).mkdir(mode=0o700, exist_ok=True)
    env.update({"REMOUNT_SERVER": state["server"], "REMOUNT_TOKEN": token})
    modal = modal_client(keys)
    sb = modal.Sandbox.from_id(state["sandbox_id"])
    workspaces = []
    tests = []

    def cli(label, argv, timeout=180, checked=True):
        progress(label + "-start")
        result = command([str(args.binary), *argv], env=env, timeout=timeout)
        save_command(args.run_dir, label, result, keys)
        if checked and result[0]:
            raise Failure(label + " failed; see sanitized evidence")
        return result

    passed = False
    try:
        native_version = cli("native-version", ["version"])[1]
        if args.candidate not in native_version:
            raise Failure("native binary version does not identify the frozen candidate")
        sb.filesystem.copy_from_local(str(args.linux_binary.resolve()), "/usr/local/bin/remount")
        result = remote(sb, ["sh", "-c", "chmod 755 /usr/local/bin/remount; sha256sum /usr/local/bin/remount; remount version"])
        save_command(args.run_dir, "remote-candidate", result, keys)
        if result[0] or manifest["binaries"]["linux"]["sha256"] not in result[1] or args.candidate not in result[1]:
            raise Failure("remote binary checksum/version does not match candidate")
        bindings = [{"id": "b_openai", "secret": "$OPENAI_API_KEY", "destinations": ["api.openai.com"],
                     "placeholder": "sk-REMOUNT-HANDOFF-OPENAI-NOT-A-KEY", "ttl_sec": 900},
                    {"id": "b_anthropic", "secret": "$ANTHROPIC_API_KEY", "destinations": ["api.anthropic.com"],
                     "placeholder": "sk-ant-REMOUNT-HANDOFF-NOT-A-KEY", "ttl_sec": 900}]
        sb.filesystem.write_text(json.dumps(bindings), "/tmp/bindings.json")
        start = '''import os, subprocess
os.makedirs('/data/server', exist_ok=True)
os.makedirs('/data/node', exist_ok=True)
for name, argv in [
 ('server', ['remount','server','--listen','0.0.0.0:7443','--data','/data/server','--lease','20','--bindings','/tmp/bindings.json']),
 ('node', ['remount','up','--server','http://127.0.0.1:7443','--data','/data/node','--backend','docker','--image',IMAGE,'--label','vendor=modal-vm','--allow','registry.npmjs.org','--allow','api.openai.com','--allow','api.anthropic.com','--allow','platform.claude.com','--allow','statsig.anthropic.com'])]:
 with open('/tmp/' + name + '.log', 'ab', buffering=0) as log:
  env = os.environ.copy()
  if name == 'node' and NESTED:
   env['PATH'] = '/opt/remount-handoff/bin:' + env.get('PATH', '/usr/bin:/bin')
  p = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
  print(name, p.pid)
'''.replace("IMAGE", repr(state["image"])).replace("NESTED", repr(bool(profile.get("enabled"))))
        save_command(args.run_dir, "start-remount", remote(sb, ["python3", "-c", start]), keys)
        deadline = time.monotonic() + 90
        while True:
            result = command(node_inventory_command(args.binary), env=env, timeout=15)
            if result[0] == 0:
                nodes = json.loads(result[1]) or []
                if any(n.get("online") for n in nodes):
                    save_command(args.run_dir, "node-ready", result, keys)
                    break
            if time.monotonic() > deadline:
                save_command(args.run_dir, "node-readiness-failed", result, keys)
                raise Failure("authenticated node readiness deadline exceeded")
            time.sleep(1)
        cli("status-before", ["status", "--json"])
        for recipe in args.recipes.split(","):
            seed_data = load_seed(args, recipe, keys)
            progress(recipe + "-seed-reused", home=seed_data["home"], checkout=seed_data["checkout"])
            prompt = ("Recall the exact continuity code from our previous turn without reading conversation files. "
                      "The no-tools instruction applied only to that first turn. Now use your file-writing tool "
                      "to create RESULT.txt in the current checkout containing only that code and a newline. "
                      "Do not search files for the code. Reply DONE after the write.")
            binding = "b_anthropic:anthropic" if recipe == "claude" else "b_openai:openai"
            result = cli(recipe + "-handoff", ["handoff", "--recipe", recipe,
                         "--dir", seed_data["checkout"], "--home", seed_data["home"],
                         "--backend", "docker", "--image", state["image"],
                         "--binding", binding, "--name", state["run_id"] + "-" + recipe,
                         "--model", seed_data["model"], "--sandbox", "workspace-write",
                         "--exclude", "node_modules", "--exclude", ".codex/plugins",
                         "--timeout", "180s", "--json", "--task", prompt], timeout=240)
            handoff = json.loads(result[1])
            ws, session = handoff["ws"], handoff["session"]
            workspaces.append(ws)
            private_json(args.run_dir / "workspaces.json", workspaces)
            container = "remount-" + ws.replace("_", "-")
            inspected = remote(sb, ["/usr/bin/docker", "inspect", "--format", "{{json .HostConfig.SecurityOpt}}", container])
            save_command(args.run_dir, recipe + "-docker-security-profile", inspected, keys)
            if inspected[0] or (profile.get("enabled") and json.loads(inspected[1]) != profile["security_options"]):
                raise Failure("actual container security options differ from approved test profile")
            if handoff["mount_path"] != str(Path(seed_data["checkout"]).resolve()):
                raise Failure("handoff mount path differs from canonical source checkout")
            reconnect_probe(args, env, ws, session, keys, recipe)
            result = cli(recipe + "-pwd", ["exec", ws, "--", "sh", "-c", "uname -s; pwd; cat RESULT.txt"])
            lines = result[1].splitlines()
            if "Linux" not in lines or handoff["mount_path"] not in lines or seed_data["nonce"] not in lines:
                raise Failure("Linux/path/conversation-recall postcondition failed")
            remote_scan = '''import base64, json, os, pathlib, subprocess
needles = []
for k in ('OPENAI_API_KEY','ANTHROPIC_API_KEY','REMOUNT_TOKEN'):
 v = os.environ.get(k, '').encode()
 if v: needles.extend([v, base64.b64encode(v)])
root = pathlib.Path('/data/node/ws') / WS
count = size = hits = 0
for p in root.rglob('*'):
 if p.is_symlink(): continue
 if not p.is_file(): continue
 size += p.stat().st_size
 if size > 268435456: raise RuntimeError('remote scan exceeds 256 MiB')
 count += 1
 hits += int(any(n in p.read_bytes() for n in needles))
container = 'remount-' + WS.replace('_','-')
env = subprocess.check_output(['docker','exec',container,'env'])
env_hits = sum(int(n in env) for n in needles)
print(json.dumps({'files':count,'bytes':size,'leaking_files':hits,'environment_hits':env_hits}))
if hits or env_hits or not count: raise RuntimeError('remote scan failed')
'''.replace("WS", repr(ws))
            if save_command(args.run_dir, recipe + "-remote-scan", remote(sb, ["python3", "-c", remote_scan]), keys)[0]:
                raise Failure("remote workspace secret scan failed")
            cli(recipe + "-inspect", ["inspect", ws, "--json"])
            deadline = time.monotonic() + 15
            while True:
                events = cli(recipe + "-events", ["events", "--ws", ws, "--json"])[1]
                if "cred.used" in events and "egress.allowed" in events:
                    break
                if time.monotonic() > deadline:
                    raise Failure("broker credential/egress audit events missing")
                time.sleep(1)
            pull = args.run_dir / ("pull-" + recipe)
            pull.mkdir(mode=0o700)
            cli(recipe + "-pull", ["pull", ws, "--dir", str(pull), "--json"], timeout=180)
            if (pull / "RESULT.txt").read_text().strip() != seed_data["nonce"]:
                raise Failure("pulled result differs from conversation nonce")
            if scan(pull, patterns(keys))["leaking_files"]:
                raise Failure("pulled workspace contains credentials")
            source_logs = list(Path(seed_data["home"]).glob(
                ".claude/projects/*/*.jsonl" if recipe == "claude" else ".codex/sessions/**/*.jsonl"))
            if len(source_logs) != 1:
                raise Failure("fixture does not identify exactly one source conversation")
            resumed_logs = list(pull.rglob(source_logs[0].name))
            if len(resumed_logs) != 1 or resumed_logs[0].stat().st_size <= source_logs[0].stat().st_size:
                raise Failure("original conversation was not extended under the same transcript identity")
            tests.append({"recipe": recipe, "ws": ws, "session": session,
                          "conversation_file": source_logs[0].name, "passed": True})
            private_json(args.run_dir / "results.json", tests)
        cli("doctor", ["doctor", "--deep", "--json"], timeout=120)
        cli("metrics", ["metrics"])
        if candidate_manifest(args) != manifest:
            raise Failure("candidate binary changed during run")
        passed = True
        print(json.dumps({"tested": tests, "candidate": args.candidate}), flush=True)
    finally:
        try:
            result = cli("cleanup-inventory", ["ws", "ls", "--json"], timeout=30, checked=False)
            if result[0] == 0:
                workspaces.extend(w["id"] for w in (json.loads(result[1]) or []))
            if passed or not args.keep_on_failure:
                for ws in sorted(set(workspaces)):
                    cli("destroy-" + ws, ["ws", "destroy", ws], timeout=60, checked=False)
            for name in ("server", "node"):
                save_command(args.run_dir, name + "-log", remote(sb, ["tail", "-c", "100000", "/tmp/" + name + ".log"], 30), keys)
        finally:
            if passed or not args.keep_on_failure:
                cleanup(args, keys)
            else:
                progress("held-for-bounded-debugging", expires_by=state["created_at"] + 1800,
                         sandbox_id=state["sandbox_id"], cleanup_required=True)


def explicit_resume(args, keys):
    state = state_load(args.run_dir)
    if state.get("cleaned"):
        raise Failure("explicit resume requires the existing held VM")
    marker = args.run_dir / "explicit-resume-started.json"
    if marker.exists():
        raise Failure("explicit resume already attempted; refusing automatic model retry")
    manifest = json.loads((args.run_dir / "candidate.json").read_text())
    binary = manifest["binaries"]["native"]
    if digest(binary["path"]) != binary["sha256"]:
        raise Failure("candidate binary changed")
    seed_data = load_seed(args, "codex", keys)
    logs = list(Path(seed_data["home"]).glob(".codex/sessions/**/*.jsonl"))
    if len(logs) != 1:
        raise Failure("expected exactly one synthetic conversation")
    with logs[0].open() as stream:
        session_id = json.loads(stream.readline())["payload"]["id"]
    token = json.loads((args.run_dir / "runtime.json").read_text())["REMOUNT_TOKEN"]
    keys = {**keys, "REMOUNT_TOKEN": token}
    env = clean_env(args.run_dir / "client-home")
    env.update({"REMOUNT_SERVER": state["server"], "REMOUNT_TOKEN": token})
    handoff = json.loads(json.loads((args.run_dir / "evidence/codex-handoff.json").read_text())["stdout"])
    ws = handoff["ws"]
    prompt = ("Recall the exact continuity code from our previous user turn without reading conversation files. "
              "Now use your file-writing tool to create RESULT.txt in the current checkout containing only "
              "that code and a newline. Do not search files for the code. Reply DONE after the write.")
    script = ('set -eu; . ./.remount/env; export CODEX_API_KEY="$OPENAI_API_KEY"; '
              'exec sh .remount/launch/codex-driver exec --sandbox workspace-write --model "$1" '
              'resume "$2" --skip-git-repo-check "$3"')
    private_json(marker, {"at": time.time(), "session_id": session_id,
                          "diagnostic_only": True, "argv_change": "explicit UUID replaces --last"})
    result = command([binary["path"], "exec", ws, "--timeout", "180s", "--", "sh", "-c", script,
                      "sh", seed_data["model"], session_id, prompt], env=env, timeout=210)
    save_command(args.run_dir, "codex-explicit-resume", result, keys)
    if result[0]:
        raise Failure("explicit UUID diagnostic failed")
    result = command([binary["path"], "exec", ws, "--", "cat", "RESULT.txt"], env=env, timeout=30)
    save_command(args.run_dir, "codex-explicit-result", result, keys)
    if result[0] or result[1].strip() != seed_data["nonce"]:
        raise Failure("explicit UUID diagnostic did not recover the conversation nonce")
    progress("codex-explicit-uuid-recall", passed=True, session_id=session_id,
             diagnostic_only=True)


def diagnose(args, keys):
    state = state_load(args.run_dir)
    if state.get("cleaned"):
        raise Failure("diagnosis requires the existing held VM")
    modal = modal_client(keys)
    sb = modal.Sandbox.from_id(state["sandbox_id"])
    handoff = json.loads(json.loads((args.run_dir / "evidence/codex-handoff.json").read_text())["stdout"])
    ws = handoff["ws"]
    script = '''import hashlib, json, pathlib
root = pathlib.Path('/data/node/ws') / WS
out = {'sessions': [], 'launchers': {}}
for path in sorted((root / '.codex/sessions').rglob('*.jsonl')):
 with path.open() as stream:
  row = json.loads(stream.readline())
 payload = row.get('payload',{})
 out['sessions'].append({'file':str(path.relative_to(root)), 'bytes':path.stat().st_size,
  'sha256':hashlib.sha256(path.read_bytes()).hexdigest(), 'id':payload.get('id'),
  'provider':payload.get('model_provider'),'cwd':payload.get('cwd')})
for path in sorted((root / '.remount/launch').glob('*')):
 if path.is_file() and path.stat().st_size < 20000:
  out['launchers'][path.name] = path.read_text(errors='replace')
print(json.dumps(out))
'''.replace("WS", repr(ws))
    result = remote(sb, ["python3", "-c", script])
    phase = "codex-after-explicit" if (args.run_dir / "explicit-resume-started.json").exists() else "codex-failed"
    save_command(args.run_dir, phase + "-state-diagnosis", result, keys)
    if result[0]:
        raise Failure("state diagnosis failed")
    data = json.loads(result[1])
    progress("codex-session-metadata", sessions=data["sessions"])
    manifest = json.loads((args.run_dir / "candidate.json").read_text())
    binary = manifest["binaries"]["native"]["path"]
    token = json.loads((args.run_dir / "runtime.json").read_text())["REMOUNT_TOKEN"]
    keys = {**keys, "REMOUNT_TOKEN": token}
    env = clean_env(args.run_dir / "client-home")
    env.update({"REMOUNT_SERVER": state["server"], "REMOUNT_TOKEN": token})
    for label, argv in ((phase + "-inspect", ["inspect", ws, "--json"]),
                        (phase + "-events", ["events", "--ws", ws, "--json"]),
                        (phase + "-usage", ["usage", "--ws", ws]),
                        (phase + "-doctor", ["doctor", "--deep", "--json"]),
                        (phase + "-metrics", ["metrics"])):
        save_command(args.run_dir, label, command([binary, *argv], env=env, timeout=120), keys)
    pull = args.run_dir / ("pull-" + phase)
    pull.mkdir(mode=0o700, exist_ok=True)
    result = command([binary, "pull", ws, "--dir", str(pull), "--json"], env=env, timeout=180)
    save_command(args.run_dir, phase + "-pull", result, keys)
    if result[0] == 0:
        private_json(args.run_dir / "evidence" / (phase + "-secret-scan.json"), scan(pull, patterns(keys)))


def summarize(args, keys):
    decoder = json.JSONDecoder()
    summary = {"candidate": json.loads((args.run_dir / "candidate.json").read_text()),
               "provider": state_load(args.run_dir), "recipes": {}}
    for recipe in ("claude", "codex"):
        prefix = recipe
        if not (args.run_dir / "evidence" / (prefix + "-events.json")).exists():
            prefix = "codex-after-explicit"
        text = json.loads((args.run_dir / "evidence" / (prefix + "-events.json")).read_text())["stdout"]
        counts = {}
        while text.strip():
            text = text.lstrip()
            event, consumed = decoder.raw_decode(text)
            text = text[consumed:]
            kind = event.get("type", "")
            counts[kind] = counts.get(kind, 0) + 1
        seed_data = load_seed(args, recipe, keys)
        pull = args.run_dir / ("pull-" + prefix)
        result = pull / "RESULT.txt"
        source = list(Path(seed_data["home"]).glob(".claude/projects/*/*.jsonl" if recipe == "claude" else ".codex/sessions/**/*.jsonl"))[0]
        dest = list(pull.rglob(source.name))[0]
        summary["recipes"][recipe] = {"events": counts,
            "result_matches": result.exists() and result.read_text().strip() == seed_data["nonce"],
            "conversation_file": source.name, "source_bytes": source.stat().st_size,
            "remote_bytes": dest.stat().st_size,
            "reconnect": json.loads((args.run_dir / "evidence" / (recipe + "-reconnect-proof.json")).read_text())}
    summary["completed_proofs"] = json.loads((args.run_dir / "results.json").read_text())
    explicit = args.run_dir / "evidence/codex-explicit-resume.json"
    if explicit.exists():
        data = json.loads(explicit.read_text())
        seed_data = load_seed(args, "codex", keys)
        summary["codex_explicit_diagnostic"] = {
            "nonce_observed": seed_data["nonce"] in data["stderr"],
            "namespace_denial_observed": "No permissions to create a new namespace" in data["stderr"],
            "not_a_frozen_handoff_pass": True}
    private_json(args.run_dir / "summary.json", summary)
    progress("summary", **summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "prepare", "test", "diagnose", "explicit-resume", "summarize", "cleanup"))
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", help="authorize billable operations")
    parser.add_argument("--recipe", choices=("claude", "codex"), default="claude")
    parser.add_argument("--model")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--linux-binary", type=Path)
    parser.add_argument("--candidate", help="expected version identifier for both frozen binaries")
    parser.add_argument("--recipes", default="claude,codex", help="test recipes, comma-separated")
    parser.add_argument("--seed-run-dir", type=Path, help="reuse prior synthetic seeds, without model calls")
    parser.add_argument("--keep-on-failure", action="store_true", help="retain failed VM until its fixed TTL for debugging; explicit cleanup required")
    parser.add_argument("--nested-sandbox-test-profile", action="store_true", help="explicit test-only Docker seccomp/AppArmor relaxation for labeled handoff containers; production unchanged")
    args = parser.parse_args()
    if any(r not in PINS for r in args.recipes.split(",")):
        parser.error("--recipes must contain only claude and/or codex")
    if not args.execute:
        parser.error("--execute is required; no provider or model calls were made")
    args.run_dir = args.run_dir.resolve()
    base = (ROOT / "remount-data").resolve()
    if not args.run_dir.is_relative_to(base) or args.run_dir == base:
        parser.error("run directory must be a child of ignored remount-data")
    os.umask(0o077)
    args.run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    keys = load_keys()
    try:
        globals()[args.mode.replace("-", "_")](args, keys)
    except Exception as exc:
        message = redact(str(exc), keys)
        private_json(args.run_dir / "evidence" / (args.mode + "-failure.json"),
                     {"type": type(exc).__name__, "error": message})
        print(type(exc).__name__ + ": " + message, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
