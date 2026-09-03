#!/usr/bin/env python3
"""Measure real two-node move latency by size and verify bytes after each move."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import statistics
import subprocess
import tempfile
import time
import urllib.parse


def size(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(KiB|MiB|GiB)?", value)
    if not match:
        raise argparse.ArgumentTypeError("size must be a positive integer with optional KiB/MiB/GiB")
    result = int(match.group(1)) * {None: 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}[match.group(2)]
    if result > 10 << 30:
        raise argparse.ArgumentTypeError("one fixture may not exceed 10 GiB")
    return result


def server_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https", "ws", "wss"} or not parsed.netloc:
        raise argparse.ArgumentTypeError("server must be an absolute HTTP or WebSocket URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise argparse.ArgumentTypeError("server URL must not contain credentials, a query or a fragment")
    return value


def command(binary: str, args: list[str], env: dict[str, str], timeout: int, stdout=None) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(
            [binary, *args], env=env, stdout=stdout or subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=timeout
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"remount {' '.join(args[:3])} exceeded {timeout}s") from error
    if result.returncode:
        raise RuntimeError(f"remount {' '.join(args[:3])} failed: {result.stderr.decode(errors='replace')}")
    return result


def fixture(path: pathlib.Path, length: int) -> str:
    digest = hashlib.sha256()
    with path.open("wb") as output:
        counter = 0
        remaining = length
        while remaining:
            block = hashlib.shake_256(f"remount-bench-{counter}".encode()).digest(min(65536, remaining))
            output.write(block)
            digest.update(block)
            remaining -= len(block)
            counter += 1
    return digest.hexdigest()


def quantile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * percentile + 0.999999))]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./remount")
    parser.add_argument("--server", required=True, type=server_url)
    parser.add_argument("--node-a", required=True)
    parser.add_argument("--node-b", required=True)
    parser.add_argument("--backend", default="process", choices=("process", "docker", "gvisor"))
    parser.add_argument("--size", action="append", type=size, dest="sizes", default=[])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600, help="per-operation timeout in seconds")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 20:
        parser.error("repetitions must be 1..20")
    if len(args.sizes) > 20:
        parser.error("at most 20 size lanes may be requested")
    if not 10 <= args.timeout <= 3600:
        parser.error("timeout must be 10..3600 seconds")
    if args.node_a == args.node_b:
        parser.error("node-a and node-b must differ")
    if not os.environ.get("REMOUNT_TOKEN"):
        parser.error("REMOUNT_TOKEN must be present in the environment; it is never put on argv or in output")
    env = os.environ.copy()
    env["REMOUNT_SERVER"] = args.server
    results = []
    for bytes_count in args.sizes or [1 << 20, 100 << 20, 500 << 20]:
        workspace = ""
        with tempfile.TemporaryDirectory(prefix="remount-move-bench-") as directory:
            root = pathlib.Path(directory)
            expected = fixture(root / "fixture.bin", bytes_count)
            try:
                created = command(
                    args.binary,
                    ["ws", "create", "--dir", str(root), "--node", args.node_a, "--backend", args.backend, "--json"],
                    env,
                    args.timeout,
                )
                workspace = json.loads(created.stdout)["id"]
                samples = []
                target = args.node_b
                for _ in range(args.repetitions):
                    started = time.perf_counter()
                    command(args.binary, ["ws", "move", workspace, "--node", target, "--json"], env, args.timeout)
                    samples.append(time.perf_counter() - started)
                    verified = command(
                        args.binary,
                        [
                            "exec",
                            workspace,
                            "--",
                            "sh",
                            "-c",
                            "if command -v sha256sum >/dev/null; then sha256sum fixture.bin; "
                            "elif command -v shasum >/dev/null; then shasum -a 256 fixture.bin; else exit 127; fi",
                        ],
                        env,
                        args.timeout,
                    )
                    actual = verified.stdout.decode(errors="replace").split()[0]
                    if actual != expected:
                        raise RuntimeError(f"workspace {workspace} changed bytes after move to {target}")
                    target = args.node_a if target == args.node_b else args.node_b
                results.append({"bytes": bytes_count, "samples_seconds": samples, "p50_seconds": statistics.median(samples), "p99_seconds": quantile(samples, 0.99)})
            finally:
                if workspace:
                    command(args.binary, ["ws", "destroy", workspace, "--json"], env, args.timeout)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], text=True, stdout=subprocess.PIPE, check=True).stdout.strip()
    dirty = bool(subprocess.run(["git", "status", "--porcelain"], text=True, stdout=subprocess.PIPE, check=True).stdout.strip())
    evidence = {
        "schema": 1,
        "commit": commit,
        "dirty": dirty,
        "measured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": f"{platform.system()} {platform.machine()} / Python {platform.python_version()}",
        "backend": args.backend,
        "node_a": args.node_a,
        "node_b": args.node_b,
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
