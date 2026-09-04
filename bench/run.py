#!/usr/bin/env python3
"""Run bounded local benchmarks and render evidence without hiding unavailable lanes."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import platform
import re
import statistics
import subprocess
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
BENCHMARK = re.compile(r"^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([0-9.]+) ns/op(.*)$")
METRIC = re.compile(r"\s+([0-9.]+)\s+(\S+/op|dedupe-ratio)")


def run(command: list[str]) -> str:
    completed = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    if completed.returncode:
        raise RuntimeError(f"{' '.join(command)} failed ({completed.returncode})\n{completed.stdout}")
    return completed.stdout


def quantile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * percentile + 0.999999)))
    return ordered[index]


def go_benchmarks(count: int, iterations: int) -> dict[str, dict[str, Any]]:
    output = run(["go", "test", "-run", "^$", "-bench", "Benchmark(Chunked|Direct|Broker)", f"-benchtime={iterations}x", f"-count={count}", "./bench"])
    samples: dict[str, list[dict[str, float]]] = {}
    for line in output.splitlines():
        match = BENCHMARK.match(line)
        if not match:
            continue
        metrics = {"ns_per_op": float(match.group(2))}
        for value, unit in METRIC.findall(match.group(3)):
            metrics[unit.replace("/", "_per_").replace("-", "_")] = float(value)
        samples.setdefault(match.group(1), []).append(metrics)
    if not samples:
        raise RuntimeError(f"no Go benchmark rows found\n{output}")
    return {name: {key: statistics.median(sample[key] for sample in rows) for key in rows[0]} | {"samples": len(rows)} for name, rows in samples.items()}


def timed_test(name: str, count: int) -> dict[str, float | int]:
    output = run(["go", "test", "-json", f"-count={count}", "-run", f"^{name}$", "./internal/sim"])
    elapsed: list[float] = []
    for line in output.splitlines():
        record = json.loads(line)
        if record.get("Action") == "pass" and record.get("Test") == name:
            elapsed.append(float(record["Elapsed"]))
    if len(elapsed) != count:
        raise RuntimeError(f"{name}: got {len(elapsed)} passing samples, want {count}")
    return {"samples": count, "p50_seconds": statistics.median(elapsed), "p99_seconds": quantile(elapsed, 0.99)}


def git_identity() -> tuple[str, bool]:
    commit = run(["git", "rev-parse", "HEAD"]).strip()
    dirty = bool(run(["git", "status", "--porcelain"]).strip())
    return commit, dirty


def move_evidence(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    result = json.loads(path.read_text(encoding="utf-8"))
    if result.get("schema") != 1 or result.get("backend") not in {"process", "docker", "gvisor"}:
        raise RuntimeError(f"{path}: unsupported move evidence")
    rows = result.get("results")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"{path}: move evidence has no result rows")
    for row in rows:
        samples = row.get("samples_seconds")
        if not isinstance(samples, list) or not samples or any(value <= 0 for value in samples):
            raise RuntimeError(f"{path}: invalid move samples")
        if row.get("bytes", 0) <= 0:
            raise RuntimeError(f"{path}: invalid move size")
    return result


def scale_evidence(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    result = json.loads(path.read_text(encoding="utf-8"))
    if result.get("schema") != 1 or result.get("backend") != "process":
        raise RuntimeError(f"{path}: unsupported scale evidence")
    if result.get("nodes") != 200 or result.get("workspaces") != 2_000:
        raise RuntimeError(f"{path}: scale evidence does not cover 200 nodes and 2,000 workspaces")
    for name in ("claim", "exec_round_trip", "move", "reattach_after_control_restart"):
        measurement = result.get("latency_seconds", {}).get(name, {})
        if measurement.get("samples", 0) <= 0 or measurement.get("p50", 0) <= 0 or measurement.get("p99", 0) <= 0:
            raise RuntimeError(f"{path}: invalid {name} latency evidence")
    if result.get("reconnect_peers") != 400 or result.get("control_restart_seconds", 0) <= 0:
        raise RuntimeError(f"{path}: incomplete failover evidence")
    return result


def render(result: dict[str, Any]) -> str:
    benches = result["microbenchmarks"]
    full = benches["BenchmarkChunkedSnapshotFull8MiB"]
    delta = benches["BenchmarkChunkedSnapshotDelta4KiBOf8MiB"]
    direct = benches["BenchmarkDirectTLSRoundTrip"]
    broker = benches["BenchmarkBrokerAllowedRoundTrip"]
    overhead_us = (broker["ns_per_op"] - direct["ns_per_op"]) / 1000
    reattach = result["scenarios"]["reconnect_mid_stream"]
    recovery = result["scenarios"]["move_then_node_loss_recovery"]
    moves = result.get("move_matrix")
    scale = result.get("scale")
    dirty = " + uncommitted changes" if result["dirty"] else ""
    move_section = """
## Two-node move matrix

Unavailable. Run `bench/move_matrix.py` against two enrolled nodes; it hashes
the file after every move and records every sample.
"""
    move_unavailable = """- **Move latency by workspace size/backend:** not earned on this host. Run
  `bench/move_matrix.py` against two enrolled nodes; it hashes the file after
  every move and records every sample. No two-node deployment was created for
  this documentation run.
"""
    if moves:
        rows = "\n".join(
            f"| {row['bytes']/1048576:g} MiB | {row['p50_seconds']:.3f} s | {row['p99_seconds']:.3f} s | {len(row['samples_seconds'])} |"
            for row in moves["results"]
        )
        move_dirty = " + uncommitted changes" if moves.get("dirty") else ""
        move_section = f"""
## Two-node move matrix

The `move_matrix.py` runner uploaded deterministic incompressible fixtures,
moved each workspace back and forth between two independently enrolled nodes,
and ran SHA-256 inside the workspace after every move. A hash mismatch fails
the run. These are local `process` backend numbers, not network or isolation
backend claims.

- Commit: `{moves['commit']}`{move_dirty}
- Measured: {moves['measured_at']}
- Host/backend: {moves['host']} / `{moves['backend']}`

| Workspace fixture | p50 | p99 (nearest-rank) | Samples |
|---:|---:|---:|---:|
{rows}
"""
        move_unavailable = ""
    scale_section = """
## Fleet-scale control failover

Unavailable. Run `TestHandoffScaleAndControlFailover`; the result is valid only
when all 200 real protocol nodes, 2,000 workspaces, and 400 reconnecting peers
complete without a replay gap or generation mismatch.
"""
    scale_unavailable = """- **200 nodes / 2,000 workspaces:** unavailable. Run the checked-in
  `TestHandoffScaleAndControlFailover` scenario; a smaller synthetic test must
  not be relabelled as fleet-scale evidence.
"""
    if scale:
        scale_rows = "\n".join(
            f"| {name.replace('_', ' ')} | {measurement['samples']} | {measurement['p50']:.3f} s | {measurement['p99']:.3f} s |"
            for name, measurement in scale["latency_seconds"].items()
        )
        scale_dirty = " + uncommitted changes" if scale.get("dirty") else ""
        scale_section = f"""
## Fleet-scale control failover

The checked-in scenario used real in-process WebSocket peers and the process
backend. It created 200 nodes and clients, claimed 2,000 workspaces, exercised
one exec and move per node, restarted the control plane over the shared SQLite
store, reconnected 400 peers, and required byte-exact session completion with
no gaps or generation mismatch. These measurements are not Docker or gVisor
performance claims.

- Commit: `{scale['commit']}`{scale_dirty}
- Measured: {scale['measured_at']}
- Host/backend: {scale['host']} / `{scale['backend']}`
- Control restart: {scale['control_restart_seconds']:.3f} s; reconnected peers: {scale['reconnect_peers']}

| Operation | Samples | p50 | p99 |
|---|---:|---:|---:|
{scale_rows}
"""
        scale_unavailable = ""
    return f"""# Benchmarks

<!-- Generated by bench/run.py. Do not edit by hand: bench/test_tools.py
     asserts this file is exactly what the runner renders from
     bench/results/latest.json, so a hand-edit fails CI. Prose about what the
     numbers mean belongs in docs/engineering/, which is written by people. -->

These are point-in-time engineering measurements, not an SLA. The runner fails
if a correctness test fails and labels every lane it could not exercise.

## Candidate and host

- Commit: `{result['commit']}`{dirty}
- Measured: {result['measured_at']}
- Host: {result['host']}
- Command: `python3 bench/run.py --count {result['count']} --iterations {result['iterations']} --output bench/results/latest.json --markdown docs/benchmarks.md`
- Samples: {full['samples']} microbenchmark processes; {reattach['samples']} reconnect and {recovery['samples']} recovery runs

## Local results

| Measurement | p50 / median | p99 | Evidence |
|---|---:|---:|---|
| Full 8 MiB chunked snapshot | {full['ns_per_op']/1e6:.2f} ms | not sampled | {full['upload_B_per_op']/1048576:.2f} MiB uploaded |
| 4 KiB edit of an 8 MiB snapshot | {delta['ns_per_op']/1e6:.2f} ms | not sampled | {delta['upload_B_per_op']/1024:.1f} KiB uploaded; {delta['dedupe_ratio']:.1f}× logical/upload ratio |
| Direct loopback TLS request | {direct['ns_per_op']/1000:.1f} µs | not sampled | Go benchmark, warm connection |
| Brokered allowed request | {broker['ns_per_op']/1000:.1f} µs | not sampled | {overhead_us:.1f} µs median incremental overhead |
| Client cut mid-stream and lossless reattach | {reattach['p50_seconds']:.3f} s | {reattach['p99_seconds']:.3f} s | `TestReconnectMidStreamIsLossless`; bytes are asserted identical |
| Graceful move, then node-loss recovery | {recovery['p50_seconds']:.3f} s | {recovery['p99_seconds']:.3f} s | `TestNodeDeathMovesWorkspaceFromSnapshot`; generation and bytes asserted |

The chunk fixture is deterministic pseudo-random data. Baseline creation for
the delta case is outside the timed interval. The HTTP comparison uses the
same local TLS origin and warm connections; it measures policy/broker overhead,
not internet latency.
{move_section}
{scale_section}

## Unavailable evidence

{move_unavailable}- **Other move backends:** Docker and gVisor were not available in the local
  two-node run. The process results must not be relabelled as isolation-backend
  performance.
{scale_unavailable}
- **Six-hour / 2 GiB session:** the tiered-log correctness suite is separate;
  the short reconnect timing above must not be relabelled as E11.

`bench/results/latest.json` is the machine-readable evidence. A release should
regenerate both files from a clean tagged commit and attach the raw JSON. A
failed or skipped lane remains unavailable, never zero or healthy.
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "bench/results/latest.json")
    parser.add_argument("--markdown", type=pathlib.Path, default=ROOT / "docs/benchmarks.md")
    parser.add_argument("--move-evidence", type=pathlib.Path, default=ROOT / "bench/results/move-process-local.json")
    parser.add_argument("--scale-evidence", type=pathlib.Path, default=ROOT / "bench/results/scale-process-local.json")
    args = parser.parse_args()
    if not 1 <= args.count <= 30 or not 1 <= args.iterations <= 100:
        parser.error("count must be 1..30 and iterations 1..100")
    commit, dirty = git_identity()
    result = {
        "schema": 1,
        "commit": commit,
        "dirty": dirty,
        "measured_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": f"{platform.system()} {platform.machine()} / {platform.processor() or 'unknown CPU'} / Python {platform.python_version()}",
        "count": args.count,
        "iterations": args.iterations,
        "microbenchmarks": go_benchmarks(args.count, args.iterations),
        "move_matrix": move_evidence(args.move_evidence),
        "scale": scale_evidence(args.scale_evidence),
        "scenarios": {
            "reconnect_mid_stream": timed_test("TestReconnectMidStreamIsLossless", args.count),
            "move_then_node_loss_recovery": timed_test("TestNodeDeathMovesWorkspaceFromSnapshot", args.count),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown.write_text(render(result), encoding="utf-8")
    print(args.output)
    print(args.markdown)


if __name__ == "__main__":
    main()
