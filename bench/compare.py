#!/usr/bin/env python3
"""Compare two scale-evidence files and report which operations moved.

`docs/engineering/performance-regressions-2026-09.md` records five regressions
that shipped in one commit and were visible for weeks in output nobody diffed.
`TestHandoffScaleAndControlFailover` already computed and logged every one of
them, and `bench/results/scale-process-local.json` already stored them. What was
missing was the step that compares one run against another and says a number got
worse.

This is that step. It is deliberately a comparison tool and not a threshold
baked into a test: an absolute wall-clock bound on a shared developer laptop is
a flaky test, while a ratio between two named files is a fact about those two
files, and the caller decides what to do about it.

Usage:

    python3 bench/compare.py --baseline bench/results/scale-process-local.json \\
                             --candidate /tmp/new-run.json

Exit status is 1 when an operation regressed past --threshold, 0 otherwise, so
a CI host with stable timing can gate on it while a laptop can just read the
table.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

# Operations are compared at both percentiles because the two answer different
# questions. The 2026-09 claim regression did not move p50 at all — it was
# entirely in the tail, which is the signature of a serialized resource rather
# than added work on every request. A comparison that looked only at p50 would
# have reported that regression as clean.
PERCENTILES = ("p50", "p99")


class Incomparable(Exception):
    """The two files do not describe runs that mean anything side by side."""


def load(path: pathlib.Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise Incomparable(f"{path} does not exist")
    except json.JSONDecodeError as exc:
        raise Incomparable(f"{path} is not valid JSON: {exc}")
    if not isinstance(data, dict) or "latency_seconds" not in data:
        raise Incomparable(f"{path} is not a scale-evidence file (no latency_seconds)")
    return data


def check_comparable(baseline: dict[str, Any], candidate: dict[str, Any], force: bool) -> list[str]:
    """Return the reasons these two runs should not be compared.

    A ratio between a laptop under load and a quiet CI box is a number with no
    meaning, and reporting it as a regression would train people to ignore this
    tool. So a mismatch is refused by default and overridable on purpose, with
    the mismatch printed either way.
    """
    reasons = []
    for field in ("host", "backend", "nodes", "workspaces", "clients"):
        old, new = baseline.get(field), candidate.get(field)
        if old != new:
            reasons.append(f"{field}: baseline {old!r}, candidate {new!r}")
    if reasons and not force:
        return reasons
    return []


def compare(baseline: dict[str, Any], candidate: dict[str, Any], threshold: float) -> tuple[list[dict[str, Any]], bool]:
    """Return one row per operation and percentile, and whether anything regressed."""
    base_lat = baseline.get("latency_seconds", {})
    cand_lat = candidate.get("latency_seconds", {})
    rows: list[dict[str, Any]] = []
    regressed = False
    for operation in sorted(set(base_lat) | set(cand_lat)):
        for percentile in PERCENTILES:
            old = base_lat.get(operation, {}).get(percentile)
            new = cand_lat.get(operation, {}).get(percentile)
            if old is None or new is None:
                # An operation present in one run and absent from the other is
                # not a pass. A renamed or dropped measurement is exactly how a
                # regression becomes invisible, so it is reported as its own
                # status rather than skipped.
                rows.append({
                    "operation": operation, "percentile": percentile,
                    "baseline": old, "candidate": new,
                    "ratio": None, "status": "unavailable",
                })
                continue
            if old <= 0:
                rows.append({
                    "operation": operation, "percentile": percentile,
                    "baseline": old, "candidate": new,
                    "ratio": None, "status": "unavailable",
                })
                continue
            ratio = new / old
            status = "regressed" if ratio > threshold else ("improved" if ratio < 1 / threshold else "unchanged")
            if status == "regressed":
                regressed = True
            rows.append({
                "operation": operation, "percentile": percentile,
                "baseline": old, "candidate": new,
                "ratio": ratio, "status": status,
            })
    return rows, regressed


def render(rows: list[dict[str, Any]], threshold: float) -> str:
    width = max((len(r["operation"]) for r in rows), default=9)
    out = [f"{'operation'.ljust(width)}  pct   baseline    candidate   ratio   status"]
    for r in rows:
        base = "—" if r["baseline"] is None else f"{r['baseline']:.4f}s"
        cand = "—" if r["candidate"] is None else f"{r['candidate']:.4f}s"
        ratio = "—" if r["ratio"] is None else f"{r['ratio']:.2f}x"
        out.append(f"{r['operation'].ljust(width)}  {r['percentile']:4s}  {base:>10s}  {cand:>10s}  {ratio:>6s}  {r['status']}")
    out.append("")
    out.append(f"threshold: an operation is 'regressed' above {threshold:.2f}x and 'improved' below {1/threshold:.2f}x.")
    unavailable = sum(1 for r in rows if r["status"] == "unavailable")
    if unavailable:
        out.append(f"{unavailable} row(s) unavailable: an operation missing from either run is not a pass.")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--threshold", type=float, default=3.0,
                        help="ratio above which an operation counts as regressed (default 3.0)")
    parser.add_argument("--force", action="store_true",
                        help="compare even when the runs describe different hosts or shapes")
    parser.add_argument("--json", action="store_true", help="emit rows as JSON instead of a table")
    args = parser.parse_args()
    if args.threshold <= 1:
        parser.error("threshold must be greater than 1")

    try:
        baseline, candidate = load(args.baseline), load(args.candidate)
    except Incomparable as exc:
        print(f"cannot compare: {exc}", file=sys.stderr)
        return 2

    mismatch = check_comparable(baseline, candidate, args.force)
    if mismatch:
        print("cannot compare: the runs do not describe the same shape:", file=sys.stderr)
        for reason in mismatch:
            print(f"  {reason}", file=sys.stderr)
        print("  pass --force to compare anyway, knowing the ratio is not meaningful.", file=sys.stderr)
        return 2

    rows, regressed = compare(baseline, candidate, args.threshold)
    print(json.dumps(rows, indent=2) if args.json else render(rows, args.threshold))
    return 1 if regressed else 0


if __name__ == "__main__":
    raise SystemExit(main())
