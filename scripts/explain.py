#!/usr/bin/env python3
"""explain.py — turn a Remount JSON snapshot or event stream into prose.

Two audiences, one tool. A human skimming a terminal, and an agent that has to
decide what to do next without holding a thousand lines of JSON in context.

    scripts/collect.sh | scripts/explain.py
    remount events --json | scripts/explain.py --events
    scripts/explain.py snapshot.json --format brief

It reads stdin or a file and prints a summary that leads with problems.
No dependencies beyond the standard library, deliberately: this runs on the
machine that is having the problem.
"""

import argparse
import json
import sys
from collections import Counter, defaultdict

SEVERITY_RANK = {"error": 0, "warn": 1, "info": 2}


def load(path):
    """Read one JSON document, or a stream of JSON objects, from a file or stdin."""
    raw = sys.stdin.read() if path in (None, "-") else open(path).read()
    raw = raw.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # A stream of concatenated objects, which is what --json produces per record.
    out, decoder, idx = [], json.JSONDecoder(), 0
    while idx < len(raw):
        while idx < len(raw) and raw[idx].isspace():
            idx += 1
        if idx >= len(raw):
            break
        obj, end = decoder.raw_decode(raw, idx)
        out.append(obj)
        idx = end
    return out


def human_bytes(n):
    n = float(n or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


def describe_findings(findings, prefix=""):
    """Findings first, worst first. This is the part that changes what you do."""
    if not findings:
        return []
    lines = []
    ordered = sorted(findings, key=lambda f: SEVERITY_RANK.get(f.get("severity"), 3))
    for f in ordered:
        sev = f.get("severity", "info").upper()
        subject = f.get("subject") or ""
        subject = f" {subject}" if subject else ""
        lines.append(f"{prefix}[{sev}] {f.get('check','?')}{subject}: {f.get('detail','')}")
        if f.get("hint"):
            lines.append(f"{prefix}        {f['hint']}")
    return lines


def as_dict(v):
    """Return v if it is a usable object, else an empty one.

    A collection can be partial: a command that failed is stored as an error
    object where a list was expected. A diagnostic tool that crashes on that
    is useless exactly when it is needed.
    """
    return v if isinstance(v, dict) else {}


def as_list(v):
    return v if isinstance(v, list) else []


def collection_errors(doc):
    """Sections that failed to collect. Naming them beats guessing from gaps."""
    out = []
    for key, val in doc.items():
        if isinstance(val, dict) and "error" in val and set(val) <= {"error", "exit"}:
            out.append((key, val.get("error")))
    return out


def explain_snapshot(doc, brief=False):
    lines = []
    broken = collection_errors(doc)
    if broken:
        lines.append(
            f"INCOMPLETE: {len(broken)} of {len(doc) - 2} sections could not be collected."
        )
        for key, err in broken:
            lines.append(f"  {key}: {err}")
        lines.append("")
    status = as_dict(doc.get("status"))
    control = as_dict(status.get("control"))
    nodes = as_list(status.get("nodes")) or as_list(doc.get("nodes"))
    workspaces = as_list(status.get("workspaces")) or as_list(doc.get("workspaces"))
    doctor = as_dict(doc.get("doctor"))
    if broken and not nodes and not workspaces:
        lines.append("Nothing further can be said; the deployment did not answer.")
        return lines

    # 1. The verdict, first, because it is what a reader needs to decide.
    # The same finding can appear in more than one section, because doctor
    # includes what status already reported. Say each thing once.
    problems, seen = [], set()
    for f in as_list(doctor.get("findings")) + as_list(status.get("findings")):
        key = (f.get("check"), f.get("subject"), f.get("detail"))
        if key in seen:
            continue
        seen.add(key)
        problems.append(f)
    errors = [f for f in problems if f.get("severity") == "error"]
    warns = [f for f in problems if f.get("severity") == "warn"]
    if doctor.get("ok") is False or errors:
        lines.append(f"VERDICT: problems found. {len(errors)} error, {len(warns)} warning.")
    elif warns:
        lines.append(f"VERDICT: healthy, with {len(warns)} warning.")
    else:
        lines.append("VERDICT: healthy.")

    # 2. Shape of the deployment.
    online = sum(1 for n in nodes if isinstance(n, dict) and n.get("online"))
    lines.append(
        f"Fleet: {online} of {len(nodes)} nodes online, {len(workspaces)} workspaces."
    )
    states = Counter()
    for w in workspaces:
        if isinstance(w, dict):
            states[w.get("state", "?")] += 1
    if states:
        lines.append("States: " + ", ".join(f"{k} {v}" for k, v in sorted(states.items())))

    for n in nodes:
        if not isinstance(n, dict):
            continue
        info = as_dict(n.get("info"))
        labels = as_dict(n.get("labels"))
        label_str = " ".join(f"{k}={v}" for k, v in sorted(labels.items()))
        lines.append(
            f"  node {n.get('id','?')}: "
            f"{info.get('os','?')}/{info.get('arch','?')} "
            f"{info.get('cpu','?')} cpu, holding {len(as_list(n.get('workspaces')))}"
            + (f" [{label_str}]" if label_str else "")
        )

    # 3. The numbers that indicate loss or risk, and only those.
    m = as_dict(control.get("metrics")) or as_dict(doc.get("metrics"))
    risky = {
        "remount_egress_leak_blocked_total": "credential placeholders sent to unbound hosts",
        "remount_session_gaps_total": "replay gaps, meaning output no client could retrieve",
        "remount_artifact_digest_mismatch_total": "artifacts that failed digest verification",
        "remount_workspace_lease_expired_total": "leases that expired, re-queueing a workspace",
        "remount_frames_dropped_total": "frames addressed to a peer that had gone",
        "remount_session_inputs_deduped_total": "duplicate inputs dropped after a reconnect",
    }
    flagged = [(k, v) for k, v in risky.items() if m.get(k)]
    if flagged:
        lines.append("Signals worth reading:")
        for k, label in flagged:
            lines.append(f"  {m[k]:.0f} {label}")

    if not brief:
        throughput = {
            "remount_sessions_opened_total": "sessions opened",
            "remount_session_bytes_total": "bytes streamed",
            "remount_credentials_substituted_total": "credentials substituted at egress",
            "remount_workspaces_moved_total": "workspace moves",
            "remount_snapshots_total": "snapshots taken",
            "remount_restores_total": "restores",
        }
        parts = []
        for k, label in throughput.items():
            v = m.get(k) or 0
            if not v:
                continue
            parts.append(f"{human_bytes(v) if 'bytes' in k else int(v)} {label}")
        if parts:
            lines.append("Throughput: " + ", ".join(parts))

    # 4. Problems, in full, last, because they are the longest part.
    if problems:
        lines.append("")
        lines.append("Findings:")
        lines += describe_findings(problems, prefix="  ")

    # 5. Per-workspace detail only where something is wrong.
    inspect = as_dict(doc.get("inspect"))
    noisy = []
    for wid, det in inspect.items():
        if not isinstance(det, dict):
            continue
        f = as_list(det.get("findings"))
        if any(x.get("severity") in ("error", "warn") for x in f):
            noisy.append((wid, det))
    if noisy:
        lines.append("")
        lines.append("Workspaces needing attention:")
        for wid, det in noisy:
            ws = det.get("workspace") or {}
            on = det.get("on_node") or {}
            lines.append(
                f"  {wid} state={ws.get('state')} gen={ws.get('gen')} "
                f"node={ws.get('node') or '-'} "
                + (f"disk={human_bytes(on.get('bytes'))} files={on.get('files')}" if on else "")
            )
            lines += describe_findings(as_list(det.get("findings")), prefix="    ")
    return lines


def explain_events(events, brief=False):
    """Summarize an event stream: what happened, to what, and what went wrong."""
    if isinstance(events, dict):
        events = events.get("events") or [events]
    lines = []
    kinds = Counter()
    per_ws = defaultdict(list)
    denials, creds, moves = [], [], []
    for e in events:
        t = e.get("type", "?")
        kinds[t] += 1
        stream = e.get("stream") or ""
        if stream:
            per_ws[stream].append(e)
        payload = e.get("payload") or {}
        if t == "egress.denied":
            denials.append((stream, payload))
        elif t == "cred.used":
            creds.append((stream, payload))
        elif t in ("ws.moved", "ws.restored", "ws.lease_expired"):
            moves.append((t, stream, payload))

    lines.append(f"{len(events)} events across {len(per_ws)} streams.")
    lines.append("Most common: " + ", ".join(f"{k} {v}" for k, v in kinds.most_common(6)))

    if denials:
        leaks = [(s, p) for s, p in denials if p.get("decision") == "leak_blocked"]
        lines.append("")
        lines.append(f"Egress denials: {len(denials)}, of which {len(leaks)} were leak attempts.")
        for stream, p in (leaks or denials)[:10]:
            lines.append(
                f"  {stream} -> {p.get('host','?')} {p.get('method','')} {p.get('path','')}"
                f"  ({p.get('reason') or p.get('decision')})"
            )
    if creds and not brief:
        hosts = Counter(p.get("host", "?") for _, p in creds)
        lines.append("")
        lines.append(
            "Credentials substituted: "
            + ", ".join(f"{h} x{c}" for h, c in hosts.most_common(5))
        )
    if moves:
        lines.append("")
        lines.append("Movement:")
        for t, stream, p in moves[:15]:
            extra = p.get("restore_from") or p.get("from") or ""
            if extra:
                extra = f" from {extra[:26]}…"
            lines.append(f"  {t} {stream}{extra}")
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("path", nargs="?", default="-", help="JSON file, or - for stdin")
    ap.add_argument("--events", action="store_true", help="input is an event stream")
    ap.add_argument("--format", choices=["full", "brief"], default="full")
    args = ap.parse_args()

    doc = load(args.path)
    if doc is None:
        print("no input", file=sys.stderr)
        return 2
    brief = args.format == "brief"
    if args.events or isinstance(doc, list):
        lines = explain_events(doc, brief)
    else:
        lines = explain_snapshot(doc, brief)
    print("\n".join(lines))
    # Exit non-zero when the snapshot says something is broken, so this is
    # usable in a health check.
    if isinstance(doc, dict):
        if collection_errors(doc):
            return 2
        if as_dict(doc.get("doctor")).get("ok") is False:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
