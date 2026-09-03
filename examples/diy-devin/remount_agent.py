#!/usr/bin/env python3
"""A DIY Devin in one file, against the Remount Agent HTTP API.

    export REMOUNT_SERVER=http://127.0.0.1:7443 REMOUNT_TOKEN=...
    ./remount_agent.py run --recipe codex --dir . -- "add a README"
    ./remount_agent.py watch ag_...
    ./remount_agent.py say ag_... "also add tests"
    ./remount_agent.py approve ap_... allow
    ./remount_agent.py diff ag_...

Standard library only. Every call is one HTTP request documented in
docs/api.md; the transcript is followed over Server-Sent Events and resumes
from the last index after a disconnect, so a laptop lid closing loses nothing.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

SERVER = os.environ.get("REMOUNT_SERVER", "http://127.0.0.1:7443").rstrip("/")
TOKEN = os.environ.get("REMOUNT_TOKEN", "")


class APIError(Exception):
    def __init__(self, status, code, message):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code = status, code


def call(method, path, body=None, idem=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SERVER + path, data=data, method=method)
    req.add_header("Accept", "application/json")
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if idem:
        req.add_header("Idempotency-Key", idem)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read())["error"]
        except Exception:
            raise APIError(e.code, "internal", e.reason) from None
        raise APIError(e.code, err.get("code", "internal"), err.get("message", "")) from None


def create(recipe, task, name=None, ws=None, repo=None, bindings=(), sleep_after=0, max_turns=0, approve="on-request", parent=None, at=None):
    """POST /v1/agents. --dir uploads are a CLI feature (they need the artifact
    store); this client seeds from a repo URL, a base, or adopts a workspace."""
    spec = {"recipe": recipe, "task": task}
    req = {"spec": spec, "policy": {"approve": approve, "sleep_after_sec": sleep_after, "max_turns": max_turns}}
    if name:
        req["name"] = name
    if parent:
        req["parent"] = parent
    if at:
        req["policy"]["start_at"] = int(at * 1000)
    if ws:
        req["ws"] = ws
    else:
        wspec = {"name": name or "diy", "bindings": list(bindings)}
        if repo:
            url, _, ref = repo.partition("@")
            wspec["repo"] = {"url": url, **({"ref": ref} if ref else {})}
        req["workspace"] = wspec
    return call("POST", "/v1/agents", req, idem=str(uuid.uuid4()))


def render(rec, show_thoughts=False):
    """Print the human-visible part of one transcript record."""
    stream = rec.get("stream")
    if stream == "acp_out":
        frame = rec["frame"]["frame"]
        if frame.get("method") == "session/update":
            upd = frame["params"]["update"]
            kind = upd.get("sessionUpdate")
            if kind == "agent_message_chunk" or (kind == "agent_thought_chunk" and show_thoughts):
                c = upd.get("content", {})
                if c.get("type") == "text":
                    sys.stdout.write(c["text"])
                    sys.stdout.flush()
            elif kind == "tool_call":
                print(f"\n[tool: {upd.get('title', '')}]")
        elif frame.get("method") == "session/request_permission":
            print(f"\n[permission requested: {frame['params'].get('toolCall', {}).get('title', '')} — approve with `approve`]")
    elif stream == "stderr":
        sys.stderr.write(rec.get("text", ""))
    elif stream == "exit":
        print("\n[harness exited]")
    elif stream == "gap":
        print(f"\n[records {rec['gap']['from']}..{rec['gap']['to'] - 1} were evicted]")


def watch(agent, start=0, show_thoughts=False):
    """Follow the transcript with SSE, resuming from the last index across
    reconnects. Ends when the server sends `done`."""
    cursor = start
    while True:
        req = urllib.request.Request(f"{SERVER}/v1/agents/{agent}/transcript?from={cursor}")
        req.add_header("Accept", "text/event-stream")
        if TOKEN:
            req.add_header("Authorization", "Bearer " + TOKEN)
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                event, data = None, []
                for line in resp:
                    line = line.decode().rstrip("\n")
                    if line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        event = line[6:].strip()
                    elif line.startswith("data:"):
                        data.append(line[5:].strip())
                    elif line.startswith("id:"):
                        cursor = int(line[3:].strip())
                    elif line == "":
                        if event == "record" and data:
                            render(json.loads("".join(data)), show_thoughts)
                        elif event == "gap" and data:
                            render({"stream": "gap", "gap": json.loads("".join(data))}, show_thoughts)
                        elif event == "done":
                            a = call("GET", f"/v1/agents/{agent}")
                            print(f"\n[{a['status']}: {a.get('status_reason', '')}]")
                            return a
                        event, data = None, []
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            print(f"\n[reconnecting after {e}]", file=sys.stderr)
            time.sleep(1)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--recipe", default="codex")
    r.add_argument("--name")
    r.add_argument("--ws", help="run in an existing workspace")
    r.add_argument("--repo", help="clone URL[@REF] into a fresh workspace")
    r.add_argument("--binding", action="append", default=[])
    r.add_argument("--sleep-after", type=int, default=0, help="seconds idle before the workspace sleeps")
    r.add_argument("--max-turns", type=int, default=0)
    r.add_argument("--approve", default="on-request", choices=["never", "on-request", "auto"])
    r.add_argument("--parent", help="make a child of this agent")
    r.add_argument("--in", dest="delay", type=float, help="start this many seconds from now")
    r.add_argument("--detach", action="store_true")
    r.add_argument("task", nargs="+")
    w = sub.add_parser("watch")
    w.add_argument("agent")
    w.add_argument("--from", dest="start", type=int, default=0)
    w.add_argument("--thoughts", action="store_true")
    s = sub.add_parser("say")
    s.add_argument("agent")
    s.add_argument("text", nargs="+")
    for name in ("get", "cancel", "sleep", "wake", "destroy", "approvals"):
        sub.add_parser(name).add_argument("agent")
    d = sub.add_parser("diff")
    d.add_argument("agent")
    d.add_argument("--wake", action="store_true")
    a = sub.add_parser("approve")
    a.add_argument("approval")
    a.add_argument("option", help="an option id from `approvals`, or `deny`")
    sub.add_parser("ls").add_argument("--parent")
    args = p.parse_args()

    try:
        if args.cmd == "run":
            at = time.time() + args.delay if args.delay else None
            ag = create(args.recipe, " ".join(args.task), args.name, args.ws, args.repo, args.binding,
                        args.sleep_after, args.max_turns, args.approve, args.parent, at)
            print(ag["id"], ag.get("url", ""))
            if not args.detach:
                watch(ag["id"])
        elif args.cmd == "watch":
            watch(args.agent, args.start, args.thoughts)
        elif args.cmd == "say":
            res = call("POST", f"/v1/agents/{args.agent}/messages", {"text": " ".join(args.text)}, idem=str(uuid.uuid4()))
            print(res["agent"]["status"], "(woken)" if res.get("woken") else "")
        elif args.cmd == "ls":
            q = f"?parent={args.parent}" if args.parent else ""
            for ag in call("GET", "/v1/agents" + q)["agents"]:
                print(ag["id"], ag["status"], ag.get("name", ""))
        elif args.cmd == "get":
            print(json.dumps(call("GET", f"/v1/agents/{args.agent}"), indent=2))
        elif args.cmd == "approvals":
            for ap in call("GET", f"/v1/agents/{args.agent}/approvals")["approvals"]:
                print(ap["id"], ap["kind"], ap.get("title", ""), [o["id"] for o in ap.get("options", [])])
        elif args.cmd == "approve":
            body = {"denied": True} if args.option == "deny" else {"option": args.option}
            print(call("POST", f"/v1/approvals/{args.approval}", body, idem=str(uuid.uuid4()))["status"])
        elif args.cmd == "diff":
            q = "?wake=true" if args.wake else ""
            d = call("GET", f"/v1/agents/{args.agent}/diff{q}")
            sys.stdout.write(d.get("diff", ""))
            if d.get("truncated"):
                print("\n[diff truncated at 4 MiB]", file=sys.stderr)
        elif args.cmd == "destroy":
            call("POST", f"/v1/agents/{args.agent}/destroy", idem=str(uuid.uuid4()))
        else:
            print(call("POST", f"/v1/agents/{args.agent}/{args.cmd}", idem=str(uuid.uuid4()))["status"])
    except APIError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(2 if e.status < 500 else 1)


if __name__ == "__main__":
    main()
