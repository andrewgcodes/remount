# 11. Claiming is not serving

**Status:** accepted, discovered through testing

## Context

The first version had two workspace states that mattered: pending and claimed.
A node won a claim and the control plane immediately marked the workspace
claimed. A client waiting for the workspace saw claimed and sent a request.

The request failed with "workspace is not on this node", because the node was
still restoring a tarball into a directory that did not exist yet.

The same bug had a second face. When a node process died and restarted, the
workspace was still marked claimed by that node id, so a client's wait returned
instantly and its next request hit a node that had not yet re-adopted anything.

## Decision

Split the held state in two. `claiming` means a node owns the workspace but is
not serving it. `claimed` means it is serving. The node promotes it with an
explicit `ws.ready`, and only `claimed` gets grants.

A node going offline demotes its `claimed` workspaces back to `claiming`. They
are still leased to it, and it still gets them back when it returns, but no
client is told they are ready.

On reconnect a node re-announces everything it is already serving, because the
control plane may have demoted it while the connection was down.

## Consequences

"Ready" is now a claim the system makes only when it is true, which is the only
kind worth making.

Waiting for a workspace is correct without polling for a side effect. Before
this, the reliable way to know a workspace was usable was to try to use it.

This ADR exists because the design was wrong and a test found it. The failure
mode was a race that appeared under the race detector and on a slow machine, and
would have appeared in production as an intermittent "not on this node" that
looked like a network problem.
