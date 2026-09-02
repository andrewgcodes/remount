# 9. Placement is a claim queue, not a scheduler

**Status:** accepted

## Context

Workspaces need to land on nodes. The Kubernetes-shaped answer is a scheduler
that knows the fleet, ranks nodes and assigns work. That requires the control
plane to model capacity accurately and to be right about it.

## Decision

A pending workspace is **offered** to every eligible online node. Nodes race to
claim it. `ws.claim` is a single compare-and-swap under one lock: the state was
pending, now it is claiming, and the generation is incremented.

The claim carries a lease. The holder renews it at no more than one third of the
lease interval. An expired lease returns the workspace to pending with
`restore_from` set to its last snapshot.

## Consequences

Split brain is impossible by construction rather than by care. Two nodes cannot
both hold a workspace because the second claim fails the CAS, and a node acts
only on its own generation. A stale grant naming an old generation is refused.

There is one recovery path. A node dying and a node being asked to give a
workspace up are the same transition, so the code that handles crashes is the
code that runs every day, which is the only way it stays correct.

The control plane does not need to model capacity. A node that is too busy
simply does not claim. Eligibility is a filter, not a ranking.

Two things we got wrong first and fixed:

**Claiming is not serving.** A node that wins a claim still has to restore the
filesystem. Marking it `claimed` immediately let a client read from a node that
was still unpacking a tarball. Hence `claiming`, promoted to `claimed` only when
the node reports ready. A node that goes offline has its workspaces demoted back
to `claiming`, because they are held but not answering.

**A claim in flight is still a claim.** Nothing renewed the lease while a node
was restoring, so a slow restore lost the workspace it was working on. The node
now renews for workspaces it is materializing, not just ones it is serving.
