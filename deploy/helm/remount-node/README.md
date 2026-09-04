# remount-node Helm chart

Schedules Remount nodes on Kubernetes and advertises their capability labels.

## What Kubernetes owns here, and what it does not

Kubernetes owns **process supervision**: which machines run a Remount node, how
those pods are placed, and what happens when one dies.

The Remount control plane owns **workspaces**: which node a workspace is claimed
by, when it moves, when it sleeps, its generation, its lease, and its checkpoint
chain. This chart contains no CustomResourceDefinition, no controller, no
reconcile loop, and no template that names a workspace. That is not an oversight
to be filled in later; it is the property Plan B phase B5 asks for. A workspace
whose placement were also a Kubernetes object would have two owners, and the
disagreement would surface as a live workspace being moved to satisfy a
reconcile.

Two consequences that look like omissions and are not:

- **No liveness probe.** Restarting a node that is slowly restoring a large
  checkpoint turns a slow operation into node loss. Lease expiry in the control
  plane already distinguishes the two.
- **No RBAC.** The service account has no RoleBinding and does not automount its
  token. A node dials the control plane outbound; it needs nothing from the
  Kubernetes API.

## The image must be the node image

`image.digest` is required and the chart refuses to render without it. It must
point at the **node** image, which carries a userland.
`packaging/container/Dockerfile` is `FROM scratch`, which is right for the
control plane and the CLI and impossible for a process-backend node: a session
in such a workspace dies with `"sh": executable file not found in $PATH`.
`deploy/compose/node.Dockerfile` builds the image this chart wants.

`integration/policy` asserts this, with a control that fails when the chart is
pointed at the scratch image.

## Usage

```sh
kubectl create secret generic remount-control-token --from-literal=token=...

helm install remount-nodes deploy/helm/remount-node \
  --namespace remount \
  --set image.digest=sha256:<the digest docker build printed> \
  --set control.endpoint=http://remount-control.remount.svc.cluster.local:7443
```

The bearer token reaches the container through `REMOUNT_TOKEN` from a Secret,
never on argv, so it is not readable in the pod spec or in the host's process
list.

## Gates

```sh
helm lint deploy/helm/remount-node -f deploy/helm/remount-node/golden/values.yaml
helm template remount-nodes deploy/helm/remount-node --namespace remount \
  -f deploy/helm/remount-node/golden/values.yaml \
  | diff -u deploy/helm/remount-node/golden/default.yaml -
```

`golden/default.yaml` is committed so chart drift is visible in review rather
than at install time. `integration/policy` runs both commands.

This chart has been linted and rendered. It has **not** been installed against a
Kubernetes cluster from this repository; there is none on the host that
validated it. See `docs/engineering/handoff-linux-host-2026-09-03.md`.
