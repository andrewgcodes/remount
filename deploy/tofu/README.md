# OpenTofu / Terraform modules for Remount infrastructure

These modules declare **Remount infrastructure**: network inputs, the control
service, the artifact store, and node-pool capacity. They deliberately do not
declare workspaces, sessions, claims, moves, or checkpoints.

That split is the point of Plan B phase B5, not an incidental detail. A
workspace is a live thing with a generation, a lease, a claiming node and a
checkpoint chain; the control plane is its only owner. If Terraform also held
those facts, two systems would each believe they decide where a workspace runs,
and the loser would be whichever one reconciled last. So the boundary is:

| Fact | Owner |
|---|---|
| VPC, subnets, ingress rules | Terraform |
| the control service and its data volume | Terraform |
| the artifact bucket and its retention | Terraform |
| how many nodes exist and what they can do | Terraform |
| which node a workspace runs on, and when it moves | the Remount control plane |
| sessions, claims, leases, generations, checkpoints | the Remount control plane |

The modules enforce that boundary in HCL rather than in prose. `node_assignments`
exists as a variable purely so that passing one fails `tofu validate` with an
explanation, because the operator who reaches for it is not being unreasonable —
they are just wrong about who owns the fact.

## No provider, on purpose

Every module pins `required_version` and declares no providers, so
`tofu init -backend=false` and `tofu validate` run offline, from a clean
checkout, **without cloud credentials and without registry access**. Validation
that needs an AWS account is validation that cannot run in CI.

`terraform_data` marks the seam where a provider resource goes. It is a builtin
resource, so substituting `aws_instance`, `google_compute_instance_group` or a
Kubernetes manifest for it is a local edit inside one module; nothing outside
that module changes, because the module's contract is its variables and its
outputs, not its resource type.

These modules are therefore validated, not operated. Nothing here has been
applied against a cloud account, and this repository makes no claim that it has.

## Layout

```
modules/network/         network inputs, normalized and range-checked
modules/control/         the single-writer control service
modules/artifact-store/  the S3-compatible artifact bucket
modules/node-pool/       node capacity and capability labels
examples/reference/      a root module wiring all four; this is what CI validates
```

## Gates

```sh
tofu fmt -check -recursive deploy/tofu
cd deploy/tofu/examples/reference && tofu init -backend=false && tofu validate
```

`integration/policy` runs both, and additionally runs `tofu validate` against
the rejection fixtures in `integration/policy/testdata`, each of which must
fail. A validation nobody has watched reject something is not yet a validation.

## Secrets

No module accepts a secret value. `admin_token_env` and `credentials_env` take
the **name** of an environment variable and their validation rejects anything
that is not a name, so a literal token cannot be passed through this interface
even by accident. Labels and tags are provider metadata: they are copied into
cloud consoles, exported to billing, and read by anyone with describe rights, so
their validation rejects credential-shaped values outright.
