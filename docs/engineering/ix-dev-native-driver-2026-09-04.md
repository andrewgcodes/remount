# ix.dev: what is done, and what a native driver still needs

`internal/provision/ix` targets **ix.dev** (Indexable). It previously described
**iximiuz Labs** playgrounds driven by `labctl`, which is a different company
and a different product; that support is removed. The confusion was already
visible in the tree: `handoff-2026-09-03.md` called this "the ix.dev
provisioner" and the credential was `IX_DEV_API_KEY`, while the package doc,
ADR 0062, and the `Playground`/`Account` config fields all described iximiuz.

## What is done

- Every iximiuz and `labctl` reference is gone from code, tests, ADR 0062, and
  `.github/workflows/vendors.yml`.
- `Playground` and `Account` are removed from the driver config and from
  `provisionerEntry`; nothing else used them.
- The credential is the vendor's own `IX_TOKEN`, and region is the vendor's own
  `IX_REGION`. Remount does not invent names for either. An unset region is
  left unset so ix.dev's own default applies rather than Remount pinning one.
- Region is now passed through rather than refused. Size stays `unavailable`:
  it is not part of ix.dev's documented machine surface.
- The operator-facing env var name remains per-deployment configurable through
  `token_env` in the provisioners file; the driver never hardcodes a read.

## Why the helper protocol is still there

`ix` covers the whole lifecycle a pool reconciler needs, and does it in a way
that satisfies Remount's secret rule without any wrapper:

| need | command |
|---|---|
| create | `ix new ix/base:latest --name N --region R --no-shell` |
| list | `ix ls --output json` |
| destroy | `ix rm <id>` |
| enrollment token off argv | `ix secret set <name>` (value from **stdin**) then `ix new --secret-env` |

`ix secret set` documents that the value "is read from a hidden prompt, stdin,
or `--value-file`; it is never taken on the command line" — the same property
`fly secrets import --stage` provides, which `fly.CLISecrets` already relies on.

So a native driver is both possible and preferable: it removes a
`remount-ix-helper` binary that is not shipped anywhere in this repo, which
means the driver as it stands cannot actually provision anything.

**The blocker is the response schema, not the design.** Verified live on
2026-09-04 with a real account token:

```
$ ix ls --output json
[]                                   # authenticates, lists, and returns nothing

$ ix new ix/base:latest --name probe --no-shell --message-format json
{"message":"server rejected RPC stream open for vm.build_commit_from_oci:
  unsupported method: vm.build_commit_from_oci"}
```

The account had no VMs, so `ls` never showed a machine object, and the create
that would have produced one is refused: the installed CLI (`ix 2026-06-28`,
`1a6382c0c7`) calls an RPC the current server no longer supports. Without one
observed machine object there are no field names to map onto
`provision.Machine`, and inventing them is exactly the guessing ADR 0062 exists
to forbid.

## To finish it

1. Install an `ix` CLI new enough that `ix new` is accepted by the server. This
   is a client-version problem, not an account problem — `ix ls` and auth both
   work with the current token.
2. Create one VM and capture `ix ls --output json` verbatim. Record the real
   field names for id, name, state, region, and creation time.
3. Replace the helper calls in `internal/provision/ix` with `providerutil`
   `Command`s, following `fly.CLISecrets` for staging the enrollment token on
   stdin and `fly` for the create/list/destroy shape.
4. Keep the tenant/pool filter in `List`: it is a Remount invariant, not a
   vendor one, and it is what stops one pool's reconciler from seeing another's
   inventory.
5. Unit-test with a fake `providerutil.Runner` asserting the enrollment token
   never appears in argv, as the current test already does.
6. Delete the helper fields once the native path is live, and update ADR 0062,
   whose ix.dev entry records this same reasoning.

## Note for whoever runs the live lane

`handoff-2026-09-03.md` records that a 2026-09-02 session hit provider-side CAS
errors in `us-west-1`, which is ix.dev's default region, and that these VMs get
no public IPv4 — so the node must dial out, and the binary upload path must not
depend on `ix shell` stdin. If the default region misbehaves, set
`REMOUNT_IX_REGION=us-east-1`; the driver passes it through as `IX_REGION`.
