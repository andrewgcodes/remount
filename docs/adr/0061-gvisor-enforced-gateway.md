# ADR 0061: gVisor workspaces use one deny-first network namespace

## Status

Accepted, subject to the E4 release gate. Registration is withheld until that
gate passes on the exact Linux/runsc candidate.

## Context

The process backend and the built-in Docker backend can tell a program to use
the credential broker, but neither can stop that program from opening a direct
socket. They therefore remain `cooperative_proxy`. An `isolated` workspace
requires a backend that can make the broker the only possible egress path.
Allowing a workspace to start and tightening its network later would create a
credential-exfiltration interval, while advertising the capability based only
on intended rules would turn an unavailable kernel check into a false pass.

The control plane's normalized `WorkspaceSpec.Security` is authoritative. The
node owns the local workspace only for its assigned generation, and already
calls `ApplyNetworkPolicy` before `ws.ready` and `RevokeNetwork` while fencing
the handle. The backend must preserve those authority and ordering boundaries;
it must not reinterpret connector host rules that belong to the broker.

## Decision

1. The `gvisor` backend runs OCI bundles with `runsc --network=sandbox` and raw
   sockets disabled. Its immutable base rootfs is separate from a host-side
   workspace directory bind-mounted at the requested mount path. Snapshots
   contain that workspace directory and not runsc state.

2. Before runsc starts, the node creates one persistent Linux network namespace
   and veth pair for the workspace. A bounded allocator assigns a link-local
   `/30`; the host receives one usable address and the sandbox the other.
   Namespace and nftables operations use `golang.org/x/sys/unix` netlink, not
   `iptables` or a subprocess in production code.

3. Setup is deny-first. The veth remains down while an inet nftables output
   chain with a drop policy is installed. `ApplyNetworkPolicy` verifies the
   workspace id and non-zero generation, starts runsc with that namespace,
   installs the sole accept rule (IPv4 TCP to the host-veth broker address and
   port), and only then raises the links. IPv6 is disabled as defense in depth.
   IPv4 to any other destination, IPv6, UDP, DNS, ICMP and raw packets have no
   accept path. Host, method, path, connector and quota rules remain enforced
   by the broker after traffic reaches it.

4. Any namespace, veth, nftables, runsc, endpoint-validation or metadata error
   synchronously deletes the veth before `ApplyNetworkPolicy` returns. The node
   consequently cannot send `ws.ready` with a partially configured boundary.
   `RevokeNetwork` synchronously deletes the host veth and releases its address;
   a failed deletion retains the reservation so another workspace cannot
   collide with a link whose removal is uncertain.

5. The protected resource is the network namespace, veth and runsc sandbox.
   The workspace generation plus the node's workspace-handle ownership are the
   stale-work fences. Starting runsc is reversible while its network is down;
   activating the veth is the security-sensitive action, permitted only after
   the kernel acknowledges the accept rule. `ws.ready` is the durable control
   commit that makes the materialization serviceable. Observable success is a
   claimed workspace for which the broker lane connects and every E4 denial
   lane fails; setup failure is an error and absent veth, and an unexercised
   host check is `unavailable`, never healthy.

6. A node restart retains the namespace as a bind-mounted namespace path and
   records only its non-secret locator and link assignment beside the bundle.
   Adoption validates the retained namespace and host veth, stops the old
   runsc container, synchronously revokes the old broker path, then constructs
   a deny-first replacement. Files remain authoritative; processes restart,
   consistent with `Snapshots: "fs"`.

7. A backend instance can be constructed only after `runsc --version`, the
   rootfs and the required Linux capabilities/netlink sockets are verified.
   Its eventual descriptor is:

   ```text
   Isolation=container
   EgressMode=enforced_gateway
   NetworkNamespace=true
   SiblingIsolation=true
   BrokerIdentity=per_session_capability
   Snapshots=fs
   ```

   This descriptor is registered only after the isolation workflow passes E4
   against the real backend: broker TCP succeeds; direct IPv4 TCP, IPv6, UDP,
   DNS to 8.8.8.8, ICMP, raw socket creation and a direct CONNECT attempt to an
   unlisted target fail; broker TCP fails after synchronous revocation. A
   missing runsc binary, privilege, nftables support or host lane is reported
   as unavailable and does not earn the descriptor.

## Consequences

- Ordinary HTTP proxy and provider base-URL clients continue using the
  broker's IP address. No in-sandbox DNS or Unix socket is needed; upstream DNS
  occurs beyond the broker policy boundary.
- The node integration must ask a materialized handle for its broker advertise
  address before starting the broker, and bind the broker to that exact host
  veth address. Loopback and `0.0.0.0` are not valid substitutes.
- The backend needs `CAP_NET_ADMIN` for link/nftables work and `CAP_SYS_ADMIN`
  to create and persist a network namespace. `doctor` reports each missing
  prerequisite as unavailable and includes `runsc --version` evidence.
- `MultiTenant` remains false. gVisor provides sibling process isolation, but
  ADR 0061 does not claim the microVM boundary required by the
  `multi_tenant` profile.
- The nftables encoder is intentionally small: one table, one base chain and
  one accept rule. Adding another destination is a security architecture
  change and requires another denial test, not a general firewall API.
