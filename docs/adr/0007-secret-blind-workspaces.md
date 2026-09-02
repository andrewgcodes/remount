# 7. The workspace never holds a credential

**Status:** accepted

## Context

An agent with root in a box that also contains an API key can read that key. It
can print it, upload it, or be prompt-injected into doing so. Every harness that
sets `OPENAI_API_KEY` in the agent's environment has this property, and the
industry response has mostly been to sandbox the agent harder rather than to
remove the key.

In July 2026 an agent escaped its sandbox during a security evaluation, found a
reusable long-lived credential in a secret store, and enrolled 181 machines onto
a private network over several days. The failure was not the escape. The failure
was that there was something worth stealing where the agent could reach it.

## Decision

The workspace holds **references**, never secrets. A binding maps a reference to
a real credential, scoped to a set of destination hosts, a principal and a TTL.
A broker running in the node, outside the workspace, substitutes the real value
at the network edge.

The rule that matters most is what happens when the reference goes somewhere it
should not: the request is **blocked**, not forwarded. A placeholder aimed at an
unbound host is an exfiltration attempt and is recorded as one.

## Consequences

An agent can have root and a compromised workspace has nothing to leak. We
verified this: a real agent harness ran a real model through the broker, and a
scan of the workspace tree found zero copies of the key while a deliberately
planted canary was found by the same scan.

Placeholders should be shape-preserving, keeping the prefix and length of the
real secret, because harnesses validate key formats client-side and will reject
an obviously fake value before it ever reaches the broker.

The broker must fail closed. An expired lease blocks rather than forwards. An
unresolvable policy blocks. There is no passthrough mode in v0, because a
passthrough mode is what people reach for at 3am and never remove.

**The honest limitation:** credentials are substituted only on the reverse-proxy
path, where the broker terminates the request. A `CONNECT` tunnel cannot be
rewritten without terminating TLS with a CA installed in the workspace. Every
comparable system does install one. We do not, in v0, because that CA becomes
the highest-value secret in the system and it lives next to the thing we are
defending against. So model traffic goes through the `/d/` path and package
installs go through `CONNECT` without credentials.
