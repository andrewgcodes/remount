# Identity E8 core

`TestE8PrincipalRevocationCoreSurvivesRestart` proves the durable identity
boundary of E8: revoking `agent:alice` invalidates her access and per-session
broker capability after a control-database restart, while `agent:bob` remains
authorized for the same workspace generation.

This test does not claim the full E8 scenario. The relay must reauthenticate
hello credentials, node renew must push the new authorization revision and
close only Alice's live sessions, and the broker must verify the session
capability on every request. Those shared-path wiring contracts are recorded
in ADR 0068.
