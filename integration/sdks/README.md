# Cross-language SDK proof

The package tests are offline and deterministic. The two `*_e12` programs are
the live E12 proof and deliberately take one shared `REMOUNT_E12_URL`. They
create an agent, send a follow-up, and consume at least one durable transcript
record. CI runs both only when the repository is configured with a
real E12 endpoint and credential; absence is reported as a skipped external
gate, not a pass. Each run uses one retry-stable identifier and destroys its
agent after the assertion so repeated probes do not accumulate resources.
