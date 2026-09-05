# Cross-language SDK proof

The package tests are offline and deterministic. The two `*_e12` programs are
the live E12 proof and deliberately take one shared `REMOUNT_E12_URL`. They
create an agent, send a follow-up, and consume at least one durable transcript
record. CI runs both only when the repository is configured with a
real E12 endpoint and credential; absence is reported as a skipped external
gate, not a pass. Each run uses one retry-stable identifier and destroys its
agent after the assertion so repeated probes do not accumulate resources.

## Strict-profile protocol gate (local)

After installing the Python test dependencies and building the TypeScript SDK:

```sh
python -m pip install -e './sdk/python[test]'
npm ci --ignore-scripts --prefix sdk/typescript
npm run build --prefix sdk/typescript
REMOUNT_SDK_CHECK=1 go test -race -count=1 -v ./integration/sdks
```

Set `REMOUNT_SDK_PYTHON` to an explicit virtualenv interpreter when needed.
Without `REMOUNT_SDK_CHECK=1`, the Go suite labels this gate unavailable. The
SDK workflow explicitly enables it after building both packages.

This starts a real Go relay and SQLite control plane under each of the
`isolated` and `multi_tenant` capability floors. Both language clients must
negotiate hello, carry controller epochs, and create/read/destroy a pending
workspace. It uses loopback, temporary databases and synthetic identities;
cleanup runs on both success and failure. No execution backend, cloud provider
or model is involved, so this proves protocol interoperability, not a complete
production deployment. Package tests separately inject stale epochs, forged
session output, early chunks and corrupt artifact bytes.
