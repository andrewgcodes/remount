# Temporal for the agent's computer

Temporal retries the orchestration step; Remount keeps the workspace, process,
and sequenced output alive. `Activities.RunStep` opens a Remount exec session
with an idempotency key derived from the Temporal workflow and Activity IDs.
Each heartbeat contains only `{session, next}`. A retry attaches to that same
session from the acknowledged cursor instead of running the command twice.

Command output deliberately does not enter Temporal history. The Activity
returns the session ID, final cursor, and exit status; use Remount's attach and
inspection APIs for output. `REMOUNT_TOKEN` is read only by the worker and is
never an Activity input, result, or heartbeat detail.

## Run it

Start a Remount server and node, create a workspace, then export its connection
details for the worker:

```sh
export REMOUNT_SERVER=https://remount.example
export REMOUNT_TOKEN=...
export TEMPORAL_ADDRESS=your-namespace.your-account.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=your-namespace.your-account
export TEMPORAL_API_KEY=...
go run ./cmd/worker
```

`LAKESIDE_TEMPORAL_API_KEY` and `NEW_LAKESIDE_TEMPORAL_API_KEY` are accepted as
fallback names for the repository's key-gated verification lane. Start a
workflow from another terminal:

```sh
go run ./cmd/start -workflow build-42 -workspace ws_... -- make race
```

To exercise recovery, terminate the worker after it logs at least one
heartbeat and start it again. Temporal retries the Activity and the test
`TestRunStepWorkerRestartReattachesFromHeartbeat` proves that the new attempt
calls `Attach(workspace, session, next)` and never calls `Open`.

The CI smoke test uses fakes and does not claim Temporal Cloud coverage. The
Cloud lane requires `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, one of the API key
variables above, plus `REMOUNT_SERVER` and (when required) `REMOUNT_TOKEN`.

References: [Temporal Activity heartbeats](https://pkg.go.dev/go.temporal.io/sdk/activity),
[Temporal Cloud environment configuration](https://docs.temporal.io/develop/go/activities#run-a-standalone-activity-execution-with-temporal-cloud).
