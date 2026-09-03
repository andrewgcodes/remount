# Warm-standby failover integration

`TestWarmStandbyFailoverE10` is the deterministic composed E10 proof. An
active at epoch 1 ships a claimed generation, begins a move that is not
shipped, and dies after the node durably prepares release. The standby takes
the expired lease at epoch 2, restores the committed database, asks the node
for retained authority, rolls the move back, and asserts:

- promotion and reconciliation complete in less than five seconds;
- exactly one assignment exists for the generation;
- the workspace is serviceable on its original node; and
- `control.reconciled` and `control.recovered` are durably epoch-stamped.

`TestMinIOControlFailoverE10Core` additionally exercises the object-store boundary:
an active controller commits a recovery point, a later SQLite transaction is
deliberately not shipped, the writer lease expires, and a standby acquires a
higher epoch and atomically restores only the committed point. The test logs
the measured core restore time and the explicit lost-window estimate.

The deterministic test uses the same coordinator, SQLite ship/restore, control
state machine, event log, and node-report wire types as production without an
external service. Locally, an unset `REMOUNT_S3_INTEGRATION_ENDPOINT` produces
an explicit skip only for the supplementary MinIO test. CI starts pinned MinIO
and runs that lane after the S3 suite has created the bucket.
