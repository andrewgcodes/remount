# ADR 0074: warm-standby control-plane replication

Status: accepted and implemented

## Decision

A deployment may run one active control plane and one or more passive
standbys. They share a strongly consistent S3-compatible named-object store.
The active owns a conditional writer lease and ships bounded, immutable SQLite
recovery data. A standby may promote only by replacing an expired lease with
`If-Match` and incrementing its epoch. This is warm standby with a nonzero RPO,
not synchronous replication.

The implementation is `internal/control/replicate`. It refuses an object store
that fails the existing real conditional-write probe. It uses `If-None-Match:
*` to create immutable objects and `If-Match: <etag>` to renew/take over the
lease and advance `current.json`. HTTP 412 is a lost race and fences the
writer. This matches S3's documented conditional-write contract [1].

The lifecycle boundary is:

- Authority: the holder of the unexpired, conditionally updated writer lease.
- Resource: the one writable control SQLite database and controller-generation
  namespace.
- Irreversible action: committing a recovery pointer or replacing the local
  database during promotion.
- Durable commit point: a successful conditional `current.json` write after
  every immutable referenced object exists; during promotion it is the
  same-directory rename followed by directory `fsync`.
- Fence: lease ETag plus monotonically increasing epoch. A controller also
  self-fences when time since its last successful lease write reaches TTL.
- Observable postcondition: a standby restores exactly one verified committed
  manifest, enters reconciliation, then emits explicit recovery and repair
  events through the control transaction log.

## Stored format and capture

Keys are rooted below a configured prefix:

```
lease.json
current.json
epochs/<20-digit epoch>/manifests/<20-digit sequence>.json
epochs/<20-digit epoch>/snapshots/<sha256>.db
epochs/<20-digit epoch>/wal/<sha256>.wal
```

Manifest and payload sizes are bounded before allocation or copy. Snapshot and
WAL objects are content addressed and include SHA-256. Publication uploads
immutable payloads, renews the lease, writes an immutable manifest, checks the
local decision gate again, then conditionally advances `current.json`. A crash
before the last operation leaves only unreachable objects; a standby never
infers a recovery point by listing.

SQLite's WAL format contains salts, rolling checksums and commit-frame database
sizes [2][3]. The capturer accepts only the checksum-valid prefix through the
last commit frame and records the WAL salts as its lineage. A truncate/restart
of the WAL forces a full snapshot. Full capture performs a truncate checkpoint,
holds `BEGIN IMMEDIATE` on the application's sole serialized writer handle,
and takes an exact file copy. This is intentionally not `VACUUM INTO`: that
operation creates a consistent alternative database [4], but can change page
layout and therefore cannot be paired with WAL frames from the source file.
The server must not create an independent SQLite pool that checkpoints behind
the capturer.

Restore downloads the manifest named by `current.json`, requires it to match
the immutable copy, verifies lengths and digests, validates/replays WAL through
SQLite, runs `quick_check`, checkpoints, syncs a same-directory staging file,
then atomically renames and syncs the parent directory. Corrupt or incomplete
data never replaces the existing database.

Old immutable payloads are retained conservatively. The collector marks the
current and newest retained manifests and their payloads, then deletes at most
the configured number of unreferenced objects older than the orphan grace
period. Objects with no trustworthy modification time are retained. A failed
or corrupt retained-manifest read aborts collection rather than guessing
reachability.

## Implemented wiring contracts

The storage core deliberately does not mutate workspaces. The server and
protocol layers wire these contracts:

1. **Server startup:** construct one `replicate.Coordinator` with a
   process-unique holder ID and call `Acquire` before opening write traffic.
   Open the control database as one serialized `*sql.DB`, create
   `SQLiteSource`, complete an initial `Ship(forceFull=true)`, and join the
   lease/ship loops on shutdown. Every mutating dispatch must call
   `CanDecide`; `ErrFenced` immediately closes readiness and write admission.
2. **Epoch persistence:** store the acquired controller epoch in the same
   control transaction as each state mutation and event. Add epoch to event,
   grant, claim, lease-renewal, ready and lifecycle request/response bodies.
   Nodes persist the greatest epoch seen and reject every lower epoch before
   touching a workspace. Equal-epoch idempotent retries remain allowed.
3. **Ship position:** expose the highest transactionally committed event
   sequence from `eventlog.Log.Transact` and pass it to `Shipper.Ship`. A
   manifest never advertises a sequence before both resource rows and durable
   outbox rows commit.
4. **Promotion:** stop write admission, call `Restorer.Promote`, reopen the
   restored DB, and enter a distinct `reconciling` readiness state. Do not
   report healthy or accept lifecycle writes yet.
5. **Node-authoritative reconciliation:** enumerate live node generations and
   prepared lifecycle operations. For each mismatch, use the existing central
   transition table and durable control transaction to complete or roll back;
   never invent a generation. Emit `control.reconciled` once per repair with
   workspace, observed generation, restored generation and action. After the
   pass commits, emit `control.recovered` with `epoch`,
   `last_replicated_at`, `lost_window_ms`, and restored event sequence; only
   then enable readiness. `lost_window_ms` is `max(0, promotion time - capture
   time)`, an upper-bound estimate, not proof that data was lost.
6. **Diagnostics:** status/doctor/metrics expose role, epoch, lease age, last
   successful ship, ship failures, restored sequence, reconciliation state and
   lost-window estimate. Store/lease checks that cannot execute are
   unavailable, never healthy.

`integration/failover/TestWarmStandbyFailoverE10` covers the composed E10
scenario: the active fails after move preparation but before a delayed ship;
the standby advances the epoch, restores, rolls the move back from retained
node evidence, preserves one generation, and records recovered/reconciled
evidence. `TestMinIOControlFailoverE10Core` separately proves conditional
failover and crash-safe SQLite restoration against a real compatible endpoint.

## Recovery objectives and limitations

The default ship interval is one second. That is a configuration target, not a
proven one-second RPO: scheduler delay, object-store latency, a large WAL and
failed uploads increase the lost window. RPO evidence is the manifest capture
timestamp/event sequence and the promotion metadata.

Likewise, five-second RTO is not claimed by the library. Lease TTL, clock-skew
allowance, download size, SQLite recovery, and reconciliation all contribute.
The MinIO lane logs measured core promotion time but does not include process
restart, service discovery or node reconciliation. A deployment may claim
RPO/RTO only from repeated end-to-end measurements at its configured maximum
database/WAL sizes and failure latency.

## Consequences

Object storage is outside the workspace credential boundary and is configured
only on controllers. Availability depends on genuine conditional-write
semantics and bounded clock skew. A partitioned old active stops authoritative
decisions after TTL even if its process continues. An overlap remains possible
only if clocks violate the configured skew assumption or a store lies about
preconditions; both are explicit readiness failures rather than silent
fallbacks.

## Sources

1. AWS S3, “Conditional writes”: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
2. SQLite, “Database File Format — Write-Ahead Log”: https://www.sqlite.org/fileformat.html#the_write_ahead_log_or_wal_file
3. SQLite, “WAL-mode File Format”: https://www.sqlite.org/walformat.html
4. SQLite, “VACUUM INTO”: https://www.sqlite.org/lang_vacuum.html#vacuum_with_an_into_clause
5. SQLite, “Online Backup API”: https://www.sqlite.org/backup.html
