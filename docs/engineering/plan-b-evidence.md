# Plan B evidence: schema, scenario registry, and the aggregate gate

This is the ledger behind `make plan-b`. It implements items B0.1 and B0.2 and
the aggregate completion gate of
[`plan-b-repository-executable-2026-09-03.md`](plan-b-repository-executable-2026-09-03.md)
(§6, §8, §16).

It exists because the dangerous output of a verification system is not a red
run. It is an unearned green one. Every mechanism here is arranged so that a
check which did not run is visible, named, and never counted.

## What a green `make plan-b` proves today

Very little, and it says so. At the time this landed, no acceptance scenario is
wired into the runner: all 57 registered rows report `unavailable`, and the
verdict is `incomplete`, not `complete`. What the command does prove is that
the B0 required gates pass against the exact commit, that the leak scan finds a
planted canary, and that the evidence it wrote carries no credential.

The recorded outcomes in the registry come from the disposition section of
[`implementation-closure-2026-09-03.md`](implementation-closure-2026-09-03.md).
They are **provenance, not evidence**: the runner reports them and never
promotes one into a pass for the candidate under test.

## The record

One JSON record per scenario:

```json
{"scenario":"B3","candidate":"<git commit>","layer":"sim","status":"passed",
 "command":"go test ...","started_at":"<UTC timestamp>","duration_ms":1234,
 "environment":{"os":"linux","arch":"amd64","backend":"docker","provider":"fake-e2b"},
 "cleanup":"verified","artifacts":["test-report.json"]}
```

`internal/evidence.Record` adds `required`, `reason`, `required_env`, `owner`,
`provider_ids` and `ephemeral`. `Record.Validate` enforces:

| Rule | Why |
|---|---|
| `layer` ∈ `code`, `sim`, `host-ci`, `live-service`, `artifact` | `production` is refused by name: production proof needs an operated deployment and belongs in another ledger |
| `status` ∈ `passed`, `failed`, `unavailable` | `skipped-success` is refused by name; a check that could not run is `unavailable` |
| a non-`passed` record must carry `reason` | an unexplained unavailable row is indistinguishable from a hidden one |
| `cleanup: failed` may not be `passed` | cancellation is not completion, and neither is a leaked resource |
| `required_env` holds names matching `[A-Za-z_][A-Za-z0-9_]*` | a record names variables, never values |
| `provider_ids` requires `ephemeral: true` | provider object ids prove cleanup in a run artifact; they are never evergreen proof |

`Record.ValidateEvergreen` additionally refuses any ephemeral record.

## The leak rules

`Record.ScanSecrets` refuses a record that carries the value of any environment
variable it names — from `required_env` or from any variable name that appears
in its text — and any credential-shaped string (`ScanText`: OpenAI, E2B, AWS,
GitHub and Slack token shapes, private-key blocks, bearer headers, and
`key: <20+ chars>` assignments).

Two properties matter more than the pattern list:

- **A `Finding` never reproduces the match.** It names the rule, the location,
  and a four-character prefix with a byte count. A finding that printed the
  secret would copy it into the report the scan exists to keep clean.
- **The scan is proved before it is trusted.** `B0.leak-canary` plants
  `evidence.Canary` in a scratch file, requires the scan to find it, verifies
  the scratch directory is gone, and only then scans the rendered evidence.
  This repository has already shipped a leak scan that reported clean because
  its pipeline always exited zero; a scan without a positive control proves
  nothing.

Gate logs get the same treatment on the way to disk: `writeLog` scans a gate's
output against every variable the registry names, and writes a redaction notice
instead of the output if it finds one.

## The registry

`internal/evidence/registry.go` is one declarative table of every acceptance
scenario: E1-E25 from [`handoff-2026-09-03.md`](handoff-2026-09-03.md) and
B1-B32 from the Plan B phase tables (§9-§15). Each row carries an id, a
one-line title, a layer, required-or-optional, the owning test or command, the
names of the environment variables it needs, and — where a source document
recorded one — the last outcome and that document's own honesty limit.

A row is **owned** or **open**, never both and never neither. `ValidateRegistry`
enforces that, and refuses a row that records a pass without an owner. An
unowned scenario is an open row naming the Plan B phase and §17 ticket that
will close it; it is never silently absent, because a missing row reads as a
satisfied one.

```sh
make plan-b-list                                  # every row
make plan-b-list PLAN_B_ARGS="--required --unowned"   # the work that remains
make plan-b-list PLAN_B_ARGS="--json"
```

## The aggregate gate

```sh
make plan-b                                       # the status report
make plan-b PLAN_B_ARGS="--require-complete"      # the Plan B §19 completion gate
make plan-b PLAN_B_ARGS="--dev"                   # accept a dirty tree
make plan-b PLAN_B_ARGS="--gates B0.mod-verify,B0.mod-tidy"
```

It refuses a dirty tree unless `--dev`, records the exact commit, runs the B0
required gates (`make lint`, `make test`, `make race`, `make conformance`,
`make fuzz FUZZTIME=30s`, `make dist`, `go mod verify`, `go mod tidy -diff`,
`go run ./cmd/protogen --check`), detects optional credentials by presence
only, and writes `evidence.json`, `junit.xml` and `summary.md` to
`dist/plan-b`.

Host capability comes from the checked-in gate scripts rather than a second,
divergent probe: `integration/chaos/backend-gates.sh` for docker and gVisor and
`integration/firecracker/host-gate.sh` for KVM. Their own `status=unavailable
reason=…` text becomes the row's reason. An absent script is unavailable, never
an assumed pass.

### Verdicts and exit codes

| Verdict | Meaning | Exit |
|---|---|---|
| `complete` | every required row passed **in this run** | 0 |
| `incomplete` | nothing failed, but required rows did not run; each is listed with its missing prerequisite | 0, or 1 under `--require-complete` |
| `failed` | a required row failed, a cleanup failed, or the leak scan found something | 1 |

A run that merely did not fail is never reported as complete. `summary.md`
always ends with an **Unavailable evidence** section that lists every row that
did not run and why — the same idiom `bench/run.py` uses. JUnit has no third
state, so an unavailable row becomes `skipped` carrying its reason; a reader
must not treat a skipped case as a pass.

## Wiring a scenario (tickets 2-12)

Set `Argv` on the registry row to the command that proves it. The runner then
executes it, times it, keeps its scanned log as an artifact, and emits a
`passed` or `failed` record instead of an `unavailable` one. Update
`TestNoScenarioIsWiredYetSoNoRowClaimsAPass` deliberately when the first row is
wired — that test exists so wiring is a decision, not a drift.

A scenario that creates external state must set its record's `cleanup` from a
verification that the state is gone, and may name provider object ids only on an
ephemeral record.

## Validating evidence elsewhere

```sh
go run ./cmd/evidence validate dist/plan-b/evidence.json
go run ./cmd/evidence validate records.json --evergreen
```

`validate` accepts the aggregate document, an array of records, or a single
record, and applies both the schema rules and the leak scan.
