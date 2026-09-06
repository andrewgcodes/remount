package control

import (
	"bytes"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

// The upgrade path this file proves is the one nobody exercises until an
// operator does it in production: a control plane whose SQLite file was
// created by an older binary is opened by a newer one. migrate() is a list of
// CREATE TABLE IF NOT EXISTS statements, which makes a *missing* table
// self-healing and makes a *changed* one silently wrong, so the interesting
// claims are that the new table appears, that every row an older release
// wrote is still there and still decodable afterwards, and that running the
// same migration twice changes nothing.
const (
	// migrationFixtureDB is a database file created by the schema at commit
	// 407cd50^ — the revision before pool_retirements existed — and then
	// seeded with one row in each durable table the control plane reads at
	// start-up. It is checked in rather than built at test time so the test
	// judges a real older file, not this release's idea of an older file.
	migrationFixtureDB = "testdata/control-pre-pool-retirements.db"
	// migrationFixtureDDL is that schema, extracted verbatim from the same
	// revision. See the comment at the top of the file for how it was taken.
	migrationFixtureDDL = "testdata/schema-pre-pool-retirements.sql"
	// migrationFixtureTable is the table the current schema adds and the old
	// one lacks. If a later schema change adds another, pin a newer fixture
	// beside this one rather than editing this one: the value of a fixture is
	// that it stops moving.
	migrationFixtureTable = "pool_retirements"
)

// migrationFixtureSigningKey is a deterministic, test-only Ed25519 key. It
// carries no authority outside this fixture and exists so the test can prove
// that upgrading preserves the grant signing key rather than minting a new one
// — a control plane that silently re-keyed on upgrade would invalidate every
// outstanding grant in the fleet.
var migrationFixtureSigningKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x17}, ed25519.SeedSize))

// migrationFixtureWorkspace is seeded in a state load() does not rewrite, so
// any difference after the upgrade is a real difference.
func migrationFixtureWorkspace() proto.Workspace {
	return proto.Workspace{
		ID: "ws_fixture0000000000000001", State: proto.WSPending,
		Spec:      proto.WorkspaceSpec{Name: "pinned-fixture", Principal: "a_fixture"},
		Tenant:    "local",
		Owner:     "a_fixture",
		CreatedAt: 1_700_000_000_000, UpdatedAt: 1_700_000_000_000,
		AuthzRevision: 1,
	}
}

func migrationFixtureTimer() proto.Timer {
	return proto.Timer{
		ID: "t_fixture0000000000000001", WS: "ws_fixture0000000000000001",
		At: 1_700_000_600_000, Action: "resume", CreatedAt: 1_700_000_000_000,
	}
}

func migrationFixtureBase() proto.Base {
	return proto.Base{
		Name: "golden", Tenant: "local", Owner: "a_fixture",
		Artifact: "art_sha256:" + strings.Repeat("ab", 32),
		Format:   proto.ArtifactFormatTar, Bytes: 4096, CreatedAt: 1_700_000_000_000,
	}
}

func migrationFixtureVolume() proto.Volume {
	return proto.Volume{
		ID: "vol_fixture000000000000001", Tenant: "local", Owner: "a_fixture",
		Artifact: "art_sha256:" + strings.Repeat("cd", 32),
		Version:  1, CreatedAt: 1_700_000_000_000, UpdatedAt: 1_700_000_000_000,
	}
}

// seedMigrationFixture writes the rows an older release would have left behind
// into a database that already has the old schema.
func seedMigrationFixture(db *sql.DB) error {
	workspace := migrationFixtureWorkspace()
	timer := migrationFixtureTimer()
	base := migrationFixtureBase()
	volume := migrationFixtureVolume()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces(id, data) VALUES(?,?)`, []any{workspace.ID, proto.MustMarshal(workspace)}},
		{`INSERT INTO timers(id, data) VALUES(?,?)`, []any{timer.ID, proto.MustMarshal(timer)}},
		{`INSERT INTO bases(tenant, name, data) VALUES(?,?,?)`, []any{base.Tenant, base.Name, proto.MustMarshal(base)}},
		{`INSERT INTO volumes(tenant, id, data) VALUES(?,?,?)`, []any{volume.Tenant, volume.ID, proto.MustMarshal(volume)}},
		{`INSERT INTO idem(key, ws) VALUES(?,?)`, []any{"idem-fixture-1", workspace.ID}},
		{`INSERT INTO mutations(scope, key, op, fingerprint, result, completed_at) VALUES(?,?,?,?,?,?)`,
			[]any{"local", "mut-fixture-1", proto.OpWSCreate, []byte{0x01, 0x02}, []byte{0x03, 0x04}, int64(1_700_000_000_000)}},
		{`INSERT INTO assignments(workspace, generation, node, tenant, created_at) VALUES(?,?,?,?,?)`,
			[]any{workspace.ID, 1, "n_fixture00000000000000001", "local", int64(1_700_000_000_000)}},
		{`INSERT INTO keys(name, priv) VALUES('grant', ?)`, []any{[]byte(migrationFixtureSigningKey)}},
		{`INSERT INTO controller_state(id, epoch, role, updated_at) VALUES(1,1,'active',?)`, []any{int64(1_700_000_000_000)}},
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement.query, statement.args...); err != nil {
			return err
		}
	}
	return nil
}

// TestGenerateMigrationFixture rewrites testdata/control-pre-pool-retirements.db
// from the pinned DDL. It is skipped unless asked for, because a fixture that
// regenerated itself on every run would prove only that today's code agrees
// with today's code:
//
//	REMOUNT_REGENERATE_MIGRATION_FIXTURE=1 \
//	  go test ./internal/control -run TestGenerateMigrationFixture -count=1
//
// The DDL it reads was extracted verbatim from 407cd50^; see the header of
// testdata/schema-pre-pool-retirements.sql.
func TestGenerateMigrationFixture(t *testing.T) {
	if os.Getenv("REMOUNT_REGENERATE_MIGRATION_FIXTURE") != "1" {
		t.Skip("set REMOUNT_REGENERATE_MIGRATION_FIXTURE=1 to rewrite " + migrationFixtureDB)
	}
	ddl, err := os.ReadFile(migrationFixtureDDL)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(migrationFixtureDB); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", migrationFixtureDB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// A small page size and a VACUUM keep the checked-in binary a few tens of
	// kilobytes instead of a few hundred; the schema has one root page per
	// table and index.
	if _, err := db.Exec(`PRAGMA page_size=1024`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(ddl)); err != nil {
		t.Fatal(err)
	}
	if err := seedMigrationFixture(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`VACUUM`); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", migrationFixtureDB)
}

// TestMigrateUpgradesPreviousSchema opens the pinned older database with the
// current control plane.
func TestMigrateUpgradesPreviousSchema(t *testing.T) {
	path := copyMigrationFixture(t)

	// Before: the old file really is old, so this test would still pass if
	// migrate() stopped adding the table.
	if before := tableNames(t, path); schemaHasTable(before, migrationFixtureTable) {
		t.Fatalf("the pinned fixture already has %s; it is not a previous schema", migrationFixtureTable)
	}

	// New(...) runs migrate() and then load(). Both must survive the older
	// file: a load() that cannot read an older row would fail here rather
	// than at an operator's next restart.
	fixture := newControlFixture(t, path, nil)

	after := schemaDump(t, fixture.c.db)
	if !schemaHasTable(after, migrationFixtureTable) {
		t.Fatalf("migrate did not create %s; tables: %v", migrationFixtureTable, after)
	}

	// Every seeded row survived, byte for byte where it was opaque and
	// decodable where it was CBOR.
	workspace := migrationFixtureWorkspace()
	loaded, ok := fixture.c.workspaces[workspace.ID]
	if !ok {
		t.Fatalf("workspace %s did not survive the upgrade", workspace.ID)
	}
	if loaded.State != workspace.State || loaded.Tenant != workspace.Tenant ||
		loaded.Owner != workspace.Owner || loaded.Spec.Name != workspace.Spec.Name ||
		loaded.CreatedAt != workspace.CreatedAt {
		t.Errorf("workspace changed across the upgrade:\n got %+v\nwant %+v", *loaded, workspace)
	}
	timer := migrationFixtureTimer()
	if loadedTimer, ok := fixture.c.timers[timer.ID]; !ok {
		t.Errorf("timer %s did not survive the upgrade", timer.ID)
	} else if loadedTimer.WS != timer.WS || loadedTimer.At != timer.At || loadedTimer.Action != timer.Action {
		t.Errorf("timer changed across the upgrade:\n got %+v\nwant %+v", *loadedTimer, timer)
	}
	base := migrationFixtureBase()
	if loadedBase, ok := fixture.c.bases[baseKey(base.Tenant, base.Name)]; !ok {
		t.Errorf("base %s/%s did not survive the upgrade", base.Tenant, base.Name)
	} else if loadedBase.Artifact != base.Artifact || loadedBase.Bytes != base.Bytes {
		t.Errorf("base changed across the upgrade:\n got %+v\nwant %+v", *loadedBase, base)
	}
	volume := migrationFixtureVolume()
	if loadedVolume, ok := fixture.c.volumes[volumeKey(volume.Tenant, volume.ID)]; !ok {
		t.Errorf("volume %s did not survive the upgrade", volume.ID)
	} else if loadedVolume.Artifact != volume.Artifact || loadedVolume.Version != volume.Version {
		t.Errorf("volume changed across the upgrade:\n got %+v\nwant %+v", *loadedVolume, volume)
	}

	// The grant signing key is the one the older release wrote. Re-keying on
	// upgrade would invalidate every outstanding grant in the fleet, and
	// loadOrCreateKey would do exactly that if the row were lost.
	if !bytes.Equal(fixture.c.key, migrationFixtureSigningKey) {
		t.Error("the upgrade replaced the grant signing key instead of preserving it")
	}

	// Rows the control plane keeps but does not load into memory are still
	// there too.
	var idempotencyWorkspace string
	if err := fixture.c.db.QueryRow(`SELECT ws FROM idem WHERE key='idem-fixture-1'`).Scan(&idempotencyWorkspace); err != nil {
		t.Fatalf("idempotency row: %v", err)
	}
	if idempotencyWorkspace != workspace.ID {
		t.Errorf("idem row = %q, want %q", idempotencyWorkspace, workspace.ID)
	}
	var mutations, assignments int
	if err := fixture.c.db.QueryRow(`SELECT COUNT(*) FROM mutations`).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if err := fixture.c.db.QueryRow(`SELECT COUNT(*) FROM assignments`).Scan(&assignments); err != nil {
		t.Fatal(err)
	}
	if mutations != 1 || assignments != 1 {
		t.Errorf("mutations=%d assignments=%d, want 1 and 1", mutations, assignments)
	}

	// And the new table is usable, not merely present.
	var retirements int
	if err := fixture.c.db.QueryRow(`SELECT COUNT(*) FROM ` + migrationFixtureTable).Scan(&retirements); err != nil {
		t.Fatalf("query %s: %v", migrationFixtureTable, err)
	}
	if retirements != 0 {
		t.Errorf("%s = %d rows on a freshly upgraded database, want 0", migrationFixtureTable, retirements)
	}

	// Idempotent: a second migrate() on the same database is a no-op. An
	// operator restarts more often than they upgrade, and every restart runs
	// this.
	schemaBefore := schemaDump(t, fixture.c.db)
	if err := fixture.c.migrate(); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if schemaAfter := schemaDump(t, fixture.c.db); !sameSchema(schemaBefore, schemaAfter) {
		t.Errorf("a second migrate changed the schema:\nbefore %v\nafter  %v", schemaBefore, schemaAfter)
	}
	if err := fixture.c.db.QueryRow(`SELECT COUNT(*) FROM mutations`).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if mutations != 1 {
		t.Errorf("a second migrate changed the rows: mutations=%d, want 1", mutations)
	}

	// Reopening the now-upgraded file loads it again from scratch, which is
	// what the operator's next restart does. Close first: a restart is a
	// second process, not a second handle on a live database.
	fixture.c.Stop()
	if err := fixture.log.Close(); err != nil {
		t.Fatalf("close the upgraded database: %v", err)
	}
	reopened := newControlFixture(t, path, nil)
	if _, ok := reopened.c.workspaces[workspace.ID]; !ok {
		t.Errorf("workspace %s did not survive reopening the upgraded database", workspace.ID)
	}
	if !bytes.Equal(reopened.c.key, migrationFixtureSigningKey) {
		t.Error("reopening the upgraded database replaced the grant signing key")
	}
}

// copyMigrationFixture copies the checked-in database into a temporary
// directory so the test never writes to testdata.
func copyMigrationFixture(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(migrationFixtureDB)
	if err != nil {
		t.Fatalf("read the pinned fixture (regenerate it with REMOUNT_REGENERATE_MIGRATION_FIXTURE=1): %v", err)
	}
	path := filepath.Join(t.TempDir(), "control.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// tableNames opens the file directly, so it reports what is on disk rather
// than what a Control believes.
func tableNames(t *testing.T, path string) []string {
	t.Helper()
	db, err := eventlog.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	return schemaDump(t, db.DB())
}

func schemaDump(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT type || ' ' || name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

func schemaHasTable(names []string, table string) bool {
	for _, name := range names {
		if name == "table "+table {
			return true
		}
	}
	return false
}

func sameSchema(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
