-- The control-plane schema exactly as (*Control).migrate created it at commit
-- 407cd50^ -- one before "control/pool: fence a node durably before its
-- scale-down destroy", which is the last revision that changed the schema and
-- the one that added pool_retirements. This file is therefore the pinned
-- "previous schema" for TestMigrateUpgradesPreviousSchema.
--
-- The first statement is the event outbox, which migrate creates through
-- eventlog.CreateOutbox before running its own DDL; eventlog.OpenSQLite's
-- schema is unchanged since that revision, so the test lets the current code
-- create it.
--
-- Regeneration is documented in migrate_upgrade_test.go; the DDL below was
-- extracted verbatim from that revision and only the leading comment is new.
CREATE TABLE IF NOT EXISTS event_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, event BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS timers (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, pubkey BLOB, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS idem (key TEXT PRIMARY KEY, ws TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mutations (
	scope TEXT NOT NULL,
	key TEXT NOT NULL,
	op TEXT NOT NULL,
	fingerprint BLOB NOT NULL,
	result BLOB NOT NULL,
	completed_at INTEGER NOT NULL,
	PRIMARY KEY(scope, key)
);
CREATE INDEX IF NOT EXISTS mutations_completed_at ON mutations(completed_at);
CREATE TABLE IF NOT EXISTS assignments (
	workspace TEXT NOT NULL,
	generation INTEGER NOT NULL,
	node TEXT NOT NULL,
	tenant TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY(workspace, generation, node)
);
CREATE TABLE IF NOT EXISTS fleet_operations (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS bases (tenant TEXT NOT NULL, name TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(tenant, name));
CREATE TABLE IF NOT EXISTS session_logs (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, expires_at INTEGER NOT NULL, data BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS session_logs_expiry ON session_logs(expires_at);
CREATE TABLE IF NOT EXISTS volumes (tenant TEXT NOT NULL, id TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(tenant, id));
CREATE TABLE IF NOT EXISTS pools (tenant TEXT NOT NULL, name TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(tenant, name));
CREATE TABLE IF NOT EXISTS queues (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS approvals (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS budgets (tenant TEXT NOT NULL, id TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(tenant, id));
CREATE TABLE IF NOT EXISTS budget_reservations (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS export_cursors (
  tenant TEXT NOT NULL,
  name TEXT NOT NULL,
  next INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(tenant, name)
);
CREATE TABLE IF NOT EXISTS transcripts (
	agent TEXT NOT NULL,
	idx INTEGER NOT NULL,
	run TEXT NOT NULL,
	seq INTEGER NOT NULL,
	stream INTEGER NOT NULL,
	at INTEGER NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY(agent, idx)
);
CREATE TABLE IF NOT EXISTS keys (name TEXT PRIMARY KEY, priv BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS controller_state (
	id INTEGER PRIMARY KEY CHECK(id=1),
	epoch INTEGER NOT NULL,
	role TEXT NOT NULL,
	updated_at INTEGER NOT NULL
);
