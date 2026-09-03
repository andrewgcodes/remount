// E15 global setup: bring up the real Remount binary twice and seed exactly
// the state the browser specs assert against.
//
//   standalone  — server + a real node in one process, plus a second real node
//                 enrolled with `remount up`, so the console drives a real
//                 fleet, a real PTY over the real terminal WebSocket, a real
//                 snapshot and a real cross-node move.
//   production  — `--mode production-single-tenant` with a bootstrap operator,
//                 the only mode where roles are distinguishable. Standalone
//                 silently promotes every credential to operator+admin
//                 (internal/server/api_console.go), so it cannot prove RBAC.
//
// Nothing here is mocked: every response the browser sees is produced by the
// Go server under test.
import { randomBytes } from 'node:crypto';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { baseURLs, ports, remountBinary, runRoot, statePath } from './env.mjs';
import { getJSON, groups, run, start, stopGroup, waitFor, writeState } from './harness.mjs';

// A synthetic credential with a real credential's shape. It is never a real
// key: MISTAKES.md records what happens when a live key is used as a canary.
const UPSTREAM_SECRET = `sk-e15-${randomBytes(24).toString('hex')}`;
// A second synthetic string that the browser is SUPPOSED to see. It is the
// positive control: if the scanner cannot find this, its failure to find the
// secret proves nothing (AGENTS.md, "prove the scan works by planting a
// canary the same scan must find").
const CONTROL_STRING = 'E15-CONTROL-STRING-THE-SCANNER-MUST-FIND';
const BINDING = 'b_e15';

export default async function globalSetup() {
  const binary = remountBinary();
  try {
    await fs.access(binary, fs.constants.X_OK);
  } catch {
    throw new Error(
      `the remount binary is missing at ${binary}.\n` +
      'Run `npm run build:server` in web/, or set REMOUNT_BIN to a prebuilt binary.',
    );
  }

  // A previous run that died before teardown leaves its servers holding these
  // ports. Reap them by recorded pid first, or the bind failure surfaces only
  // as a readiness timeout.
  try {
    const stale = JSON.parse(await fs.readFile(statePath, 'utf8'));
    for (const pid of stale.pids ?? []) stopGroup(pid);
  } catch { /* no previous run */ }
  await fs.rm(runRoot, { recursive: true, force: true });
  await fs.mkdir(runRoot, { recursive: true, mode: 0o700 });
  const logs = path.join(runRoot, 'logs');
  await fs.mkdir(logs, { recursive: true });

  const state = {
    runRoot,
    binary,
    ports,
    baseURLs,
    binding: BINDING,
    upstreamSecret: UPSTREAM_SECRET,
    controlString: CONTROL_STRING,
    pids: [],
  };

  try {
    // ---- upstreams -------------------------------------------------------
    start('upstream', process.execPath, [path.join(path.dirname(fileURLToPath(import.meta.url)), 'upstream.mjs'), String(ports.upstream), String(ports.foreign)], {
      logFile: path.join(logs, 'upstream.log'),
    });
    await waitFor('the allowed upstream to listen', async () => (await getJSON(`${baseURLs.upstream}/__hits`)).label === 'allowed');
    await waitFor('the foreign upstream to listen', async () => (await getJSON(`${baseURLs.foreign}/__hits`)).label === 'foreign');

    // ---- standalone: server + node --------------------------------------
    // The binding covers the allowed upstream only. A placeholder aimed at the
    // foreign upstream must be refused by the node's broker.
    const bindingsFile = path.join(runRoot, 'bindings.json');
    await fs.writeFile(bindingsFile, `${JSON.stringify([{
      id: BINDING,
      secret: UPSTREAM_SECRET,
      destinations: [`127.0.0.1:${ports.upstream}`],
      ttl_sec: 600,
    }], null, 2)}\n`, { mode: 0o600 });

    start('standalone', binary, [
      'standalone',
      '--listen', `127.0.0.1:${ports.standalone}`,
      '--data', path.join(runRoot, 'standalone'),
      '--bindings', bindingsFile,
    ], { logFile: path.join(logs, 'standalone.log') });

    await waitFor('the standalone server to serve', async () => {
      const health = await getJSON(`${baseURLs.standalone}/healthz`);
      return health.ok === true && health.serving === true;
    });

    // A second real node makes `move` a real cross-node relocation rather
    // than a no-op against the only node in the fleet.
    start('node2', binary, [
      'up',
      '--data', path.join(runRoot, 'node2'),
      '--label', 'e15=second',
    ], { logFile: path.join(logs, 'node2.log'), env: { REMOUNT_SERVER: baseURLs.standalone } });

    const cli = { REMOUNT_SERVER: baseURLs.standalone };
    const fleet = await waitFor('two online nodes', async () => {
      const value = await getJSON(`${baseURLs.standalone}/v1/console/fleet`, 'e15-standalone');
      const online = value.nodes.filter((node) => node.state === 'online');
      return online.length === 2 ? online : false;
    }, { timeoutMs: 90_000 });
    state.nodes = fleet.map((node) => node.id);

    // ---- seeded workspaces ----------------------------------------------
    const opsSeed = path.join(runRoot, 'seed-ops');
    await fs.mkdir(opsSeed, { recursive: true });
    await fs.writeFile(path.join(opsSeed, 'hello.txt'), 'hello\n');

    const secSeed = path.join(runRoot, 'seed-sec');
    await fs.mkdir(secSeed, { recursive: true });
    await fs.writeFile(path.join(secSeed, 'canary-control.txt'), `${CONTROL_STRING}\n`);

    const created = await run(binary, [
      'ws', 'create', '--name', 'E15 operator flow', '--dir', opsSeed, '--json',
    ], cli);
    state.opsWorkspace = JSON.parse(created.stdout).id;

    const secure = await run(binary, [
      'ws', 'create', '--name', 'E15 credential boundary', '--dir', secSeed, '--json',
      '--binding', BINDING,
      '--env', `API_KEY=ref:${BINDING}`,
      '--network-default', 'deny',
      '--egress-rule', JSON.stringify({
        id: 'read', mode: 'allow', protocol: 'http',
        hosts: [`127.0.0.1:${ports.upstream}`], methods: ['GET'], path_prefixes: ['/ok'],
      }),
      '--egress-rule', JSON.stringify({
        id: 'approve', mode: 'approve', protocol: 'http',
        hosts: [`127.0.0.1:${ports.upstream}`], methods: ['GET'], path_prefixes: ['/approve'],
      }),
    ], cli);
    state.secureWorkspace = JSON.parse(secure.stdout).id;

    for (const id of [state.opsWorkspace, state.secureWorkspace]) {
      await waitFor(`workspace ${id} to reach claimed`, async () => {
        const detail = await getJSON(`${baseURLs.standalone}/v1/console/workspaces/${id}`, 'e15-standalone');
        return detail.state === 'claimed';
      });
    }

    // ---- a real leak_blocked decision -----------------------------------
    // The workspace holds only the placeholder "ref:b_e15". Aiming it at a
    // host the binding does not cover must be refused before any byte leaves
    // the node, and must land in the canonical event log.
    const leak = await run(binary, [
      'exec', state.secureWorkspace, '--', 'sh', '-c',
      `curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$REMOUNT_BROKER/http/127.0.0.1:${ports.foreign}/leak"`,
    ], cli);
    const leakStatus = `${leak.stdout}${leak.stderr}`;
    if (!leakStatus.includes('403')) {
      throw new Error(`the foreign-destination leak was not refused: ${leakStatus}`);
    }
    await waitFor('a leak_blocked decision in the canonical event log', async () => {
      const page = await getJSON(`${baseURLs.standalone}/v1/console/events?types=egress.denied&limit=200`, 'e15-standalone');
      return page.events.some((event) => event.payload?.decision === 'leak_blocked' && event.credential === BINDING);
    });

    // ---- a real parked approval -----------------------------------------
    // Approve-mode egress releases nothing until a durable decision commits.
    // The broker answers a still-undecided request with 403 and Retry-After
    // once its bounded wait elapses; the approval itself stays durable and
    // retryable, which is exactly what this loop exercises. The fingerprint is
    // deterministic, so every retry re-joins the same approval id rather than
    // queueing a new one. The browser commits the decision; the upstream
    // proves the release.
    start('parked-egress', binary, [
      'exec', state.secureWorkspace, '--', 'sh', '-c',
      `i=0; while [ $i -lt 600 ]; do ` +
      `code=$(curl -s -m 30 -o /dev/null -w "%{http_code}" "$REMOUNT_BROKER/http/127.0.0.1:${ports.upstream}/approve"); ` +
      `if [ "$code" = "200" ]; then echo released; exit 0; fi; i=$((i+1)); sleep 1; done; echo gave-up; exit 1`,
    ], { logFile: path.join(logs, 'parked-egress.log'), env: cli });

    state.approval = await waitFor('a pending egress approval', async () => {
      const page = await getJSON(`${baseURLs.standalone}/v1/console/approvals?status=pending`, 'e15-standalone');
      return page.approvals[0]?.id ?? false;
    });
    const beforeApproval = await getJSON(`${baseURLs.upstream}/__hits`);
    if (beforeApproval.hits.length !== 0) {
      throw new Error(`the parked request reached the upstream before any decision: ${JSON.stringify(beforeApproval)}`);
    }

    // ---- production server for RBAC --------------------------------------
    const masterKey = randomBytes(32).toString('base64');
    const bootstrapFile = path.join(runRoot, 'bootstrap.token');
    start('production', binary, [
      'server',
      '--listen', `127.0.0.1:${ports.rbac}`,
      '--data', path.join(runRoot, 'production'),
      '--mode', 'production-single-tenant',
      '--bootstrap-principal', 'root',
      '--bootstrap-token-file', bootstrapFile,
      // The identity manager refuses an access token whose lifetime exceeds
      // its 1h access TTL, so a longer bootstrap TTL mints an unusable bearer.
      '--bootstrap-ttl', '55m',
    ], { logFile: path.join(logs, 'production.log'), env: { REMOUNT_MASTER_KEY: masterKey } });

    await waitFor('the production server to serve', async () => {
      const health = await getJSON(`${baseURLs.rbac}/healthz`);
      return health.ok === true && health.serving === true && health.security_mode === 'production-single-tenant';
    });

    const bootstrap = (await waitFor('the bootstrap operator bearer', async () => {
      const value = await fs.readFile(bootstrapFile, 'utf8');
      return value.trim() || false;
    }));
    const admin = { REMOUNT_SERVER: baseURLs.rbac, REMOUNT_TOKEN: bootstrap };

    await run(binary, ['principal', 'create', 'e15-operator', '--tenant', 'acme', '--roles', 'operator'], admin);
    await run(binary, ['principal', 'create', 'e15-viewer', '--tenant', 'acme', '--roles', 'viewer'], admin);
    const operatorToken = await run(binary, ['token', 'issue', 'e15-operator', '--tenant', 'acme', '--role', 'operator', '--ttl', '55m'], admin);
    const viewerToken = await run(binary, ['token', 'issue', 'e15-viewer', '--tenant', 'acme', '--role', 'viewer', '--ttl', '55m'], admin);
    state.tokens = {
      operator: operatorToken.stdout.trim().split('\n').pop().trim(),
      viewer: viewerToken.stdout.trim().split('\n').pop().trim(),
      denied: `not-a-real-bearer-${randomBytes(8).toString('hex')}`,
    };
    if (!state.tokens.operator || !state.tokens.viewer) {
      throw new Error('the production server did not mint both role tokens');
    }

    state.pids = groups.slice();
    await writeState(statePath, state);
  } catch (error) {
    for (const pid of groups) stopGroup(pid);
    await writeState(statePath, { ...state, pids: groups.slice(), failed: String(error) }).catch(() => {});
    throw error;
  }
}
