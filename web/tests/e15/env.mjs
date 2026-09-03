// Shared, deterministic locations and ports for the E15 browser-to-server
// fixture. playwright.config.ts, the global setup/teardown and the specs all
// resolve them from here so a port override moves every participant at once.
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

function port(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65_535) {
    throw new Error(`${name}=${raw} is not a TCP port`);
  }
  return value;
}

export const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
export const repoRoot = path.resolve(webRoot, '..');

// 7443/7455/7456 are the documented demo and hand-testing ports; the console
// fixture stays clear of them so a running demo never collides with a test run.
export const ports = {
  standalone: port('E15_STANDALONE_PORT', 7681),
  rbac: port('E15_RBAC_PORT', 7682),
  upstream: port('E15_UPSTREAM_PORT', 7683),
  foreign: port('E15_FOREIGN_PORT', 7684),
};

export const baseURLs = {
  standalone: `http://127.0.0.1:${ports.standalone}`,
  rbac: `http://127.0.0.1:${ports.rbac}`,
  upstream: `http://127.0.0.1:${ports.upstream}`,
  foreign: `http://127.0.0.1:${ports.foreign}`,
};

// Every byte the servers write lives under here, never in the repository.
export const runRoot = path.join(os.tmpdir(), `remount-e15-${ports.standalone}`);
export const statePath = path.join(runRoot, 'state.json');

const binaryName = process.platform === 'win32' ? 'remount.exe' : 'remount';

// node_modules/.cache is already ignored, so a locally built binary never
// becomes a tracked file. CI sets REMOUNT_BIN to the artifact it built.
export const defaultBinary = path.join(webRoot, 'node_modules', '.cache', 'remount', binaryName);

export function remountBinary() {
  return process.env.REMOUNT_BIN || defaultBinary;
}
