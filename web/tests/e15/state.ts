// Typed access to the state written by tests/e15/global-setup.mjs.
//
// The port defaults and the run-directory formula are duplicated from
// tests/e15/env.mjs because tsconfig sets allowJs:false, so a .ts spec cannot
// import the .mjs module. loadState() re-checks the values it reads against
// the ones the setup recorded, so drift fails immediately and by name rather
// than silently pointing the browser at the wrong server.
import { readFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function port(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65_535) throw new Error(`${name}=${raw} is not a TCP port`);
  return value;
}

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

export const statePath = path.join(os.tmpdir(), `remount-e15-${ports.standalone}`, 'state.json');

export interface E15State {
  runRoot: string;
  binary: string;
  baseURLs: typeof baseURLs;
  binding: string;
  upstreamSecret: string;
  controlString: string;
  nodes: string[];
  opsWorkspace: string;
  secureWorkspace: string;
  approval: string;
  tokens: { operator: string; viewer: string; denied: string };
}

export function loadState(): E15State {
  let raw: string;
  try {
    raw = readFileSync(statePath, 'utf8');
  } catch {
    throw new Error(`${statePath} is missing; the E15 global setup did not run or did not finish`);
  }
  const state = JSON.parse(raw) as E15State;
  for (const key of Object.keys(baseURLs) as (keyof typeof baseURLs)[]) {
    if (state.baseURLs[key] !== baseURLs[key]) {
      throw new Error(`E15 fixture drift: setup used ${key}=${state.baseURLs[key]}, the specs expect ${baseURLs[key]}`);
    }
  }
  return state;
}
