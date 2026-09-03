// Process and readiness helpers for the E15 fixture. Everything started here
// is recorded by process-group id so global teardown can reap it even though
// Playwright may run teardown from a different module instance.
import { spawn } from 'node:child_process';
import { createWriteStream, existsSync } from 'node:fs';
import { promises as fs } from 'node:fs';
import path from 'node:path';

export const groups = [];

export function start(name, command, args, options = {}) {
  if (!existsSync(command) && path.isAbsolute(command)) {
    throw new Error(`${name}: ${command} does not exist`);
  }
  const log = createWriteStream(options.logFile, { flags: 'a' });
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: { ...process.env, ...options.env },
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true, // its own process group, so one kill reaps grandchildren
  });
  child.stdout.pipe(log);
  child.stderr.pipe(log);
  child.on('error', (error) => log.write(`${name}: spawn failed: ${error.message}\n`));
  groups.push(child.pid);
  return { name, child, logFile: options.logFile };
}

export function stopGroup(pid) {
  if (!pid) return;
  for (const signal of ['SIGTERM', 'SIGKILL']) {
    try { process.kill(-pid, signal); } catch { /* already gone */ }
    try { process.kill(pid, signal); } catch { /* already gone */ }
  }
}

async function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

// waitFor is the only place this fixture is allowed to block. Every caller
// passes a bound deadline and a label, so a hang names what never became true
// instead of stalling the run.
export async function waitFor(label, predicate, { timeoutMs = 60_000, intervalMs = 250 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last = 'no attempt completed';
  while (Date.now() < deadline) {
    try {
      const value = await predicate();
      if (value !== undefined && value !== false && value !== null) return value;
      last = 'predicate was not satisfied';
    } catch (error) {
      last = error instanceof Error ? error.message : String(error);
    }
    await sleep(intervalMs);
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}: ${last}`);
}

export async function getJSON(url, token) {
  const headers = { accept: 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const response = await fetch(url, { headers });
  const text = await response.text();
  if (!response.ok) throw new Error(`GET ${url} -> ${response.status} ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

// run executes the remount CLI once and fails loudly. The CLI is the only
// supported way to seed bindings, typed egress rules and workspace env, none
// of which the console's create endpoint accepts.
export function run(binary, args, env, { timeoutMs = 120_000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error(`remount ${args.join(' ')} timed out`)); }, timeoutMs);
    child.stdout.on('data', (chunk) => { out += chunk; });
    child.stderr.on('data', (chunk) => { err += chunk; });
    child.on('error', (error) => { clearTimeout(timer); reject(error); });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) reject(new Error(`remount ${args.join(' ')} exited ${code}\n${out}\n${err}`));
      else resolve({ stdout: out, stderr: err });
    });
  });
}

export async function writeState(statePath, state) {
  await fs.mkdir(path.dirname(statePath), { recursive: true });
  await fs.writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
}

export async function readState(statePath) {
  return JSON.parse(await fs.readFile(statePath, 'utf8'));
}
