// Build the remount binary the Playwright fixture drives.
//
// The console suite is a browser-to-real-server proof, so it needs the actual
// Go binary. Set REMOUNT_BIN to reuse one that is already built (CI does this
// with the artifact from its own build step); otherwise this compiles into
// web/node_modules/.cache, which is already ignored.
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';

import { defaultBinary, repoRoot } from '../tests/e15/env.mjs';

if (process.env.REMOUNT_BIN) {
  if (!existsSync(process.env.REMOUNT_BIN)) {
    console.error(`REMOUNT_BIN=${process.env.REMOUNT_BIN} does not exist`);
    process.exit(1);
  }
  console.log(`remount binary: ${process.env.REMOUNT_BIN} (REMOUNT_BIN)`);
  process.exit(0);
}

mkdirSync(path.dirname(defaultBinary), { recursive: true });
const result = spawnSync('go', ['build', '-o', defaultBinary, './cmd/remount'], {
  cwd: repoRoot,
  stdio: 'inherit',
  env: { ...process.env, CGO_ENABLED: '0' },
});
if (result.error) {
  console.error(`go build failed: ${result.error.message}. Install Go 1.27+ or set REMOUNT_BIN.`);
  process.exit(1);
}
if (result.status !== 0) process.exit(result.status ?? 1);
console.log(`remount binary: ${defaultBinary}`);
