// Reap every process group the fixture started and delete the run directory.
// Teardown reads the recorded pids rather than relying on module state, so it
// works even when Playwright loads setup and teardown separately.
import { promises as fs } from 'node:fs';

import { runRoot, statePath } from './env.mjs';
import { readState, stopGroup } from './harness.mjs';

export default async function globalTeardown() {
  let state;
  try { state = await readState(statePath); } catch { state = undefined; }
  for (const pid of state?.pids ?? []) stopGroup(pid);
  if (process.env.E15_KEEP_RUN_DIR === '1') {
    process.stdout.write(`E15 run directory kept at ${runRoot}\n`);
    return;
  }
  await fs.rm(runRoot, { recursive: true, force: true }).catch(() => {});
}
