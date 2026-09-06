import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { expect, it } from 'vitest';

it('gives panels a readable surface in both color schemes', () => {
  const styles = readFileSync(resolve(process.cwd(), 'src/styles.css'), 'utf8');
  expect(styles).toMatch(/--panel:\s*rgb\(17 23 34 \/ 88%\)/);
  expect(styles).toMatch(/prefers-color-scheme:\s*light[\s\S]*--panel:\s*rgb\(255 255 255 \/ 88%\)/);
  expect(styles).toMatch(/\.panel\s*\{[^}]*background:\s*var\(--panel\)/);
});
