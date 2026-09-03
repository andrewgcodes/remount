import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { promises as fs } from 'node:fs';
import path from 'node:path';

const root = new URL('../', import.meta.url);
const dist = new URL('./dist/', root);
const checksumFile = new URL('./dist.sha256', root);
const maxGzipBytes = 1_500_000;

async function walk(directory, prefix = '') {
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    const relative = path.posix.join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...await walk(new URL(`${entry.name}/`, directory), relative));
    else files.push(relative);
  }
  return files;
}

const files = await walk(dist);
const lines = [];
let gzipBytes = 0;
for (const file of files) {
  const content = await fs.readFile(new URL(file, dist));
  gzipBytes += gzipSync(content, { level: 9 }).byteLength;
  lines.push(`${createHash('sha256').update(content).digest('hex')}  dist/${file}`);
}
const manifest = `${lines.join('\n')}\n`;
if (process.argv.includes('--write')) await fs.writeFile(checksumFile, manifest);
else {
  const committed = await fs.readFile(checksumFile, 'utf8');
  if (committed !== manifest) throw new Error('web/dist differs from web/dist.sha256; run npm run build and commit both');
}
if (gzipBytes > maxGzipBytes) throw new Error(`console is ${gzipBytes} gzipped bytes, over the ${maxGzipBytes} byte limit`);
console.log(`console assets: ${gzipBytes} / ${maxGzipBytes} gzipped bytes`);
