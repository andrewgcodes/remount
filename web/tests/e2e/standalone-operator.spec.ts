import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';

import { loadState } from '../e15/state';
import { recordSockets, signIn } from './helpers';

// Every assertion here runs against `remount standalone`: a real control
// plane, two real enrolled nodes, a real process backend and the console the
// Go binary embeds and sha256-verifies at start-up.
const state = loadState();
const AUTH = { authorization: 'Bearer e15-standalone' };

test.describe.configure({ mode: 'serial' });

test('the fleet the browser renders is the real fleet, and creating a workspace is a real mutation', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'light' });
  await signIn(page, 'e15-standalone');
  await expect(page.getByRole('heading', { name: 'Fleet' })).toBeVisible();

  // Both node ids were minted by the real enrollment, not by a fixture.
  for (const node of state.nodes) {
    expect(node).toMatch(/^n_/);
    await expect(page.getByText(node).first()).toBeVisible();
  }
  await expect(page.getByText('online').first()).toBeVisible();

  const audit = await new AxeBuilder({ page }).analyze();
  expect(audit.violations.filter((item) => item.impact === 'critical' || item.impact === 'serious')).toEqual([]);

  const name = `E15 browser created ${Date.now()}`;
  await page.getByRole('button', { name: 'New workspace' }).click();
  await page.getByLabel('Name').fill(name);
  await page.getByRole('button', { name: 'Create' }).click();

  await expect(page.getByRole('heading', { name })).toBeVisible();
  await expect.poll(() => page.evaluate(() => window.location.hash)).toMatch(/#\/workspaces\/ws_/);

  const id = (await page.evaluate(() => window.location.hash)).replace('#/workspaces/', '');
  const detail = await page.request.get(`/v1/console/workspaces/${id}`, { headers: AUTH });
  expect(detail.status()).toBe(200);
  expect((await detail.json()).name).toBe(name);
});

test('the terminal streams real PTY bytes over the real terminal WebSocket, and replay comes from the server', async ({ page }) => {
  const sockets = recordSockets(page);
  await signIn(page, 'e15-standalone');
  await page.getByRole('link', { name: /E15 operator flow/ }).click();
  await expect(page.getByRole('heading', { name: 'E15 operator flow' })).toBeVisible();

  const marker = `e15-real-pty-${Date.now()}`;
  await page.getByRole('tab', { name: 'terminal' }).click();
  await page.getByLabel('Run command').fill(`printf "%s\\n" "${marker} with spaces"`);
  await page.getByRole('button', { name: 'Run' }).click();
  await expect(page.locator('.xterm')).toContainText(`${marker} with spaces`);

  // The socket really went to the server's terminal endpoint for this
  // workspace, and the bytes arrived as base64 chunk frames from the node.
  await expect.poll(() => sockets.length).toBeGreaterThan(0);
  const first = sockets[0]!;
  expect(first.url).toContain(`/v1/console/workspaces/${state.opsWorkspace}/terminal`);
  expect(first.url.startsWith('ws://127.0.0.1:')).toBe(true);
  expect(first.received.join('\n')).toContain('"type":"chunk"');
  expect(first.received.join('\n')).toContain(marker);

  // Changing the replay window re-dials, and the server replays the session
  // log from its own store rather than from anything the page kept.
  await page.getByLabel('Replay').selectOption('60');
  await expect.poll(() => sockets.length).toBeGreaterThan(1);
  const replayed = sockets[sockets.length - 1]!;
  expect(replayed.url).toContain('since=');
  await expect.poll(() => replayed.received.join('\n')).toContain(marker);
  await expect(page.locator('.xterm')).toContainText(marker);
});

test('files, snapshot and a cross-node move are real control-plane operations', async ({ page }) => {
  await signIn(page, 'e15-standalone');
  await page.getByRole('link', { name: /E15 operator flow/ }).click();

  await page.getByRole('tab', { name: 'files' }).click();
  await page.getByRole('button', { name: /hello\.txt/ }).click();
  await expect(page.getByLabel('File contents')).toHaveValue('hello\n');

  const written = `edited by the browser ${Date.now()}\n`;
  await page.getByLabel('File contents').fill(written);
  await page.getByRole('button', { name: 'Save' }).click();
  // The proof of the write is the node's own filesystem, read back out of
  // band, not the textarea the browser just typed into.
  await expect.poll(async () => {
    const response = await page.request.get(
      `/v1/console/workspaces/${state.opsWorkspace}/file?path=%2Fhello.txt`,
      { headers: AUTH },
    );
    return (await response.json()).content as string;
  }).toBe(written);

  await page.getByRole('tab', { name: 'overview' }).click();
  await page.getByRole('button', { name: 'Snapshot' }).click();
  await expect(page.getByText(/^art_sha256:/).first()).toBeVisible();

  const before = await (await page.request.get(`/v1/console/workspaces/${state.opsWorkspace}`, { headers: AUTH })).json();
  const target = state.nodes.find((node) => node !== before.node);
  expect(target, 'the fixture must enroll a second node for the move to be real').toBeTruthy();

  await page.getByLabel('Move target node').fill(target!);
  await page.getByRole('button', { name: 'Move' }).click();
  await expect(page.getByText('Generation 2')).toBeVisible();

  const after = await (await page.request.get(`/v1/console/workspaces/${state.opsWorkspace}`, { headers: AUTH })).json();
  expect(after.generation).toBeGreaterThan(before.generation);
  await expect.poll(async () => {
    const response = await page.request.get(`/v1/console/workspaces/${state.opsWorkspace}`, { headers: AUTH });
    const value = await response.json();
    return `${value.state}:${value.node}`;
  }).toBe(`claimed:${target}`);
});
