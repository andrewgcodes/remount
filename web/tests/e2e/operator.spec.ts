import { expect, test } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test('E15 operator flow: create, exec, files, snapshot, move, reattach, approve', async ({ page }) => {
  const terminalURLs: string[] = [];
  let cut = false;
  await page.routeWebSocket(/\/terminal(?:\?|$)/, (socket) => {
    terminalURLs.push(socket.url());
    expect(socket.protocols()).toContain('remount.bearer.b3BlcmF0b3ItdG9rZW4');
    socket.onMessage((message) => {
      if (typeof message === 'string' && message.startsWith('{')) return;
      socket.send(JSON.stringify({ type: 'chunk', seq: 1, data: Buffer.from(`echo:${String(message)}`).toString('base64') }));
    });
    socket.send(JSON.stringify({ type: 'open', session: 's_console', next: 0 }));
    socket.send(JSON.stringify({ type: 'chunk', seq: 0, data: Buffer.from('$ console attached\r\n').toString('base64') }));
    if (!cut) {
      cut = true;
      setTimeout(() => void socket.close({ code: 1012, reason: 'simulated node move' }), 100);
    }
  });
  await page.goto('/console/');
  await page.getByLabel('Operator credential').fill('operator-token');
  await page.getByRole('button', { name: 'Open console' }).click();
  await expect(page.getByRole('heading', { name: 'Fleet' })).toBeVisible();
  const audit = await new AxeBuilder({ page }).analyze();
  expect(audit.violations.filter((item) => item.impact === 'critical' || item.impact === 'serious')).toEqual([]);
  await page.getByRole('button', { name: 'New workspace' }).click();
  await page.getByLabel('Name').fill('E15 workspace');
  await page.getByRole('button', { name: 'Create' }).click();
  await expect(page.getByRole('heading', { name: 'E15 workspace' })).toBeVisible();

  await page.getByRole('tab', { name: 'terminal' }).click();
  await page.getByLabel('Run command').fill('printf ready');
  await page.getByRole('button', { name: 'Run' }).click();
  await expect(page.locator('.xterm')).toContainText('console attached');
  await expect.poll(() => terminalURLs.some((value) => value.includes('from=1'))).toBe(true);

  await page.getByRole('tab', { name: 'files' }).click();
  await page.getByRole('button', { name: /hello\.txt/ }).click();
  await expect(page.getByLabel('File contents')).toHaveValue('hello\n');
  await page.getByLabel('File contents').fill('updated\n');
  await page.getByRole('button', { name: 'Save' }).click();

  await page.getByRole('tab', { name: 'overview' }).click();
  await page.getByRole('button', { name: 'Snapshot' }).click();
  await expect(page.getByText('snap_one')).toBeVisible();
  await page.getByLabel('Move target node').fill('n_west');
  await page.getByRole('button', { name: 'Move' }).click();
  await expect(page.getByText('Generation 2')).toBeVisible();

  await page.getByRole('tab', { name: 'terminal' }).click();
  await page.getByLabel('Replay').selectOption('10');
  await expect(page.locator('.xterm')).toContainText('console attached');

  await page.getByRole('link', { name: 'Security' }).click();
  await expect(page.getByText('Allow api.openai.com?')).toBeVisible();
  await expect(page.getByText('leak_blocked')).toBeVisible();
  await page.getByRole('button', { name: 'Approve' }).click();
  await expect(page.getByText('No pending approvals.')).toBeVisible();
});
