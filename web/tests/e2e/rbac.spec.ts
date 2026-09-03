import { expect, test } from '@playwright/test';

import { loadState } from '../e15/state';
import { signIn } from './helpers';

// Standalone mode promotes every credential to operator+admin
// (internal/server/api_console.go), so roles are only observable against a
// production-mode server. These bearers were minted by that server's own
// identity manager: an operator and a viewer in tenant "acme", plus a string
// that was never issued at all.
const state = loadState();

test.describe.configure({ mode: 'serial' });

test('an operator may read the fleet and complete a mutation', async ({ page }) => {
  await signIn(page, state.tokens.operator);
  await expect(page.getByRole('heading', { name: 'Fleet' })).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);

  const name = `E15 rbac operator ${Date.now()}`;
  await page.getByRole('button', { name: 'New workspace' }).click();
  await page.getByLabel('Name').fill(name);
  await page.getByRole('button', { name: 'Create' }).click();

  await expect(page.getByRole('heading', { name })).toBeVisible();
  await expect.poll(() => page.evaluate(() => window.location.hash)).toMatch(/#\/workspaces\/ws_/);
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('a viewer is refused the operator surface but keeps its read scope', async ({ page }) => {
  await signIn(page, state.tokens.viewer);

  // The fleet view is operator-scoped, so the control plane answers before any
  // row is rendered. This is authorization, not authentication.
  await expect(page.locator('#main')).toContainText('role does not permit action');

  const name = `E15 rbac viewer ${Date.now()}`;
  await page.getByRole('button', { name: 'New workspace' }).click();
  await page.getByLabel('Name').fill(name);
  await page.getByRole('button', { name: 'Create' }).click();

  // The console's own operator pre-check refuses the mutation before the SDK
  // client is even acquired (internal/server/api_console.go consoleClient).
  await expect(page.locator('#main')).toContainText('operator role required');
  await expect(page.getByRole('heading', { name })).toHaveCount(0);
  expect(await page.evaluate(() => window.location.hash)).not.toMatch(/#\/workspaces\//);

  // The refused mutation leaves the dialog open with the operator's input
  // intact, so dismiss it before navigating.
  await page.getByRole('button', { name: 'Cancel' }).click();

  // The same credential still reads the canonical audit log, which is what
  // separates a denied role from a rejected credential.
  await page.getByRole('link', { name: 'Timeline' }).click();
  await expect(page.getByRole('heading', { name: 'Timeline' })).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('a credential the server never issued is refused on every surface', async ({ page }) => {
  await signIn(page, state.tokens.denied);
  await expect(page.locator('#main')).toContainText('invalid credential');

  await page.getByRole('link', { name: 'Timeline' }).click();
  await expect(page.getByRole('heading', { name: 'Timeline' })).toBeVisible();
  await expect(page.locator('#main')).toContainText('invalid credential');

  await page.getByRole('link', { name: 'Security' }).click();
  await expect(page.locator('#main')).toContainText('invalid credential');
});
