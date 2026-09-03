import { expect, test } from '@playwright/test';

import { baseURLs, loadState } from '../e15/state';
import { recordBodies, recordSockets, signIn } from './helpers';

// The workspace never holds a credential: it holds the placeholder
// "ref:<binding>", and the node's broker substitutes the real secret at the
// network edge. This spec proves the browser is on the far side of that
// boundary — it can see the binding's NAME everywhere and its VALUE nowhere.
//
// The scan is only worth something if it can find things, so it carries a
// positive control: a synthetic string that is deliberately visible and that
// every assertion below must locate before the absence of the secret counts.
const state = loadState();
const AUTH = { authorization: 'Bearer e15-standalone' };
const SECRET = state.upstreamSecret;
const CONTROL = state.controlString;
const PLACEHOLDER = `ref:${state.binding}`;

test('the browser sees the binding name and the placeholder, never the upstream secret', async ({ page }) => {
  expect(SECRET, 'the fixture must seed a distinctive synthetic secret').toMatch(/^sk-e15-[0-9a-f]{48}$/);

  const recorder = recordBodies(page);
  const sockets = recordSockets(page);
  const rendered: string[] = [];
  // page.content() serializes markup; a textarea's current value lives on the
  // DOM property, so field values are captured separately or an edited file
  // would escape the scan.
  const snapshot = async () => {
    rendered.push(await page.content());
    rendered.push(await page.evaluate(() => Array.from(document.querySelectorAll('input, textarea'))
      .map((element) => (element as HTMLInputElement | HTMLTextAreaElement).value).join('\n')));
  };

  await signIn(page, 'e15-standalone');
  await page.getByRole('link', { name: /E15 credential boundary/ }).click();
  await expect(page.getByRole('heading', { name: 'E15 credential boundary' })).toBeVisible();

  // 1. The workspace's own process environment, streamed live over the real
  //    terminal WebSocket. It holds the placeholder and nothing else.
  await page.getByRole('tab', { name: 'terminal' }).click();
  await page.getByLabel('Run command').fill('env');
  await page.getByRole('button', { name: 'Run' }).click();
  await expect(page.locator('.xterm')).toContainText(PLACEHOLDER);
  await snapshot();

  // 2. The env file the node rewrites on every materialize, read through the
  //    console's file surface.
  await page.getByRole('tab', { name: 'files' }).click();
  await page.getByLabel('Path').fill('/.remount');
  await page.getByRole('button', { name: 'Refresh' }).click();
  await page.getByRole('button', { name: /env/ }).click();
  await expect.poll(() => page.getByLabel('File contents').inputValue()).toContain(`REMOUNT_REF_E15=${PLACEHOLDER}`);
  await snapshot();

  // 3. The positive control: a file the browser is supposed to be able to read
  //    in full. If this does not surface, nothing below is evidence.
  await page.getByLabel('Path').fill('/');
  await page.getByRole('button', { name: 'Refresh' }).click();
  await page.getByRole('button', { name: /canary-control\.txt/ }).click();
  await expect.poll(() => page.getByLabel('File contents').inputValue()).toContain(CONTROL);
  await snapshot();

  // 4. The security timeline: a real leak_blocked decision, named by binding.
  await page.getByRole('link', { name: 'Security' }).click();
  await expect(page.getByRole('heading', { name: 'Security' })).toBeVisible();
  await expect(page.getByText('egress.denied').first()).toBeVisible();
  await expect(page.getByText(state.binding).first()).toBeVisible();
  await snapshot();

  // 5. A parked approve-mode request is released only by a durable decision
  //    the browser commits.
  const parkedBefore = await page.request.get(`${baseURLs.upstream}/__hits`);
  expect((await parkedBefore.json()).hits).toHaveLength(0);
  await expect(page.getByText(/^Egress to 127\.0\.0\.1:/)).toBeVisible();
  await page.getByRole('button', { name: 'Approve' }).click();
  await expect(page.getByText('No pending approvals.')).toBeVisible();
  await expect.poll(async () => {
    const response = await page.request.get(`${baseURLs.upstream}/__hits`);
    return (await response.json()).hits.length as number;
  }, { timeout: 120_000, message: 'the approved request must reach the upstream' }).toBeGreaterThanOrEqual(1);
  await snapshot();

  await page.getByRole('link', { name: 'Timeline' }).click();
  await expect(page.getByRole('heading', { name: 'Timeline' })).toBeVisible();
  await snapshot();

  // ---- the scan ---------------------------------------------------------
  await recorder.settle();
  const bodyText = recorder.bodies.join('\n');
  const socketText = sockets.flatMap((socket) => [...socket.received, ...socket.sent]).join('\n');
  const domText = rendered.join('\n');

  // Positive controls first: each channel demonstrably carries content the
  // scan can find, so an absence below is a real absence.
  expect(bodyText, 'response bodies must carry the control string').toContain(CONTROL);
  expect(bodyText, 'response bodies must name the binding').toContain(state.binding);
  expect(socketText, 'decoded terminal frames must carry the placeholder').toContain(PLACEHOLDER);
  expect(domText, 'the rendered DOM must carry the control string').toContain(CONTROL);

  // The secret itself never crossed the boundary.
  expect(bodyText).not.toContain(SECRET);
  expect(socketText).not.toContain(SECRET);
  expect(domText).not.toContain(SECRET);

  // Nor did it reach the host the binding does not cover: the placeholder
  // aimed there was refused before any byte left the node.
  const foreign = await (await page.request.get(`${baseURLs.foreign}/__hits`)).json();
  expect(foreign.hits, 'a placeholder aimed at a foreign host must not be forwarded').toHaveLength(0);
  expect(JSON.stringify(foreign)).not.toContain(SECRET);

  // And the console's own canonical event feed names the binding, not its value.
  const events = await page.request.get('/v1/console/events?types=egress.denied&limit=200', { headers: AUTH });
  const raw = await events.text();
  expect(raw).toContain('"decision":"leak_blocked"');
  expect(raw).toContain(state.binding);
  expect(raw).not.toContain(SECRET);
});
