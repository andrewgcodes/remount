import type { Page, WebSocket } from '@playwright/test';

/**
 * signIn drives the console's own credential form. The console keeps the
 * bearer in memory only, so every reload needs a fresh sign-in and no test may
 * navigate with page.goto() once it is authenticated.
 */
export async function signIn(page: Page, credential: string): Promise<void> {
  await page.goto('/console/');
  await page.getByLabel('Operator credential').fill(credential);
  await page.getByRole('button', { name: 'Open console' }).click();
}

export interface SocketRecord {
  url: string;
  received: string[];
  sent: string[];
}

/**
 * recordSockets captures every terminal WebSocket the page opens, with both
 * the literal frame text and the base64-decoded chunk payload. The decoded
 * form matters: a leaked secret would travel inside `data`, where a scan of
 * the raw JSON would never see it.
 */
export function recordSockets(page: Page): SocketRecord[] {
  const sockets: SocketRecord[] = [];
  page.on('websocket', (socket: WebSocket) => {
    const record: SocketRecord = { url: socket.url(), received: [], sent: [] };
    sockets.push(record);
    socket.on('framereceived', (frame) => {
      const text = typeof frame.payload === 'string' ? frame.payload : frame.payload.toString('utf8');
      record.received.push(text, decodeChunk(text));
    });
    socket.on('framesent', (frame) => {
      const text = typeof frame.payload === 'string' ? frame.payload : frame.payload.toString('utf8');
      record.sent.push(text, decodeChunk(text));
    });
  });
  return sockets;
}

function decodeChunk(text: string): string {
  try {
    const value = JSON.parse(text) as { data?: string };
    if (typeof value.data !== 'string') return '';
    return Buffer.from(value.data, 'base64').toString('utf8');
  } catch {
    return '';
  }
}

/**
 * recordBodies captures the text of every HTTP response the page receives, so
 * a scan can cover what the server actually sent and not only what the DOM
 * chose to render. settle() waits for the bodies still being read, so the scan
 * never races the last response of the flow.
 */
export interface BodyRecorder {
  bodies: string[];
  settle(): Promise<void>;
}

export function recordBodies(page: Page): BodyRecorder {
  const bodies: string[] = [];
  const pending: Promise<unknown>[] = [];
  page.on('response', (response) => {
    pending.push(response.text().then((text) => { bodies.push(text); }).catch(() => { /* a redirect or an aborted body has nothing to scan */ }));
  });
  return {
    bodies,
    async settle() { await Promise.allSettled(pending.slice()); },
  };
}
