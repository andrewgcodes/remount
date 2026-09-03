import { describe, expect, it, vi } from 'vitest';
import { loadConfig, validateConfig } from '../src/config';

describe('runtime config', () => {
  it('accepts only bounded same-origin values', () => {
    expect(validateConfig({ apiBase: '/control/v1/', refreshMs: 50 }, 'https://cp.example')).toEqual({ apiBase: '/control/v1', refreshMs: 1000 });
    expect(validateConfig({ apiBase: '/v1', refreshMs: 99_999 }, 'https://cp.example').refreshMs).toBe(60_000);
  });

  it('rejects remote or credential-bearing API roots', () => {
    expect(() => validateConfig({ apiBase: 'https://evil.example/v1' }, 'https://cp.example')).toThrow('same-origin');
    expect(() => validateConfig({ apiBase: 'https://token@cp.example/v1' }, 'https://cp.example')).toThrow('credentials');
  });

  it('uses safe defaults when the optional config is absent', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 404 })));
    await expect(loadConfig()).resolves.toEqual({ apiBase: '/v1', refreshMs: 5000 });
    vi.unstubAllGlobals();
  });
});
