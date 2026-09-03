import type { RuntimeConfig } from './types';

const defaults: RuntimeConfig = { apiBase: '/v1', refreshMs: 5000 };

export function validateConfig(value: unknown, origin = window.location.origin): RuntimeConfig {
  if (!value || typeof value !== 'object') return defaults;
  const raw = value as Record<string, unknown>;
  const base = typeof raw.apiBase === 'string' ? raw.apiBase : defaults.apiBase;
  const parsed = new URL(base, origin);
  if (parsed.origin !== origin || parsed.username || parsed.password) {
    throw new Error('console apiBase must be same-origin and contain no credentials');
  }
  const refreshMs = typeof raw.refreshMs === 'number' && Number.isFinite(raw.refreshMs)
    ? Math.max(1000, Math.min(60_000, raw.refreshMs))
    : defaults.refreshMs;
  return { apiBase: parsed.pathname.replace(/\/$/, ''), refreshMs };
}

export async function loadConfig(): Promise<RuntimeConfig> {
  try {
    const response = await fetch(`${import.meta.env.BASE_URL}config.json`, {
      credentials: 'same-origin',
      cache: 'no-store',
    });
    if (!response.ok) return defaults;
    return validateConfig(await response.json());
  } catch (error) {
    if (error instanceof Error && error.message.includes('same-origin')) throw error;
    return defaults;
  }
}
