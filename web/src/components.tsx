import type { ComponentChildren } from 'preact';
import type { WorkspaceState } from './types';

export function Badge({ children, tone = 'neutral' }: { children: ComponentChildren; tone?: 'neutral' | 'good' | 'warn' | 'bad' }) {
  return <span class={`badge badge-${tone}`}>{children}</span>;
}

export function StateBadge({ state }: { state: WorkspaceState | string }) {
  const tone = state === 'claimed' || state === 'online' ? 'good' : state === 'failed' || state === 'quarantined' ? 'bad' : state === 'moving' || state === 'pending' ? 'warn' : 'neutral';
  return <Badge tone={tone}>{state}</Badge>;
}

export function Empty({ children }: { children: ComponentChildren }) { return <div class="empty">{children}</div>; }

export function ErrorNotice({ error }: { error?: Error }) {
  if (!error) return null;
  return <div class="notice notice-error" role="alert"><strong>Could not load data.</strong> {error.message}</div>;
}

export function ConfirmButton({ label, confirm, onConfirm, danger = false, disabled = false }: {
  label: string; confirm: string; onConfirm: () => void | Promise<void>; danger?: boolean; disabled?: boolean;
}) {
  const act = () => { if (window.confirm(confirm)) void onConfirm(); };
  return <button class={danger ? 'button danger' : 'button secondary'} type="button" onClick={act} disabled={disabled}>{label}</button>;
}

export function PageHeader({ eyebrow, title, actions }: { eyebrow?: ComponentChildren; title: string; actions?: ComponentChildren }) {
  return <header class="page-header"><div>{eyebrow && <p class="eyebrow">{eyebrow}</p>}<h1>{title}</h1></div><div class="actions">{actions}</div></header>;
}

export function Loading() { return <div class="loading" role="status"><span /> Loading current state…</div>; }

export function formatDate(value?: string) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString();
}

export function bytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 ** 2).toFixed(1)} MiB`;
}
