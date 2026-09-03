export type WorkspaceState = 'pending' | 'claiming' | 'claimed' | 'sleeping' | 'moving' | 'quarantined' | 'failed' | 'destroyed';

export interface NodeSummary {
  id: string;
  state: string;
  labels: Record<string, string>;
  assignments: number;
  capacity: number;
  lastSeen: string;
}

export interface PoolSummary {
  id: string;
  vendor: string;
  desired: number;
  actual: number;
  min: number;
  max: number;
  state: string;
}

export interface WorkspaceSummary {
  id: string;
  name: string;
  tenant: string;
  state: WorkspaceState;
  node?: string;
  generation: number;
  updatedAt: string;
}

export interface SessionSummary {
  id: string;
  kind: 'exec' | 'pty' | 'port';
  command?: string[];
  startedAt: string;
  endedAt?: string;
  next: number;
  earliest: number;
}

export interface SnapshotSummary {
  id: string;
  artifact: string;
  generation: number;
  createdAt: string;
  bytes: number;
}

export interface GenerationSummary {
  generation: number;
  node?: string;
  state: string;
  at: string;
  eventSeq: number;
}

export interface WorkspaceDetail extends WorkspaceSummary {
  spec: { image?: string; labels?: Record<string, string>; security?: string };
  sessions: SessionSummary[];
  snapshots: SnapshotSummary[];
  generations: GenerationSummary[];
}

export interface FleetResponse {
  nodes: NodeSummary[];
  pools: PoolSummary[];
  workspaces: WorkspaceSummary[];
  observedAt: string;
}

export interface FileEntry {
  name: string;
  path: string;
  kind: 'file' | 'directory' | 'symlink';
  size: number;
  modifiedAt: string;
}

export interface EventRecord {
  seq: number;
  at: string;
  type: string;
  tenant: string;
  workspace?: string;
  session?: string;
  principal?: string;
  credential?: string;
  payload?: Record<string, unknown>;
}

export interface Approval {
  id: string;
  agent?: string;
  workspace: string;
  tenant: string;
  kind: 'egress' | 'tool_call' | 'elicitation';
  status: 'pending' | 'approved' | 'denied' | 'expired';
  prompt: string;
  options?: string[];
  createdAt: string;
}

export interface Usage {
  budget_id: string;
  tenant: string;
  principal?: string;
  binding?: string;
  window: string;
  requests: number;
  tokens: number;
  cost_micros: number;
  max_requests?: number;
  max_tokens?: number;
  max_cost_micros?: number;
}

export interface RuntimeConfig {
  apiBase: string;
  refreshMs: number;
}
