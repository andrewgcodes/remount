/**
 * Brokered credentials: bindings, session principals, and the audit trail.
 *
 * The workspace never holds a provider key. An operator defines a *binding* —
 * a credential plus the destinations, methods, paths and substitution location
 * it may be spent on — and the workspace receives only an opaque placeholder.
 * The node's broker substitutes the real value at the network edge and records
 * the decision.
 *
 * The functions here take any transport with a `call` method, and `Client`
 * exposes them as methods so they share its one reconnecting connection:
 *
 * ```ts
 * await client.createBinding({
 *   id: "b_openai", kind: "api_key", secret: key,
 *   destinations: ["api.openai.com"], ttl_sec: 900,
 * });
 * await client.createWorkspace({
 *   bindings: ["b_openai"],
 *   env: { OPENAI_API_KEY: "ref:b_openai",
 *          OPENAI_BASE_URL: "${REMOUNT_BROKER}/d/api.openai.com/v1" },
 * });
 * ```
 *
 * `secret` is write-only: it is accepted by `createBinding` and
 * `rotateBinding` and never appears in a response, an event, or a diagnostic,
 * so nothing here ever returns a credential.
 */

import { Decoder } from "cbor-x";
import type { BindingSpec, Event, PrincipalSessionCreateRes } from "./types.js";

const payloadDecoder = new Decoder({ mapsAsObjects: true });

/** The audit events the broker emits per request. None carries a credential. */
export const CREDENTIAL_EVENT_TYPES = ["cred.used", "egress.allowed", "egress.denied", "egress.redacted"];

/**
 * The lifecycle events a binding emits. They stream under the binding id
 * rather than a workspace, so they are read from the tenant-wide stream.
 */
export const BINDING_EVENT_TYPES = ["binding.created", "binding.rotated", "binding.revoked"];

/** The minimum transport these operations need. */
export interface CredentialTransport {
  call(op: string, body?: Record<string, any>, to?: string): Promise<Record<string, any>>;
}

/**
 * Selects credential-use and egress-decision events. Every field is optional.
 * `ws`, `binding` and `host` are applied by the control plane, so setting them
 * is materially cheaper than reading the whole log; `decision` is matched here
 * against the audit payload.
 */
export interface CredentialFilter {
  ws?: string;
  binding?: string;
  host?: string;
  decision?: string;
  since?: number;
}

/**
 * One recorded broker decision: who used which binding, for which workspace,
 * against which host, with what outcome, under which tenant, at what time. It
 * never carries a credential value.
 */
export interface CredentialEvent {
  seq: number;
  at: number;
  type: string;
  tenant: string;
  ws: string;
  generation: number;
  node: string;
  principal: string;
  binding: string;
  host: string;
  method: string;
  path: string;
  decision: string;
  reason: string;
  status: number;
  event: Event;
}

function idem(): string {
  return `idem_${globalThis.crypto.randomUUID().replaceAll("-", "")}`;
}

function text(value: unknown): string {
  return value === undefined || value === null ? "" : String(value);
}

function count(value: unknown): number {
  return value === undefined || value === null ? 0 : Number(value);
}

/** Decode one log entry, or return undefined when it is not a broker decision. */
export function decodeCredentialEvent(event: Record<string, any>): CredentialEvent | undefined {
  if (!CREDENTIAL_EVENT_TYPES.includes(text(event.type))) return undefined;
  let audit: Record<string, any> = {};
  if (event.payload) {
    try {
      const decoded = payloadDecoder.decode(event.payload);
      if (decoded && typeof decoded === "object") audit = decoded as Record<string, any>;
    } catch {
      // A payload this client cannot decode is still a real decision: report
      // what the envelope says rather than dropping the audit record.
      audit = {};
    }
  }
  return {
    seq: count(event.seq),
    at: count(event.at),
    type: text(event.type),
    tenant: text(event.tenant),
    ws: text(event.workspace || event.stream),
    generation: count(event.generation) || count(audit.generation),
    node: text(event.node),
    principal: text(event.principal),
    binding: text(audit.binding),
    host: text(audit.host),
    method: text(audit.method),
    path: text(audit.path),
    decision: text(audit.decision),
    reason: text(audit.reason),
    status: count(audit.status),
    event: event as Event,
  };
}

/**
 * Define a tenant-scoped brokered credential. `binding.secret` is write-only;
 * name `binding.source` instead to keep the credential in an external manager.
 * Exactly one of the two is set.
 */
export function createBinding(
  transport: CredentialTransport,
  binding: BindingSpec,
  idempotencyKey = idem(),
): Promise<BindingSpec> {
  return transport.call("binding.create", { binding, idem: idempotencyKey }) as Promise<BindingSpec>;
}

/**
 * Return the bindings visible to the caller's tenant, without secrets. Revoked
 * rows are retained so an audit can resolve a binding id seen in an older
 * event; they are returned only on request.
 */
export async function listBindings(
  transport: CredentialTransport,
  options: { tenant?: string; includeRevoked?: boolean } = {},
): Promise<BindingSpec[]> {
  const body: Record<string, any> = {};
  if (options.tenant) body.tenant = options.tenant;
  if (options.includeRevoked) body.include_revoked = true;
  const response = await transport.call("binding.list", body);
  return (response.bindings ?? []) as BindingSpec[];
}

/** Return one binding definition without its secret. */
export function getBinding(
  transport: CredentialTransport,
  id: string,
  tenant = "",
): Promise<BindingSpec> {
  const body: Record<string, any> = { id };
  if (tenant) body.tenant = tenant;
  return transport.call("binding.get", body) as Promise<BindingSpec>;
}

/**
 * Replace the credential behind a binding and bump its revision. A node
 * holding a lease minted from the previous revision re-leases within one renew
 * interval, after which the old secret is no longer substituted.
 */
export function rotateBinding(
  transport: CredentialTransport,
  id: string,
  options: { secret?: string; source?: string; tenant?: string; idempotencyKey?: string } = {},
): Promise<BindingSpec> {
  const body: Record<string, any> = { id, idem: options.idempotencyKey ?? idem() };
  if (options.tenant) body.tenant = options.tenant;
  if (options.secret) body.secret = options.secret;
  if (options.source) body.source = options.source;
  return transport.call("binding.rotate", body) as Promise<BindingSpec>;
}

/**
 * Permanently stop substitution for a binding. This is not provider-side
 * revocation: Remount stops substituting within one renew interval, and the
 * credential stays valid at the provider until it is rotated or deleted there.
 */
export function revokeBinding(
  transport: CredentialTransport,
  id: string,
  options: { reason?: string; tenant?: string; idempotencyKey?: string } = {},
): Promise<BindingSpec> {
  const body: Record<string, any> = { id, idem: options.idempotencyKey ?? idem() };
  if (options.tenant) body.tenant = options.tenant;
  if (options.reason) body.reason = options.reason;
  return transport.call("binding.revoke", body) as Promise<BindingSpec>;
}

/**
 * Mint an ephemeral principal and its workspace- and generation-bound
 * capability in one call. The returned token is delivered exactly once and is
 * never written to durable control state; it stops verifying when the
 * workspace moves, when the TTL passes, or when the principal is revoked.
 */
export function createSessionPrincipal(
  transport: CredentialTransport,
  workspace: string,
  options: { subject?: string; roles?: string[]; tenant?: string; ttlSec?: number; idempotencyKey?: string } = {},
): Promise<PrincipalSessionCreateRes> {
  const body: Record<string, any> = { ws: workspace, idem: options.idempotencyKey ?? idem() };
  if (options.tenant) body.tenant = options.tenant;
  if (options.subject) body.subject = options.subject;
  if (options.roles?.length) body.roles = options.roles;
  if (options.ttlSec) body.ttl_sec = options.ttlSec;
  return transport.call("principal.session.create", body) as Promise<PrincipalSessionCreateRes>;
}

/** Invalidate every credential and live workspace authority of one principal. */
export async function revokePrincipal(
  transport: CredentialTransport,
  principal: string,
  options: { tenant?: string; idempotencyKey?: string } = {},
): Promise<number> {
  const body: Record<string, any> = { principal, idem: options.idempotencyKey ?? idem() };
  if (options.tenant) body.tenant = options.tenant;
  const response = await transport.call("principal.revoke", body);
  return count(response.revision);
}

/**
 * Return the credential-use audit trail matching `filter`. The binding, host
 * and workspace filters are applied by the control plane; the decision filter
 * is applied here because it is a field of the audit payload rather than of
 * the log envelope.
 */
export async function credentialEvents(
  transport: CredentialTransport,
  filter: CredentialFilter = {},
): Promise<CredentialEvent[]> {
  const body: Record<string, any> = { from: filter.since ?? 0, types: CREDENTIAL_EVENT_TYPES };
  if (filter.ws) body.ws = filter.ws;
  if (filter.binding) body.binding = filter.binding;
  if (filter.host) body.host = filter.host;
  const response = await transport.call("events.tail", body);
  const out: CredentialEvent[] = [];
  for (const raw of (response.events ?? []) as Record<string, any>[]) {
    const decoded = decodeCredentialEvent(raw);
    if (!decoded) continue;
    if (filter.decision && decoded.decision !== filter.decision) continue;
    if (filter.host && decoded.host.toLowerCase() !== filter.host.toLowerCase()) continue;
    out.push(decoded);
  }
  return out;
}
