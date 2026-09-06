// Typed Remount protocol errors.
//
// A wire error carries a stable `code` and, when the code alone is ambiguous, a
// stable `reason` that refines it (`proto.Reason*` in the Go tree). This module
// gives every reason one error class so a caller can write
//
//   try { await client.run(ws, ["curl", "https://api.example.com"]); }
//   catch (error) { if (error instanceof EgressDeniedError) { /* ... */ } }
//
// instead of comparing strings. Every class extends ProtocolError, so existing
// `instanceof ProtocolError` handling keeps working and an unknown reason — a
// newer server than this package — degrades to the base class rather than to an
// unhandled case.
//
// The reason vocabulary itself is generated into ./types.ts from the protocol,
// so the table below is checked against the protocol rather than maintained
// beside it.

import type { Reason } from "./types.js";

/**
 * A stable Remount protocol error. `code` is the classification every peer
 * sends; `reason` refines it and may be empty. Match on `code` first and narrow
 * on `reason` (or on a subclass) only when the distinction matters. `oldest` is
 * set with the `evicted` code and names the oldest sequence still replayable.
 */
export class ProtocolError extends Error {
  /** The reason this class represents. Empty on the base class. */
  static readonly reason: string = "";

  readonly reason: string;

  constructor(public readonly code: string, message = "", public readonly oldest = 0, reason = "") {
    super(message ? `${code}: ${message}` : code);
    this.name = "ProtocolError";
    this.reason = reason || (new.target as typeof ProtocolError).reason;
  }
}

/** The principal lacks a role or tenant scope. */
export class PermissionDeniedError extends ProtocolError {
  static override readonly reason = "permission_denied";
}

/** The broker refused a destination or a placeholder. */
export class EgressDeniedError extends ProtocolError {
  static override readonly reason = "egress_denied";
}

/** Approve-mode egress is waiting for a durable approval decision. */
export class ApprovalRequiredError extends ProtocolError {
  static override readonly reason = "approval_required";
}

/** No credential binding covers the placeholder that was used. */
export class BindingMissingError extends ProtocolError {
  static override readonly reason = "binding_missing";
}

/** The grant or capability TTL passed; fetch a fresh one. */
export class GrantExpiredError extends ProtocolError {
  static override readonly reason = "grant_expired";
}

/** The principal, binding or grant was revoked. */
export class RevokedError extends ProtocolError {
  static override readonly reason = "revoked";
}

/** A tenant quota or hard budget is spent. */
export class QuotaExceededError extends ProtocolError {
  static override readonly reason = "quota_exceeded";
}

/** The workspace has not reached `ws.ready` yet. */
export class WorkspaceNotReadyError extends ProtocolError {
  static override readonly reason = "workspace_not_ready";
}

/** The workspace now lives on another node. */
export class WorkspaceMovedError extends ProtocolError {
  static override readonly reason = "workspace_moved";
}

/** The request names a stale workspace generation or controller epoch. */
export class GenerationMismatchError extends ProtocolError {
  static override readonly reason = "generation_mismatch";
}

/** The workspace backend cannot perform this operation. */
export class BackendUnsupportedError extends ProtocolError {
  static override readonly reason = "backend_unsupported";
}

/** The requested session range is no longer retained. */
export class OutputEvictedError extends ProtocolError {
  static override readonly reason = "output_evicted";
}

/** A lease or idle deadline already fired. */
export class LifecycleDeadlineExpiredError extends ProtocolError {
  static override readonly reason = "lifecycle_deadline_expired";
}

/** The computer session's browser exited. */
export class BrowserCrashedError extends ProtocolError {
  static override readonly reason = "browser_crashed";
}

/** No display or CDP endpoint could be reached. */
export class DisplayUnavailableError extends ProtocolError {
  static override readonly reason = "display_unavailable";
}

/** A computer input action was malformed. */
export class InputRejectedError extends ProtocolError {
  static override readonly reason = "input_rejected";
}

/** Egress policy blocked a browser navigation. */
export class NavigationDeniedError extends ProtocolError {
  static override readonly reason = "navigation_denied";
}

/** A persisted browser profile could not be opened. */
export class ProfileCorruptError extends ProtocolError {
  static override readonly reason = "profile_corrupt";
}

/** Policy refused a browser download. */
export class DownloadBlockedError extends ProtocolError {
  static override readonly reason = "download_blocked";
}

/** No node currently satisfies the requested runtime profile. */
export class ProfileUnschedulableError extends ProtocolError {
  static override readonly reason = "profile_unschedulable";
}

const CLASSES = [
  ApprovalRequiredError,
  BackendUnsupportedError,
  BindingMissingError,
  BrowserCrashedError,
  DisplayUnavailableError,
  DownloadBlockedError,
  EgressDeniedError,
  GenerationMismatchError,
  GrantExpiredError,
  InputRejectedError,
  LifecycleDeadlineExpiredError,
  NavigationDeniedError,
  OutputEvictedError,
  PermissionDeniedError,
  ProfileCorruptError,
  ProfileUnschedulableError,
  QuotaExceededError,
  RevokedError,
  WorkspaceMovedError,
  WorkspaceNotReadyError,
] as const;

const BY_REASON = new Map<string, typeof ProtocolError>(CLASSES.map((cls) => [cls.reason, cls]));

/** The reason-to-class table, for tests and tooling. */
export function errorClasses(): Map<string, typeof ProtocolError> {
  return new Map(BY_REASON);
}

/**
 * Build the typed error for a wire failure. An empty or unrecognised reason
 * yields the base ProtocolError carrying the reason verbatim, so a newer server
 * never produces an unconstructable class in an older client.
 */
export function fromWire(code: string, reason = "", message = "", oldest = 0): ProtocolError {
  const cls = BY_REASON.get(reason);
  if (!cls) return new ProtocolError(code, message, oldest, reason);
  return new cls(code, message, oldest, reason);
}

/** Reasons the protocol declares that this package has no class for. */
export function unmappedReasons(reasons: readonly Reason[]): string[] {
  return reasons.filter((reason) => !BY_REASON.has(reason));
}
