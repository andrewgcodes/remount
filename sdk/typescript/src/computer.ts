// Browser and computer-use sessions over the Remount port substrate.
//
// A computer is a Chrome DevTools Protocol conversation the node holds with a
// browser inside the workspace; nothing here talks to the browser directly.
// See ADR 0088 for why the node owns that conversation.
//
// Coordinates are CSS pixels with the origin at the top-left of the viewport
// declared at create, which is the same rectangle a screenshot is clipped to.
import type { Client } from "./client.js";
import type {
  ComputerAction,
  ComputerDownload,
  ComputerGetRes,
  ComputerLaunch,
  ComputerNavigateRes,
  ComputerScreenshotRes,
  ComputerViewport,
} from "./types.js";

/** The DevTools port a computer uses when create names none. */
export const DEFAULT_COMPUTER_PORT = 9222;
/** The viewport a computer uses when create declares none. */
export const DEFAULT_COMPUTER_VIEWPORT: ComputerViewport = { w: 1280, h: 720 };
/** The profile directory name a computer uses when create names none. */
export const DEFAULT_COMPUTER_PROFILE = "default";

/** Modifier bits for an action; the values are CDP's own. */
export const MODIFIER_ALT = 1;
export const MODIFIER_CTRL = 2;
export const MODIFIER_META = 4;
export const MODIFIER_SHIFT = 8;

/** Action kinds accepted by computer.input. */
export const ACTION_CLICK = "click";
export const ACTION_TYPE = "type";
export const ACTION_KEY = "key";
export const ACTION_SCROLL = "scroll";
export const ACTION_DRAG = "drag";
export const ACTION_MOVE = "move";

export interface ComputerCreateOptions {
  launch?: ComputerLaunch;
  viewport?: ComputerViewport;
  profile?: string;
  env?: Record<string, string>;
  idempotencyKey?: string;
}

function idem(): string {
  return `idem_${Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

/**
 * One browser conversation, addressed by workspace and computer id.
 *
 * The input sequence this handle keeps is what makes a retried action batch
 * idempotent: the node drops any sequence at or below the last it applied, so
 * a click replayed after a dropped connection is never a second click.
 */
export class Computer {
  session = "";
  cdpVersion = "";
  viewport: ComputerViewport = {};
  private iseq = 0;
  // A handle addressed by id has to learn the node's input sequence before it
  // acts. A sequence restarted at one is one the node has already applied, and
  // the action would be correctly dropped.
  private synced = false;
  private inputTail: Promise<unknown> = Promise.resolve();

  constructor(readonly client: Client, readonly workspace: string, readonly id: string) {}

  /** Start, or attach to, a browser in the workspace. */
  static async create(client: Client, workspace: string, options: ComputerCreateOptions = {}): Promise<Computer> {
    const body: Record<string, any> = { ws: workspace, idem: options.idempotencyKey ?? idem() };
    if (options.launch) body.launch = options.launch;
    if (options.viewport) body.viewport = options.viewport;
    if (options.profile) body.profile = options.profile;
    if (options.env) body.env = options.env;
    const response = await client.nodeCall(workspace, "computer.create", body);
    const computer = new Computer(client, workspace, String(response.computer));
    computer.session = String(response.s ?? "");
    computer.cdpVersion = String(response.cdp ?? "");
    computer.viewport = (response.viewport ?? {}) as ComputerViewport;
    computer.synced = true;
    return computer;
  }

  /**
   * Report the computer's state, so a reconnecting caller can tell a live
   * browser from one that crashed while it was away.
   */
  async get(): Promise<ComputerGetRes> {
    const response = await this.call("computer.get", {});
    this.viewport = (response.viewport ?? this.viewport) as ComputerViewport;
    this.session = String(response.s ?? this.session);
    this.iseq = Math.max(this.iseq, Number(response.last_iseq ?? 0));
    this.synced = true;
    return response as ComputerGetRes;
  }

  /** Capture the viewport as a PNG. */
  async screenshot(): Promise<ComputerScreenshotRes> {
    return (await this.call("computer.screenshot", {})) as ComputerScreenshotRes;
  }

  /**
   * Apply a batch of actions under one input sequence. The batch is validated
   * whole by the node, so a malformed action cannot leave half a batch applied.
   *
   * A handle that did not create the computer reads the node's sequence first:
   * without that, every such handle would start at one and every action after
   * the first would be dropped as a duplicate.
   */
  async input(...actions: ComputerAction[]): Promise<void> {
    const operation = this.inputTail.then(async () => {
      if (!this.synced) await this.get();
      const sequence = ++this.iseq;
      await this.call("computer.input", { iseq: sequence, actions });
    });
    this.inputTail = operation.catch(() => {});
    await operation;
  }

  /** Press and release the left button at a viewport coordinate. */
  async click(x: number, y: number, modifiers = 0): Promise<void> {
    await this.input({ kind: ACTION_CLICK, x, y, mod: modifiers });
  }

  /** Move the pointer without pressing a button. */
  async move(x: number, y: number): Promise<void> {
    await this.input({ kind: ACTION_MOVE, x, y });
  }

  /**
   * Insert text into whatever has focus. This is Input.insertText, the one
   * path that behaves the same in an ordinary field and a contenteditable
   * region and does not depend on a keyboard layout the image may not carry.
   */
  async type(text: string): Promise<void> {
    await this.input({ kind: ACTION_TYPE, text });
  }

  /** Press one named key, such as Enter or ArrowDown. */
  async key(key: string, modifiers = 0): Promise<void> {
    await this.input({ kind: ACTION_KEY, key, mod: modifiers });
  }

  /** Dispatch a wheel event at a viewport coordinate. */
  async scroll(x: number, y: number, dx: number, dy: number): Promise<void> {
    await this.input({ kind: ACTION_SCROLL, x, y, dx, dy });
  }

  /** Press at one coordinate, move, and release at another. */
  async drag(x: number, y: number, toX: number, toY: number): Promise<void> {
    await this.input({ kind: ACTION_DRAG, x, y, tox: toX, toy: toY });
  }

  /**
   * Load a URL and wait for the page's load event or the node's timeout; the
   * response says which of the two happened. A destination the egress policy
   * refuses rejects with code "denied" and reason "navigation_denied". That
   * policy is per host, never per URL: the broker's CONNECT tunnel is opaque
   * by design and cannot see a path.
   */
  async navigate(url: string, idempotencyKey = idem()): Promise<ComputerNavigateRes> {
    return (await this.call("computer.navigate", { url, idem: idempotencyKey })) as ComputerNavigateRes;
  }

  /**
   * Run an expression in the page and return its decoded JSON value. The value
   * is page-controlled data: decode it, never execute it.
   */
  async eval(expression: string): Promise<unknown> {
    const response = await this.call("computer.eval", { expr: expression });
    const raw = response.value as Uint8Array | undefined;
    if (!raw || raw.byteLength === 0) return null;
    return JSON.parse(new TextDecoder().decode(raw));
  }

  /**
   * List what the browser fetched, and the artifact each became. A file that
   * finished but could not be published is reported with state "blocked" and a
   * reason, never omitted.
   */
  async downloads(): Promise<ComputerDownload[]> {
    const response = await this.call("computer.downloads", {});
    return (response.downloads ?? []) as ComputerDownload[];
  }

  /**
   * End the conversation and kill a browser the node spawned. Closing a
   * computer that is already gone succeeds: that is the postcondition the
   * caller asked for.
   */
  async close(idempotencyKey = idem()): Promise<void> {
    await this.call("computer.close", { idem: idempotencyKey });
  }

  private call(op: string, body: Record<string, any>): Promise<Record<string, any>> {
    return this.client.nodeCall(this.workspace, op, { ...body, ws: this.workspace, computer: this.id });
  }
}
