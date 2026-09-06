import { describe, expect, it } from "vitest";
import {
  EgressDeniedError,
  errorClasses,
  fromWire,
  OutputEvictedError,
  ProtocolError,
  REASONS,
  unmappedReasons,
} from "../src/index.js";

describe("typed protocol errors", () => {
  it("covers the whole generated reason vocabulary", () => {
    expect(REASONS.length).toBeGreaterThan(0);
    expect(unmappedReasons(REASONS)).toEqual([]);
    const table = errorClasses();
    expect([...table.keys()].sort()).toEqual([...REASONS].sort());
    for (const [reason, cls] of table) {
      expect(cls.reason).toBe(reason);
      expect(new cls("denied").constructor.prototype).toBeInstanceOf(ProtocolError);
    }
  });

  it("builds the typed class for a known reason", () => {
    const error = fromWire("denied", "egress_denied", "api.example.com is not bound");
    expect(error).toBeInstanceOf(EgressDeniedError);
    expect(error).toBeInstanceOf(ProtocolError);
    expect(error.code).toBe("denied");
    expect(error.reason).toBe("egress_denied");
    expect(error.message).toBe("denied: api.example.com is not bound");
  });

  it("falls back to the base class for an unknown reason", () => {
    // A server newer than this package sends a reason it has never heard of.
    const future = fromWire("denied", "a_reason_from_the_future", "nope");
    expect(future.constructor).toBe(ProtocolError);
    expect(future).not.toBeInstanceOf(EgressDeniedError);
    expect(future.reason).toBe("a_reason_from_the_future");

    const bare = fromWire("conflict", "", "workspace is pending");
    expect(bare.constructor).toBe(ProtocolError);
    expect(bare.reason).toBe("");
  });

  it("keeps the oldest replayable sequence on an eviction", () => {
    const evicted = fromWire("evicted", "output_evicted", "seq 3 is gone", 12);
    expect(evicted).toBeInstanceOf(OutputEvictedError);
    expect(evicted.oldest).toBe(12);
  });
});
