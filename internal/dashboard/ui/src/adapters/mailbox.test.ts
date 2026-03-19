import assert from "node:assert/strict";
import test from "node:test";
import { adaptMailbox } from "./mailbox.ts";

test("adaptMailbox derives summary counts from generic mailbox items", () => {
  const out = adaptMailbox([
    { id: "m1", status: "pending", priority: "critical", summary: "Need review" },
    { id: "m2", status: "approved", priority: "normal", summary: "Approved" },
    { id: "m3", status: "deferred", priority: "low", summary: "Deferred" },
  ]);

  assert.equal(out.items.length, 3);
  assert.equal(out.summary?.pending, 1);
  assert.equal(out.summary?.approved, 1);
  assert.equal(out.summary?.deferred, 1);
  assert.equal(out.summary?.decided, 2);
});
