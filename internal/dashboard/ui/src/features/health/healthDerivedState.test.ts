import test from "node:test";
import assert from "node:assert/strict";
import { deriveHealthState } from "./useHealthDerivedState.ts";

test("deriveHealthState surfaces hotspot diagnostics", () => {
  const result = deriveHealthState({
    health: {
      auth: { auth_errors_1h: 1, auth_errors_24h: 2 },
      workflow_audit: { warnings: ["stage drift"] },
      vertical_health: [{ vertical_id: "v-1", slug: "alpha", health_status: "degraded", deploy_status: "paused" }],
    },
    contractWorkflow: { version: "2.1.0" },
    contractPlatform: { version: "2.1.0", compliance_rule_count: 4 },
    contractVerification: { count: 6, priority_counts: { must_pass: 3 } },
  });

  assert.equal(result.hotspots.length, 4);
  assert.equal(result.unhealthyVerticals.length, 1);
  assert.equal(result.contractSummary.mustPass, 3);
  assert.equal(result.authErrors1h, 1);
  assert.equal(result.warnings.length, 1);
});
