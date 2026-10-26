package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func closeServedScenarioFixtureRun(t *testing.T, endpoint, bundleHash, runID string) {
	t.Helper()
	var result map[string]any
	requireServedJSONRPCResult(t, endpoint, "event.publish", map[string]any{
		"bundle_hash": bundleHash, "run_id": runID,
		"event_name": canonicalrouting.ScenarioFixtureCloseEvent,
		"payload":    map[string]any{}, "idempotency_key": "close-scenario-" + runID,
	}, &result)
	waitIssue2564LiveRunSettlement(t, issue2564ServedFixture{Endpoint: endpoint}, runID)
	requireServedOKJSONRPC(t, endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "pause-scenario-" + runID})
}
