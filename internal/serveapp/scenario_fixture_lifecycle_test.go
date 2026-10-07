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
	requireServedRunStatus(t, endpoint, runID, "completed")
}
