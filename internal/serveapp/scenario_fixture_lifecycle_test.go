package serveapp

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
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
	var last struct {
		Run            operatorread.RunHeader                      `json:"run"`
		TestQuiescence operatorread.RunTestQuiescence              `json:"test_quiescence"`
		Instances      []operatorread.RunInstanceOperationalStatus `json:"instances"`
	}
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": runID}, &last)
		if last.Run.RunID != runID || last.Run.Failure != nil || last.Run.Status != "running" || last.Run.EndedAt != nil {
			t.Fatalf("served scenario changed its live run: %+v", last)
		}
		final := len(last.Instances) != 0
		for _, instance := range last.Instances {
			final = final && instance.Stage == "done" && instance.Disposition == "final"
		}
		if final && last.TestQuiescence.Ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("served scenario instances did not finish and settle within the original bound: %+v", last)
}
