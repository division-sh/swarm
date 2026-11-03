package serveapp

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/servedparity"
)

type instanceLifeJourneyReadback struct {
	EntityID string  `json:"entity_id"`
	Life     *int64  `json:"life"`
	State    *string `json:"current_state"`
}

func TestInstanceLifeResetPublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			runProviderEvidenceJourney(t, backend, "instance_life", false)
		})
	}
}

func waitInstanceLifeFinished(t *testing.T, h *channelOnboardingE2EHarness, runID, inputID string) instanceLifeJourneyReadback {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			Entities []instanceLifeJourneyReadback `json:"entities"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "entity.list", map[string]any{"run_id": runID, "flow": "worker"}, &result)
		if len(result.Entities) == 1 && result.Entities[0].State != nil && *result.Entities[0].State == "done" {
			return result.Entities[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("instance did not finish for input %s", inputID)
	return instanceLifeJourneyReadback{}
}
