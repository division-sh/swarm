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

func readInstanceLifeTrace(t *testing.T, h *channelOnboardingE2EHarness, runID, entityID string, life *int64) map[string]bool {
	t.Helper()
	filter := map[string]any{"entity_id": []string{entityID}}
	if life != nil {
		filter["iter"] = *life
	}
	var result struct {
		Trace []struct {
			EventID string `json:"event_id"`
		} `json:"trace"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.trace", map[string]any{"run_id": runID, "filter": filter, "limit": 200}, &result)
	ids := map[string]bool{}
	for _, row := range result.Trace {
		if row.EventID == "" {
			t.Fatal("life trace omitted its event identity")
		}
		ids[row.EventID] = true
	}
	if len(ids) == 0 {
		t.Fatal("life trace lost the public journey's emitted event")
	}
	return ids
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
