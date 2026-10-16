package agentpersistence

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/manager"
)

func TestClaimDescriptorRevisionPreservesExplicitEmptyConfiguration(t *testing.T) {
	actor := persistedIntentTestAgent(t)
	plan, err := actor.Identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	actor.Config = json.RawMessage(`{}`)
	before, err := manager.AgentConfigPlanRevision(actor, plan)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := ProjectPersistedAgentConfig(actor, "")
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := HydratePersistedAgentConfig(projection)
	if err != nil {
		t.Fatal(err)
	}
	after, err := manager.AgentConfigPlanRevision(hydrated, plan)
	if err != nil || before != after {
		t.Fatalf("explicit native configuration changed revision: before=%s after=%s error=%v", before, after, err)
	}
	actor.Config = nil
	absent, err := manager.AgentConfigPlanRevision(actor, plan)
	if err != nil || absent == before {
		t.Fatalf("absent configuration aliased the explicit persisted object: error=%v", err)
	}
}
