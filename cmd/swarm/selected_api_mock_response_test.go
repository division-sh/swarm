package main

import (
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/store"
)

func TestSelectedPostgresAPIOptionalCapabilityBuilderCarriesExactMockConnectorResponseOwner(t *testing.T) {
	plan, err := providerconnectors.NewMockResponsePlan(map[string]map[string]any{
		"provider.write": {"ok": true},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	builder := selectedPostgresAPIOptionalCapabilityBuilder(&store.PostgresStore{}, storeBundle{})
	caps, err := builder(selectedAPICapabilityRequest{MockConnectorResponses: plan})
	if err != nil {
		t.Fatalf("build selected API capabilities: %v", err)
	}
	executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
	if !ok {
		t.Fatalf("run fork executor = %T, want SelectedContractRunForkExecutor", caps.RunFork)
	}
	if executor.AgentRuntime.MockConnectorResponses != plan {
		t.Fatal("production selected-contract capability builder dropped the exact mock connector response owner")
	}
}
