package registration

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func registrationChannelPlanForTest(t testing.TB, overrides map[string]runtimecontracts.ToolSchemaEntry) packs.SatisfactionPlan {
	t.Helper()
	spec, err := runtimecontracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	channels := packfixture.ChannelPacks(t)
	if len(channels) != 1 || channels[0].Manifest.Provider != "telegram" {
		t.Fatal("expected the shipped Telegram channel fixture")
	}
	connectors := packfixture.ConnectorRegistry(t).PackDescriptors()
	for toolID, tool := range overrides {
		found := false
		for i := range connectors {
			if connectors[i].Identity.ID() != channels[0].Envelope.Requires.Packs[packs.TypeConnector] {
				continue
			}
			if _, exists := connectors[i].Tools[toolID]; !exists {
				t.Fatalf("unknown connector override %s", toolID)
			}
			connectors[i].Tools[toolID] = tool
			found = true
		}
		if !found {
			t.Fatalf("missing accepted connector for %s", toolID)
		}
	}
	plan, err := packs.CompileChannel(registry, channels[0], packfixture.TriggerCatalog(t).PackDescriptors(), connectors)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func registrationOperationForTest(t testing.TB, name, toolID string, tool runtimecontracts.ToolSchemaEntry) packs.RegistrationOperationPlan {
	t.Helper()
	plan := registrationChannelPlanForTest(t, map[string]runtimecontracts.ToolSchemaEntry{toolID: tool})
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("missing real compiled registration slots")
	}
	operation, err := registration.Operation(name)
	if err != nil || operation.ToolID() != toolID {
		t.Fatalf("exact registration slot %s/%s: %v", name, toolID, err)
	}
	return operation
}
