package registration

import (
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"gopkg.in/yaml.v3"
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
	var provider packartifact.AdmittedProvider
	for _, channel := range packfixture.ChannelPacks(t) {
		if channel.Key() == "telegram" {
			if provider.Valid() {
				t.Fatal("duplicate Telegram channel fixture")
			}
			provider = channel
		}
	}
	if !provider.Valid() {
		t.Fatal("expected the shipped Telegram channel fixture")
	}
	members := provider.Members()
	var connectorDocument map[string]any
	if err := yaml.Unmarshal(members["connector.yaml"], &connectorDocument); err != nil {
		t.Fatal(err)
	}
	tools, ok := connectorDocument["tools"].(map[string]any)
	if !ok {
		t.Fatal("captured Telegram connector has no tool declarations")
	}
	for toolID, tool := range overrides {
		if !strings.HasPrefix(toolID, provider.Key()+".") {
			t.Fatalf("foreign connector override %s", toolID)
		}
		name := strings.TrimPrefix(toolID, provider.Key()+".")
		if _, exists := tools[name]; !exists {
			t.Fatalf("unknown connector override %s", toolID)
		}
		tools[name] = tool
	}
	members["connector.yaml"], err = yaml.Marshal(connectorDocument)
	if err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{}
	for name, body := range members {
		files[provider.Key()+"/"+name] = &fstest.MapFile{Data: body}
	}
	provider, err = packartifact.LoadProviderFS(files, provider.Key(), "0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	triggerBody, _ := provider.Body(packmodel.BodyTrigger)
	trigger, err := providertriggers.AdmitBody(triggerBody)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := providertriggers.NewCatalogSnapshotFromAdmittedBodies(trigger)
	if err != nil {
		t.Fatal(err)
	}
	triggerDescriptor, present := catalog.PackDescriptorForProvider(provider.Key())
	if !present {
		t.Fatal("captured Telegram trigger descriptor is unavailable")
	}
	connectorBody, _ := provider.Body(packmodel.BodyConnector)
	connector, err := providerconnectors.AdmitBody(connectorBody)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := packs.NewProviderBodyIdentity(connectorBody)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := packs.CompileProviderChannel(registry, provider, triggerDescriptor, packs.ConnectorPackDescriptor{Identity: identity, Provider: connector.Provider, Tools: connector.Tools})
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
