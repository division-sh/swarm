package contracts

import (
	"testing"
)

func TestPhase1SchemaRegistryUsesMASContractsSource(t *testing.T) {
	t.Parallel()

	repoRoot := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, currentWorkflowContractsDirForTest(t), DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Skipf("no contract bundle: %v", err)
	}
	registry := EventSchemaRegistryFromCatalog(bundle.EventEntries())
	if len(registry) == 0 {
		t.Fatal("expected dynamic event schema registry entries from contract bundle")
	}
	for eventType, schema := range registry {
		if schema.Schema == nil {
			t.Fatalf("schema for %s has nil Schema map", eventType)
		}
		props, _ := schema.Schema["properties"].(map[string]any)
		if props == nil {
			t.Fatalf("schema for %s has no properties", eventType)
		}
	}
}
