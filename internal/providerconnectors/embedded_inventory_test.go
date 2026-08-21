package providerconnectors

import (
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	platformpacks "github.com/division-sh/swarm/packs"
)

func testEmbeddedPackInventory(t testing.TB) *packartifact.EffectivePackInventory {
	t.Helper()
	base, err := packartifact.LoadPlatformPackInventoryFS(platformpacks.FS(), packartifact.InventoryManifestFileName, "0.7.0", packartifact.SelectionEmbedded)
	if err != nil {
		t.Fatalf("load embedded platform pack inventory: %v", err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, nil)
	if err != nil {
		t.Fatalf("build effective embedded pack inventory: %v", err)
	}
	return inventory
}

func testPackRegistry(t testing.TB) *PackRegistry {
	t.Helper()
	registry, err := NewPackRegistryFromInventory(testEmbeddedPackInventory(t), "0.7.0")
	if err != nil {
		t.Fatalf("load embedded connector pack registry: %v", err)
	}
	return registry
}

func testBuiltinTool(t testing.TB, provider, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	t.Helper()
	pack, ok := testPackRegistry(t).Lookup(provider, toolID)
	if !ok {
		return runtimecontracts.ToolSchemaEntry{}, false
	}
	tool, ok := pack.Manifest.Tools[toolID]
	return tool, ok
}
