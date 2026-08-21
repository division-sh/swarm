package packfixture

import (
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packruntime"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	platformpacks "github.com/division-sh/swarm/packs"
)

const PlatformVersion = "0.7.0"

func EmbeddedBase(t testing.TB) *packartifact.PlatformPackInventory {
	t.Helper()
	base, err := packartifact.LoadPlatformPackInventoryFS(
		platformpacks.FS(),
		packartifact.InventoryManifestFileName,
		PlatformVersion,
		packartifact.SelectionEmbedded,
	)
	if err != nil {
		t.Fatalf("load embedded platform pack inventory: %v", err)
	}
	return base
}

func EmbeddedInventory(t testing.TB) *packartifact.EffectivePackInventory {
	t.Helper()
	inventory, err := packartifact.NewEffectivePackInventory(EmbeddedBase(t), nil)
	if err != nil {
		t.Fatalf("build embedded effective pack inventory: %v", err)
	}
	return inventory
}

func TriggerCatalog(t testing.TB) *providertriggers.CatalogSnapshot {
	t.Helper()
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(EmbeddedInventory(t), PlatformVersion)
	if err != nil {
		t.Fatalf("load embedded provider trigger catalog: %v", err)
	}
	return catalog
}

func ConnectorRegistry(t testing.TB) *providerconnectors.PackRegistry {
	t.Helper()
	registry, err := providerconnectors.NewPackRegistryFromInventory(EmbeddedInventory(t), PlatformVersion)
	if err != nil {
		t.Fatalf("load embedded provider connector registry: %v", err)
	}
	return registry
}

func ChannelPacks(t testing.TB) []packs.LoadedChannelPack {
	t.Helper()
	loaded, err := packruntime.LoadChannelPacks(EmbeddedInventory(t), PlatformVersion)
	if err != nil {
		t.Fatalf("load embedded channel packs: %v", err)
	}
	return loaded
}

func ConnectorTool(t testing.TB, provider, toolID string) providerconnectors.InstalledTool {
	t.Helper()
	registry := ConnectorRegistry(t)
	pack, ok := registry.Lookup(provider, toolID)
	if !ok {
		t.Fatalf("embedded connector tool %s/%s is not installed", provider, toolID)
	}
	tool, ok := pack.Manifest.Tools[toolID]
	if !ok {
		t.Fatalf("embedded connector pack %q does not declare %q", pack.Envelope.ID, toolID)
	}
	return providerconnectors.InstalledTool{Provider: provider, ToolID: toolID, Tool: tool, Pack: pack}
}
