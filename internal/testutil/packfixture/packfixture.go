package packfixture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packruntime"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	platformpacks "github.com/division-sh/swarm/packs"
	"gopkg.in/yaml.v3"
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

func DevelopmentBase(t testing.TB, replacements map[string][]byte) (*packartifact.PlatformPackInventory, []string) {
	t.Helper()
	embedded := EmbeddedBase(t)
	root := t.TempDir()
	dirs := make([]string, 0, len(embedded.Entries()))
	for _, entry := range embedded.Entries() {
		directory := filepath.Join(root, entry.ID())
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		envelope := entry.Envelope()
		manifestBody := entry.ManifestBody()
		if replacement, ok := replacements[entry.ID()]; ok {
			manifestBody = replacement
			var err error
			envelope, _, err = packartifact.StampEnvelope(envelope, manifestBody)
			if err != nil {
				t.Fatal(err)
			}
		}
		envelopeBody, err := yaml.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, packartifact.EnvelopeFileName), envelopeBody, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, packartifact.ManifestFileNameForType(entry.Type())), manifestBody, 0o644); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, directory)
	}
	development, err := packartifact.LoadDevelopmentPlatformPackInventory(PlatformVersion, dirs, embedded)
	if err != nil {
		t.Fatal(err)
	}
	return development, dirs
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
