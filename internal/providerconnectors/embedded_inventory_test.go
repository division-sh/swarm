package providerconnectors

import (
	"io/fs"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	platformpacks "github.com/division-sh/swarm/packs"
	"gopkg.in/yaml.v3"
)

var embeddedTestInventory = sync.OnceValues(func() (*packartifact.EffectivePackInventory, error) {
	base, err := packartifact.LoadPlatformPackInventoryFS(platformpacks.FS(), packartifact.InventoryManifestFileName, "0.7.0", packartifact.SelectionEmbedded)
	if err != nil {
		return nil, err
	}
	return packartifact.NewEffectivePackInventory(base)
})

var embeddedTestRegistry = sync.OnceValues(func() (*PackRegistry, error) {
	inventory, err := embeddedTestInventory()
	if err != nil {
		return nil, err
	}
	return NewPackRegistryFromInventory(inventory, "0.7.0")
})

func testPackRegistry(t testing.TB) *PackRegistry {
	t.Helper()
	registry, err := embeddedTestRegistry()
	if err != nil {
		t.Fatalf("load embedded connector pack registry: %v", err)
	}
	// Each fixture owns its mutable projection, not the shared admitted template.
	out := &PackRegistry{byProvider: make(map[string]map[string]LoadedPack, len(registry.byProvider))}
	for provider, tools := range registry.byProvider {
		out.byProvider[provider] = make(map[string]LoadedPack, len(tools))
		for toolID, pack := range tools {
			out.byProvider[provider][toolID] = cloneLoadedPack(pack)
		}
	}
	return out
}

func TestEffectiveConnectorAdmissionValidatesGeneratedIndexEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ConnectorManifest)
		want   string
	}{
		{name: "profile hash", mutate: func(manifest *ConnectorManifest) {
			manifest.Generation.Profile.SHA256 = "sha256:" + strings.Repeat("a", 64)
		}, want: "profile hash mismatch"},
		{name: "source hash", mutate: func(manifest *ConnectorManifest) {
			manifest.Generation.Source.SHA256 = "sha256:" + strings.Repeat("b", 64)
		}, want: "source evidence does not match indexed profile"},
		{name: "missing evidence", mutate: func(manifest *ConnectorManifest) {
			manifest.Generation = nil
		}, want: "generation evidence is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := mutablePlatformPackFS(t)
			rewriteConnectorManifest(t, files, "providers/github", tc.mutate)
			inventory := effectivePlatformPackInventoryFS(t, files)
			if _, err := NewPackRegistryFromInventory(inventory, "0.7.0"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("forged generated evidence error = %v, want %q", err, tc.want)
			}
		})
	}

	t.Run("unindexed evidence", func(t *testing.T) {
		files := mutablePlatformPackFS(t)
		files["providers/notion/connector.yaml"].Data = append([]byte(nil), files["providers/github/connector.yaml"].Data...)
		inventory := effectivePlatformPackInventoryFS(t, files)
		if _, err := NewPackRegistryFromInventory(inventory, "0.7.0"); err == nil || !strings.Contains(err.Error(), "carries generation evidence but is not in the generated pack index") {
			t.Fatalf("unindexed generated evidence error = %v", err)
		}
	})
}

func mutablePlatformPackFS(t testing.TB) fstest.MapFS {
	t.Helper()
	files := fstest.MapFS{}
	err := fs.WalkDir(platformpacks.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		body, err := fs.ReadFile(platformpacks.FS(), name)
		if err != nil {
			return err
		}
		files[name] = &fstest.MapFile{Data: body, Mode: 0o444}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func rewriteConnectorManifest(t testing.TB, files fstest.MapFS, directory string, mutate func(*ConnectorManifest)) {
	t.Helper()
	manifestPath := directory + "/" + packmodel.ConnectorManifestFileName
	var members map[string]yaml.Node
	if err := yaml.Unmarshal(files[manifestPath].Data, &members); err != nil {
		t.Fatal(err)
	}
	var manifest ConnectorManifest
	generation := members["generation"]
	if err := generation.Decode(&manifest.Generation); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest)
	if manifest.Generation == nil {
		delete(members, "generation")
	} else {
		if err := generation.Encode(manifest.Generation); err != nil {
			t.Fatal(err)
		}
		members["generation"] = generation
	}
	manifestBody, err := yaml.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	files[manifestPath].Data = manifestBody
}

func effectivePlatformPackInventoryFS(t testing.TB, files fstest.MapFS) *packartifact.EffectivePackInventory {
	t.Helper()
	base, err := packartifact.LoadPlatformPackInventoryFS(files, packartifact.InventoryManifestFileName, "0.7.0", packartifact.SelectionDevelopmentOverride)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base)
	if err != nil {
		t.Fatal(err)
	}
	return inventory
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
