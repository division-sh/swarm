package providerconnectors

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	platformpacks "github.com/division-sh/swarm/packs"
	"gopkg.in/yaml.v3"
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
			rewriteConnectorManifest(t, files, "provider-connectors/github", tc.mutate)
			inventory := effectivePlatformPackInventoryFS(t, files)
			if _, err := NewPackRegistryFromInventory(inventory, "0.7.0"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("forged generated evidence error = %v, want %q", err, tc.want)
			}
		})
	}

	t.Run("unindexed evidence", func(t *testing.T) {
		files := mutablePlatformPackFS(t)
		var generated ConnectorManifest
		if err := yaml.Unmarshal(files["provider-connectors/github/"+packartifact.ConnectorManifestFileName].Data, &generated); err != nil {
			t.Fatal(err)
		}
		rewriteConnectorManifest(t, files, "provider-connectors/notion", func(manifest *ConnectorManifest) {
			*manifest = generated
		})
		copyConnectorEnvelopeContract(t, files, "provider-connectors/github", "provider-connectors/notion")
		inventory := effectivePlatformPackInventoryFS(t, files)
		if _, err := NewPackRegistryFromInventory(inventory, "0.7.0"); err == nil || !strings.Contains(err.Error(), "carries generation evidence but is not in the generated pack index") {
			t.Fatalf("unindexed generated evidence error = %v", err)
		}
	})
}

func copyConnectorEnvelopeContract(t testing.TB, files fstest.MapFS, sourceDirectory, targetDirectory string) {
	t.Helper()
	var source, target packartifact.Envelope
	if err := yaml.Unmarshal(files[sourceDirectory+"/"+packartifact.EnvelopeFileName].Data, &source); err != nil {
		t.Fatal(err)
	}
	targetPath := targetDirectory + "/" + packartifact.EnvelopeFileName
	if err := yaml.Unmarshal(files[targetPath].Data, &target); err != nil {
		t.Fatal(err)
	}
	target.Implements = source.Implements
	target.Capabilities = source.Capabilities
	target.Requires = source.Requires
	manifestBody := files[targetDirectory+"/"+packartifact.ConnectorManifestFileName].Data
	var err error
	target, _, err = packartifact.StampEnvelope(target, manifestBody)
	if err != nil {
		t.Fatal(err)
	}
	files[targetPath].Data, err = yaml.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
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
	manifestPath := directory + "/" + packartifact.ConnectorManifestFileName
	var manifest ConnectorManifest
	if err := yaml.Unmarshal(files[manifestPath].Data, &manifest); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest)
	manifestBody, err := yaml.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	files[manifestPath].Data = manifestBody

	envelopePath := directory + "/" + packartifact.EnvelopeFileName
	var envelope packartifact.Envelope
	if err := yaml.Unmarshal(files[envelopePath].Data, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope, _, err = packartifact.StampEnvelope(envelope, manifestBody)
	if err != nil {
		t.Fatal(err)
	}
	files[envelopePath].Data, err = yaml.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
}

func effectivePlatformPackInventoryFS(t testing.TB, files fstest.MapFS) *packartifact.EffectivePackInventory {
	t.Helper()
	base, err := packartifact.LoadPlatformPackInventoryFS(files, packartifact.InventoryManifestFileName, "0.7.0", packartifact.SelectionDevelopmentOverride)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, nil)
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
