package packadmission

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"gopkg.in/yaml.v3"
)

func generatedProviderDirectoryMembers(t *testing.T, artifact providerconnectors.GeneratedCatalogArtifact, change func(*providerconnectors.GenerationEvidence)) map[string][]byte {
	t.Helper()
	tools := map[string]any{}
	for qualified, tool := range artifact.Manifest.Tools {
		key, local, found := strings.Cut(qualified, ".")
		if !found || key != artifact.Manifest.Provider {
			t.Fatal("generated fixture has no canonical provider tool coordinate")
		}
		tools[local] = tool
	}
	generation := *artifact.Manifest.Generation
	generation.Operations = append([]providerconnectors.GenerationOperationEvidence(nil), generation.Operations...)
	for index := range generation.Operations {
		_, local, _ := strings.Cut(generation.Operations[index].ToolID, ".")
		generation.Operations[index].ToolID = local
	}
	if change != nil {
		change(&generation)
	}
	body, err := yaml.Marshal(map[string]any{"tools": tools, "generation": generation})
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{
		"manifest.yaml":  []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [create_issue]}, cannot: [bypass journal]}\n"),
		"connector.yaml": body,
	}
}

func TestS1ProviderDirectoryGenerationRetainsReviewedSourceWithoutNameInference(t *testing.T) {
	artifacts, err := providerconnectors.GenerateCatalog(os.DirFS(filepath.Join(repoRoot(t), "internal", "providerconnectors")))
	if err != nil {
		t.Fatal(err)
	}
	platform := providerDefinitionPlatform(t)
	var builtin, fixture providerconnectors.GeneratedCatalogArtifact
	for _, artifact := range artifacts {
		switch artifact.IndexEntry.Kind {
		case providerconnectors.GeneratedPackKindBuiltin:
			builtin = artifact
		case providerconnectors.GeneratedPackKindFixture:
			fixture = artifact
		}
	}
	if builtin.Manifest.Generation == nil || fixture.Manifest.Generation == nil {
		t.Fatal("generated builtin/fixture controls missing")
	}
	for _, key := range []string{"team-bot", "team_bot"} {
		members := generatedProviderDirectoryMembers(t, builtin, nil)
		provider, err := packartifact.AdmitProviderMembers(key, members)
		if err != nil {
			t.Fatal(err)
		}
		definition, err := AdmitProviderDirectory(provider, platform)
		if err != nil || !definition.Valid() {
			t.Fatalf("directory key was mistaken for a generated profile provider: %s %v", key, err)
		}
		descriptor, present := definition.ConnectorDescriptor()
		if !present {
			t.Fatal("generated definition lost its connector role")
		}
		for name := range descriptor.Tools {
			generation, present := definition.ConnectorGeneration(name)
			if !present || !strings.HasPrefix(name, key+".") || generation.ManifestSHA256() != descriptor.Identity.ManifestHash() || generation.ProfilePath() != builtin.Manifest.Generation.Profile.Path {
				t.Fatal("generation readback lost its exact qualified tool, BODY or reviewed profile")
			}
			if _, present := definition.ConnectorGeneration(" " + name); present {
				t.Fatal("generation lookup repaired a foreign spelling")
			}
		}
	}
	for _, cell := range []struct {
		name   string
		change func(*providerconnectors.GenerationEvidence)
	}{
		{"profile hash", func(g *providerconnectors.GenerationEvidence) { g.Profile.SHA256 = strings.Repeat("f", 64) }},
		{"source hash", func(g *providerconnectors.GenerationEvidence) { g.Source.SHA256 = strings.Repeat("f", 64) }},
		{"source path", func(g *providerconnectors.GenerationEvidence) { g.Source.Path = "unrelated.json" }},
		{"unknown profile", func(g *providerconnectors.GenerationEvidence) {
			g.Profile.Path = "catalog/generator-profiles/unknown.yaml"
		}},
		{"fixture profile", func(g *providerconnectors.GenerationEvidence) { g.Profile = fixture.Manifest.Generation.Profile }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			provider, err := packartifact.AdmitProviderMembers("team-bot", generatedProviderDirectoryMembers(t, builtin, cell.change))
			if err != nil {
				t.Fatal(err)
			}
			definition, err := AdmitProviderDirectory(provider, platform)
			if err == nil || definition.Valid() {
				t.Fatalf("contradictory generated identity published a definition: %v", err)
			}
		})
	}
}
