package providerconnectors

import (
	"os"
	"strings"
	"testing"
	"testing/fstest"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

func TestS1ConnectorGenerationUsesExactPortableKeys(t *testing.T) {
	for _, key := range []string{"acme-sales", "acme_sales", "9-team"} {
		t.Run(key, func(t *testing.T) {
			files := catalogWorkingTreeFS(t)
			index, err := loadGeneratedPackIndex(files)
			if err != nil {
				t.Fatal(err)
			}
			var selected GeneratedPackIndexEntry
			for _, entry := range index.Packs {
				if entry.Provider == "acme" {
					selected = entry
				}
			}
			if selected.Profile == "" {
				t.Fatal("missing synthetic generator profile")
			}
			profile, err := parseGeneratorProfileAt(files[selected.Profile].Data, selected.Profile)
			if err != nil {
				t.Fatal(err)
			}
			profile.Provider, selected.Provider = key, key
			for i := range profile.Operations {
				_, action, ok := splitToolID(profile.Operations[i].ToolID)
				if !ok {
					t.Fatal("invalid fixture tool ID")
				}
				profile.Operations[i].ToolID = key + "." + action
			}
			body, err := yaml.Marshal(profile)
			if err != nil {
				t.Fatal(err)
			}
			files[selected.Profile] = &fstest.MapFile{Data: body}
			artifact, err := generateCatalogPack(files, selected)
			if err != nil || artifact.Manifest.Provider != key {
				t.Fatalf("generation lost exact portable key: got=%q want=%q err=%v", artifact.Manifest.Provider, key, err)
			}
			if err := artifact.Manifest.Generation.Validate(key, artifact.Manifest.Tools); err != nil {
				t.Fatal(err)
			}
			for _, alias := range []string{strings.ToUpper(key), " " + key, key + " ", strings.ReplaceAll(key, "-", "_")} {
				if alias != key && artifact.Manifest.Generation.Validate(alias, artifact.Manifest.Tools) == nil {
					t.Errorf("generation evidence for %q adopted provider alias %q", key, alias)
				}
			}
			for toolID, tool := range artifact.Manifest.Tools {
				if !strings.HasPrefix(toolID, key+".") {
					t.Errorf("generated tool %q lost key %q", toolID, key)
				}
				httpSpec, ok := tool.HTTP()
				if !ok || !strings.HasPrefix(httpSpec.URL, "https://api.acme.test/") || !strings.Contains(httpSpec.Headers["Authorization"], "credentials.acme_api_key") {
					t.Fatalf("key selection rewrote physical protocol or credential references: %+v", httpSpec)
				}
				operation, ok := artifact.Manifest.Generation.OperationForTool(toolID)
				if !ok || operation.ToolID != toolID {
					t.Fatal("exact generation operation lost")
				}
				if _, ok := artifact.Manifest.Generation.OperationForTool(" " + toolID); ok {
					t.Fatal("generation readback repaired a tool identity")
				}
			}
		})
	}
}

func TestS1ConnectorGeneratedAndConformanceBindingsRefuseProviderAliases(t *testing.T) {
	files := catalogWorkingTreeFS(t)
	artifacts, err := GenerateCatalog(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range artifacts {
		pack := LoadedPack{Envelope: artifact.PackEnvelope, Manifest: artifact.Manifest}
		for _, alias := range []string{strings.ToUpper(artifact.Manifest.Provider), " " + artifact.Manifest.Provider, artifact.Manifest.Provider + " "} {
			indexed := artifact.IndexEntry
			indexed.Provider = alias
			if err := validateGeneratedPackIdentity(files, &pack, indexed); err == nil {
				t.Errorf("indexed provider alias %q adopted artifact %q", alias, artifact.Manifest.Provider)
			}
			for _, operation := range artifact.Manifest.Generation.Operations {
				body, err := os.ReadFile(catalogConformanceRoot + "/" + operation.FixtureID + ".yaml")
				if err != nil {
					t.Fatal(err)
				}
				fixture, err := ParseCatalogConformanceFixture(body)
				if err != nil || validateCatalogFixtureBinding(artifact, operation, fixture) != nil {
					t.Fatalf("invalid positive conformance fixture: %v", err)
				}
				fixture.Provider = alias
				if fixture.Validate() == nil || validateCatalogFixtureBinding(artifact, operation, fixture) == nil {
					t.Errorf("conformance provider alias %q adopted generated evidence", alias)
				}
			}
		}
	}
	if catalogConformanceKey("team-bot", "send", "team-bot.send") == catalogConformanceKey("team_bot", "send", "team-bot.send") {
		t.Fatal("conformance identity collapsed distinct provider keys")
	}
}

func TestS1ConnectorRawProviderAndToolFieldsAreNotRepaired(t *testing.T) {
	for _, key := range []string{"TEAM", " team", "team ", "_team", "con", strings.Repeat("a", 101)} {
		body := []byte("provider: " + `"` + key + `"` + "\ntools: {}\n")
		if _, err := ParseConnectorManifest(body); err == nil || !strings.Contains(err.Error(), "portable directory key") || !strings.Contains(err.Error(), "connector.yaml:") {
			t.Errorf("raw provider field %q escaped located key admission: %v", key, err)
		}
	}
	for _, name := range []string{"", " team.send", "team.send ", "TEAM.send", "team_bot.send"} {
		pack := s1ConnectorPack("team")
		pack.Manifest.Tools = map[string]runtimecontracts.ToolSchemaEntry{name: telegramConnectorTool("https://api.telegram.org")}
		if _, err := NewPackRegistry(pack); err == nil {
			t.Errorf("malformed or foreign tool key %q escaped manifest admission", name)
		}
	}
}
