package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProjectPackageDocumentProviderTriggerEventsStrictDecode(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: demo
provider_trigger_events:
  imports:
    - provider: Telegram
      event: inbound.telegram.text_message
`), &doc); err != nil {
		t.Fatalf("Unmarshal provider_trigger_events: %v", err)
	}
	if len(doc.ProviderTriggerEvents.Imports) != 1 {
		t.Fatalf("imports = %#v, want one", doc.ProviderTriggerEvents.Imports)
	}
	got := doc.ProviderTriggerEvents.Imports[0]
	if got.Provider != "telegram" || got.Event != "inbound.telegram.text_message" {
		t.Fatalf("import = %#v, want normalized exact event", got)
	}

	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "container must be mapping",
			yaml: "provider_trigger_events: []\n",
			want: "provider_trigger_events must be a mapping",
		},
		{
			name: "entry must be mapping",
			yaml: "provider_trigger_events:\n  imports: [telegram]\n",
			want: "provider_trigger_events.imports entries must be mappings",
		},
		{
			name: "unknown container field",
			yaml: "provider_trigger_events:\n  import: []\n",
			want: `provider_trigger_events field "import" is not supported.`,
		},
		{
			name: "unknown entry field",
			yaml: "provider_trigger_events:\n  imports:\n    - provider: telegram\n      event: inbound.telegram.text_message\n      shadow: true\n",
			want: `provider_trigger_events.imports field "shadow" is not supported.`,
		},
		{
			name: "missing provider",
			yaml: "provider_trigger_events:\n  imports:\n    - event: inbound.telegram.text_message\n",
			want: "provider_trigger_events.imports provider is required",
		},
		{
			name: "missing event",
			yaml: "provider_trigger_events:\n  imports:\n    - provider: telegram\n",
			want: "provider_trigger_events.imports event is required",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var invalid ProjectPackageDocument
			err := yaml.Unmarshal([]byte("name: demo\n"+test.yaml), &invalid)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Unmarshal error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestBundleCatalogPackageJSONProjectsProviderTriggerEventImports(t *testing.T) {
	projected := bundleCatalogPackageJSON(ProjectPackageDocument{
		Name: "demo",
		ProviderTriggerEvents: ProviderTriggerEventImports{Imports: []ProviderTriggerEventImport{
			{Provider: "telegram", Event: "inbound.telegram.text_message"},
		}},
	})
	declaration, ok := projected["provider_trigger_events"].(map[string]any)
	if !ok {
		t.Fatalf("provider_trigger_events readback = %#v", projected["provider_trigger_events"])
	}
	imports, ok := declaration["imports"].([]map[string]string)
	if !ok || len(imports) != 1 || imports[0]["provider"] != "telegram" || imports[0]["event"] != "inbound.telegram.text_message" {
		t.Fatalf("provider_trigger_events imports readback = %#v", declaration["imports"])
	}
}

func TestProviderTriggerEventImportParticipatesInBundleHashAndCatalogReplay(t *testing.T) {
	plainRoot, plainPlatform := writeEquivalentBundleHashFixture(t, "\n", "name: provider-event-import\nversion: \"1.0.0\"\nflows: []\n")
	importRoot, importPlatform := writeEquivalentBundleHashFixture(t, "\n", `name: provider-event-import
version: "1.0.0"
provider_trigger_events:
  imports:
    - provider: telegram
      event: inbound.telegram.text_message
flows: []
`)
	plainHash, err := BundleHash(bundleHashTestBundleWithIntent(t, plainRoot, plainPlatform, "prompts/guide.md"))
	if err != nil {
		t.Fatalf("plain BundleHash: %v", err)
	}
	withImport := bundleHashTestBundleWithIntent(t, importRoot, importPlatform, "prompts/guide.md")
	withImport.Package = ProjectPackageDocument{
		Name:            "provider-event-import",
		Version:         "1.0.0",
		PlatformVersion: ">=0.7.0 <0.8.0",
		ProviderTriggerEvents: ProviderTriggerEventImports{Imports: []ProviderTriggerEventImport{
			{Provider: "telegram", Event: "inbound.telegram.text_message"},
		}},
	}
	projection, err := BuildBundleCatalogProjection(withImport)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if projection.BundleHash == plainHash {
		t.Fatalf("provider-trigger import did not change bundle hash %s", plainHash)
	}
	loaded, err := LoadBundleCatalogRuntimeSource(repoRootForContractsTest(t), BundleCatalogRuntimeLoadRequest{
		BundleHash: projection.BundleHash, ContentYAML: projection.ContentYAML, DataBlob: projection.DataBlob,
	})
	if err != nil {
		t.Fatalf("LoadBundleCatalogRuntimeSource: %v", err)
	}
	defer loaded.Cleanup()
	imports := loaded.Bundle.Package.ProviderTriggerEvents.Imports
	if len(imports) != 1 || imports[0].Provider != "telegram" || imports[0].Event != "inbound.telegram.text_message" {
		t.Fatalf("catalog replay provider-trigger imports = %#v", imports)
	}
}
