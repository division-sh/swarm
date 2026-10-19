package runtime_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func replaceConnectorProofText(t *testing.T, path, old, replacement string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw), old) != 1 {
		t.Fatalf("replacement in %s must match exactly once: %q", path, old)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), old, replacement, 1)), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorGeneratedSchemaRejectsToolFromAnotherFlow(t *testing.T) {
	root := canonicalrouting.CopyPublicationConnector(t, "static")
	imports := "imports:\n  connector_packs:\n    - {provider: telegram, tool: telegram.send_message}\n"
	replaceConnectorProofText(t, filepath.Join(root, "source", "schema.yaml"), imports, "")
	replaceConnectorProofText(t, filepath.Join(root, "sibling", "schema.yaml"), "name: sibling\n", "name: sibling\n"+imports)
	base := semanticview.Wrap(loadRuntimeBundleRoot(t, root))
	source, err := providerconnectors.SourceWithConnectorPackImports(base, telegramConnectorSupportedSurfacePackRegistry(t, "http://127.0.0.1"))
	if err == nil || source != nil || !strings.Contains(err.Error(), "admitted in its declaring flow") {
		t.Fatalf("foreign tool import = %T, %v", source, err)
	}
}

func TestConnectorGeneratedSchemasOwnAllArmsAndConnectedPins(t *testing.T) {
	registry := telegramConnectorSupportedSurfacePackRegistry(t, "http://127.0.0.1")
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyPublicationConnector(t, mode))
			base := semanticview.Wrap(bundle)
			source, err := providerconnectors.SourceWithConnectorPackImports(base, registry)
			if err != nil {
				t.Fatal(err)
			}
			flow := "source"
			if mode == "root" {
				flow = "."
			} else if mode == "nested_template" {
				flow = "outer/source"
			}
			for _, name := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
				compiled, found, err := source.ResolveEffectiveCompiledFlowEventSchema(flow, name)
				if err != nil || !found || compiled.FlowPath() != flow || compiled.Classification() != contracts.CompiledEventSchemaGenerated || compiled.Importable() {
					t.Fatalf("%s binding = %#v, %v, %v", name, compiled, found, err)
				}
				want := compiled.AcceptanceSchema()
				output, found := source.FlowOutputEventPin(flow, name)
				if !found {
					t.Fatalf("missing output %s", name)
				}
				outputSchema, found := output.EventSchema()
				if !found || outputSchema.AcceptanceSchemaDigest() != compiled.AcceptanceSchemaDigest() {
					t.Fatalf("unbound output %s", name)
				}
				input, found := source.FlowInputEventPin("sink", name)
				if !found {
					t.Fatalf("missing connected input %s", name)
				}
				inputSchema, found := input.ProducerEventSchema()
				if !found || inputSchema.AcceptanceSchemaDigest() != compiled.AcceptanceSchemaDigest() || inputSchema.FlowPath() != flow {
					t.Fatalf("connected input %s borrowed another declaration", name)
				}
				for _, sibling := range []string{"sibling", "absent"} {
					other, found, err := source.ResolveEffectiveCompiledFlowEventSchema(sibling, compiled.EventName())
					if err != nil || found && other.AcceptanceSchemaDigest() == compiled.AcceptanceSchemaDigest() {
						t.Fatalf("foreign flow %s acquired %s", sibling, name)
					}
				}
				entry, resolved, found := source.ResolveFlowEventCatalogEntry(flow, name)
				if !found || resolved != compiled.EventName() || len(entry.Payload.Properties) == 0 {
					t.Fatalf("missing retained catalog %s", name)
				}
				wantField, exists := entry.Payload.Properties["activity_id"]
				if !exists {
					t.Fatalf("activity identity field missing for %s", name)
				}
				delete(entry.Payload.Properties, "activity_id")
				delete(want, "properties")
				for _, catalog := range []map[string]contracts.EventCatalogEntry{source.EventEntries(), source.ResolvedEventCatalog()} {
					item, found := catalog[compiled.EventName()]
					if !found {
						t.Fatalf("global readback missing %s", compiled.EventName())
					}
					delete(item.Payload.Properties, "activity_id")
				}
				unchanged, _, _ := source.ResolveEffectiveCompiledFlowEventSchema(flow, name)
				if _, exists := unchanged.AcceptanceSchema()["properties"]; !exists || !reflect.DeepEqual(unchanged.CanonicalAcceptanceSchema(), compiled.CanonicalAcceptanceSchema()) {
					t.Fatalf("readback mutated %s", name)
				}
				if raw, found := source.EventEntry(compiled.EventName()); !found || !reflect.DeepEqual(raw.Payload.Properties["activity_id"], wantField) {
					t.Fatalf("catalog mutation survived for %s", name)
				}
				if _, found, _ := base.ResolveEffectiveCompiledFlowEventSchema(flow, name); found {
					t.Fatalf("compilation mutated uncomposed source for %s", name)
				}
			}
		})
	}
}
