package contracts

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testcatalog"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// This fixed rejection inventory is independent of the admitted field sets.
// Each witness traverses its declaration root and nested production projectors.
func TestRemovedClosedFieldsUseCurrentVocabularyAcrossPresenceAndMerges(t *testing.T) {
	admitNodes := func(d yamlsource.Document) error { _, err := projectNodeDeclarationsValue(d.Root()); return err }
	admitSchema := func(d yamlsource.Document) error { _, err := AdmitFlowSchemaValue(d.Root()); return err }
	handler := "worker: {event_handlers: {work.ready: {%s}}}\n"
	for _, tc := range []struct {
		name, file, format string
		keys               []string
		admit              func(yamlsource.Document) error
	}{
		{"node", "nodes.yaml", "worker: {%s}\n", []string{"id", "permissions", "implementation", "owned_transitions", "idempotency_table"}, admitNodes},
		{"handler", "nodes.yaml", handler, []string{"condition", "logic", "from", "dedup_by", "action", "select_entity", "select_or_create_entity", "branch", "emits", "payload_transform", "on_below_threshold", "on_dedup", "on_pass"}, admitNodes},
		{"rules", "nodes.yaml", fmt.Sprintf(handler, "rules: [{%s}]"), []string{"condition", "action", "element_id", "emits", "payload_transform", "switch", "threshold", "policy", "temporal", "join", "loop", "collection", "schedule"}, admitNodes},
		{"on_success", "nodes.yaml", fmt.Sprintf(handler, "on_success: {%s}"), []string{"action"}, admitNodes},
		{"sets_gate", "nodes.yaml", fmt.Sprintf(handler, "sets_gate: {%s}"), []string{"value"}, admitNodes},
		{"timer", "nodes.yaml", "worker: {timers: [{%s}]}\n", []string{"delay_seconds", "delay_minutes", "delay_hours", "delay_days"}, admitNodes},
		{"emit", "nodes.yaml", fmt.Sprintf(handler, "emit: {%s}"), []string{"target", "broadcast"}, admitNodes},
		{"fan_out", "nodes.yaml", fmt.Sprintf(handler, "fan_out: {%s}"), []string{"element_id", "target", "emit_per_item", "emit_mapping"}, admitNodes},
		{"filter", "nodes.yaml", fmt.Sprintf(handler, "filter: {%s}"), []string{"predicate"}, admitNodes},
		{"reduce", "nodes.yaml", fmt.Sprintf(handler, "reduce: {%s}"), []string{"params"}, admitNodes},
		{"query", "nodes.yaml", fmt.Sprintf(handler, "query: {%s}"), []string{"operation"}, admitNodes},
		{"compute", "nodes.yaml", fmt.Sprintf(handler, "compute: {%s}"), []string{"params", "value_field", "weight_field"}, admitNodes},
		{"clear", "nodes.yaml", fmt.Sprintf(handler, "clear: {%s}"), []string{"target", "pending_dedup", "entity_fields"}, admitNodes},
		{"schema", "schema.yaml", "name: supported\n%s\n", []string{"mode", "initial_state", "states", "terminal_states", "tool_surface", "entity", "namespace_prefix", "namespace_rule"}, admitSchema},
		{"connect", "schema.yaml", "name: supported\nconnect: [{%s}]\n", []string{"adapter", "delivery", "reply", "map", "using"}, admitSchema},
		{"input_pin", "schema.yaml", "name: supported\npins: {inputs: [{event: work.ready, initialize: {note: payload.note}, %s}]}\n", []string{"name", "address", "carries", "key", "optional", "convert", "source", "resolution", "replies_to", "correlation_key"}, admitSchema},
		{"agent", "agents.yaml", "worker: {%s}\n", []string{"model_tier", "mode", "conversation_mode", "session_scope", "session_scope_authority", "tools_tier2", "subscriptions_bootstrap", "subscribes_to", "prompt_ref", "prompt_inputs", "profile", "agent_defaults", "agent_profiles", "runtime_id_template"}, func(d yamlsource.Document) error { _, err := projectAgentDeclarationsValue(d.Root()); return err }},
		{"tool", "tools.yaml", "worker: {%s}\n", []string{"parameters", "returns", "endpoint", "type", "required_permission", "kind"}, func(d yamlsource.Document) error { _, err := projectToolDeclarationsValue(d.Root()); return err }},
	} {
		for _, key := range tc.keys {
			for _, shape := range []string{"null", "''", "false", "7", "{}", "[]"} {
				for _, merged := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/merged=%v", tc.name, key, shape, merged), func(t *testing.T) {
						member := key + ": " + shape
						if merged {
							member = "<<: {" + member + "}"
						}
						snapshot, err := yamlsource.Load([]byte(fmt.Sprintf(tc.format, member)))
						if err != nil {
							t.Fatal(err)
						}
						err = tc.admit(snapshot.Document(tc.file))
						diagnostic, ok := AsLoaderDiagnostic(err)
						if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Problem, fmt.Sprintf("%q", key)) || slices.Contains(diagnostic.ValidOptions, key) {
							t.Fatalf("removed key bypassed current vocabulary: %v", err)
						}
						if diagnostic.Location.File != tc.file || diagnostic.Location.Line <= 0 || diagnostic.Location.Column <= 0 || !strings.Contains(diagnostic.Location.YAMLPath, key) || len(diagnostic.ValidOptions) == 0 {
							t.Fatalf("lost diagnostic evidence: %+v", diagnostic)
						}
					})
				}
			}
		}
	}
}

func TestRetiredOutputPinOptionMapsRejectPresenceAndMerges(t *testing.T) {
	for _, key := range []string{"name", "address", "carries", "key", "optional", "convert", "sink"} {
		for _, shape := range []string{"null", "''", "false", "7", "{}", "[]"} {
			for _, merged := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/merged=%v", key, shape, merged), func(t *testing.T) {
					member := key + ": " + shape
					if merged {
						member = "<<: {" + member + "}"
					}
					_, err := admitSchemaFragment("pins: {outputs: [{event: work.ready, " + member + "}]}\n")
					if err == nil || !strings.Contains(err.Error(), "must be a scalar text") || !strings.Contains(err.Error(), `outputs"][0]`) || !strings.Contains(err.Error(), "schema.yaml:") {
						t.Fatalf("retired output mapping bypassed scalar admission: %v", err)
					}
				})
			}
		}
	}
}

func TestCurrentVocabularyDiagnosticsAcrossDeclarationFamilies(t *testing.T) {
	for _, tc := range []struct {
		file, body, key, nearest string
		admit                    func(yamlsource.Document) error
	}{
		{"nodes.yaml", "worker: {descriptoin: typo}\n", "descriptoin", "description", func(d yamlsource.Document) error { _, err := projectNodeDeclarationsValue(d.Root()); return err }},
		{"schema.yaml", "name: sample\nnaem: typo\n", "naem", "name", func(d yamlsource.Document) error { _, err := AdmitFlowSchemaValue(d.Root()); return err }},
		{"agents.yaml", "worker: {intent: {inline: business intent}, modle: typo}\n", "modle", "model", func(d yamlsource.Document) error { _, err := projectAgentDeclarationsValue(d.Root()); return err }},
		{"tools.yaml", "worker: {descriptoin: typo}\n", "descriptoin", "description", func(d yamlsource.Document) error { _, err := projectToolDeclarationsValue(d.Root()); return err }},
		{"types.yaml", "enmus: {}\n", "enmus", "enums", func(d yamlsource.Document) error { _, err := projectTypeCatalogDocument(d.Root()); return err }},
		{"entities.yaml", "Item: {_descriptoin: typo, value: text}\n", "_descriptoin", "_description", func(d yamlsource.Document) error { _, err := projectEntityContractsDocument(d.Root()); return err }},
		{"events.yaml", "work.ready: {value: {type: text, descriptoin: typo}}\n", "descriptoin", "description", func(d yamlsource.Document) error { _, err := admitEventCatalogDocument(d); return err }},
	} {
		t.Run(tc.file, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			err = tc.admit(snapshot.Document(tc.file))
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Problem, tc.key) {
				t.Fatalf("missing current unknown-key diagnostic: %v", err)
			}
			if diagnostic.Location.File != tc.file || diagnostic.Location.Line <= 0 || diagnostic.Location.Column <= 0 || !strings.Contains(diagnostic.Location.YAMLPath, tc.key) {
				t.Fatalf("lost source path/coordinates: %+v", diagnostic.Location)
			}
			if !slices.Contains(diagnostic.ValidOptions, tc.nearest) || !strings.Contains(diagnostic.Remediation, `Did you mean "`+tc.nearest+`"?`) {
				t.Fatalf("missing current vocabulary/suggestion: %+v", diagnostic)
			}
			for _, fragment := range []string{tc.file, tc.key, tc.nearest, "Valid fields:", "line ", "column "} {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatalf("ordinary error omits %q: %v", fragment, err)
				}
			}
			if strings.Contains(err.Error(), "RETIRED") || strings.Contains(err.Error(), "migration") {
				t.Fatalf("obsolete teaching survives: %v", err)
			}
		})
	}
}

func TestCurrentVocabularyDiskDiagnosticsPreserveSource(t *testing.T) {
	for _, tc := range []struct {
		file, body string
		load       func(string) error
	}{
		{"types.yaml", "enmus: {}\n", func(path string) error { _, err := loadOptionalTypeDeclarations(path); return err }},
		{"entities.yaml", "Item: {_descriptoin: typo, value: text}\n", func(path string) error { _, err := loadOptionalEntityDeclarations(path); return err }},
		{"events.yaml", "work.ready: {value: {type: text, descriptoin: typo}}\n", func(path string) error { _, err := loadOptionalEventCatalog(path); return err }},
	} {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.file)
			writeFixtureFile(t, path, tc.body)
			diagnostic, ok := AsLoaderDiagnostic(tc.load(path))
			if !ok || filepath.Base(diagnostic.Location.File) != tc.file || diagnostic.Location.Line <= 0 || diagnostic.Location.Column <= 0 || len(diagnostic.ValidOptions) == 0 {
				t.Fatalf("disk owner lost current teaching: %+v", diagnostic)
			}
		})
	}
}

func TestCurrentVocabularyKeepsOpenNameReservations(t *testing.T) {
	for reserved := range reservedEventCatalogFieldNames {
		t.Run(reserved, func(t *testing.T) {
			for _, value := range []string{"text", "null", "''", "{}", "[]"} {
				snapshot, err := yamlsource.Load([]byte("work.ready: {" + reserved + ": " + value + "}\n"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := admitEventCatalogDocument(snapshot.Document("events.yaml")); err == nil || !strings.Contains(err.Error(), reserved) || !strings.Contains(err.Error(), "reserved") {
					t.Fatalf("reserved name became business data: %v", err)
				}
			}
		})
	}
	snapshot, err := yamlsource.Load([]byte("work.ready: {ordinary_business_name: text}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admitEventCatalogDocument(snapshot.Document("events.yaml")); err != nil {
		t.Fatalf("open payload names narrowed to a finite vocabulary: %v", err)
	}
}

func TestCurrentVocabularyPreservesAdmittedCatalog(t *testing.T) {
	repo := repoRootForContractsTest(t)
	inventory, err := testcatalog.Load(repo)
	if err != nil {
		t.Fatal(err)
	}
	loaded := 0
	for _, fixture := range inventory.Fixtures {
		if fixture.Metadata.Disposition == testcatalog.DispositionRetired || fixture.Metadata.Verify == testcatalog.VerifyReject {
			continue
		}
		if _, err := LoadWorkflowContractBundleWithOverrides(repo, fixture.Root, DefaultPlatformSpecFile(repo)); err != nil {
			t.Errorf("supported fixture %s changed admission: %v", fixture.RelativePath, err)
		} else {
			loaded++
		}
	}
	if loaded == 0 {
		t.Fatal("empty preservation corpus")
	}
	t.Logf("unchanged source corpus: %d admitted positive roots", loaded)
}
