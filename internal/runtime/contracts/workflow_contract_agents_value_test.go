package contracts

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestW5AgentFieldPresenceMatrix(t *testing.T) {
	const text = "missing,null,empty,scalar,empty_sequence,sequence,empty_mapping,mapping"
	states := strings.Split(text, ",")
	for _, tc := range []struct {
		field, scalar, sequence, mapping, admitted string
	}{
		{"id", "public-worker", "[worker]", "{id: worker}", "missing,scalar"},
		{"type", "review-worker", "[generic]", "{type: generic}", "missing,scalar"},
		{"model", "regular", "[regular]", "{model: regular}", "missing,empty,scalar"},
		{"memory", "true", "[true]", "{enabled: true}", "missing,scalar"},
		{"max_turns_per_task", "7", "[7]", "{count: 7}", "missing,scalar"},
		{"workspace_class", "sandbox", "[sandbox]", "{class: sandbox}", "missing,scalar"},
		{"intent", "intents/worker.md", "[worker]", "{inline: business intent}", "scalar,mapping"},
		{"entity_writes", "all", "[value]", "{Item: {create: all, save: [value]}}", "missing,empty_mapping,mapping"},
		{"data_access", "records", "[{data: records, flow_path: support}]", "{data: records}", "missing,empty_sequence,sequence"},
		{"native_tools", "true", "[bash]", "{bash: true, web_search: false, file_io: true}", "missing,empty_mapping,mapping"},
		{"mock", "python", "[python]", "{kind: python, module: mocks/worker.py, post_tool_tail_latency_ms: 3}", "missing,empty_mapping,mapping"},
	} {
		for _, state := range states {
			t.Run(tc.field+"/"+state, func(t *testing.T) {
				value := map[string]string{"null": "null", "empty": "''", "scalar": tc.scalar, "empty_sequence": "[]", "sequence": tc.sequence, "empty_mapping": "{}", "mapping": tc.mapping}[state]
				body := "worker:\n"
				if tc.field != "intent" {
					body += "  intent: {inline: business intent}\n"
				}
				if state != "missing" {
					body += "  " + tc.field + ": " + value + "\n"
				}
				path := filepath.Join(t.TempDir(), "agents.yaml")
				writeFixtureFile(t, path, body)
				entries, err := loadOptionalAgentDeclarations(path)
				want := strings.Contains(","+tc.admitted+",", ","+state+",")
				if (err == nil) != want {
					t.Fatalf("admit=%t, want %t: %v", err == nil, want, err)
				}
				if err == nil {
					entry := entries["worker"]
					if entry.AuthoredFields[tc.field] != (state != "missing") {
						t.Fatal("authored presence lost")
					}
					texts := map[string]string{"id": entry.ID, "type": entry.Type, "model": entry.Model, "workspace_class": entry.WorkspaceClass}
					if got, ok := texts[tc.field]; ok {
						want := ""
						if state == "scalar" {
							want = tc.scalar
						}
						if got != want {
							t.Fatalf("typed text=%q, want %q", got, want)
						}
					}
					if state == "scalar" && tc.field == "memory" && !entry.Memory {
						t.Fatal("true lost")
					}
					if state == "scalar" && tc.field == "max_turns_per_task" && entry.MaxTurnsPerTask != 7 {
						t.Fatal("integer lost")
					}
					if state == "mapping" && tc.field == "entity_writes" && (!entry.EntityWrites["Item"].Create.All || !reflect.DeepEqual(entry.EntityWrites["Item"].Save.Fields, []string{"value"})) {
						t.Fatal("grants lost")
					}
					if state == "sequence" && tc.field == "data_access" && !reflect.DeepEqual(entry.DataAccess, []DurableDataAccessRef{{Data: "records", FlowPath: "support"}}) {
						t.Fatal("data grant lost")
					}
					if state == "mapping" && tc.field == "native_tools" && !reflect.DeepEqual(entry.NativeTools, map[string]any{"bash": true, "web_search": false, "file_io": true}) {
						t.Fatal("typed native capabilities lost")
					}
					if state == "mapping" && tc.field == "mock" && (entry.Mock.Kind != "python" || entry.Mock.Module != "mocks/worker.py" || entry.Mock.PostToolTailLatencyMS != 3) {
						t.Fatal("typed mock coordinates lost")
					}
				}
			})
		}
	}
	for _, field := range []string{"role", "permissions_bundle", "manager_fallback", "node_type", "implementation"} {
		for _, state := range states {
			t.Run(field+"/"+state, func(t *testing.T) {
				value := map[string]string{"null": "null", "empty": "''", "scalar": "worker", "empty_sequence": "[]", "sequence": "[worker]", "empty_mapping": "{}", "mapping": "{worker: true}"}[state]
				body := "worker:\n  intent: {inline: business intent}\n"
				if state != "missing" {
					body += "  " + field + ": " + value + "\n"
				}
				entries, err := admitW5Agents(t, body)
				want := state == "missing" || state == "empty" || state == "scalar"
				if (err == nil) != want {
					t.Fatalf("%s: %v", state, err)
				}
				if err == nil && entries["worker"].AuthoredFields[field] != (state != "missing") {
					t.Fatal("presence lost")
				}
				if err == nil {
					entry := entries["worker"]
					got := map[string]string{"role": entry.Role, "permissions_bundle": entry.PermissionsBundle, "manager_fallback": entry.ManagerFallback, "node_type": entry.NodeType, "implementation": entry.Implementation}[field]
					want := ""
					if state == "scalar" {
						want = "worker"
					}
					if got != want {
						t.Fatalf("typed text=%q, want %q", got, want)
					}
				}
			})
		}
	}
	for _, field := range []string{"subscriptions", "tools", "permissions", "flow_data_access", "criteria", "emit_events"} {
		for _, state := range states {
			t.Run(field+"/"+state, func(t *testing.T) {
				value := map[string]string{"null": "null", "empty": "''", "scalar": "worker", "empty_sequence": "[]", "sequence": "[worker]", "empty_mapping": "{}", "mapping": "{worker: true}"}[state]
				body := "worker:\n  intent: {inline: business intent}\n"
				if state != "missing" {
					body += "  " + field + ": " + value + "\n"
				}
				entries, err := admitW5Agents(t, body)
				want := state == "missing" || state == "empty_sequence" || state == "sequence"
				if (err == nil) != want {
					t.Fatalf("%s: %v", state, err)
				}
				if err == nil {
					entry := entries["worker"]
					got := map[string][]string{"subscriptions": entry.Subscriptions, "tools": entry.Tools, "permissions": entry.Permissions, "flow_data_access": entry.FlowDataAccess, "criteria": entry.Criteria, "emit_events": entry.EmitEvents}[field]
					var want []string
					if state == "sequence" {
						want = []string{"worker"}
					} else if state == "empty_sequence" {
						want = []string{}
					}
					if !reflect.DeepEqual(got, want) || entry.AuthoredFields[field] != (state != "missing") {
						t.Fatalf("typed list=%#v, want %#v", got, want)
					}
				}
			})
		}
	}
}

func admitW5Agents(t testing.TB, body string) (map[string]AgentRegistryEntry, error) {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return projectAgentDeclarationsValue(snapshot.Document("agents.yaml").Root())
}

func TestW5AgentNestedBranchPresence(t *testing.T) {
	for _, tc := range []struct{ name, field string }{
		{"null_create", "entity_writes: {Item: {create: null}}"},
		{"unknown_null_write", "entity_writes: {Item: {unknown: null}}"},
		{"numeric_write_list", "entity_writes: {Item: {save: [7]}}"},
		{"empty_write", "entity_writes: {Item: {create: []}}"},
		{"object_write", "entity_writes: {Item: {create: {fields: [value]}}}"},
		{"mixed_all", "entity_writes: {Item: {save: [all, value]}}"},
		{"numeric_data", "data_access: [{data: 7}]"},
		{"missing_data", "data_access: [{flow_path: support}]"},
		{"null_data", "data_access: null"},
		{"unknown_data", "data_access: [{data: records, typo: null}]"},
		{"empty_data_path", "data_access: [{data: records, flow_path: ''}]"},
		{"null_native", "native_tools: {bash: null}"},
		{"unknown_native", "native_tools: {exec: true}"},
		{"quoted_native", "native_tools: {bash: 'true'}"},
		{"unknown_mock", "mock: {kind: python, module: mocks/a.py, typo: null}"},
		{"derived_mock_source", "mock: {source: bytes}"},
		{"derived_mock_digest", "mock: {digest: 'sha256:abc'}"},
		{"derived_mock_path", "mock: {source_path: mocks/a.py}"},
		{"fractional_mock_latency", "mock: {post_tool_tail_latency_ms: 1.5}"},
		{"negative_mock_latency", "mock: {post_tool_tail_latency_ms: -1}"},
		{"numeric_model", "model: 7"},
		{"null_intent", "intent: null"},
		{"numeric_intent", "intent: 7"},
		{"mixed_intent", "intent: {inline: business intent, import: '@pack/intent'}"},
		{"unknown_intent", "intent: {inline: business intent, unknown: null}"},
		{"null_inline", "intent: {inline: null}"},
		{"fractional_turn_cap", "max_turns_per_task: 1.5"},
		{"quoted_turn_cap", "max_turns_per_task: '7'"},
		{"zero_turn_cap", "max_turns_per_task: 0"},
		{"padded_id", "id: ' worker-2 '"},
		{"interpolated_id", "id: 'worker-{item}'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "worker:\n"
			if !strings.HasPrefix(tc.field, "intent:") {
				body += "  intent: {inline: business intent}\n"
			}
			body += "  " + tc.field + "\n"
			path := filepath.Join(t.TempDir(), "agents.yaml")
			writeFixtureFile(t, path, body)
			_, err := loadOptionalAgentDeclarations(path)
			if err == nil {
				t.Fatal("invalid nested shape admitted")
			}
			if diagnostic, ok := AsLoaderDiagnostic(err); ok {
				if diagnostic.Location.File != path || diagnostic.Location.Line == 0 || !strings.Contains(diagnostic.Location.YAMLPath, "worker") {
					t.Fatalf("source-located refusal missing: %+v", diagnostic)
				}
			} else if !strings.Contains(err.Error(), "agents.yaml:") {
				t.Fatalf("source-located refusal missing: %v", err)
			}
		})
	}
}

func TestW5AgentDefaultRetirementAndMergedFields(t *testing.T) {
	for _, field := range []string{"id: worker", "type: generic", "memory: false", "max_turns_per_task: 100", "workspace_class: ''"} {
		for _, merged := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/merged=%v", field, merged), func(t *testing.T) {
				body := "worker:\n  intent: {inline: business intent}\n  "
				if merged {
					body += "<<: {" + field + "}\n"
				} else {
					body += field + "\n"
				}
				if _, err := admitW5Agents(t, body); err == nil || !strings.Contains(err.Error(), "repeats the implicit default") {
					t.Fatalf("default not retired: %v", err)
				}
			})
		}
	}
	for _, field := range []string{"model_tier", "mode", "conversation_mode", "session_scope", "session_scope_authority", "tools_tier2", "subscriptions_bootstrap", "subscribes_to", "prompt_ref", "prompt_inputs", "profile", "agent_defaults", "agent_profiles", "runtime_id_template"} {
		for _, value := range []string{"null", "''", "[]", "{}", "legacy"} {
			body := fmt.Sprintf("worker:\n  intent: {inline: business intent}\n  <<: {%s: %s}\n", field, value)
			if _, err := admitW5Agents(t, body); err == nil || !strings.Contains(err.Error(), "not supported") || !strings.Contains(err.Error(), field) {
				t.Errorf("%s %s not retired: %v", field, value, err)
			}
		}
	}
}

func TestW5AgentAdmissionDiskCatalogRetainedParity(t *testing.T) {
	for _, body := range []string{
		"worker:\n  intent: {inline: '  exact business bytes  '}\n  model: regular\n  entity_writes: {Item: {save: [value]}}\n",
		"worker:\n  intent: {inline: business intent}\n  entity_writes: {Item: {create: null}}\n",
		"worker:\n  intent: {inline: business intent}\n  <<: {memory: false}\n",
	} {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: w5-parity\n")
		writeFixtureFile(t, filepath.Join(root, "agents.yaml"), body)
		disk, diskErr := loadOptionalAgentDeclarations(filepath.Join(root, "agents.yaml"))
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := persisted.Decode()
		if err != nil {
			t.Fatal(err)
		}
		retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
		if err != nil {
			t.Fatal(err)
		}
		for name, source := range map[string]*sourceartifact.AdmittedSourceArtifact{"artifact": artifact, "catalog": catalog, "retained": retained} {
			entries, err := loadOptionalAgentDeclarationsFromSource(source, "agents.yaml")
			if (err == nil) != (diskErr == nil) {
				t.Fatalf("%s admission differs: %v / %v", name, err, diskErr)
			}
			if !bytes.Equal(artifact.LogicalBlob(), source.LogicalBlob()) || artifact.BundleHash() != source.BundleHash() {
				t.Fatal("source hash/blob changed")
			}
			if err == nil {
				left, right := disk["worker"], entries["worker"]
				left.admissionProvenance, right.admissionProvenance = nil, nil
				if !reflect.DeepEqual(left, right) {
					t.Fatalf("%s typed projection differs", name)
				}
			} else if !strings.Contains(err.Error(), "agents.yaml:") {
				t.Fatalf("%s lost diagnostic: %v", name, err)
			}
		}
	}
}

func TestW5AgentDocumentWideExpansionBudget(t *testing.T) {
	for _, mode := range []string{"alias", "merge"} {
		for _, count := range []int{10, 10000} {
			t.Run(fmt.Sprintf("%s/%d", mode, count), func(t *testing.T) {
				var body strings.Builder
				body.WriteString("first: &agent\n  intent: {inline: business intent}\n  native_tools: {bash: false, web_search: false, file_io: false}\n")
				for i := 0; i < count; i++ {
					if mode == "alias" {
						fmt.Fprintf(&body, "agent%d: *agent\n", i)
					} else {
						fmt.Fprintf(&body, "agent%d: {<<: *agent}\n", i)
					}
				}
				_, err := admitW5Agents(t, body.String())
				if count == 10 && err != nil {
					t.Fatal(err)
				}
				if count == 10000 && (err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT")) {
					t.Fatalf("aggregate budget not enforced: %v", err)
				}
			})
		}
	}
}

func TestW5AgentProvenanceComposesWithoutMemorySource(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: provenance\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\ntask.done:\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  event_handlers:\n    task.ready:\n      emit: task.done\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: business intent}\n  model: regular\n")
	writeFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\n")
	writeFixtureFile(t, filepath.Join(root, "child", "agents.yaml"), "worker:\n  intent: {inline: child intent}\n  model: regular\n  memory: true\n")
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"disk", "artifact"} {
		t.Run(source, func(t *testing.T) {
			var bundle *WorkflowContractBundle
			var err error
			if source == "disk" {
				bundle, err = LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			} else {
				bundle, err = LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			families := map[string]bool{}
			for _, entry := range bundle.EffectiveProvenance().Entries() {
				for _, family := range []string{"agents[", "nodes[", "events[", "schemas["} {
					if strings.HasPrefix(entry.Path, family) {
						families[family] = true
					}
				}
				if strings.HasPrefix(entry.Path, "agents[") && (strings.HasSuffix(entry.Path, ".memory") || strings.Contains(entry.Path, "memory_source")) {
					t.Fatalf("retired memory source survived: %+v", entry)
				}
			}
			for _, family := range []string{"agents[", "nodes[", "events[", "schemas["} {
				if !families[family] {
					t.Fatalf("%s provenance overwritten or missing", family)
				}
			}
			for _, record := range bundle.AgentDeclarationRecords() {
				effective := EffectiveAgentRegistryEntry(record.LogicalID, record.Entry)
				if effective.MemoryPlan.Enabled != (record.Source.FlowPath == "child") || effective.EffectiveSourceForField("memory") != "" {
					t.Fatalf("scope or enablement changed: %+v", record)
				}
			}
		})
	}
}

func TestW5AgentAliasMergeNestedProvenance(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		line, column int
	}{
		{"alias", "first: &agent\n  intent: {inline: business intent}\n  entity_writes: {Item: {save: [value]}}\nsecond: *agent\n", 4, 9},
		{"merge", "first: &agent\n  intent: {inline: business intent}\n  entity_writes: {Item: {save: [value]}}\nsecond:\n  <<: *agent\n", 5, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries, err := admitW5Agents(t, tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(entries["second"].EntityWrites["Item"].Save.Fields, []string{"value"}) {
				t.Fatal("nested alias grant changed")
			}
			proof := entries["second"].admissionProvenance["entity_writes.Item.save[0]"]
			if proof.SourceFile != "agents.yaml" || proof.SourceLine != tc.line || proof.SourceColumn != tc.column {
				t.Fatalf("authored introduction coordinate lost: %+v", proof)
			}
		})
	}
}
