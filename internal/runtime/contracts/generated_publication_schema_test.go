package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledActivityResultBindingsRetainCurrentLoopRevision(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", false)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := map[string]string{"root": ".", "static": "source", "template": "source", "nested_template": "outer/source"}[mode]
			view := bundle.FlowTree.ByID[flow]
			if view == nil {
				t.Fatalf("missing declaring flow %s", flow)
			}
			handler := view.Nodes["producer"].EventHandlers["activity.requested"]
			handler.Loop = &LoopOperationSpec{Admit: "revision", From: "review"}
			view.Nodes["producer"].EventHandlers["activity.requested"] = handler
			// Recompilation must use current declarations, not the previous semantic
			// generation. Pins must capture the completed result schema on each pass.
			for _, fieldName := range []string{"revision_id", "next_revision"} {
				view.Schema.LoopDeclarations = FlowLoopDeclarations{Declared: true, Entries: []FlowLoopDeclaration{{
					ID: "revision", RevisionField: fieldName, MaxAttempts: LoopAttemptLimit{Literal: 3},
					Escape: LoopEscapeSpec{AdvancesTo: "exhausted"},
				}}}
				if flow == "." {
					*bundle.RootSchema = view.Schema
				} else {
					bundle.FlowSchemas[flow] = view.Schema
				}
				if err := CompileWorkflowSemantics(bundle); err != nil {
					t.Fatal(err)
				}
				for _, result := range []string{"send.succeeded", "send.failed"} {
					for _, receiver := range []string{flow, "sink"} {
						schema, found, err := bundle.ResolveEffectiveCompiledFlowEventSchema(receiver, result)
						if err != nil || !found || !slices.Contains(schema.RequiredFieldNames(), fieldName) {
							t.Fatalf("%s/%s lost required %s: fields=%v found=%t err=%v", receiver, result, fieldName, schema.RequiredFieldNames(), found, err)
						}
						field, found := schema.StructuralField(fieldName)
						if !found || field.IsOptional || field.Type.Kind != "text" {
							t.Fatalf("%s/%s revision structural evidence = %+v found=%t", receiver, result, field, found)
						}
						if fieldName == "next_revision" && slices.Contains(schema.RequiredFieldNames(), "revision_id") {
							t.Fatal("schema retained the prior loop declaration's field")
						}
					}
				}
			}
		})
	}
}

func TestGeneratedPublicationSchemaOwnershipDoesNotBorrowSibling(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := "source"
			if mode == "root" {
				flow = "."
			} else if mode == "nested_template" {
				flow = "outer/source"
			}
			for _, local := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
				t.Run(local, func(t *testing.T) {
					qualified := local
					if flow != "." {
						qualified = flow + "/" + local
					}
					generated, ok := bundle.GeneratedActivityEventEntries()[qualified]
					if !ok || generated.Payload.Properties["activity_id"].Type != "string" {
						t.Fatalf("activity producer missing: found=%t entry=%+v", ok, generated)
					}
					for _, receiver := range []string{flow, "sink"} {
						entry, key, ok := bundle.ResolveFlowEventCatalogEntry(receiver, local)
						if !ok || entry.Payload.Properties["activity_id"].Type != generated.Payload.Properties["activity_id"].Type {
							t.Errorf("%s catalog borrowed or lost generated owner: found=%t key=%q type=%q want=%q", receiver, ok, key, entry.Payload.Properties["activity_id"].Type, generated.Payload.Properties["activity_id"].Type)
						}
						compiled, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(receiver, local)
						if err != nil || !ok || compiled.Classification() != CompiledEventSchemaGenerated || compiled.Importable() {
							t.Errorf("%s immutable generated owner: found=%t class=%s importable=%t err=%v", receiver, ok, compiled.Classification(), compiled.Importable(), err)
						}
					}
					sibling, _, ok := bundle.ResolveFlowEventCatalogEntry("sibling", local)
					if !ok || sibling.Payload.Properties["activity_id"].Type != "integer" {
						t.Errorf("sibling lost its independent integer declaration: found=%t entry=%+v", ok, sibling)
					}
				})
			}
			// Routing's generated schemas must not become resource declarations.
			importable, err := bundle.CompiledEventSchemas()
			if err != nil {
				t.Fatal(err)
			}
			for _, schema := range importable {
				if schema.FlowPath() == flow && strings.Contains(schema.EventName(), "send.") {
					t.Errorf("generated activity became importable: %+v", schema)
				}
			}
		})
	}
}

func TestGeneratedPublicationSchemaExactCoordinatesAndReadback(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			flow := map[string]string{"root": ".", "static": "source", "template": "source", "nested_template": "outer/source"}[mode]
			// Rename only the connected delivery. The producer and incompatible
			// sibling keep their declaration spelling and must remain independent.
			path := filepath.Join(root, "schema.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			before := "event: send.succeeded, from: " + flow + ", to: sink"
			if strings.Count(string(raw), before) != 1 {
				t.Fatal("missing exact connect to rename")
			}
			if err := os.WriteFile(path, []byte(strings.Replace(string(raw), before, before+", rename: delivery.observed", 1)), 0600); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"schema.yaml", "nodes.yaml"} {
				path = filepath.Join(root, "sink", name)
				raw, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(raw), "send.succeeded", "delivery.observed")), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			producer, ok, err := bundle.ResolveCompiledFlowEventSchema(flow, "send.succeeded")
			if err != nil || !ok {
				t.Fatalf("producer=%t %v", ok, err)
			}
			receiver, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema("sink", "delivery.observed")
			if err != nil || !ok || receiver.FlowPath() != producer.FlowPath() || receiver.EventName() != producer.EventName() || receiver.AcceptanceSchemaDigest() != producer.AcceptanceSchemaDigest() {
				t.Fatalf("renamed receiver lost exact source: producer=%#v receiver=%#v found=%t err=%v", producer, receiver, ok, err)
			}
			for _, absent := range []string{"absent", flow + "/not-a-declaration"} {
				if _, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(absent, producer.EventName()); ok || err != nil {
					t.Fatalf("non-owner %s admitted: %t %v", absent, ok, err)
				}
			}
			if flow != "." {
				if _, ok, _ := bundle.ResolveCompiledFlowEventSchema("sibling", producer.EventName()); ok {
					t.Fatal("sibling borrowed qualified producer")
				}
			} else {
				alias, ok, err := bundle.ResolveCompiledFlowEventSchema("", "send.succeeded")
				if err != nil || !ok || alias.FlowPath() != "." || alias.AcceptanceSchemaDigest() != producer.AcceptanceSchemaDigest() {
					t.Fatal("empty/dot root diverged")
				}
			}
			beforeSchema := producer.EventSchema()
			mutated := receiver.EventSchema()
			mutated.Schema["properties"].(map[string]any)["activity_id"].(map[string]any)["type"] = "integer"
			mutated.CitationFields["injected"] = CriteriaCitation{}
			if !reflect.DeepEqual(beforeSchema, producer.EventSchema()) || !reflect.DeepEqual(beforeSchema, receiver.EventSchema()) {
				t.Fatal("readback mutation crossed schema owner")
			}
			result, ok := producer.StructuralField("result")
			delivered, fieldOK := result.Type.Field("delivered")
			if !ok || !fieldOK || delivered.IsOptional || delivered.Type.Kind != "boolean" {
				t.Fatalf("tool output structural type lost: %#v", result)
			}
		})
	}
}

func TestCompiledPublicationBindingsSurviveRawMapMutation(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := map[string]string{"root": ".", "static": "source", "template": "source", "nested_template": "outer/source"}[mode]
			producer, ok, err := bundle.ResolveCompiledFlowEventSchema(flow, "send.succeeded")
			if err != nil || !ok {
				t.Fatalf("producer admission: %t %v", ok, err)
			}
			resources, err := bundle.CompiledEventSchemas()
			if err != nil {
				t.Fatal(err)
			}
			generated := bundle.GeneratedActivityEventSchemas()
			registry := EventSchemaRegistryFromBundle(bundle)
			for _, view := range bundle.FlowTree.ByID {
				view.Events = nil
				view.Nodes = nil
			}
			bundle.Nodes, bundle.Events, bundle.Tools = nil, nil, nil
			for _, receiver := range []string{flow, "sink"} {
				compiled, found, err := bundle.ResolveEffectiveCompiledFlowEventSchema(receiver, "send.succeeded")
				if err != nil || !found || compiled.value != producer.value {
					t.Fatalf("%s recompiled or lost admitted reference: found=%t err=%v", receiver, found, err)
				}
				entry, key, found := bundle.ResolveFlowEventCatalogEntry(receiver, "send.succeeded")
				if !found || key != producer.EventName() || entry.Payload.Properties["activity_id"].Type != "string" {
					t.Fatalf("%s catalog reread mutable syntax: key=%q found=%t entry=%+v", receiver, key, found, entry)
				}
			}
			again, err := bundle.CompiledEventSchemas()
			if err != nil || !reflect.DeepEqual(resources, again) || !reflect.DeepEqual(generated, bundle.GeneratedActivityEventSchemas()) || !reflect.DeepEqual(registry, EventSchemaRegistryFromBundle(bundle)) {
				t.Fatalf("resource/generated readback recompiled declarations: %v", err)
			}
			if source := producer.Source(); source.Layer != "generated_activity" || !strings.HasSuffix(source.File, "nodes.yaml") {
				t.Fatalf("generated provenance does not name the declaring handler: %+v", source)
			}
		})
	}
}

func TestCompiledPublicationBindingsIgnoreUnconnectedSiblingChanges(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			repo := canonicalrouting.RepoRoot(t)
			flow := map[string]string{"root": ".", "static": "source", "template": "source", "nested_template": "outer/source"}[mode]
			load := func() *WorkflowContractBundle {
				bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				return bundle
			}
			original := load()
			check := func(bundle *WorkflowContractBundle) {
				for _, name := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
					for _, receiver := range []string{flow, "sink"} {
						before, ok, err := original.ResolveEffectiveCompiledFlowEventSchema(receiver, name)
						if err != nil || !ok {
							t.Fatalf("original %s/%s: %t %v", receiver, name, ok, err)
						}
						after, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(receiver, name)
						if err != nil || !ok || before.FlowPath() != after.FlowPath() || before.EventName() != after.EventName() || before.Classification() != after.Classification() || !reflect.DeepEqual(before.EventSchema(), after.EventSchema()) {
							t.Fatalf("unconnected sibling changed %s/%s: found=%t err=%v", receiver, name, ok, err)
						}
					}
				}
			}
			// Moving this incompatible declaration first or last in the source
			// census must not influence the connected source's exact binding.
			if err := os.Rename(filepath.Join(root, "sibling"), filepath.Join(root, "aaa_sibling")); err != nil {
				t.Fatal(err)
			}
			check(load())
			if err := os.Rename(filepath.Join(root, "aaa_sibling"), filepath.Join(root, "zzz_sibling")); err != nil {
				t.Fatal(err)
			}
			check(load())
			if err := os.RemoveAll(filepath.Join(root, "zzz_sibling")); err != nil {
				t.Fatal(err)
			}
			check(load())
		})
	}
}

func TestFailedPublicationSchemaAdmissionCannotRetainOldBindings(t *testing.T) {
	root := canonicalrouting.CopyPublicationActivity(t, "root", "http://127.0.0.1:1/send", false)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	entry := bundle.FlowTree.Root.Events["activity.requested"]
	entry.Payload.Properties["message"] = EventFieldSpec{Type: "MissingType"}
	bundle.FlowTree.Root.Events["activity.requested"] = entry
	if err := CompileWorkflowSemantics(bundle); err == nil || !strings.Contains(err.Error(), "MissingType") {
		t.Fatalf("readmission error = %v, want invalid type refusal", err)
	}
	for _, receiver := range []string{".", "sink"} {
		if _, ok, _ := bundle.ResolveEffectiveCompiledFlowEventSchema(receiver, "send.succeeded"); ok {
			t.Fatalf("%s retained a previous compiled binding after rejected admission", receiver)
		}
	}
	if _, err := bundle.CompiledEventSchemas(); err == nil || len(bundle.GeneratedActivityEventSchemas()) != 0 {
		t.Fatal("rejected admission exposed previous resource or generated schemas")
	}
	if len(effectiveEventSchemaOwnershipRows(bundle)) != 0 || len(eventSchemaOwnershipRowsForReceiver(bundle, "sink")) != 0 {
		t.Fatal("rejected admission retained previous connect ownership")
	}
}
