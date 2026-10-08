package manager

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestMaterializedAgentEmitPermissionRetainsDeclarationOnEveryScope(t *testing.T) {
	root := t.TempDir()
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: emit-owner-proof\nstages: []\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), "result.done:\n  owned: text\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  role: worker\n  intent: {inline: Emit the root result.}\n  emit_events: [result.done]\n")
	for _, flow := range []struct{ id, mode string }{{"left", "singleton"}, {"right", "template"}, {"nested/deeper", "template"}} {
		shape := ""
		if flow.mode == "template" {
			shape = "instance: instance_key\n"
		}
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "schema.yaml"), fmt.Sprintf("%sstages:\n  active: {}\n", shape))
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "entities.yaml"), "item:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "events.yaml"), "result.done:\n  owned: text\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "agents.yaml"), "worker:\n  role: worker\n  intent: {inline: Emit this flow's result.}\n  emit_events: [result.done]\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "nested", "schema.yaml"), "stages: []\n")
	repo := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	registry := runtimetools.NewEmitRegistry(source, nil)
	records, err := StaticAgentMaterializationRecords(managerIdentityTestRunID, source)
	if err != nil {
		t.Fatal(err)
	}
	parent := flowidentity.Stored(source, ".", managerIdentityTestRunID, managerIdentityTestRunID, managerIdentityTestRunID, "")
	left, err := flowidentity.KeylessChild(source, parent, "left")
	if err != nil {
		t.Fatal(err)
	}
	constructed, err := ConstructedFlowMaterialization(source, managerIdentityTestRunID, left)
	if err != nil || len(constructed.Agents) != 1 {
		t.Fatalf("constructed static declaration: %+v %v", constructed, err)
	}
	leftRecord, err := constructed.Agents[0].Materialize(managerIdentityTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	records = append(records, leftRecord)
	for _, flow := range []string{"right", "nested/deeper"} {
		view, found := bundle.FlowViewByID(flow)
		if !found || view.Parent == nil {
			t.Fatal("materialization fixture requires its exact parent declaration")
		}
		parent, err := flowidentity.StandingForGeneration(source, view.Parent.Paths.FlowPath, managerIdentityTestRunID)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := flowidentity.KeyedChild(source, parent, flow, "instance-1")
		if err != nil {
			t.Fatal(err)
		}
		materialized, err := ConstructedFlowMaterialization(source, managerIdentityTestRunID, identity)
		if err != nil {
			t.Fatal(err)
		}
		for _, blueprint := range materialized.Agents {
			record, err := blueprint.Materialize(managerIdentityTestRunID)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
	}
	if len(records) != 4 {
		t.Fatalf("real loader/materializer did not produce all four owners: %+v", records)
	}
	for _, record := range records {
		t.Run(record.Config.FlowID, func(t *testing.T) {
			actor := record.Config
			if !reflect.DeepEqual(actor.EmitEvents, []string{"result.done"}) {
				t.Errorf("materialization specialized declaration permission into runtime spelling: %+v", actor.EmitEvents)
			}
			generated := registry.GenerateEmitToolsForActor(actor, nil)
			if len(generated) != 1 || generated[0].Name != "emit_result_done" {
				t.Fatalf("materialized exact owner lost emit tool: %+v", generated)
			}
			event, schema, ok := registry.EventSchemaForActorTool(actor, generated[0].Name)
			if !ok || event != "result.done" || runtimetools.ValidatePayloadAgainstSchema(schema.Schema, map[string]any{"owned": "yes"}) != nil {
				t.Fatalf("tool handoff lost declaration/schema: event=%s schema=%+v ok=%t", event, schema, ok)
			}
			actor.EmitEvents = []string{"foreign/result.done"}
			if _, _, ok := registry.EventSchemaForActorTool(actor, "emit_result_done"); ok {
				t.Fatal("foreign same-leaf permission acquired this declaration")
			}
		})
	}
}
