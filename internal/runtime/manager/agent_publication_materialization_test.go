package manager

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func TestMaterializedAgentEmitPermissionRetainsDeclarationOnEveryScope(t *testing.T) {
	root := t.TempDir()
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: emit-owner-proof\nstages: {}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), "result.done:\n  owned: text\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  type: generic\n  role: worker\n  intent: {inline: Emit the root result.}\n  emit_events: [result.done]\n")
	for _, flow := range []struct{ id, mode string }{{"left", "singleton"}, {"right", "template"}, {"nested/deeper", "template"}} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "schema.yaml"), fmt.Sprintf("mode: %s\nstages:\n  active: {initial: true}\n", flow.mode))
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "entities.yaml"), "item: {}\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "events.yaml"), "result.done:\n  owned: text\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow.id, "agents.yaml"), "worker:\n  type: generic\n  role: worker\n  intent: {inline: Emit this flow's result.}\n  emit_events: [result.done]\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "nested", "schema.yaml"), "mode: singleton\nstages: {}\n")
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
	for _, flow := range []string{"right", "nested/deeper"} {
		materialized, err := TemplateFlowAgentMaterializationRecords(managerIdentityTestRunID, source, flow, flow+"/instance-1", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, materialized...)
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
