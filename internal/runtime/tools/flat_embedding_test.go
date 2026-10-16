package tools

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestFlatEmbeddingPublicationTool(t *testing.T) {
	rootDir := canonicalrouting.CopyRootAgentLocalEmission(t)
	for name, body := range map[string]string{
		"types.yaml":  "types:\n  Marker:\n    marker: {type: text, pattern: '^[a-z]+$'}\n    key: text\n",
		"events.yaml": "cycle.ping:\n  _description: Publish an embedded marker\n  _key: marker\n  _embed: [Marker]\ncycle.pong:\n",
		"nodes.yaml":  "test-node:\n  execution_type: system_node\n  subscribes_to: [cycle.ping]\n  event_handlers:\n    cycle.ping:\n      guard: {id: received, check: payload.key != ''}\n",
	} {
		if err := os.WriteFile(filepath.Join(rootDir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, rootDir, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	store := newEmitRoutePlanStore()
	root, err := runtimeflowidentity.StandingForGeneration(source, ".", toolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	store.instanceIndex = emitInstanceObservationFixture{observations: []runtimepipeline.FlowInstanceObservation{
		emitInstanceObservation(t, source, root, ""),
	}}
	bus := newEmitRoutePlanEventBus(t, store, source, emitInstanceSourceFact(t, source))
	actor := emitInstanceActor(t, source, root, "cycle.ping")
	registry := NewEmitRegistry(source, nil)
	executor := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: registry})
	executor.workflowInstances = emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{root.InstancePath: {
		WorkflowName: root.TemplateID, InstanceID: root.InstanceID, StorageRef: root.InstancePath, EntityID: root.EntityID,
	}}}
	definitions := registry.GenerateEmitToolsForActor(actor, nil)
	if len(definitions) != 1 || definitions[0].Description != "Publish an embedded marker" {
		t.Fatalf("embedded tool schema lost: %+v", definitions)
	}
	for _, value := range []map[string]any{
		{"marker": 1, "key": "business"}, {"marker": "123", "key": "business"}, {"marker": "ready"},
	} {
		if _, err := executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_cycle_ping", value); err == nil {
			t.Fatalf("invalid embedded tool payload published: %+v", value)
		}
	}
	if len(store.events) != 0 {
		t.Fatalf("invalid tool payload left events: %+v", store.events)
	}
	out, err := executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_cycle_ping", map[string]any{"marker": "ready", "key": "ordinary-business-value"})
	if err != nil {
		t.Fatal(err)
	}
	id := emitToolResultString(t, out, "event_id")
	persisted := store.events[id]
	if persisted.Type() != events.EventType("cycle.ping") || persisted.EntityID() != root.EntityID || len(store.routes[id]) != 1 {
		t.Fatalf("business field became envelope/routing authority: event=%+v routes=%+v", persisted, store.routes[id])
	}
}
