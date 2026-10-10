package pipeline

import (
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func VerifyCompositionReceiverInitializationAndRepeatedBusinessWritesBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "name: budget\nstages:\n  active: {}\n  done: {final: true}\n",
		"entities.yaml": "budget:\n  spent_usd: {type: number, initial: 0}\n  status: {type: text, initial: pending}\n",
		"events.yaml":   "spend.recorded:\n  amount_usd: number\n  business_key: text\n",
		"nodes.yaml": `budget-writer:
  execution_type: system_node
  subscribes_to: [spend.recorded]
  event_handlers:
    spend.recorded:
      data_accumulation:
        writes:
          - {source_field: amount_usd, target_field: spent_usd}
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("source bundle missing")
	}
	fixture := open(t, source)
	newCoordinator := func() *PipelineCoordinator {
		return fixture.NewCoordinator(PipelineCoordinatorOptions{Module: handlerTestWorkflowModuleWithBundle(bundle, "budget", "budget-writer")})
	}
	ctx := correlation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	node := pipelineSourceNode(t, source, ".", "budget-writer")
	constructor, err := CompileFlowConstructor(source, semanticview.RootExecutionFlowID(source), "")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := constructor.InitialFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit component setup; public construction is qualified separately.
	at := time.Now().UTC()
	if err := fixture.Construct(ctx, WorkflowInstance{
		InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: testPipelineRunID,
		EntityType: "budget", WorkflowName: semanticview.RootExecutionFlowID(source), WorkflowVersion: source.WorkflowVersion(), Mode: "static",
		CurrentState: "active", StageDefined: true, Fields: fields, CreatedAt: at, EnteredStageAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	owner := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: testPipelineRunID, EntityID: testPipelineRunID})
	ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: owner})
	handler := bundle.Nodes["budget-writer"].EventHandlers["spend.recorded"]
	for _, amount := range []int{42, 99} {
		event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(fmt.Sprintf("budget-%d", amount)), "spend.recorded", "", "", mustJSON(map[string]any{"amount_usd": amount, "business_key": fmt.Sprintf("unrelated-%d", amount)}), 0, testPipelineRunID, events.EventEnvelope{}, eventtest.RootRoutingSource(testPipelineRunID), time.Time{})
		pc := newCoordinator()
		result, err := executeNativeWorkflowScenarioHandlerForTest(t, fixture, pc, ctx, node, handler, event)
		if err != nil || !result.Handled {
			t.Fatalf("write %d: handled=%t err=%v", amount, result.Handled, err)
		}
		current, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, testPipelineRunID))
		if err != nil || !found {
			t.Fatalf("load %d: found=%t err=%v", amount, found, err)
		}
		if current.EntityID != testPipelineRunID || fmt.Sprint(current.Fields["spent_usd"]) != fmt.Sprint(amount) || current.Fields["status"] != "pending" || current.CurrentState != "active" {
			t.Fatalf("business write or lifecycle changed incorrectly: %#v", current)
		}
		instances, err := pc.ListWorkflowInstances(ctx, testPipelineRunID)
		if err != nil || len(instances) != 1 {
			t.Fatalf("repeated initialization made extra state: %#v %v", instances, err)
		}
	}
}
