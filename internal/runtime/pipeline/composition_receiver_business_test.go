package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCompositionReceiverInitializationAndRepeatedBusinessWritesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml":   "name: budget\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n",
				"entities.yaml": "budget:\n  spent_usd: {type: number, initial: 0}\n  status: {type: text, initial: pending}\n",
				"events.yaml":   "spend.recorded:\n  amount_usd: number\n  business_key: text\n",
				"nodes.yaml": `budget-writer:
  execution_type: system_node
  subscribes_to: [spend.recorded]
  event_handlers:
    spend.recorded:
      create_entity: true
      data_accumulation:
        writes:
          - {source_field: amount_usd, target_field: spent_usd}
`,
			})
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("source bundle missing")
			}
			newCoordinator := func() *PipelineCoordinator {
				return &PipelineCoordinator{workflowStore: store, bus: &recordingPipelineBus{}, module: handlerTestWorkflowModuleWithBundle(bundle, "budget", "budget-writer"), entityLocks: map[string]*sync.Mutex{}}
			}
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			node := pipelineSourceNode(t, source, ".", "budget-writer")
			owner := events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: testPipelineRunID})
			ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: owner})
			handler := bundle.Nodes["budget-writer"].EventHandlers["spend.recorded"]
			for _, amount := range []int{42, 99} {
				event := handlerTestRootIngress(eventtest.UUID(fmt.Sprintf("budget-%d", amount)), "spend.recorded", "", "", mustJSON(map[string]any{"amount_usd": amount, "business_key": fmt.Sprintf("unrelated-%d", amount)}), 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{})
				seedExactOnceEvent(t, store, ctx, event)
				pc := newCoordinator()
				result, err := pc.executeNodeContractHandler(ctx, node, handler, workflowTriggerContext{Event: event}, false)
				if err != nil || !result.Handled {
					t.Fatalf("write %d: handled=%t err=%v", amount, result.Handled, err)
				}
				current, found, err := store.Load(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, testPipelineRunID))
				if err != nil || !found {
					t.Fatalf("load %d: found=%t err=%v", amount, found, err)
				}
				if current.EntityID != testPipelineRunID || fmt.Sprint(current.Fields["spent_usd"]) != fmt.Sprint(amount) || current.Fields["status"] != "pending" || current.CurrentState != "active" {
					t.Fatalf("business write or lifecycle changed incorrectly: %#v", current)
				}
				instances, err := store.list(ctx, testPipelineRunID)
				if err != nil || len(instances) != 1 {
					t.Fatalf("repeated initialization made extra state: %#v %v", instances, err)
				}
			}
		})
	}
}
