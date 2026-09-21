package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// These are ordinary handler effects, not a replacement Git or mailbox capability.
func TestSupportedHandlerAppendEmitReadbackAndRollbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"accepted", "rejected", "outbox_failure"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				db, store := openHandlerEntityRequirementStore(t, backend)
				source := loadWorkflowTempSource(t, map[string]string{
					"schema.yaml":   "name: findings\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n",
					"types.yaml":    "types:\n  Finding:\n    summary: text\n",
					"entities.yaml": "work:\n  findings: {type: '[Finding]', initial: []}\n  status: {type: text, initial: pending}\n",
					"events.yaml":   "finding.received:\n  summary: text\n  status: text\nfinding.recorded:\n  status: text\n",
					"nodes.yaml": `writer:
  execution_type: system_node
  subscribes_to: [finding.received]
  event_handlers:
    finding.received:
      create_entity: true
      data_accumulation:
        writes:
          - op: append
            target: entity.findings
            value:
              cel: '{"summary": payload.summary}'
          - source_field: status
            target_field: status
      emit:
        event: finding.recorded
        fields:
          status: entity.status
`,
				})
				bundle, ok := semanticview.Bundle(source)
				if !ok {
					t.Fatal("missing source bundle")
				}
				bus := &recordingPipelineBus{}
				newCoordinator := func() *PipelineCoordinator {
					var reopened *workflowInstanceStore
					if backend == "sqlite" {
						reopened = newSQLiteWorkflowInstanceStoreForTest(t, db)
					} else {
						reopened = newPostgresWorkflowInstanceStoreForTest(db)
					}
					return &PipelineCoordinator{
						workflowStore: reopened, bus: bus,
						module:      handlerTestWorkflowModuleWithBundle(bundle, "findings", "writer"),
						entityLocks: map[string]*sync.Mutex{},
					}
				}
				var ctx context.Context
				if backend == "sqlite" {
					ctx = sqliteExactOnceRunContext(t, db)
				} else {
					ctx = testPipelineRunContext(t, db)
				}
				node := pipelineSourceNode(t, source, ".", "writer")
				receiver := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: testPipelineRunID}
				ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
					Recipient: events.MustNodeDeliveryRecipient(node),
					Target:    events.MustMaterializingEntityTarget(receiver),
				})
				producerID := eventtest.UUID("different-producer")
				handler := bundle.Nodes["writer"].EventHandlers["finding.received"]
				for i := 0; i < 2; i++ {
					status := outcome
					if outcome == "outbox_failure" {
						status = "accepted"
					}
					event := handlerTestRootIngress(eventtest.UUID(fmt.Sprintf("%s-%s-%d", backend, outcome, i)), "finding.received", "", "",
						mustJSON(map[string]any{"summary": fmt.Sprintf("finding-%d", i), "status": status}), 3, testPipelineRunID, "",
						events.EnvelopeForEntityID(events.EventEnvelope{}, producerID), time.Time{})
					seedExactOnceEvent(t, store, ctx, event)
					if outcome == "outbox_failure" && i == 1 {
						bus.outboxErr = errors.New("outbox unavailable")
					}
					result, err := newCoordinator().executeNodeContractHandler(ctx, node, handler, workflowTriggerContext{Event: event}, false)
					failed := outcome == "outbox_failure" && i == 1
					if failed {
						if err == nil || !strings.Contains(err.Error(), "outbox unavailable") {
							t.Fatalf("rollback error = %v", err)
						}
					} else if err != nil || !result.Handled {
						t.Fatalf("execute: handled=%t err=%v", result.Handled, err)
					}
					// A new store and coordinator read the durable state after each invocation.
					restarted := newCoordinator()
					current, found, err := restarted.workflowStore.Load(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, testPipelineRunID))
					if err != nil || !found {
						t.Fatalf("readback: found=%t err=%v", found, err)
					}
					wantCount := i + 1
					if failed {
						wantCount--
					}
					if current.EntityID != receiver.EntityID || current.EntityID == producerID || current.CurrentState != "active" || current.Fields["status"] != status {
						t.Fatalf("receiver business state = %#v", current)
					}
					findings, ok := current.Fields["findings"].([]any)
					if !ok || len(findings) != wantCount {
						t.Fatalf("findings = %#v, want %d", current.Fields["findings"], wantCount)
					}
					for j, finding := range findings {
						if got := finding.(map[string]any)["summary"]; got != fmt.Sprintf("finding-%d", j) {
							t.Fatalf("finding %d = %#v", j, got)
						}
					}
					if bus.outboxCount() != wantCount || bus.publishedCount() != wantCount {
						t.Fatalf("emissions: outbox=%d published=%d want=%d", bus.outboxCount(), bus.publishedCount(), wantCount)
					}
					if !failed {
						emitted := bus.outboxIntent(i).Event
						if emitted.SourceRoute() != (events.RouteIdentity{EntityID: receiver.EntityID}) || emitted.ParentEventID() != event.ID() || emitted.RunID() != event.RunID() || emitted.ChainDepth() != event.ChainDepth()+1 {
							t.Fatalf("emission lineage: %#v", emitted)
						}
					}
				}
			})
		}
	}
}
