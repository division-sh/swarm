package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// These are ordinary handler effects, not a replacement Git or mailbox capability.
func VerifySupportedHandlerAppendEmitReadbackAndRollbackBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"accepted", "rejected", "outbox_failure"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				source := loadWorkflowTempSource(t, map[string]string{
					"schema.yaml":   "name: findings\nstages:\n  active: {}\n  done: {final: true}\n",
					"types.yaml":    "types:\n  Finding:\n    summary: text\n",
					"entities.yaml": "work:\n  findings: {type: '[Finding]', initial: []}\n  status: {type: text, initial: pending}\n",
					"events.yaml":   "finding.received:\n  summary: text\n  status: text\nfinding.recorded:\n  status: text\n",
					"nodes.yaml": `writer:
  execution_type: system_node
  subscribes_to: [finding.received]
  event_handlers:
    finding.received:
      data_accumulation:
        writes:
          - op: append
            target: entity.findings
            value:
              summary: payload.summary
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
				fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
				bus := observeNativePipelineDeliveryBusForTest(t, pc)
				runID := correlation.RunIDFromContext(ctx)
				node := pipelineSourceNode(t, source, ".", "writer")
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
					InstanceID: runID, StorageRef: runID, EntityID: runID,
					EntityType: "work", WorkflowName: semanticview.RootExecutionFlowID(source), WorkflowVersion: source.WorkflowVersion(), Mode: "static",
					CurrentState: "active", StageDefined: true, Status: "active", Fields: fields, CreatedAt: at, EnteredStageAt: at, UpdatedAt: at,
				}); err != nil {
					t.Fatal(err)
				}
				receiver := events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: runID, EntityID: runID}
				route := events.DeliveryRoute{
					Recipient: events.MustNodeDeliveryRecipient(node),
					Target:    events.MustExistingEntityTarget(receiver),
				}
				producerID := eventtest.UUID("different-producer")
				handler := bundle.Nodes["writer"].EventHandlers["finding.received"]
				for i := 0; i < 2; i++ {
					status := outcome
					if outcome == "outbox_failure" {
						status = "accepted"
					}
					envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, receiver)
					event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(fmt.Sprintf("%s-%s-%d", backend, outcome, i)), "finding.received", "", "",
						mustJSON(map[string]any{"summary": fmt.Sprintf("finding-%d", i), "status": status}), 3, runID,
						envelope, testWorkflowRoutingSource(".", producerID, producerID), time.Now().UTC())
					if err := fixture.PublishNode(ctx, event, route); err != nil {
						t.Fatal(err)
					}
					if outcome == "outbox_failure" && i == 1 {
						bus.prepareFailure = errors.New("outbox unavailable")
					}
					deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, route)
					result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, deliveryCtx, node, handler,
						workflowTriggerContext{Event: eventtest.TargetRouted(event, receiver), HandlerEventKey: "finding.received"})
					failed := outcome == "outbox_failure" && i == 1
					if failed {
						if err == nil || !strings.Contains(err.Error(), "outbox unavailable") {
							t.Fatalf("rollback error = %v", err)
						}
					} else if err != nil || !result.Handled {
						t.Fatalf("execute: handled=%t err=%v", result.Handled, err)
					}
					// A fresh read adapter consumes the original selected projection.
					restarted := fixture.FreshProjection()
					if restarted.store == fixture.Persistence.store {
						t.Fatal("readback reused the predecessor projection adapter")
					}
					current, found, err := restarted.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceForRun(runID, runID))
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
					if bus.committedCount() != wantCount || bus.publishedCount() != wantCount {
						t.Fatalf("emissions: committed=%d published=%d want=%d", bus.committedCount(), bus.publishedCount(), wantCount)
					}
					if !failed {
						emitted := bus.persistedPublishedEvent(t, fixture, ctx, i)
						if emitted.RoutingSource().Kind() != events.RoutingSourceStaticFlow || emitted.SourceRoute() != receiver || emitted.ParentEventID() != event.ID() || emitted.RunID() != event.RunID() || emitted.ChainDepth() != event.ChainDepth()+1 {
							t.Fatalf("emission lineage: %#v", emitted)
						}
					}
				}
			})
		}
	}
}
