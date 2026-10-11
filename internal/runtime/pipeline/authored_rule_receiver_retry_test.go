package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type authoredRuleRetryDiagnosticStore struct {
	runtimedelivery.Store
	failures []handlerselection.Observation
}

func (s *authoredRuleRetryDiagnosticStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	s.failures = append(s.failures, settlement.RuleSelection)
	return s.Store.SettleFailure(ctx, claim, settlement)
}

// Exercise the original source-loaded receiver failure through real claim,
// retry and engine settlement owners; only the persistence read is faulted.
func VerifyNativeAuthoredRuleReceiverPreparationRetryBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"no_fault_control", "fresh_retry", "recovered_retry"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				bundle := loadWorkflowTempBundle(t, map[string]string{
					"schema.yaml":   "name: delivery-authority\nstages:\n  queued: {}\n  done: {final: true}\n",
					"entities.yaml": "test_entity: {}\n",
					"events.yaml":   "source.evt:\n",
					"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      rules:\n        - id: complete\n          when: |-\n                  true\n          advances_to: done\n        - id: unmatched\n          else: true\n",
				})
				module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
				node := pipelineNode(t, ".", "node-a")
				module.workflowNodes = []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"source.evt"}}}
				fixture := open(t, backend, semanticview.Wrap(bundle))
				pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
				store := fixture.Persistence.store
				owner := fixture.Store
				observed := &authoredRuleRetryDiagnosticStore{Store: owner}
				bus := observeNativePipelineDeliveryBusForTest(t, pc)
				retention := &review2460NativeRetentionObserver{WorkflowDeliveryRuntime: pc.deliveryRuntime}
				pc.deliveryRuntime = retention
				pc.deliveryStore = observed
				handler, found := pc.SemanticSource().ExecutableNodeEventHandler(node, "source.evt")
				if !found || len(handler.Rules) != 2 || !handler.Rules[0].Authored() || !handler.Rules[1].Authored() || handler.Rules[1].Condition != "else" {
					t.Fatalf("source lacks the authored predicate and fallback: found=%v handler=%#v", found, handler)
				}
				ref, qualified := handler.Rules[0].DeclarationIdentity()
				if !qualified {
					t.Fatal("authored source rule lacks canonical identity")
				}
				selected, err := handlerselection.Selected(handlerselection.ContextRules, ref, "complete")
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("source: authored=%v rule=%s condition=%s target=%s ref=%s/%s/%s", handler.Rules[0].Authored(), handler.Rules[0].ID, handler.Rules[0].Condition, handler.Rules[0].AdvancesTo, ref.Flow(), ref.Family(), ref.SemanticPath())
				runID := uuid.NewString()
				ctx = runtimecorrelation.WithRunID(ctx, runID)
				if err := fixture.RequireRun(ctx, runID); err != nil {
					t.Fatal(err)
				}
				entityID := runID
				evt := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, events.EnvelopeForTargetRoute(handlerTestWorkflowEnvelope(".", runID, entityID), events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}), eventtest.StaticFlowRoutingSource(".", runID, entityID), time.Now().UTC())
				if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: pc.SemanticSource().WorkflowVersion(),
					CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
				})); err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
				if err := fixture.PublishNode(ctx, evt, route); err != nil {
					t.Fatal(err)
				}
				id, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				_, initialFacts, err := fixture.SelectionFact(ctx, evt.ID(), id)
				if err != nil || initialFacts != 0 {
					t.Fatalf("unexecuted delivery already has rule evidence: count=%d err=%v", initialFacts, err)
				}
				readFact := func() handlerselection.HandlerRuleSelectionFact {
					fact, count, err := fixture.SelectionFact(ctx, evt.ID(), id)
					if err != nil || count != 1 {
						t.Fatalf("exact stored selection count=%d error=%v", count, err)
					}
					return fact
				}
				attemptCtx := withWorkflowNodeDeliveryRoute(ctx, route)
				if variant != "no_fault_control" {
					reader := &failingReceiverPersistenceReader{WorkflowTargetPersistenceReader: pc.workflowStore.targetReader,
						failure: runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "receiver_read_unavailable", "receiver-diagnostic", "load_target", nil, errors.New("injected pre-handler receiver read failure"))}
					pc.workflowStore.targetReader = reader
					if variant == "recovered_retry" {
						acquisition, err := fixture.Continuations.Acquire(id)
						if err != nil || acquisition.Validate(id) != nil {
							t.Fatalf("recover exact native carrier: %v", err)
						}
						continuation, carrierAcquired := acquisition.Acquired()
						if !carrierAcquired {
							t.Fatal("native recovery carrier was not acquired")
						}
						guard, err := worklifetime.NewDeliveryContinuationGuard(ctx, continuation)
						if err != nil {
							t.Fatal(err)
						}
						if resolution, err := guard.Consume(nil); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
							t.Fatalf("recover native attempt ownership: %v/%v", resolution, err)
						}
						snapshot, err := owner.Snapshot(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						claimResult, err := owner.ClaimDelivery(ctx, snapshot.Authority, evt, route)
						if err != nil {
							t.Fatal(err)
						}
						acquired, ok := claimResult.Acquired()
						if !ok {
							t.Fatal("initial recovered claim not acquired")
						}
						attemptCtx = runtimedelivery.WithClaim(attemptCtx, acquired.Claim)
					}
					if handled, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt); err != nil || !handled {
						t.Fatalf("pre-handler transient failure was not accepted for retry: handled=%v err=%v", handled, err)
					}
					snapshot, err := owner.Snapshot(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					outcomes, err := owner.Outcomes(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if reader.calls == 0 || snapshot.Status != runtimedelivery.StatusFailed || snapshot.RetryCount != 1 || snapshot.NextEligibleAt.IsZero() || len(outcomes) != 1 || outcomes[0].Outcome != "retry_scheduled" {
						t.Fatalf("retry not explicitly scheduled: reader_calls=%d snapshot=%#v outcomes=%#v", reader.calls, snapshot, outcomes)
					}
					acquisition, acquisitionErr := fixture.Continuations.Acquire(id)
					held := acquisitionErr == nil && acquisition.Validate(id) == nil && acquisition.Disposition() == worklifetime.DeliveryAlreadyOwned
					if !held || retention.calls.Load() != 1 || bus.publishedCount() != 0 {
						t.Fatal("pre-handler failure lost native retention or emitted business work")
					}
					_, finalCount, err := fixture.SelectionFact(ctx, evt.ID(), id)
					if err != nil {
						t.Fatal(err)
					}
					if finalCount != 0 || snapshot.FinalSelection.Present() || len(observed.failures) != 1 || !observed.failures[0].Equal(handlerselection.NotReached()) {
						t.Fatal("pre-handler retry persisted or invented rule selection")
					}
					instance, found, err := store.Load(ctx, runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: testWorkflowInstanceRoute(runID)})
					if err != nil || !found || instance.CurrentState != "queued" {
						t.Fatalf("pre-handler failure changed source state: stage=%s found=%v err=%v", instance.CurrentState, found, err)
					}
					t.Logf("retry accepted: status=%s retries=%d claim_version=%d next_eligible=%s retained=%v outcome=%s", snapshot.Status, snapshot.RetryCount, snapshot.ClaimVersion, snapshot.NextEligibleAt, held, outcomes[0].Outcome)
					reader.failure = nil
					if err := fixture.RetryEligible(ctx, evt, route); err != nil {
						t.Fatal(err)
					}
					recoverNativePipelineRetryForTest(t, fixture, ctx)
				}
				handled, dispatchErr := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
				after, err := owner.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				persisted := readFact()
				for index, fact := range observed.failures {
					t.Logf("failure settlement attempt %d: reached=%v", index+1, fact.Reached())
				}
				instance, found, err := store.Load(ctx, runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: testWorkflowInstanceRoute(runID)})
				if err != nil || !found {
					t.Fatalf("final source state missing: found=%v err=%v", found, err)
				}
				t.Logf("final workflow stage: %s", instance.CurrentState)
				t.Logf("final execution: handled=%v err=%v status=%s retries=%d claim_version=%d", handled, dispatchErr, after.Status, after.RetryCount, after.ClaimVersion)
				outcomes, err := owner.Outcomes(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				for index, outcome := range outcomes {
					t.Logf("durable outcome %d: %s", index+1, outcome.Outcome)
				}
				if dispatchErr != nil || !handled || after.Status != runtimedelivery.StatusDelivered || !persisted.Equal(selected) || instance.CurrentState != "done" {
					t.Errorf("legitimate authored-rule delivery must complete after cleared receiver fault: handled=%v err=%v status=%s selected_fact=%v", handled, dispatchErr, after.Status, persisted.Equal(selected))
				}
			})
		}
	}
}
