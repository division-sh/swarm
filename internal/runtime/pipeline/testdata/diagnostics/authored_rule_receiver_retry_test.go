package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type authoredRuleRetryDiagnosticStore struct {
	runtimedelivery.Store
	failures []handlerselection.HandlerRuleSelectionFact
}

func (s *authoredRuleRetryDiagnosticStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	s.failures = append(s.failures, settlement.RuleSelection)
	return s.Store.SettleFailure(ctx, claim, settlement)
}

// This separate diagnostic is overlaid into package pipeline for explicit runs.
// The retry cases assert the legitimate successful-retry behavior and expose
// the current failure; they do not bless the conflict as expected behavior.
func TestAuthoredRuleReceiverPreparationRetryDiagnosticBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		for _, variant := range []string{"no_fault_control", "fresh_retry", "recovered_retry"} {
			t.Run(backend.name+"/"+variant, func(t *testing.T) {
				store, ctx := backend.open(t)
				bundle := loadWorkflowTempBundle(t, map[string]string{
					"schema.yaml":   "name: delivery-authority\ninitial_state: queued\nstates: [queued, done]\nterminal_states: [done]\n",
					"entities.yaml": "test_entity: {}\n",
					"events.yaml":   "source.evt: {}\n",
					"nodes.yaml":    "node-a:\n  id: node-a\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      rules:\n        - id: complete\n          condition: 'true'\n          advances_to: done\n",
				})
				module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
				node := pipelineNode(t, ".", "node-a")
				module.workflowNodes = []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"source.evt"}, Policies: map[string]WorkflowEventPolicy{"source.evt": {Consume: true}}}}
				owner := newPipelineTestDeliveryOwnerForDB(t, store.testDB())
				observed := &authoredRuleRetryDiagnosticStore{Store: owner}
				bus := &recordingPipelineBus{}
				pc := newPostgresPipelineCoordinatorForTest(bus, store.testDB(), PipelineCoordinatorOptions{Module: module, DeliveryStore: owner})
				pc.workflowStore = store
				configurePipelineTestDeliveryOwner(t, pc)
				pc.deliveryStore = observed
				handler, found := pc.SemanticSource().ExecutableNodeEventHandler(node, "source.evt")
				if !found || len(handler.Rules) != 1 || !handler.Rules[0].Authored() {
					t.Fatalf("source lacks the single authored rule: found=%v handler=%#v", found, handler)
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
				runID := runtimecorrelation.RunIDFromContext(ctx)
				entityID := uuid.NewString()
				evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, "", handlerTestWorkflowEnvelope(".", runID, entityID), time.Now().UTC())
				dialect := authoractivityfixture.DialectPostgres
				if store.isSQLite() {
					dialect = authoractivityfixture.DialectSQLite
				}
				seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
				if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: pc.SemanticSource().WorkflowVersion(),
					CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
				})); err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
				if err := owner.commitInitial(ctx, evt, route); err != nil {
					t.Fatal(err)
				}
				id, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				var initialFacts int
				if err := store.testDB().QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, id).Scan(&initialFacts); err != nil || initialFacts != 0 {
					t.Fatalf("unexecuted delivery already has rule evidence: count=%d err=%v", initialFacts, err)
				}
				readFact := func() handlerselection.HandlerRuleSelectionFact {
					var selectionContext, disposition, flow, family, path, label string
					err := store.testDB().QueryRowContext(ctx, `SELECT selection_context, disposition, COALESCE(flow_path, ''), COALESCE(declaration_family, ''), COALESCE(semantic_path, ''), display_label FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, id).Scan(&selectionContext, &disposition, &flow, &family, &path, &label)
					if err != nil {
						t.Fatal(err)
					}
					fact, err := handlerselection.Hydrate(selectionContext, disposition, flow, family, path, label)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("persisted per-delivery fact: context=%s disposition=%s ref=%s/%s/%s label=%s", selectionContext, disposition, flow, family, path, label)
					return fact
				}
				attemptCtx := withWorkflowNodeDeliveryRoute(ctx, route)
				if variant != "no_fault_control" {
					reader := &failingReceiverPersistenceReader{WorkflowTargetPersistenceReader: store.targetReader,
						failure: runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "receiver_read_unavailable", "receiver-diagnostic", "load_target", nil, errors.New("injected pre-handler receiver read failure"))}
					store.targetReader = reader
					if variant == "recovered_retry" {
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
					bus.deliveryContinuations.mu.Lock()
					held := bus.deliveryContinuations.held[id]
					bus.deliveryContinuations.mu.Unlock()
					if !held || bus.publishedCount() != 0 {
						t.Fatal("pre-handler failure lost continuation or emitted business work")
					}
					persisted := readFact()
					if len(observed.failures) != 1 || !observed.failures[0].Equal(handlerselection.NotApplicable()) || !persisted.Equal(handlerselection.NotApplicable()) {
						t.Fatal("pre-handler failure did not freeze NotApplicable")
					}
					instance, found, err := store.Load(ctx, testWorkflowInstanceRoute(runID))
					if err != nil || !found || instance.CurrentState != "queued" {
						t.Fatalf("pre-handler failure changed source state: stage=%s found=%v err=%v", instance.CurrentState, found, err)
					}
					t.Logf("retry accepted: status=%s retries=%d claim_version=%d next_eligible=%s retained=%v outcome=%s", snapshot.Status, snapshot.RetryCount, snapshot.ClaimVersion, snapshot.NextEligibleAt, held, outcomes[0].Outcome)
					reader.failure = nil
					if err := owner.makeRetryEligible(ctx, id); err != nil {
						t.Fatal(err)
					}
				}
				handled, dispatchErr := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
				after, err := owner.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				persisted := readFact()
				for index, fact := range observed.failures {
					t.Logf("failure settlement attempt %d: context=%s disposition=%s label=%s equals_authored_selection=%v", index+1, fact.Context(), fact.Disposition(), fact.DisplayLabel(), fact.Equal(selected))
				}
				instance, found, err := store.Load(ctx, testWorkflowInstanceRoute(runID))
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
