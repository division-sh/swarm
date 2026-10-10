package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type selectionRetryMutationFault struct {
	WorkflowEngineMutationOwner
	selection handlerselection.HandlerRuleSelectionFact
	fail      bool
}

type selectionRetryQueryFault struct {
	entityquery.Reader
	fail bool
}

func (s *selectionRetryQueryFault) CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error) {
	if s.fail {
		return 0, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "selection_query_unavailable", "selection-test", "query", nil)
	}
	return s.Reader.CountWorkflowEntities(ctx, request)
}

func (s *selectionRetryMutationFault) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	if s.fail && command.DeliverySuccess != nil {
		s.selection = command.DeliverySuccess.RuleSelection
		return CommittedWorkflowEngineMutation{}, runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "selection_test_commit_unavailable", "selection-test", "commit", nil, errors.New("controlled pre-commit failure"))
	}
	return s.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
}

func VerifyNativeAuthoredSelectionRetryReloadsCurrentStateBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, first := range []string{"selected", "fallback", "evaluation_failed"} {
			t.Run(backend+"/"+first, func(t *testing.T) {
				nodes := `router:
  execution_type: system_node
  subscribes_to: [source.evt]
  event_handlers:
    source.evt:
      rules:
        - {id: first, when: entity.marker == 'first', advances_to: done}
        - {id: second, when: entity.marker == 'second', advances_to: done}
        - {id: unmatched, else: true}
`
				if first == "evaluation_failed" {
					nodes = strings.Replace(nodes, "entity.marker == 'first'", "query_entities(marker == 'first').count == 1", 1)
				}
				bundle := loadWorkflowTempBundle(t, map[string]string{
					"schema.yaml":   "name: selection-retry\nstages:\n  queued: {}\n  done: {final: true}\n",
					"entities.yaml": "test_entity:\n  marker: text\n",
					"events.yaml":   "source.evt:\n",
					"nodes.yaml":    nodes,
				})
				fixture := open(t, backend, semanticview.Wrap(bundle))
				module := handlerTestWorkflowModuleWithBundle(bundle, ".", "router").(*previewWorkflowModule)
				node := pipelineNode(t, ".", "router")
				module.workflowNodes = []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"source.evt"}}}
				pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
				store := pc.workflowStore
				runID := uuid.NewString()
				ctx = correlation.WithRunID(ctx, runID)
				if err := fixture.RequireRun(ctx, runID); err != nil {
					t.Fatal(err)
				}
				if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(),
					EntityType: "test_entity", CurrentState: "queued", Fields: map[string]any{"marker": "unchanged"},
				})); err != nil {
					t.Fatal(err)
				}
				load := func() (WorkflowInstance, bool) {
					value, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
					if err != nil {
						t.Fatal(err)
					}
					return value, found
				}
				identity := testRunScopedWorkflowInstanceFromContext(ctx, runID)
				if first == "selected" {
					if err := store.mutateE(ctx, identity, func(i *WorkflowInstance) error { i.Fields["marker"] = "first"; return nil }); err != nil {
						t.Fatal(err)
					}
				}
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "source.evt", "", "", []byte("{}"), 0, runID,
					events.EnvelopeForTargetRoute(testWorkflowSourceEnvelope(".", runID, runID), events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID}),
					testWorkflowRoutingSource(".", runID, runID), time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
				owner := fixture.Store
				if err := fixture.PublishNode(ctx, event, route); err != nil {
					t.Fatal(err)
				}
				observed := &authoredRuleRetryDiagnosticStore{Store: owner}
				pc.deliveryStore = observed
				fault := &selectionRetryMutationFault{WorkflowEngineMutationOwner: store.engineMutations, fail: first != "evaluation_failed"}
				store.engineMutations = fault
				queryFault := &selectionRetryQueryFault{Reader: store.entityQuery, fail: first == "evaluation_failed"}
				store.entityQuery = queryFault
				ctx = withWorkflowNodeDeliveryRoute(ctx, route)
				if handled, err := pc.dispatchWorkflowNodeEventResult(ctx, event); err != nil || !handled {
					t.Fatalf("retry not retained: handled=%v err=%v", handled, err)
				}
				wantFirst := handlerselection.DispositionSelected
				if first == "evaluation_failed" {
					wantFirst = handlerselection.DispositionEvaluationFailed
				}
				if len(observed.failures) != 1 {
					t.Fatalf("failure observations = %#v", observed.failures)
				}
				attempt, err := observed.failures[0].ResolvedFact()
				if err != nil || attempt.Disposition() != wantFirst {
					t.Fatalf("did not reach intended attempt selection: %#v %v", attempt, err)
				}
				id, err := runtimedelivery.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				failed, err := owner.Snapshot(ctx, id)
				if err != nil || failed.Status != runtimedelivery.StatusFailed || failed.FinalSelection.Present() {
					t.Fatalf("attempt froze selection: %#v %v", failed, err)
				}
				before, found := load()
				if !found || before.CurrentState != "queued" || len(before.TransitionHistory) != 0 {
					t.Fatalf("failed commit mutated state: %#v", before)
				}
				fault.fail = false
				queryFault.fail = false
				if err := store.mutateE(ctx, identity, func(i *WorkflowInstance) error { i.Fields["marker"] = "second"; return nil }); err != nil {
					t.Fatal(err)
				}
				if err := fixture.RetryEligible(ctx, event, route); err != nil {
					t.Fatal(err)
				}
				recoverNativePipelineRetryForTest(t, fixture, ctx)
				if handled, err := pc.dispatchWorkflowNodeEventResult(ctx, event); err != nil || !handled {
					t.Fatalf("changed-state retry failed: %v/%v", handled, err)
				}
				settled, err := owner.Snapshot(ctx, id)
				if err != nil || settled.Status != runtimedelivery.StatusDelivered || settled.ClaimVersion != failed.ClaimVersion+1 {
					t.Fatalf("final delivery: %#v %v", settled, err)
				}
				fact, err := settled.FinalSelection.Fact()
				if err != nil || fact.DisplayLabel() != "second" || fact.Disposition() != handlerselection.DispositionSelected {
					t.Fatalf("did not reevaluate current state: %#v %v", fact, err)
				}
				after, found := load()
				if !found || after.CurrentState != "done" || len(after.TransitionHistory) != 1 || after.Fields["marker"] != "second" {
					t.Fatalf("final state/evidence: %#v", after)
				}
				if !after.TransitionHistory[0].Evidence.RuleSelection().Equal(fact) {
					t.Fatal("transition and final delivery disagree")
				}
				outcomes, err := owner.Outcomes(ctx, id)
				if err != nil || len(outcomes) != 2 || outcomes[0].Outcome != "retry_scheduled" || outcomes[1].Outcome != "delivered" {
					t.Fatalf("outcomes: %#v %v", outcomes, err)
				}
				if !settled.NextEligibleAt.IsZero() || !settled.ClaimExpiresAt.IsZero() || settled.SettledAt.IsZero() {
					t.Fatal("final delivery retained retry/claim authority")
				}
			})
		}
	}
}
