package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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

func TestAuthoredSelectionRetryReloadsCurrentStateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, first := range []string{"selected", "no_match", "evaluation_failed"} {
			t.Run(backend+"/"+first, func(t *testing.T) {
				nodes := `router:
  execution_type: system_node
  subscribes_to: [source.evt]
  event_handlers:
    source.evt:
      rules:
        - {id: first, condition: "entity.marker == 'first'", advances_to: done}
        - {id: second, condition: "entity.marker == 'second'", advances_to: done}
`
				if first == "evaluation_failed" {
					nodes = strings.Replace(nodes, "entity.marker == 'first'", "query_entities(marker == 'first').count == 1", 1)
				}
				bundle := loadWorkflowTempBundle(t, map[string]string{
					"schema.yaml":   "name: selection-retry\nstages:\n  queued: {initial: true}\n  done: {terminal: true}\n",
					"entities.yaml": "test_entity:\n  marker: text\n",
					"events.yaml":   "source.evt: {}\n",
					"nodes.yaml":    nodes,
				})
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "queued", true)
				identity := testRunScopedWorkflowInstanceFromContext(f.ctx, f.path)
				if first == "selected" {
					if err := f.store.mutateE(f.ctx, identity, func(i *WorkflowInstance) error { i.Fields["marker"] = "first"; return nil }); err != nil {
						t.Fatal(err)
					}
				}
				module := f.pc.module.(*previewWorkflowModule)
				module.workflowNodes = []WorkflowNode{{Node: f.node, Subscriptions: []events.EventType{"source.evt"}, Policies: map[string]WorkflowEventPolicy{"source.evt": {Consume: true}}}}
				event := f.event("source.evt")
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(f.node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: f.path, EntityID: f.entityID})}
				owner := newPipelineTestDeliveryOwnerForDB(t, f.db)
				if err := owner.commitInitial(f.ctx, event, route); err != nil {
					t.Fatal(err)
				}
				observed := &authoredRuleRetryDiagnosticStore{Store: owner}
				f.pc.deliveryStore = observed
				fault := &selectionRetryMutationFault{WorkflowEngineMutationOwner: f.store.engineMutations, fail: first != "evaluation_failed"}
				f.store.engineMutations = fault
				queryFault := &selectionRetryQueryFault{Reader: f.store.entityQuery, fail: first == "evaluation_failed"}
				f.store.entityQuery = queryFault
				ctx := withWorkflowNodeDeliveryRoute(f.ctx, route)
				if handled, err := f.pc.dispatchWorkflowNodeEventResult(ctx, event); err != nil || !handled {
					t.Fatalf("retry not retained: handled=%v err=%v", handled, err)
				}
				wantFirst := handlerselection.DispositionSelected
				if first == "no_match" {
					wantFirst = handlerselection.DispositionNoMatch
				}
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
				failed, err := owner.Snapshot(f.ctx, id)
				if err != nil || failed.Status != runtimedelivery.StatusFailed || failed.FinalSelection.Present() {
					t.Fatalf("attempt froze selection: %#v %v", failed, err)
				}
				before, found := f.load()
				if !found || before.CurrentState != "queued" || len(before.TransitionHistory) != 0 {
					t.Fatalf("failed commit mutated state: %#v", before)
				}
				fault.fail = false
				queryFault.fail = false
				if err := f.store.mutateE(f.ctx, identity, func(i *WorkflowInstance) error { i.Fields["marker"] = "second"; return nil }); err != nil {
					t.Fatal(err)
				}
				if err := owner.makeRetryEligible(f.ctx, id); err != nil {
					t.Fatal(err)
				}
				if handled, err := f.pc.dispatchWorkflowNodeEventResult(ctx, event); err != nil || !handled {
					t.Fatalf("changed-state retry failed: %v/%v", handled, err)
				}
				settled, err := owner.Snapshot(f.ctx, id)
				if err != nil || settled.Status != runtimedelivery.StatusDelivered || settled.ClaimVersion != failed.ClaimVersion+1 {
					t.Fatalf("final delivery: %#v %v", settled, err)
				}
				fact, err := settled.FinalSelection.Fact()
				if err != nil || fact.DisplayLabel() != "second" || fact.Disposition() != handlerselection.DispositionSelected {
					t.Fatalf("did not reevaluate current state: %#v %v", fact, err)
				}
				after, found := f.load()
				if !found || after.CurrentState != "done" || len(after.TransitionHistory) != 1 || after.Fields["marker"] != "second" {
					t.Fatalf("final state/evidence: %#v", after)
				}
				if !after.TransitionHistory[0].Evidence.RuleSelection().Equal(fact) {
					t.Fatal("transition and final delivery disagree")
				}
				outcomes, err := owner.Outcomes(f.ctx, id)
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
