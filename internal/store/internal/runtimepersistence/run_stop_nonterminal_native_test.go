package runtimepersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func newStopNonterminalNativeFixture(t *testing.T, backend string) receiverConfigActivationFixture {
	t.Helper()
	return newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":        "name: stop-nonterminal-native\n",
		"review/schema.yaml": "name: review\nstages:\n  active: {}\n  done: {final: true}\n",
		"review/events.yaml": "work.requested:\nfinish.requested:\n",
		"review/agents.yaml": "reviewer:\n  role: reviewer\n  intent: {inline: \"Review the work.\"}\n  model: regular\n  subscriptions: [work.requested]\n",
		"review/nodes.yaml": `progress:
  execution_type: system_node
  subscribes_to: [work.requested, finish.requested]
  event_handlers:
    work.requested: {}
    finish.requested:
      advances_to: done
      terminate: true
`,
	}, nil)
}

func stopNonterminalNativePlan(t *testing.T, f receiverConfigActivationFixture, runID string) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	ctx := correlation.WithRunID(f.ctx, runID)
	requireRunFixtureForTest(t, ctx, f.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, Artifact: f.bundle.SourceArtifact, BundleHash: f.bundle.SourceArtifact.BundleHash()})
	source := semanticview.Wrap(f.bundle)
	root := flowidentity.Stored(source, ".", runID, runID, runID, "")
	child, err := flowidentity.KeylessChild(source, root, "review")
	if err != nil {
		t.Fatal(err)
	}
	req := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
	req.Instance = child
	plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !result.Created || !result.Acknowledged || len(plan.Readiness.Agents) != 1 {
		t.Fatalf("native stop construction=%+v, agents=%d, err=%v", result, len(plan.Readiness.Agents), err)
	}
	if err := f.manager.FinalizeCommittedFlowInstanceActivation(ctx, result); err != nil {
		t.Fatal(err)
	}
	return plan
}

func stopNonterminalNativeAuthority(t *testing.T, f receiverConfigActivationFixture) deliverylifecycle.ExecutionAuthority {
	t.Helper()
	evidence, err := f.grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(mustStoreTestSourceArtifactFact(evidence.BundleHash), evidence.GrantID, evidence.RuntimeGeneration)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func stopNonterminalNativeDelivery(t *testing.T, f receiverConfigActivationFixture, plan pipeline.FlowInstanceActivationPlan, authority deliverylifecycle.ExecutionAuthority, agent bool) (events.Event, events.DeliveryRoute) {
	t.Helper()
	runID := plan.Readiness.RunID
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/work.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("review", "progress")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: plan.Identity.Route().InstancePath, EntityID: plan.Instance.EntityID})}
	if agent {
		identity := plan.Readiness.Agents[0].Identity
		route.Recipient, route.AgentIdentity = events.MustAgentDeliveryRecipient(identity.AgentID()), identity
	}
	event = eventtest.TargetRouted(event, route.Target.Route())
	commitSelectedReceiverClaimEvent(t, correlation.WithRunID(f.ctx, runID), f.store, event, route, authority)
	return event, route
}

func stopNonterminalNativeClaim(t *testing.T, f receiverConfigActivationFixture, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) deliverylifecycle.Claim {
	t.Helper()
	result, err := f.store.(deliverylifecycle.Store).ClaimDelivery(correlation.WithRunID(f.ctx, event.RunID()), authority, event, route)
	claimed, acquired := result.Acquired()
	if err != nil || !acquired || claimed.Snapshot.Status != deliverylifecycle.StatusInProgress || claimed.Snapshot.Authority != authority {
		t.Fatalf("real native claim=%+v, acquired=%t, err=%v", result, acquired, err)
	}
	return claimed.Claim
}

func assertStopNonterminalNativeCount(t *testing.T, f receiverConfigActivationFixture, runID string, want int) {
	t.Helper()
	storage, err := ReadRunStopStorageForTest(f.ctx, f.store, runID)
	if err != nil || storage.Pending != want {
		t.Errorf("nonterminal physical count=%d, want=%d, err=%v", storage.Pending, want, err)
	}
	count, err := CountRunStopNonterminalDeliveriesForTest(f.ctx, f.store, runID)
	if err != nil || count != want {
		t.Errorf("independent nonterminal count=%d, want=%d, err=%v", count, want, err)
	}
}

func TestRunStopWitnessCountsRealClaimAndRetryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newStopNonterminalNativeFixture(t, backend)
			authority := stopNonterminalNativeAuthority(t, f)
			target := stopNonterminalNativePlan(t, f, correlation.RunIDFromContext(f.ctx))
			foreign := stopNonterminalNativePlan(t, f, uuid.NewString())
			foreignEvent, foreignRoute := stopNonterminalNativeDelivery(t, f, foreign, authority, false)
			foreignClaim := stopNonterminalNativeClaim(t, f, authority, foreignEvent, foreignRoute)
			nodeEvent, nodeRoute := stopNonterminalNativeDelivery(t, f, target, authority, false)
			agentEvent, agentRoute := stopNonterminalNativeDelivery(t, f, target, authority, true)
			assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 2)
			nodeClaim := stopNonterminalNativeClaim(t, f, authority, nodeEvent, nodeRoute)
			agentClaim := stopNonterminalNativeClaim(t, f, authority, agentEvent, agentRoute)
			assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 2)
			failed, err := f.store.(deliverylifecycle.Store).SettleFailure(f.ctx, agentClaim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Millisecond, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()})
			if err != nil || failed.Status != deliverylifecycle.StatusFailed || failed.NextEligibleAt.IsZero() {
				t.Fatalf("real retry settlement=%+v, err=%v", failed, err)
			}
			assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 2)
			assertStopNonterminalNativeCount(t, f, foreign.Readiness.RunID, 1)
			if _, err := f.store.(deliverylifecycle.Store).SettleSuccess(f.ctx, nodeClaim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 1)
			// Wait once for the actual stored eligibility cut, never rewrite it.
			if wait := time.Until(failed.NextEligibleAt); wait > 0 {
				time.Sleep(wait)
			}
			retry := stopNonterminalNativeClaim(t, f, authority, agentEvent, agentRoute)
			if retry.Version() != agentClaim.Version()+1 {
				t.Fatal("retry did not acquire the actual successor attempt")
			}
			if _, err := f.store.(deliverylifecycle.Store).SettleSuccess(f.ctx, retry, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 0)
			stopNonterminalNativeHeldStop(t, f, target, foreign, authority)
			if _, err := f.store.(deliverylifecycle.Store).SettleSuccess(correlation.WithRunID(f.ctx, foreign.Readiness.RunID), foreignClaim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			assertStopNonterminalNativeCount(t, f, foreign.Readiness.RunID, 0)
		})
	}
}

func stopNonterminalNativeHeldStop(t *testing.T, f receiverConfigActivationFixture, target, foreign pipeline.FlowInstanceActivationPlan, authority deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	stopNonterminalNativeDelivery(t, f, target, authority, false)
	claimedEvent, claimedRoute := stopNonterminalNativeDelivery(t, f, target, authority, true)
	claimed := stopNonterminalNativeClaim(t, f, authority, claimedEvent, claimedRoute)
	failedEvent, failedRoute := stopNonterminalNativeDelivery(t, f, target, authority, false)
	failedClaim := stopNonterminalNativeClaim(t, f, authority, failedEvent, failedRoute)
	failed, err := f.store.(deliverylifecycle.Store).SettleFailure(f.ctx, failedClaim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()})
	if err != nil || failed.Status != deliverylifecycle.StatusFailed || !failed.NextEligibleAt.After(time.Now()) {
		t.Fatalf("future retry evidence=%+v, err=%v", failed, err)
	}
	assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 3)
	outcome, err := f.store.(runcontrol.Store).StopRunControlOutcome(f.ctx, runcontrol.TransitionRequest{RunID: target.Readiness.RunID, Reason: "native-stop-proof"})
	if err != nil || !outcome.Acknowledged {
		t.Fatalf("acknowledged native stop=%+v, err=%v", outcome, err)
	}
	assertStopNonterminalNativeCount(t, f, target.Readiness.RunID, 0)
	assertStopNonterminalNativeCount(t, f, foreign.Readiness.RunID, 1)
	for _, claim := range []deliverylifecycle.Claim{claimed, failedClaim} {
		snapshot, err := f.store.(deliverylifecycle.Store).Snapshot(f.ctx, claim.DeliveryID())
		if err != nil || !snapshot.Status.Terminal() || snapshot.Status != deliverylifecycle.StatusDeadLetter {
			t.Fatalf("acknowledged stop retained claimed/failed work=%+v, err=%v", snapshot, err)
		}
	}
}

func TestRunStopWitnessExcludesNativeTerminalStatesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newStopNonterminalNativeFixture(t, backend)
			authority := stopNonterminalNativeAuthority(t, f)
			plan := stopNonterminalNativePlan(t, f, correlation.RunIDFromContext(f.ctx))
			stopNonterminalNativeDelivery(t, f, plan, authority, false)
			for _, status := range []deliverylifecycle.Status{deliverylifecycle.StatusDelivered, deliverylifecycle.StatusDeadLetter} {
				event, route := stopNonterminalNativeDelivery(t, f, plan, authority, false)
				claim := stopNonterminalNativeClaim(t, f, authority, event, route)
				var snapshot deliverylifecycle.Snapshot
				var err error
				if status == deliverylifecycle.StatusDelivered {
					snapshot, err = f.store.(deliverylifecycle.Store).SettleSuccess(f.ctx, claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
				} else {
					snapshot, err = f.store.(deliverylifecycle.Store).SettleFailure(f.ctx, claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: "proof_failure", Failure: testRetryableFailure(), RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()})
				}
				if err != nil || snapshot.Status != status {
					t.Fatalf("native terminal settlement=%+v, want=%s, err=%v", snapshot, status, err)
				}
			}
			queuedEvent, queuedRoute := stopNonterminalNativeDelivery(t, f, plan, authority, true)
			stopNonterminalNativeCancelQueued(t, f, plan, authority)
			deliveryID, err := deliverylifecycle.DeliveryID(queuedEvent.ID(), queuedRoute)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := f.store.(deliverylifecycle.Store).Snapshot(f.ctx, deliveryID)
			if err != nil || snapshot.Status != deliverylifecycle.StatusCanceled {
				t.Fatalf("supported termination failed to cancel queued agent=%+v, err=%v", snapshot, err)
			}
			assertStopNonterminalNativeCount(t, f, plan.Readiness.RunID, 1)
		})
	}
}

func stopNonterminalNativeCancelQueued(t *testing.T, f receiverConfigActivationFixture, plan pipeline.FlowInstanceActivationPlan, authority deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	node := mustPersistenceNode("review", "progress")
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, plan.Readiness.RunID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: plan.Identity.Route().InstancePath, EntityID: plan.Instance.EntityID})}
	event = eventtest.TargetRouted(event, route.Target.Route())
	commitSelectedReceiverClaimEvent(t, f.ctx, f.store, event, route, authority)
	claim := stopNonterminalNativeClaim(t, f, authority, event, route)
	graph, found := f.bundle.WorkflowStageTopology("review")
	if !found {
		t.Fatal("native cancellation lost its compiled lifecycle")
	}
	compiled, err := graph.AdmitTransition(contracts.WorkflowTransitionSite{Node: node, HandlerEvent: "finish.requested", AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler}, "active", "done")
	if err != nil {
		t.Fatal(err)
	}
	transition, err := workflowlifecycle.NewCompiledTransition(compiled, deliverylifecycle.NotApplicableHandlerRuleSelection(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cause, err := workflowlifecycle.NewAcceptedEvent(record.State.Identity.Route, identity.NormalizeEntityID(record.State.EntityID), event.ID(), string(event.Type()), event.ExecutionMode(), event.CreatedAt(), &transition)
	if err != nil {
		t.Fatal(err)
	}
	cause, err = cause.WithExecutionOccurrence("delivery", claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	termination, err := workflowlifecycle.NewTurnTermination(record.State.Identity, cause)
	if err != nil {
		t.Fatal(err)
	}
	state := record.State
	entry, enters, err := cause.StageEntry(state.Identity)
	if err != nil || !enters {
		t.Fatalf("compiled termination stage entry=%+v, enters=%t, err=%v", entry, enters, err)
	}
	var bookkeeping map[string]any
	if err := json.Unmarshal(state.Bookkeeping, &bookkeeping); err != nil {
		t.Fatal(err)
	}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	state.Bookkeeping, err = json.Marshal(bookkeeping)
	if err != nil {
		t.Fatal(err)
	}
	state.CurrentState, state.ExpectedState, state.ExpectedRevision = "done", "active", 1
	state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	state.UpdatedAt, state.EnteredStageAt = event.CreatedAt(), event.CreatedAt()
	result, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: state, Lifecycle: pipeline.WorkflowLifecycleMutationPlan{StageEntry: &entry, TurnTermination: &termination}, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}})
	if err != nil || !result.Committed {
		t.Fatalf("native compiled termination=%+v, err=%v", result, err)
	}
}
