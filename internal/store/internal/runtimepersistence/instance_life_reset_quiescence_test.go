package runtimepersistence

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func proveInstanceResetQuiescence(t *testing.T, backend string) {
	t.Helper()
	for _, group := range []string{"deliveries", "pipeline_events", "session_leases", "fanout", "due_timers", "future_timer_only"} {
		t.Run(group, func(t *testing.T) {
			var extra map[string]string
			if group == "fanout" {
				extra = map[string]string{
					"review/events.yaml": "review.created:\n  account_id: text\nfinish.requested:\n  items: [text]\nitem.completed:\n  item: text\ntask.started:\ntimer.elapsed:\n",
					"review/nodes.yaml":  "progress:\n  finish.requested:\n    advances_to: done\n    fan_out:\n      items_from: payload.items\n      as: member\n      emit:\n        event: item.completed\n        fields: {item: member}\n",
				}
			}
			f, first := newWorkflowTargetConstructionFixtureDocuments(t, backend, true, extra)
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			committed, err := committer.CommitFlowInstanceActivation(f.ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err != nil {
				t.Fatal(err)
			}
			agents, err := f.store.LoadAgents(f.ctx)
			if err != nil || len(agents) != 1 {
				t.Fatalf("exact reset actor: %+v err=%v", agents, err)
			}
			var drain func()
			if group == "fanout" {
				drain = finishInstanceResetWithFanOut(t, f, first)
			} else {
				finishNativeResetLife(t, f, first)
				drain = holdInstanceResetWork(t, f, first, agents[0].Config.Identity, group)
			}
			reset := prepareNativeResetPlan(t, f, first)
			if group != "future_timer_only" {
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				committed, err := committer.CommitFlowInstanceActivation(f.ctx, reset)
				failure, typed := failures.EnvelopeFromError(err)
				if !typed || failure.Detail.Code != "instance_not_quiescent" || committed.Acknowledged || committed.StartedOver {
					t.Fatalf("%s did not refuse an actual reset: %+v err=%v", group, committed, err)
				}
				for _, key := range []string{"deliveries", "pipeline_events", "session_leases", "fanout", "due_timers"} {
					want := "0"
					if key == group {
						want = "1"
					}
					if fmt.Sprint(failure.Detail.Attributes[key]) != want {
						t.Fatalf("%s refusal attributed %s=%v want=%s", group, key, failure.Detail.Attributes[key], want)
					}
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("refused reset changed durable state, sessions or history")
				}
				drain()
			}
			committed, err = committer.CommitFlowInstanceActivation(f.ctx, reset)
			if err != nil || !committed.Acknowledged || !committed.StartedOver || committed.Plan.Instance.Life != 2 {
				t.Fatalf("drained/future-only reset: %+v err=%v", committed, err)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			if replay, err := committer.CommitFlowInstanceActivation(f.ctx, reset); err != nil || !replay.StartedOver {
				t.Fatalf("exact reset replay: %+v err=%v", replay, err)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("drained reset replay rotated life or changed history again")
			}
			current, found, err := f.workflows.LoadConstructedFlowInstance(f.ctx, planOwner(first), identity.NormalizeEntityID(first.Identity.EntityID))
			if err != nil || !found || current.Life != 2 || current.CurrentState != "active" {
				t.Fatalf("reset did not land exactly once: %+v found=%t err=%v", current, found, err)
			}
		})
	}
}

func settleInstanceResetEvent(t *testing.T, f receiverConfigActivationFixture, eventID string) {
	t.Helper()
	owner := f.store.(pipelineObligationParityStore).PipelineObligations()
	work, err := owner.ClaimEvent(f.ctx, eventID, pipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Settle(f.ctx, work.Claim, pipelineobligation.Acknowledged("processed")); err != nil {
		t.Fatal(err)
	}
}

func holdInstanceResetWork(t *testing.T, f receiverConfigActivationFixture, first pipeline.FlowInstanceActivationPlan, actor agentidentity.Identity, group string) func() {
	t.Helper()
	life := events.InstanceLife{RunID: first.Readiness.RunID, FlowInstance: first.Identity.InstancePath, Life: 1}
	if group == "session_leases" {
		ctx := events.WithDeliveryContext(effects.WithDifferentOwner(f.ctx, effects.OwnerBuildTestInfrastructure), events.DeliveryContext{Life: &life})
		registry := f.store.(sessions.Registry)
		lease, err := registry.Acquire(ctx, actor, "reset-quiescence-proof")
		if err != nil {
			t.Fatal(err)
		}
		return func() {
			if result, err := registry.ReleaseOutcome(ctx, lease); err != nil || !result.Acknowledged {
				t.Fatalf("drain exact session lease: %+v err=%v", result, err)
			}
		}
	}
	if group == "due_timers" || group == "future_timer_only" {
		return holdInstanceResetTimer(t, f, first, actor, group == "future_timer_only")
	}
	source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "review", FlowInstance: life.FlowInstance, EntityID: first.Identity.EntityID})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RuntimeControlWithRoutingSource(uuid.NewString(), "review/task.started", "fixture", "", []byte(`{}`), 0,
		life.RunID, "", events.EventEnvelope{}, source, time.Now().UTC())
	routes := []events.DeliveryRoute(nil)
	if group == "deliveries" {
		routes = append(routes, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor, Context: events.DeliveryContext{Life: &life}})
	}
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, routes); err != nil {
		t.Fatal(err)
	}
	if group == "pipeline_events" {
		return func() { settleInstanceResetEvent(t, f, event.ID()) }
	}
	settleInstanceResetEvent(t, f, event.ID())
	store := f.store.(deliveryFixtureStore)
	claimed, err := claimDeliveryFixture(f.ctx, store, event, routes[0])
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		if result, err := store.SettleSuccess(f.ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable()); err != nil || !result.MatchesSettlementClaim(claimed.Claim) {
			t.Fatalf("drain exact delivery: %+v err=%v", result, err)
		}
	}
}

func holdInstanceResetTimer(t *testing.T, f receiverConfigActivationFixture, first pipeline.FlowInstanceActivationPlan, actor agentidentity.Identity, future bool) func() {
	t.Helper()
	store := f.store.(genericschedule.Store)
	setGenericScheduleClock(t, store, func() time.Time { return time.Now().UTC().Add(-time.Minute) })
	due := time.Now().UTC().Add(-time.Second)
	if future {
		due = time.Now().UTC().Add(time.Hour)
	}
	command := testAgentGenericScheduleCommand(t, first.Readiness.RunID, actor.AgentID(), first.Identity.InstancePath, first.Identity.EntityID, uuid.NewString(), genericschedule.AbsoluteDue(due))
	command.AgentIdentity, command.OwnerID, command.EventType = actor, actor.AgentID(), "review/timer.elapsed"
	activation := admitGenericScheduleFixture(t, f.ctx, store, command)
	setGenericScheduleClock(t, store, func() time.Time { return time.Now().UTC() })
	return func() {
		mutation, err := startupownership.CaptureMutationAuthority(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
		scope, scoped := authoractivity.ScopeFromContext(f.ctx)
		if !present || !scoped || scope.BundleHash != fact.BundleHash() || scope.RuntimeInstanceID != mutation.ProcessAuthority().RuntimeInstanceID {
			t.Fatal("timer fixture requires its exact native source/runtime scope")
		}
		catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{{EventType: command.EventType, Disposition: authoractivity.StoryAuthored}})
		if err != nil {
			t.Fatal(err)
		}
		defer catalog.Release()
		publisher, err := newStoreTestEventBus(t, f.store.(storeTestDurableEventBusStore), bus.EventBusOptions{
			RuntimeInstanceID: mutation.ProcessAuthority().RuntimeInstanceID, SourceArtifactFact: fact, ContractBundle: semanticview.Wrap(f.bundle),
		})
		if err != nil {
			t.Fatal(err)
		}
		scheduler := &selectedStoreLifecycleScheduler{}
		dispatch := publisher.EngineDispatcher()
		dispatcher := &instanceClockReceiptDispatcher{PostCommitDispatcher: dispatch, handoff: dispatch.(engine.CommittedPublicationDispatcher)}
		lifecycle, err := genericschedule.NewLifecycle(store, scheduler, publisher, dispatcher, forkCompletionScheduleLogger{t: t}, executionposture.Live)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := lifecycle.Stop(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		if err := lifecycle.ReconcileRunWakeups(f.ctx, first.Readiness.RunID); err != nil || len(scheduler.registered) != 1 {
			t.Fatalf("restore exact due wakeup: %+v err=%v", scheduler.registered, err)
		}
		scheduler.callback(f.ctx, scheduler.registered[0])
		fired, found, err := store.LoadGenericScheduleActivation(f.ctx, activation.ID)
		if err != nil || !found || fired.Status != genericschedule.StatusFired || dispatcher.calls != 1 {
			t.Fatalf("drain due timer by actual publication: %+v dispatch=%d err=%v", fired, dispatcher.calls, err)
		}
	}
}

func finishInstanceResetWithFanOut(t *testing.T, f receiverConfigActivationFixture, first pipeline.FlowInstanceActivationPlan) func() {
	t.Helper()
	record, err := first.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	state := record.State
	state.Transition, state.ExpectedRevision, state.ExpectedState = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, 1, "active"
	state.CurrentState, state.EnteredStageAt, state.UpdatedAt = "done", time.Now().UTC(), time.Now().UTC()
	var claim deliverylifecycle.Claim
	state = workflowTargetMutationEntryPayload(t, f, state, "finish.requested", []byte(`{"items":["one"]}`), &claim)
	snapshot, err := f.store.(deliveryFixtureStore).Snapshot(f.ctx, claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	node := mustPersistenceNode("review", "progress")
	plans := f.bundle.FanOutPlansForHandler(node, "finish.requested")
	if len(plans) != 1 {
		t.Fatal("fan-out reset proof requires its exact compiled declaration")
	}
	plan := plans[0]
	intent := fanoutobligation.IntentRequest{
		Key:     fanoutobligation.IntentKey{RunID: first.Readiness.RunID, TriggeringDeliveryID: claim.DeliveryID(), ElementRef: plan.Ref.ElementRef},
		PlanRef: plan.Ref, Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: snapshot.EventID, Field: "items"}, Cardinality: 1,
		Capsule: fanoutobligation.Capsule{SourcePlanRef: plan.Ref, SourceProjection: plan.SemanticEvidence(), NodeKey: node.Key(), ExecutionFlowID: "review",
			Route: first.Identity.Route(), EntityID: first.Identity.EntityID, HandlerEventKey: "finish.requested", ProducerSource: eventtest.RootRoutingSource(first.Readiness.RunID),
			Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: snapshot.Route.Target},
			Lineage:  events.EventLineage{RunID: first.Readiness.RunID, ParentEventID: snapshot.EventID, ExecutionMode: executionmode.Live}, CurrentState: "active"},
	}
	if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{
		State: state, FanOutIntent: &intent, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}); err != nil {
		t.Fatal(err)
	}
	return func() {
		owner := f.store.(selectedFanOutOwner)
		_, claim, found, err := owner.ClaimFanOutIntent(f.ctx, pipeline.FanOutClaimRequest{Owner: "reset-proof", BundleHash: plan.Ref.BundleHash, Now: time.Now().UTC(), Lease: time.Minute})
		if err != nil || !found {
			t.Fatalf("claim exact held fan-out: found=%t err=%v", found, err)
		}
		if result, err := owner.CommitFanOutChunk(f.ctx, rejectedFanOutChunk(claim, 0, 1, time.Now().UTC())); err != nil || result.Intent.Cursor != 1 || result.Intent.Status != fanoutobligation.StatusClosed {
			t.Fatalf("settle held fan-out terminal result: %+v err=%v", result, err)
		}
	}
}
