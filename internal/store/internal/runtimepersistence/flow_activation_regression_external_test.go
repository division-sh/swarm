package runtimepersistence_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/google/uuid"
)

func TestFlowActivationTerminalTimerMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"terminal_wins", "timer_wins", "revision_wins", "attempt_retired_wins", "stopped_run_wins"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				f := newDynamicFlowCreationAtomicityFixture(t, backend)
				route := f.plan.Identity.Route()
				source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: f.plan.Identity.TemplateID, FlowInstance: route.InstancePath, EntityID: f.plan.Identity.EntityID})
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Truncate(time.Microsecond)
				activation := runtimepipeline.WorkflowTimerActivation{
					Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "review/deadline", DeclarationRevision: "review-revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial},
					RunID: f.runID, EntityID: f.plan.Identity.EntityID, Route: route, RoutingSource: source,
					OwnerAgent: "workflow", EventType: route.InstancePath + "/deadline", ExecutionMode: executionmode.Live,
					Payload: []byte(`{}`), CreatedAt: now, FireAt: now.Add(time.Hour), Status: "active",
				}
				command := runtimepipeline.WorkflowTimerReconciliationCommand{
					RunID: f.runID, Route: route, EntityID: f.plan.Identity.EntityID, ActivationAttempt: &f.attempt,
					Plan: runtimepipeline.WorkflowLifecycleMutationPlan{Timers: []runtimepipeline.WorkflowTimerMutation{{Kind: runtimepipeline.WorkflowTimerMutationInsert, Activation: activation}}},
				}
				if err := command.Validate(); err != nil {
					t.Fatal(err)
				}
				commit := func() (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
					return f.selected.(runtimepipeline.WorkflowTimerActivationPersistence).CommitWorkflowTimerReconciliation(f.ctx, command)
				}
				terminate := func() {
					if err := f.workflow.MarkTerminated(f.ctx, runtimeflowidentity.RunScopedFlowInstance{RunID: f.runID, Route: route}, identity.NormalizeEntityID(f.plan.Identity.EntityID), now); err != nil {
						t.Fatal(err)
					}
				}
				if order == "timer_wins" {
					result, err := commit()
					if err != nil || !result.Committed {
						t.Fatalf("healthy timer mutation: result=%+v err=%v", result, err)
					}
					terminate()
				} else {
					switch order {
					case "terminal_wins":
						terminate()
					case "revision_wins":
						observed, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, route)
						if err != nil || !found {
							t.Fatalf("load plan before revision: found=%v err=%v", found, err)
						}
						revised := observed.Plan
						revised.WorkflowVersion = "next-version"
						if _, err := f.selected.ReconcileDynamicFlowRuntimeReadinessPlans(f.ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: revised}}, now); err != nil {
							t.Fatal(err)
						}
					case "attempt_retired_wins":
						if err := f.selected.RetireDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
							t.Fatal(err)
						}
					case "stopped_run_wins":
						controller := f.selected.(interface {
							StopRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
						})
						if outcome, err := controller.StopRunControlOutcome(f.ctx, runcontrol.TransitionRequest{RunID: f.runID}); err != nil || !outcome.Acknowledged {
							t.Fatalf("stop: outcome=%+v err=%v", outcome, err)
						}
					}
					if err := f.selected.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err == nil {
						t.Fatalf("%s retained verification authority", order)
					}
					result, err := commit()
					if err == nil || result.Committed {
						t.Fatalf("%s timer mutation committed=%v err=%v", order, result.Committed, err)
					}
				}
				var rows int
				if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM timers WHERE timer_id = $1", activation.Ref.ActivationID).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				want := 0
				if order == "timer_wins" {
					want = 1
				}
				if rows != want {
					t.Fatalf("timer rows=%d, want %d", rows, want)
				}
			})
		}
	}
}

func TestFailedFlowActivationRetirementIsPendingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDynamicFlowCreationAtomicityFixture(t, backend)
			observed, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
			if err != nil || !found {
				t.Fatalf("load: found=%v err=%v", found, err)
			}
			plan := observed.Plan
			plan.CreationEvent = nil
			results, err := f.selected.ReconcileDynamicFlowRuntimeReadinessPlans(f.ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: plan}}, time.Now().UTC())
			if err != nil || len(results) != 1 {
				t.Fatalf("reconcile: %v", err)
			}
			if err := f.selected.RetireDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
				t.Fatal(err)
			}
			admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, plan, results[0].PlanRevision, f.attempt.ProcessBinding())
			if err != nil || !admitted.Acknowledged {
				t.Fatalf("begin: %+v %v", admitted, err)
			}
			ready, err := f.selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(f.ctx, admitted.Attempt, plan, time.Now().UTC())
			if err != nil || !ready.Acknowledged {
				t.Fatalf("mark: %+v %v", ready, err)
			}
			if err := f.selected.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, admitted.Attempt); err != nil {
				t.Fatal(err)
			}
			if err := f.selected.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, admitted.Attempt); err != nil {
				t.Fatalf("repeat failure abandonment: %v", err)
			}
			if err := f.selected.RetireDynamicFlowRuntimeActivationAttempt(f.ctx, admitted.Attempt); err == nil {
				t.Fatal("orderly disposition replaced settled failure abandonment")
			}
			projection, err := f.workflow.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, observed.OwningRunSource)
			if err != nil {
				t.Fatal(err)
			}
			if len(projection.CurrentPending) != 1 || len(projection.CurrentCompleted) != 0 {
				t.Fatalf("failed attempt is not retry-visible: pending=%d completed=%d", len(projection.CurrentPending), len(projection.CurrentCompleted))
			}
		})
	}
}

func TestFailedFlowActivationCreationPosturesRemainRetryableBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, posture := range []string{"pending_creation", "already_emitted"} {
			t.Run(backend+"/"+posture, func(t *testing.T) {
				f := newDynamicFlowCreationAtomicityFixture(t, backend)
				if posture == "already_emitted" {
					if err := f.commit(); err != nil {
						t.Fatal(err)
					}
				}
				before, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || before.TopologyReadyAt.IsZero() {
					t.Fatalf("load completed attempt: found=%v readiness=%+v err=%v", found, before, err)
				}
				if err := f.selected.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
					t.Fatal(err)
				}
				projection, err := f.workflow.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, before.OwningRunSource)
				if err != nil || len(projection.CurrentPending) != 1 {
					t.Fatalf("failed attempt lost retry projection: pending=%d err=%v", len(projection.CurrentPending), err)
				}
				after := projection.CurrentPending[0]
				if !after.TopologyReadyAt.IsZero() || !after.CreationEventEmittedAt.Equal(before.CreationEventEmittedAt) {
					t.Fatalf("abandonment changed creation evidence: before=%+v after=%+v", before, after)
				}
				admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, after.Plan, after.PlanRevision, f.attempt.ProcessBinding())
				if err != nil || !admitted.Acknowledged || admitted.Reused || admitted.Attempt.ID() == f.attempt.ID() {
					t.Fatalf("retry admission: %+v %v", admitted, err)
				}
			})
		}
	}
}
