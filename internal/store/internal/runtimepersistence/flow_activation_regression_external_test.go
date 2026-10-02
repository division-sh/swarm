package runtimepersistence_test

import (
	"context"
	"errors"
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

func TestFlowAttachmentConditionalPhaseProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDynamicFlowCreationAtomicityFixture(t, backend)
			// This component fixture installs no process resources. Settlement here
			// exercises the store protocol, not a claim of Manager cleanup proof.
			if err := f.selected.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
				t.Fatal(err)
			}
			observed, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
			if err != nil || !found {
				t.Fatalf("load abandoned predecessor: found=%v err=%v", found, err)
			}
			admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(observed.Plan, observed.AttemptOrdinal, observed.AttemptState, f.attempt.ProcessBinding()))
			if err != nil || !admitted.Acknowledged {
				t.Fatalf("admit successor: %+v %v", admitted, err)
			}
			attempt := admitted.Attempt
			if result, err := f.selected.AdvanceFlowAttachment(f.ctx, attempt, runtimepipeline.FlowAttachmentAgentsRegistered, time.Now().UTC()); err == nil || result.Acknowledged {
				t.Fatalf("skipped initial phase: %+v %v", result, err)
			}
			for _, previous := range []runtimepipeline.FlowAttachmentPhase{runtimepipeline.FlowAttachmentPlanned, runtimepipeline.FlowAttachmentAgentsRegistered, runtimepipeline.FlowAttachmentRouteInstalled, runtimepipeline.FlowAttachmentTimersArmed} {
				canceled, cancel := context.WithCancel(f.ctx)
				cancel()
				result, err := f.selected.AdvanceFlowAttachment(canceled, attempt, previous, time.Now().UTC())
				if !errors.Is(err, context.Canceled) || result.Acknowledged {
					t.Fatalf("canceled %s progressed: %+v %v", previous, result, err)
				}
				row, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || row.Phase != previous || row.AttemptOrdinal != attempt.Ordinal() {
					t.Fatalf("cancellation changed %s: found=%v row=%+v err=%v", previous, found, row, err)
				}
				next, err := previous.Next()
				if err != nil {
					t.Fatal(err)
				}
				result, err = f.selected.AdvanceFlowAttachment(f.ctx, attempt, previous, time.Now().UTC())
				if err != nil || !result.Acknowledged || result.Progress != runtimepipeline.FlowAttachmentAdvanced || result.Phase != next {
					t.Fatalf("advance %s: %+v %v", previous, result, err)
				}
				result, err = f.selected.AdvanceFlowAttachment(f.ctx, attempt, previous, time.Now().UTC())
				if err != nil || !result.Acknowledged || result.Progress != runtimepipeline.FlowAttachmentAlreadyAdvanced || result.Phase != next {
					t.Fatalf("idempotent %s: %+v %v", previous, result, err)
				}
			}
			if result, err := f.selected.AdvanceFlowAttachment(f.ctx, attempt, runtimepipeline.FlowAttachmentReady, time.Now().UTC()); err == nil || result.Acknowledged {
				t.Fatalf("ready had a sixth attachment phase: %+v %v", result, err)
			}
			stale, err := f.selected.AdvanceFlowAttachment(f.ctx, f.attempt, runtimepipeline.FlowAttachmentPlanned, time.Now().UTC())
			if err != nil || !stale.Acknowledged || stale.Progress != runtimepipeline.FlowAttachmentStale {
				t.Fatalf("old predecessor progressed successor: %+v %v", stale, err)
			}
		})
	}
}

func TestFlowAttachmentPlanABARetainsPredecessorUntilSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDynamicFlowCreationAtomicityFixture(t, backend)
			original, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
			if err != nil || !found || original.AttemptState != "accepted" {
				t.Fatalf("load admitted predecessor: found=%v row=%+v err=%v", found, original, err)
			}
			current := original
			for _, version := range []string{original.Plan.WorkflowVersion + "-changed", original.Plan.WorkflowVersion} {
				desired := current.Plan
				desired.WorkflowVersion = version
				results, err := f.selected.ReconcileDynamicFlowRuntimeReadinessPlans(f.ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: current, Expected: desired}}, time.Now().UTC())
				if err != nil || len(results) != 1 || !results[0].Changed || results[0].AttemptOrdinal != original.AttemptOrdinal {
					t.Fatalf("plan change disposed an unsettled predecessor: result=%+v err=%v", results, err)
				}
				current, found, err = f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || current.AttemptState != "superseded" || current.AttemptOrdinal != original.AttemptOrdinal {
					t.Fatalf("lost exact predecessor: found=%v row=%+v err=%v", found, current, err)
				}
			}
			if current.PlanHash != original.PlanHash {
				t.Fatal("ABA did not restore exact planning equality")
			}
			_, err = f.selected.ReconcileDynamicFlowRuntimeReadinessPlans(f.ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: original, Expected: original.Plan}}, time.Now().UTC())
			if !errors.Is(err, runtimepipeline.ErrDynamicFlowRuntimeReadinessObservationStale) {
				t.Fatalf("planning equality revived stale observed authority: %v", err)
			}
			stale, err := f.selected.AdvanceFlowAttachment(f.ctx, f.attempt, runtimepipeline.FlowAttachmentTimersArmed, time.Now().UTC())
			if err != nil || !stale.Acknowledged || stale.Progress != runtimepipeline.FlowAttachmentStale {
				t.Fatalf("superseded predecessor progressed restored plan: %+v %v", stale, err)
			}
			if _, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(current.Plan, current.AttemptOrdinal, current.AttemptState, f.attempt.ProcessBinding())); err == nil {
				t.Fatal("successor bypassed predecessor settlement")
			}
			if err := f.selected.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
				t.Fatal(err)
			}
			successor, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(current.Plan, current.AttemptOrdinal, current.AttemptState, f.attempt.ProcessBinding()))
			if err != nil || !successor.Acknowledged || successor.Reused || successor.Attempt.Ordinal() != original.AttemptOrdinal+1 {
				t.Fatalf("settled ABA successor: %+v %v", successor, err)
			}
			row, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
			if err != nil || !found || row.Phase != runtimepipeline.FlowAttachmentPlanned || row.PlanHash != original.PlanHash {
				t.Fatalf("successor inherited removed resources: found=%v row=%+v err=%v", found, row, err)
			}
			stale, err = f.selected.AdvanceFlowAttachment(f.ctx, f.attempt, runtimepipeline.FlowAttachmentPlanned, time.Now().UTC())
			if err != nil || !stale.Acknowledged || stale.Progress != runtimepipeline.FlowAttachmentStale {
				t.Fatalf("late predecessor changed successor: %+v %v", stale, err)
			}
		})
	}
}

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
			predecessor, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, plan.Identity.Route())
			if err != nil || !found {
				t.Fatalf("load retired predecessor: found=%v err=%v", found, err)
			}
			admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan, predecessor.AttemptOrdinal, predecessor.AttemptState, f.attempt.ProcessBinding()))
			if err != nil || !admitted.Acknowledged {
				t.Fatalf("begin: %+v %v", admitted, err)
			}
			ready, err := completeFlowAttachmentFixture(f.ctx, f.selected, admitted.Attempt, time.Now().UTC())
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
				if err != nil || !found || (before.Phase != runtimepipeline.FlowAttachmentReady) {
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
				if after.AttemptState != "aborted" || after.AttemptOrdinal != before.AttemptOrdinal || after.Phase != before.Phase || !after.CreationEventEmittedAt.Equal(before.CreationEventEmittedAt) {
					t.Fatalf("abandonment changed creation evidence: before=%+v after=%+v", before, after)
				}
				admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(after.Plan, after.AttemptOrdinal, after.AttemptState, f.attempt.ProcessBinding()))
				if err != nil || !admitted.Acknowledged || admitted.Reused || admitted.Attempt.ID() == f.attempt.ID() {
					t.Fatalf("retry admission: %+v %v", admitted, err)
				}
				retried, found, err := f.selected.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
				if err != nil || !found || retried.Phase != runtimepipeline.FlowAttachmentPlanned || retried.AttemptState != "accepted" || retried.AttemptOrdinal != after.AttemptOrdinal+1 {
					t.Fatalf("retry inherited abandoned physical progress: found=%v row=%+v err=%v", found, retried, err)
				}
			})
		}
	}
}
