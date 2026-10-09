package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type attachmentAgentRouteProbe struct {
	*sqliteFlowActivationBus
	mu       sync.Mutex
	prepared map[effects.LifecycleToken]struct{}
	fenced   map[effects.LifecycleToken]struct{}
	removed  map[effects.LifecycleToken]struct{}
}

func (p *attachmentAgentRouteProbe) PrepareAgentRoute(token effects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) bus.AgentRoutePreparation {
	p.mu.Lock()
	p.prepared[token] = struct{}{}
	p.mu.Unlock()
	return p.sqliteFlowActivationBus.PrepareAgentRoute(token, admission)
}

func (p *attachmentAgentRouteProbe) FenceAgentRoute(token effects.LifecycleToken) {
	p.mu.Lock()
	p.fenced[token] = struct{}{}
	p.mu.Unlock()
	p.sqliteFlowActivationBus.FenceAgentRoute(token)
}

func (p *attachmentAgentRouteProbe) RemoveAgentRoute(token effects.LifecycleToken) {
	p.mu.Lock()
	p.removed[token] = struct{}{}
	p.mu.Unlock()
	p.sqliteFlowActivationBus.RemoveAgentRoute(token)
}

func (p *attachmentAgentRouteProbe) requireOwnedRoutes(t *testing.T, live, retired int) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.prepared) != live+retired || len(p.fenced) != retired || len(p.removed) != retired {
		t.Fatalf("agent route ownership: prepared=%d fenced=%d removed=%d, want live=%d retired=%d", len(p.prepared), len(p.fenced), len(p.removed), live, retired)
	}
	for token := range p.removed {
		if _, prepared := p.prepared[token]; !prepared {
			t.Fatal("cleanup removed an unowned agent route")
		}
		if _, fenced := p.fenced[token]; !fenced {
			t.Fatal("cleanup removed an unfenced agent route")
		}
	}
}

type attachmentPhaseFaultWorkflow struct {
	*pipeline.PipelineCoordinator
	previous      pipeline.FlowAttachmentPhase
	disposition   string
	fault         error
	enabled       atomic.Bool
	mu            sync.Mutex
	failedAttempt pipeline.DynamicFlowRuntimeActivationAttempt
	retryRelease  chan struct{}
	abandoned     chan error
	ready         chan pipeline.DynamicFlowRuntimeActivationAttempt
}

func (w *attachmentPhaseFaultWorkflow) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	err := w.PipelineCoordinator.AbandonDynamicFlowRuntimeActivationAttempt(ctx, attempt)
	w.abandoned <- err
	return err
}

func (w *attachmentPhaseFaultWorkflow) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	select {
	case <-w.retryRelease:
		return w.PipelineCoordinator.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	case <-ctx.Done():
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, ctx.Err()
	}
}

func (w *attachmentPhaseFaultWorkflow) requireQuiescence(t *testing.T, am *manager.AgentManager, ctx context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := am.WaitForQuiescence(ctx)
	if err == nil {
		return
	}
	want := "terminal retirement: " + w.fault.Error() + "\nattachment progress was not acknowledged"
	if w.disposition == "panic_after_commit" {
		want = "terminal retirement: dynamic flow readiness attempt panic: " + w.fault.Error()
	}
	if err.Error() != want {
		t.Fatalf("owned readiness work did not settle with exact fault evidence: %v", err)
	}
}

func (w *attachmentPhaseFaultWorkflow) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	inject := previous == w.previous && w.enabled.CompareAndSwap(true, false)
	if inject {
		w.mu.Lock()
		w.failedAttempt = attempt
		w.mu.Unlock()
		if w.disposition == "before_commit" {
			return pipeline.FlowAttachmentAdvanceResult{}, w.fault
		}
	}
	result, err := w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	if previous == pipeline.FlowAttachmentTimersArmed && result.Admitted() && result.Phase == pipeline.FlowAttachmentReady {
		w.ready <- attempt
	}
	if !inject || err != nil || !result.Acknowledged {
		return result, err
	}
	switch w.disposition {
	case "unacknowledged_response":
		return pipeline.FlowAttachmentAdvanceResult{}, w.fault
	case "acknowledged_error":
		return result, w.fault
	case "panic_after_commit":
		panic(w.fault)
	default:
		return result, nil
	}
}

func TestFlowAttachmentPhaseFailureRetainsConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, previous := range []pipeline.FlowAttachmentPhase{pipeline.FlowAttachmentPlanned, pipeline.FlowAttachmentAgentsRegistered, pipeline.FlowAttachmentRouteInstalled, pipeline.FlowAttachmentTimersArmed} {
			for _, disposition := range []string{"before_commit", "unacknowledged_response", "acknowledged_error", "panic_after_commit"} {
				t.Run(backend+"/"+string(previous)+"/"+disposition, func(t *testing.T) {
					scheduler := pipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t))
					t.Cleanup(scheduler.Stop)
					workflow := &attachmentPhaseFaultWorkflow{previous: previous, disposition: disposition, fault: errors.New("injected attachment phase response failure"), retryRelease: make(chan struct{}), abandoned: make(chan error, 1), ready: make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 2)}
					routes := &attachmentAgentRouteProbe{prepared: make(map[effects.LifecycleToken]struct{}), fenced: make(map[effects.LifecycleToken]struct{}), removed: make(map[effects.LifecycleToken]struct{})}
					workflow.enabled.Store(true)
					f := newReceiverConfigActivationFixtureWithOwnership(t, backend, true, map[string]string{
						"schema.yaml":          "name: phase-failure\n",
						"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending:\n    timers:\n      - {id: pending.timeout, after: 2h, emit: timer.timeout}\npins:\n  inputs:\n    - task.started\n",
						"review/entities.yaml": "review_item:\n  request_id: text\n",
						"review/events.yaml":   "task.started:\ntimer.timeout:\n",
					}, nil, func(t *testing.T, am *manager.AgentManager) *manager.AgentManager {
						t.Cleanup(func() {
							err := am.Shutdown()
							if err == nil {
								return
							}
							want := "terminal retirement: " + workflow.fault.Error() + "\nattachment progress was not acknowledged"
							if disposition == "panic_after_commit" {
								want = "terminal retirement: dynamic flow readiness attempt panic: " + workflow.fault.Error()
							}
							if err.Error() != want {
								t.Errorf("shutdown lost fault evidence or added cleanup failure: %v", err)
							}
						})
						return am
					}, func(options *pipeline.PipelineCoordinatorOptions) {
						options.TimerScheduler = scheduler
					}, func(options *manager.AgentManagerOptions) {
						workflow.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
						options.WorkflowInstances = workflow
						routes.sqliteFlowActivationBus = options.PersistenceRoles.AgentRoutes.(*sqliteFlowActivationBus)
						options.PersistenceRoles.AgentRoutes = routes
					})
					f.constructKeylessRoot(t)
					binding, err := f.grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					admission, err := managedexecution.New(managedexecution.KindNormalRuntime, binding.RuntimeInstanceID, binding.RuntimeGeneration, "", "attachment-phase-matrix", binding.BundleHash, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.manager.Run(managedexecution.WithAdmission(f.ctx, admission)); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { close(workflow.retryRelease) })
					req := f.request("business-key", "phase-failure", "unchanged")
					req.OccurredAt = time.Now().UTC()
					req.TriggerEvent = eventtest.ExistingRunRootIngress(req.TriggerEvent.ID(), "task.started", "constructor-fixture", "", []byte(`{}`), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.OccurredAt)
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil || !committed.Acknowledged || !committed.Created {
						t.Fatalf("constructor commit: %+v %v", committed, err)
					}
					owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), plan.Identity.Route())
					if err != nil {
						t.Fatal(err)
					}
					initial, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found {
						t.Fatalf("initial header: %+v %t %v", initial, found, err)
					}
					ledger := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID)
					timerStore := f.store.(interface {
						ListWorkflowTimerActivations(context.Context, string, string, bool) ([]pipeline.WorkflowTimerActivation, error)
					})
					initialTimers, err := timerStore.ListWorkflowTimerActivations(f.ctx, owner.RunID, initial.EntityID, true)
					if err != nil || len(initialTimers) != 1 {
						t.Fatalf("initial timer receipt: %+v %v", initialTimers, err)
					}
					var receipt []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&receipt); err != nil {
						t.Fatal(err)
					}
					if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err == nil {
						t.Fatal("missing phase fault")
					}
					if disposition != "acknowledged_error" {
						select {
						case err := <-workflow.abandoned:
							if err != nil {
								t.Fatalf("predecessor abandonment: %v", err)
							}
						case <-time.After(5 * time.Second):
							t.Fatal("exact predecessor cleanup did not acknowledge abandonment")
						}
						workflow.requireQuiescence(t, f.manager, f.ctx)
					}
					workflow.mu.Lock()
					predecessor := workflow.failedAttempt
					workflow.mu.Unlock()
					if workflow.enabled.Load() || predecessor.Validate() != nil {
						t.Fatal("matrix cell never reached its exact phase write")
					}
					row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, owner.RunID, owner.Route)
					if err != nil || !found || row.AttemptOrdinal != predecessor.Ordinal() {
						t.Fatalf("post-fault attempt: %+v %t %v", row, found, err)
					}
					acknowledged := disposition == "acknowledged_error"
					if acknowledged {
						if row.Phase != pipeline.FlowAttachmentReady || row.AttemptState != "accepted" || len(f.manager.ListAgentConfigs()) != 1 {
							t.Fatalf("acknowledged progress lost its exact resource owner: %+v agents=%v", row, f.manager.ListAgentConfigs())
						}
						if err := f.workflows.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, predecessor); err != nil {
							t.Fatalf("acknowledged attachment lost current attempt: %v", err)
						}
						routes.requireOwnedRoutes(t, 1, 0)
					} else {
						if row.AttemptState != "aborted" {
							t.Fatalf("failed progress did not join and abandon predecessor: %+v agents=%v", row, f.manager.ListAgentConfigs())
						}
						routes.requireOwnedRoutes(t, 0, 1)
						waitCtx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
						waitErr := scheduler.Wait(waitCtx)
						cancel()
						if waitErr != nil {
							t.Fatalf("failed attachment retained an unjoined timer wakeup: %v", waitErr)
						}
						if err := f.workflows.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, predecessor); err == nil {
							t.Fatal("joined/abandoned predecessor retained execution permission")
						}
					}
					if created, err := f.manager.EnsureFlowInstance(f.ctx, req); created || (err != nil && err.Error() != "dynamic flow runtime readiness retains predecessor retirement") {
						t.Fatalf("attachment retry repeated construction: created=%t err=%v", created, err)
					}
					wantOrdinal := predecessor.Ordinal()
					if !acknowledged {
						wantOrdinal++
					}
					readyCtx, cancelReady := context.WithTimeout(f.ctx, 5*time.Second)
					defer cancelReady()
				ready:
					for {
						select {
						case attempt := <-workflow.ready:
							if attempt.Ordinal() == wantOrdinal {
								break ready
							}
						case <-readyCtx.Done():
							t.Fatal("successor did not acknowledge its exact ready phase")
						}
					}
					if !acknowledged {
						routes.requireOwnedRoutes(t, 1, 1)
					}
					row, found, err = f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, owner.RunID, owner.Route)
					if err != nil || !found || row.AttemptOrdinal != wantOrdinal || row.Phase != pipeline.FlowAttachmentReady || row.AttemptState != "accepted" || len(f.manager.ListAgentConfigs()) != 1 {
						t.Fatalf("retry inherited removed progress or lost topology: %+v found=%t err=%v agents=%v", row, found, err, f.manager.ListAgentConfigs())
					}
					if !acknowledged {
						stale, err := f.workflows.AdvanceFlowAttachment(f.ctx, predecessor, pipeline.FlowAttachmentPlanned, time.Now().UTC())
						if err != nil || !stale.Acknowledged || stale.Progress != pipeline.FlowAttachmentStale {
							t.Fatalf("late predecessor progressed successor: %+v %v", stale, err)
						}
					} else {
						routes.requireOwnedRoutes(t, 1, 0)
					}
					current, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(current, initial) {
						t.Fatalf("reattachment changed immutable construction/header: before=%+v after=%+v found=%t err=%v", initial, current, found, err)
					}
					var currentReceipt []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&currentReceipt); err != nil || string(currentReceipt) != string(receipt) {
						t.Fatalf("reattachment changed creating/lifecycle receipt: before=%s after=%s err=%v", receipt, currentReceipt, err)
					}
					if currentLedger := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID); !reflect.DeepEqual(currentLedger, ledger) {
						t.Fatal("reattachment appended construction or stage-entry mutation")
					}
					currentTimers, err := timerStore.ListWorkflowTimerActivations(f.ctx, owner.RunID, initial.EntityID, true)
					if err != nil || !reflect.DeepEqual(currentTimers, initialTimers) {
						t.Fatalf("attachment retry changed initial timer identity/clock: %+v -> %+v, err=%v", initialTimers, currentTimers, err)
					}
					cancelled, cancel := context.WithCancel(f.ctx)
					cancel()
					if err := scheduler.Wait(cancelled); !errors.Is(err, context.Canceled) {
						t.Fatalf("ready attachment has no live scheduler projection: %v", err)
					}
					if err := workflow.RetireInitialEntryTimerWakeups(f.ctx, owner); err != nil {
						t.Fatalf("retire exact live timer projection: %v", err)
					}
					workflow.requireQuiescence(t, f.manager, f.ctx)
					assertConstructorRows(t, f, backend, 1)
				})
			}
		}
	}
}
