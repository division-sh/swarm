package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
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
)

type attachmentCleanupFault struct {
	sink       string
	persistent bool
	enabled    atomic.Bool
	hits       atomic.Int32
	err        error
}

func (f *attachmentCleanupFault) inject(sink string) error {
	if f.sink != sink || !f.enabled.Load() {
		return nil
	}
	if !f.persistent && !f.enabled.CompareAndSwap(true, false) {
		return nil
	}
	f.hits.Add(1)
	return f.err
}

type attachmentCleanupWorkflow struct {
	*pipeline.PipelineCoordinator
	fault         *attachmentCleanupFault
	activationErr error
	activation    atomic.Bool
	mu            sync.Mutex
	predecessor   pipeline.DynamicFlowRuntimeActivationAttempt
	retryRelease  chan struct{}
	ready         chan pipeline.DynamicFlowRuntimeActivationAttempt
}

func (w *attachmentCleanupWorkflow) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	select {
	case <-w.retryRelease:
		return w.PipelineCoordinator.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	case <-ctx.Done():
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, ctx.Err()
	}
}

func (w *attachmentCleanupWorkflow) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	if previous == pipeline.FlowAttachmentTimersArmed && w.activation.CompareAndSwap(true, false) {
		w.mu.Lock()
		w.predecessor = attempt
		w.mu.Unlock()
		w.fault.enabled.Store(true)
		return pipeline.FlowAttachmentAdvanceResult{}, w.activationErr
	}
	result, err := w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	if previous == pipeline.FlowAttachmentTimersArmed && result.Admitted() && result.Phase == pipeline.FlowAttachmentReady {
		w.ready <- attempt
	}
	return result, err
}

func (w *attachmentCleanupWorkflow) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := w.fault.inject("abandonment"); err != nil {
		return err
	}
	if err := w.PipelineCoordinator.AbandonDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
		return err
	}
	// The durable operation really commits, but its response can still be lost.
	return w.fault.inject("abandonment_response_loss")
}

func (w *attachmentCleanupWorkflow) RetireInitialEntryTimerWakeups(ctx context.Context, owner flowidentity.RunScopedFlowInstance) error {
	if err := w.fault.inject("timer"); err != nil {
		return err
	}
	return w.PipelineCoordinator.RetireInitialEntryTimerWakeups(ctx, owner)
}

type attachmentCleanupRouteOwner struct {
	*sqliteFlowActivationBus
	fault *attachmentCleanupFault
}

type attachmentCleanupPublication struct {
	bus.FlowRoutePublicationHandle
	fault *attachmentCleanupFault
}

func (p attachmentCleanupPublication) Retire() error {
	if err := p.fault.inject("route"); err != nil {
		return err
	}
	return p.FlowRoutePublicationHandle.Retire()
}

func (r *attachmentCleanupRouteOwner) PublishPersistedFlowInstanceRouteForAttempt(ctx context.Context, req bus.FlowInstanceRouteMaterializationRequest, attempt pipeline.DynamicFlowRuntimeActivationAttempt) (bus.FlowRoutePublicationHandle, error) {
	publication, err := r.sqliteFlowActivationBus.PublishPersistedFlowInstanceRouteForAttempt(ctx, req, attempt)
	if err != nil {
		return nil, err
	}
	return attachmentCleanupPublication{FlowRoutePublicationHandle: publication, fault: r.fault}, nil
}

type attachmentCleanupAgentRoutes struct {
	*attachmentAgentRouteProbe
	fault    *attachmentCleanupFault
	mu       sync.Mutex
	removals map[effects.LifecycleToken]int
}

func (r *attachmentCleanupAgentRoutes) RemoveAgentRoute(token effects.LifecycleToken) {
	r.mu.Lock()
	r.removals[token]++
	joinedRemoval := r.removals[token] > 1
	r.mu.Unlock()
	// Permit the loop finalizer's first removal; inject only in its owning join.
	if joinedRemoval {
		if err := r.fault.inject("agent_join"); err != nil {
			panic(err)
		}
	}
	r.attachmentAgentRouteProbe.RemoveAgentRoute(token)
}

func TestFlowAttachmentCleanupRetainsExactPredecessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, sink := range []string{"route", "agent_join", "timer", "abandonment", "abandonment_response_loss"} {
			for _, persistent := range []bool{false, true} {
				mode := "fail_once"
				if persistent {
					mode = "persistent"
				}
				t.Run(backend+"/"+sink+"/"+mode, func(t *testing.T) {
					var cleanupEvidence error
					fault := &attachmentCleanupFault{sink: sink, persistent: persistent, err: errors.New("injected exact " + sink + " cleanup failure")}
					workflow := &attachmentCleanupWorkflow{fault: fault, activationErr: errors.New("injected readiness completion failure"), retryRelease: make(chan struct{}), ready: make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 4)}
					workflow.activation.Store(true)
					routeOwner := &attachmentCleanupRouteOwner{fault: fault}
					probe := &attachmentAgentRouteProbe{prepared: make(map[effects.LifecycleToken]struct{}), fenced: make(map[effects.LifecycleToken]struct{}), removed: make(map[effects.LifecycleToken]struct{})}
					agentRoutes := &attachmentCleanupAgentRoutes{attachmentAgentRouteProbe: probe, fault: fault, removals: make(map[effects.LifecycleToken]int)}
					scheduler := pipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t))
					t.Cleanup(scheduler.Stop)
					f := newReceiverConfigActivationFixtureWithOwnership(t, backend, true, map[string]string{
						"schema.yaml":          "name: cleanup-failure\n",
						"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending:\n    initial: true\n    timers:\n      - {id: pending.timeout, after: 2h, emit: timer.timeout}\npins:\n  inputs:\n    - task.started\n",
						"review/entities.yaml": "review_item:\n  request_id: text\n",
						"review/events.yaml":   "task.started:\ntimer.timeout:\n",
					}, nil, func(t *testing.T, am *manager.AgentManager) *manager.AgentManager {
						t.Cleanup(func() {
							fault.enabled.Store(false)
							close(workflow.retryRelease)
							if err := am.Shutdown(); err != nil && (cleanupEvidence == nil || err.Error() != cleanupEvidence.Error()) {
								t.Errorf("shutdown changed joined cleanup evidence: got=%v want=%v", err, cleanupEvidence)
							}
						})
						return am
					}, func(options *pipeline.PipelineCoordinatorOptions) {
						options.TimerScheduler = scheduler
					}, func(options *manager.AgentManagerOptions) {
						workflow.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
						options.WorkflowInstances = workflow
						probe.sqliteFlowActivationBus = options.PersistenceRoles.AgentRoutes.(*sqliteFlowActivationBus)
						routeOwner.sqliteFlowActivationBus = probe.sqliteFlowActivationBus
						options.PersistenceRoles.AgentRoutes = agentRoutes
						options.PersistenceRoles.RouteRestorer = routeOwner
					})
					binding, err := f.grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					admission, err := managedexecution.New(managedexecution.KindNormalRuntime, binding.RuntimeInstanceID, binding.RuntimeGeneration, "", "attachment-cleanup-matrix", binding.BundleHash, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.manager.Run(managedexecution.WithAdmission(f.ctx, admission)); err != nil {
						t.Fatal(err)
					}
					req := f.request("business-key", "cleanup-failure", "unchanged")
					req.Config = map[string]any{"request_id": "business-key"}
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
						t.Fatal("missing readiness failure")
					}
					join := func() {
						t.Helper()
						ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
						defer cancel()
						joined := f.manager.WaitForQuiescence(ctx)
						if joined == nil {
							t.Fatal("missing joined cleanup failure")
						}
						parts, ok := joined.(interface{ Unwrap() []error })
						if !ok {
							t.Fatalf("missing structured work/cleanup evidence: %v", joined)
						}
						cleanupEvidence = nil
						for _, part := range parts.Unwrap() {
							if errors.Is(part, context.DeadlineExceeded) {
								// The failed sink precedes timer retirement. Its exact timer
								// remains owned; the accepted attachment work must be joined.
								if (sink != "route" && sink != "agent_join" && sink != "timer") || part.Error() != "wait for 1 active work lease(s): context deadline exceeded" {
									t.Fatalf("unexpected retained work: %v", joined)
								}
								cancelled, stop := context.WithCancel(f.ctx)
								stop()
								if err := scheduler.Wait(cancelled); !errors.Is(err, context.Canceled) {
									t.Fatalf("retained lease has no owned timer projection: %v", err)
								}
								continue
							}
							cleanupEvidence = errors.Join(cleanupEvidence, part)
						}
						if cleanupEvidence == nil || !strings.Contains(cleanupEvidence.Error(), fault.err.Error()) {
							t.Fatalf("cleanup failure was not joined with evidence: %v", cleanupEvidence)
						}
					}
					join()
					workflow.mu.Lock()
					predecessor := workflow.predecessor
					workflow.mu.Unlock()
					if predecessor.Validate() != nil || fault.hits.Load() != 1 {
						t.Fatalf("first exact cleanup cut was not reached: %+v hits=%d", predecessor, fault.hits.Load())
					}
					requirePredecessor := func() {
						t.Helper()
						wantState := "accepted"
						if sink == "abandonment_response_loss" {
							wantState = "aborted"
						}
						row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, owner.RunID, owner.Route)
						if err != nil || !found || row.AttemptOrdinal != predecessor.Ordinal() || row.AttemptState != wantState || row.Phase != pipeline.FlowAttachmentTimersArmed {
							t.Fatalf("incomplete cleanup changed durable predecessor: %+v found=%t err=%v", row, found, err)
						}
						probe.mu.Lock()
						defer probe.mu.Unlock()
						if len(probe.prepared) != 1 || len(probe.fenced) != 1 {
							t.Fatalf("incomplete cleanup installed a successor: prepared=%d fenced=%d", len(probe.prepared), len(probe.fenced))
						}
					}
					requirePredecessor()
					if persistent {
						for range 2 {
							if created, err := f.manager.EnsureFlowInstance(f.ctx, req); created || err == nil {
								t.Fatalf("persistent cleanup admitted successor: created=%t err=%v", created, err)
							}
							join()
							requirePredecessor()
						}
						if fault.hits.Load() != 3 {
							t.Fatalf("retained owner did not retry exact sink: hits=%d", fault.hits.Load())
						}
						fault.enabled.Store(false)
					}
					if created, err := f.manager.EnsureFlowInstance(f.ctx, req); created || (err != nil && err.Error() != "dynamic flow runtime readiness retains predecessor retirement") {
						t.Fatalf("settled attachment retry repeated construction: created=%t err=%v", created, err)
					}
					select {
					case successor := <-workflow.ready:
						if successor.Ordinal() != predecessor.Ordinal()+1 || successor.ID() == predecessor.ID() {
							t.Fatalf("retry revived predecessor: old=%+v new=%+v", predecessor, successor)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("joined predecessor did not admit a fresh ready successor")
					}
					probe.requireOwnedRoutes(t, 1, 1)
					if len(f.bus.routePaths()) != 1 || len(f.manager.ListAgentConfigs()) != 1 {
						t.Fatalf("successor topology is not exact: routes=%v agents=%v", f.bus.routePaths(), f.manager.ListAgentConfigs())
					}
					if err := f.workflows.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, predecessor); err == nil {
						t.Fatal("settled predecessor retained attachment authority")
					}
					current, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(current, initial) {
						t.Fatalf("cleanup retry mutated construction: before=%+v after=%+v found=%t err=%v", initial, current, found, err)
					}
					var currentReceipt []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&currentReceipt); err != nil || string(currentReceipt) != string(receipt) {
						t.Fatalf("cleanup retry changed immutable creating/lifecycle receipt: %v", err)
					}
					if currentLedger := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID); !reflect.DeepEqual(currentLedger, ledger) {
						t.Fatal("cleanup retry appended construction or initial-entry work")
					}
					currentTimers, err := timerStore.ListWorkflowTimerActivations(f.ctx, owner.RunID, initial.EntityID, true)
					if err != nil || !reflect.DeepEqual(currentTimers, initialTimers) {
						t.Fatalf("cleanup retry changed timer identity or clock: before=%+v after=%+v err=%v", initialTimers, currentTimers, err)
					}
					if err := workflow.RetireInitialEntryTimerWakeups(f.ctx, owner); err != nil {
						t.Fatal(err)
					}
					assertConstructorRows(t, f, backend, 1)
				})
			}
		}
	}
}
