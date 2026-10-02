package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type attachmentTimerAcquisitionWorkflow struct {
	*pipeline.PipelineCoordinator
	retryRelease     chan struct{}
	cut, disposition string
	enabled          atomic.Bool
	acquired         atomic.Int32
	cancel           context.CancelFunc
	fault            error
	ready            chan pipeline.DynamicFlowRuntimeActivationAttempt
}

type attachmentTimerAcquisitionOccurrence struct {
	worklifetime.Occurrence
	workflow *attachmentTimerAcquisitionWorkflow
	calls    int
}

func (o *attachmentTimerAcquisitionOccurrence) Begin(ctx context.Context) (*worklifetime.Lease, error) {
	o.calls++
	if o.calls == 2 && o.workflow.cut == "before_second" {
		if err := o.workflow.inject(); err != nil {
			return nil, err
		}
	}
	lease, err := o.Occurrence.Begin(ctx)
	if err == nil {
		o.workflow.acquired.Add(1)
	}
	return lease, err
}

func (w *attachmentTimerAcquisitionWorkflow) inject() error {
	if !w.enabled.CompareAndSwap(true, false) {
		return nil
	}
	switch w.disposition {
	case "panic":
		panic(w.fault)
	case "caller_cancel":
		w.cancel()
		return nil
	default:
		return w.fault
	}
}

func (w *attachmentTimerAcquisitionWorkflow) ReconcileInitialEntryTimersForAttempt(ctx context.Context, identity flowidentity.RunScopedFlowInstance, attempt pipeline.DynamicFlowRuntimeActivationAttempt, plan pipeline.DynamicFlowRuntimeReadinessPlan) error {
	if w.cut == "stopped" {
		return w.PipelineCoordinator.ReconcileInitialEntryTimersForAttempt(ctx, identity, attempt, plan)
	}
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		return errors.New("timer acquisition proof requires the admitted execution occurrence")
	}
	probe := &attachmentTimerAcquisitionOccurrence{Occurrence: owner, workflow: w}
	err := w.PipelineCoordinator.ReconcileInitialEntryTimersForAttempt(worklifetime.WithOccurrence(ctx, probe), identity, attempt, plan)
	if err == nil && w.cut == "after_projection" {
		return w.inject()
	}
	return err
}

func (w *attachmentTimerAcquisitionWorkflow) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	result, err := w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	if err == nil && result.Admitted() && result.Phase == pipeline.FlowAttachmentReady {
		w.ready <- attempt
	}
	return result, err
}

func (w *attachmentTimerAcquisitionWorkflow) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	select {
	case <-w.retryRelease:
		return w.PipelineCoordinator.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	case <-ctx.Done():
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, ctx.Err()
	}
}

func TestFlowAttachmentTimerAcquisitionFailureCannotBecomeReadyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runFlowAttachmentTimerAcquisition(t, backend, "stopped", "error")
		})
	}
}

func TestFlowAttachmentTimerAcquisitionCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"before_second", "after_projection"} {
				for _, disposition := range []string{"error", "panic", "caller_cancel"} {
					t.Run(cut+"/"+disposition, func(t *testing.T) {
						runFlowAttachmentTimerAcquisition(t, backend, cut, disposition)
					})
				}
			}
		})
	}
}

func runFlowAttachmentTimerAcquisition(t *testing.T, backend, cut, disposition string) {
	t.Helper()
	scheduler := pipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t))
	t.Cleanup(scheduler.Stop)
	workflow := &attachmentTimerAcquisitionWorkflow{retryRelease: make(chan struct{}), cut: cut, disposition: disposition, fault: errors.New("injected partial timer acquisition failure"), ready: make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 2)}
	workflow.enabled.Store(true)
	f := newReceiverConfigActivationFixtureWithOwnership(t, backend, false, map[string]string{
		"schema.yaml":          "name: timer-acquisition\n",
		"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending:\n    initial: true\n    timers:\n      - {id: pending.timeout, after: 2h, emit: timer.timeout}\n      - {id: pending.second, after: 3h, emit: timer.timeout}\npins:\n  inputs:\n    events: [task.started]\n",
		"review/entities.yaml": "review_item:\n  request_id: text\n",
		"review/events.yaml":   "task.started:\ntimer.timeout:\n",
	}, nil, ownStoreTestAgentManager, func(options *pipeline.PipelineCoordinatorOptions) {
		options.TimerScheduler = scheduler
	}, func(options *manager.AgentManagerOptions) {
		workflow.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
		options.WorkflowInstances = workflow
	})
	t.Cleanup(func() { close(workflow.retryRelease) })
	binding, err := f.grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, binding.RuntimeInstanceID, binding.RuntimeGeneration, "", "attachment-timer-acquisition", binding.BundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Run(managedexecution.WithAdmission(f.ctx, admission)); err != nil {
		t.Fatal(err)
	}
	req := f.request("business-key", "timer-acquisition", "unchanged")
	req.Config = map[string]any{"request_id": "business-key"}
	req.OccurredAt = time.Now().UTC()
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
	timers, err := timerStore.ListWorkflowTimerActivations(f.ctx, owner.RunID, initial.EntityID, true)
	if err != nil || len(timers) != 2 {
		t.Fatalf("initial timer evidence: %+v %v", timers, err)
	}
	var receipt []byte
	if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	if cut == "stopped" {
		scheduler.Stop()
	}
	caller, cancelCaller := context.WithCancel(f.ctx)
	defer cancelCaller()
	workflow.cancel = cancelCaller
	err = f.manager.FinalizeCommittedFlowInstanceActivation(caller, committed)
	want := workflow.fault.Error()
	if cut == "stopped" {
		want = "scheduler stopped"
	}
	if disposition == "caller_cancel" {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation changed accepted timer work: %v", err)
		}
	} else if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("physical timer acquisition failure became ready: %v", err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if disposition != "caller_cancel" {
		if err := f.manager.WaitForQuiescence(ctx); err != nil {
			t.Fatalf("failed acquisition did not join exact cleanup: %v", err)
		}
		if err := scheduler.Wait(ctx); err != nil {
			t.Fatalf("partial timer acquisition retained a wakeup lease: %v", err)
		}
		row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, owner.RunID, owner.Route)
		if err != nil || !found || row.AttemptState != "aborted" || row.Phase != pipeline.FlowAttachmentRouteInstalled || row.AttemptOrdinal != 1 || len(f.bus.routePaths()) != 0 {
			t.Fatalf("failed timer acquisition retained executable progress: %+v found=%t err=%v routes=%v", row, found, err, f.bus.routePaths())
		}
		wantAcquired := int32(1)
		if cut == "after_projection" {
			wantAcquired = 2
		}
		if cut == "stopped" {
			wantAcquired = 0
		}
		if workflow.acquired.Load() != wantAcquired {
			t.Fatalf("physical acquisition cut was not reached: acquired=%d want=%d", workflow.acquired.Load(), wantAcquired)
		}
		if cut != "stopped" {
			if created, err := f.manager.EnsureFlowInstance(f.ctx, req); created || err != nil {
				t.Fatalf("timer retry repeated construction: created=%t err=%v", created, err)
			}
		}
	}
	if cut != "stopped" {
		select {
		case attempt := <-workflow.ready:
			wantOrdinal := uint64(2)
			if disposition == "caller_cancel" {
				wantOrdinal = 1
			}
			if attempt.Ordinal() != wantOrdinal || workflow.enabled.Load() {
				t.Fatalf("wrong ready attempt or unreached cut: %+v enabled=%t", attempt, workflow.enabled.Load())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("accepted timer work did not reach ready")
		}
		wantAcquired := int32(4)
		if cut == "before_second" {
			wantAcquired = 3
		}
		if disposition == "caller_cancel" {
			wantAcquired = 2
		}
		if workflow.acquired.Load() != wantAcquired {
			t.Fatalf("retry skipped or repeated wakeups: acquired=%d want=%d", workflow.acquired.Load(), wantAcquired)
		}
	}
	current, found, err := f.workflows.Load(f.ctx, owner)
	if err != nil || !found || !reflect.DeepEqual(current, initial) {
		t.Fatalf("failed attachment changed construction: %+v -> %+v, %v", initial, current, err)
	}
	var currentReceipt []byte
	if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&currentReceipt); err != nil || string(currentReceipt) != string(receipt) {
		t.Fatalf("failed attachment changed constructor receipt: %v", err)
	}
	if currentLedger := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID); !reflect.DeepEqual(currentLedger, ledger) {
		t.Fatal("failed attachment repeated initial lifecycle mutation")
	}
	currentTimers, err := timerStore.ListWorkflowTimerActivations(f.ctx, owner.RunID, initial.EntityID, true)
	if err != nil || !reflect.DeepEqual(currentTimers, timers) {
		t.Fatalf("failed attachment changed durable timer identity/clock: %+v -> %+v, %v", timers, currentTimers, err)
	}
	assertConstructorRows(t, f, backend, 1)
}
