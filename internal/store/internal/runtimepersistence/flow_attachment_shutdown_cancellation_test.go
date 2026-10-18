package runtimepersistence

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type attachmentShutdownProgressObservation struct {
	result pipeline.FlowAttachmentAdvanceResult
	err    error
	native transactiontest.Snapshot
}

type attachmentShutdownProgressWorkflow struct {
	*pipeline.PipelineCoordinator
	selected  any
	enabled   atomic.Bool
	entered   chan context.Context
	release   chan struct{}
	observed  chan attachmentShutdownProgressObservation
	abandoned chan error
}

func (w *attachmentShutdownProgressWorkflow) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	if previous != pipeline.FlowAttachmentPlanned || !w.enabled.CompareAndSwap(true, false) {
		return w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	}
	probe, restore, err := InstallTransactionProbeForTest(w.selected, transactiontest.Options{})
	if err != nil {
		return pipeline.FlowAttachmentAdvanceResult{}, err
	}
	defer restore()
	w.entered <- ctx
	<-w.release
	result, err := w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	joinCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err = errors.Join(err, probe.StopAndJoin(joinCtx))
	w.observed <- attachmentShutdownProgressObservation{result: result, err: err, native: probe.Snapshot()}
	return result, err
}

func (w *attachmentShutdownProgressWorkflow) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	err := w.PipelineCoordinator.AbandonDynamicFlowRuntimeActivationAttempt(ctx, attempt)
	w.abandoned <- err
	return err
}

// Adds the shutdown-cancellation reach to the existing native phase-failure
// invariant: a cancelled progress call is not itself proof of clean settlement.
func proveAttachmentShutdownBeforeProgress(t *testing.T, backend string) {
	t.Helper()
	workflow := &attachmentShutdownProgressWorkflow{
		entered: make(chan context.Context, 1), release: make(chan struct{}),
		observed: make(chan attachmentShutdownProgressObservation, 1), abandoned: make(chan error, 1),
	}
	release := sync.OnceFunc(func() { close(workflow.release) })
	defer release()
	f := newReceiverConfigActivationFixtureWithOwnership(t, backend, true, map[string]string{
		"schema.yaml":          "name: shutdown-progress\n",
		"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {}\npins:\n  inputs:\n    - task.started\n",
		"review/entities.yaml": "review_item:\n  request_id: text\n",
		"review/events.yaml":   "task.started:\n",
	}, nil, ownStoreTestAgentManager, nil, func(options *manager.AgentManagerOptions) {
		workflow.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
		options.WorkflowInstances = workflow
	})
	f.constructKeylessRoot(t)
	workflow.selected = f.store
	binding, err := f.grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, binding.RuntimeInstanceID, binding.RuntimeGeneration, "", "shutdown-progress", binding.BundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Run(managedexecution.WithAdmission(f.ctx, admission)); err != nil {
		t.Fatal(err)
	}
	req := f.request("business-key", "shutdown-progress", "unchanged")
	req.OccurredAt = time.Now().UTC()
	req.TriggerEvent = eventtest.ExistingRunRootIngress(req.TriggerEvent.ID(), "task.started", "constructor-fixture", "", []byte(`{}`), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.OccurredAt)
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	workflow.enabled.Store(true)
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("exact construction commit: %+v %v", committed, err)
	}
	finalized := make(chan error, 1)
	go func() { finalized <- f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed) }()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	var progressCtx context.Context
	select {
	case progressCtx = <-workflow.entered:
	case <-ctx.Done():
		t.Fatal("admitted attempt did not reach the real progress owner")
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- f.manager.ShutdownWithOptions(manager.ShutdownOptions{Grace: 5 * time.Second}) }()
	select {
	case <-progressCtx.Done():
	case <-ctx.Done():
		t.Fatal("shutdown did not cancel its admitted progress occurrence")
	}
	release()
	var observed attachmentShutdownProgressObservation
	select {
	case observed = <-workflow.observed:
	case <-ctx.Done():
		t.Fatal("native progress result did not join")
	}
	if observed.result.Acknowledged || !errors.Is(observed.err, context.Canceled) || observed.native.Total.WriteCommits != 0 || observed.native.Active != 0 {
		t.Fatalf("cancelled-before-entry result: %+v error=%v native=%+v", observed.result, observed.err, observed.native)
	}
	select {
	case err := <-workflow.abandoned:
		if err != nil {
			t.Fatalf("exact durable abandonment failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("cancelled attempt did not acknowledge durable abandonment")
	}
	row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, correlation.RunIDFromContext(f.ctx), plan.Identity.Route())
	if err != nil || !found || row.Phase != pipeline.FlowAttachmentPlanned || row.AttemptState != "aborted" {
		t.Fatalf("exact cancelled attachment evidence: %+v found=%t err=%v", row, found, err)
	}
	t.Logf("shutdown progress pin: backend=%s acknowledgement=%t phase=%s attempt=%s native_begun=%d commits=%d commit_attempts=%d rollbacks=%d cleanup_failures=%d active=%d abandonment=acknowledged", backend, observed.result.Acknowledged, row.Phase, row.AttemptState, observed.native.Total.Begun, observed.native.Total.WriteCommits, observed.native.Total.CommitAttempts, observed.native.Total.RollbackAttempts, observed.native.Total.CleanupFailures, observed.native.Active)
	select {
	case err := <-finalized:
		t.Logf("readiness caller returned after retirement handoff: %v", err)
	case <-ctx.Done():
		t.Fatal("readiness caller retained its accepted attempt")
	}
	select {
	case err := <-shutdown:
		if len(f.manager.ListAgentConfigs()) != 0 {
			t.Fatal("shutdown retained cancelled attachment agents")
		}
		assertConstructorRows(t, f, backend, 1)
		if err != nil {
			t.Fatalf("cleanly abandoned shutdown progress became terminal failure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("shutdown did not join its cancelled readiness attempt")
	}
}
