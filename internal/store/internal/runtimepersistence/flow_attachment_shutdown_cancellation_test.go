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
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type attachmentShutdownProgressObservation struct {
	result                 pipeline.FlowAttachmentAdvanceResult
	err                    error
	rollbackJoinedAtReturn bool
}

type attachmentShutdownProgressWorkflow struct {
	*pipeline.PipelineCoordinator
	barrier      *attachmentProgressNativeBarrier
	cut          string
	verification bool
	enabled      atomic.Bool
	entered      chan context.Context
	release      chan struct{}
	observed     chan attachmentShutdownProgressObservation
	abandoned    chan error
}

func (w *attachmentShutdownProgressWorkflow) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	if w.verification || previous != pipeline.FlowAttachmentPlanned || !w.enabled.CompareAndSwap(true, false) {
		return w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	}
	readiness, found, err := w.LoadDynamicFlowRuntimeReadiness(ctx, attempt.RunID(), flowidentity.RouteForInstancePath(attempt.InstancePath()))
	if err != nil || !found || readiness.AttemptOrdinal != attempt.Ordinal() {
		return pipeline.FlowAttachmentAdvanceResult{}, errors.Join(err, errors.New("shutdown fixture requires the exact admitted attempt"))
	}
	w.barrier.arm(attempt, previous, readiness.PlanHash)
	w.entered <- ctx
	if w.cut == "before_progress" {
		<-w.release
	}
	result, err := w.PipelineCoordinator.AdvanceFlowAttachment(ctx, attempt, previous, at)
	w.observed <- attachmentShutdownProgressObservation{result: result, err: err, rollbackJoinedAtReturn: w.barrier.rollbackJoined()}
	return result, err
}

func (w *attachmentShutdownProgressWorkflow) VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	if !w.verification || !w.enabled.CompareAndSwap(true, false) {
		return w.PipelineCoordinator.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt)
	}
	w.entered <- ctx
	<-w.release
	err := w.PipelineCoordinator.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt)
	w.observed <- attachmentShutdownProgressObservation{err: err}
	return err
}

func (w *attachmentShutdownProgressWorkflow) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	err := w.PipelineCoordinator.AbandonDynamicFlowRuntimeActivationAttempt(ctx, attempt)
	w.abandoned <- err
	return err
}

// Adds the shutdown-cancellation reach to the existing native phase-failure
// invariant: a cancelled progress call is not itself proof of clean settlement.
func proveAttachmentShutdownProgress(t *testing.T, backend, cut string) {
	t.Helper()
	selected, barrier := newAttachmentProgressBarrierStore(t, backend, cut == "after_progress")
	workflow := &attachmentShutdownProgressWorkflow{
		barrier: barrier, cut: cut, verification: cut == "before_verification" || cut == "reused_verification",
		entered: make(chan context.Context, 1), release: make(chan struct{}),
		observed: make(chan attachmentShutdownProgressObservation, 1), abandoned: make(chan error, 1),
	}
	release := sync.OnceFunc(func() { close(workflow.release); barrier.releaseStatement() })
	defer release()
	f := newReceiverConfigActivationFixtureForStore(t, selected, true, map[string]string{
		"schema.yaml":          "stages: []\n",
		"review/schema.yaml":   "instance: request_id\nstages:\n  pending: {}\npins:\n  inputs:\n    - task.started\n",
		"review/entities.yaml": "review_item:\n  request_id: text\n",
		"review/events.yaml":   "task.started:\n", "manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "review/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
	}, nil, ownStoreTestAgentManager, nil, func(options *manager.AgentManagerOptions) {
		workflow.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
		options.WorkflowInstances = workflow
	})
	f.constructKeylessRoot(t)
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
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("exact construction commit: %+v %v", committed, err)
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), plan.Identity.Route())
	if err != nil {
		t.Fatal(err)
	}
	initial, found, err := f.workflows.Load(f.ctx, owner)
	if err != nil || !found {
		t.Fatalf("original construction: found=%t err=%v", found, err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	wantPhase := pipeline.FlowAttachmentPlanned
	if cut == "reused_verification" {
		if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err != nil {
			t.Fatal(err)
		}
		if err := f.manager.WaitForQuiescence(ctx); err != nil {
			t.Fatal(err)
		}
		wantPhase = pipeline.FlowAttachmentReady
	} else if workflow.verification {
		wantPhase = pipeline.FlowAttachmentAgentsRegistered
	}
	workflow.enabled.Store(true)
	finalized := make(chan error, 1)
	go func() { finalized <- f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed) }()
	var progressCtx context.Context
	select {
	case progressCtx = <-workflow.entered:
	case <-ctx.Done():
		t.Fatal("admitted attempt did not reach the real progress owner")
	}
	if !workflow.verification && cut != "before_progress" {
		select {
		case <-barrier.entered:
			barrier.requireExactCoordinates(t)
			if cut == "after_progress" && barrier.updatedRows() != 1 {
				t.Fatal("post-UPDATE cut arrived without a successful one-row native UPDATE")
			}
		case failed := <-workflow.observed:
			t.Fatalf("native progress returned without reaching cut %s: result=%+v err=%v", cut, failed.result, failed.err)
		case <-ctx.Done():
			t.Fatal("exact attachment UPDATE did not enter its real native transaction")
		}
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
	if observed.result.Acknowledged || !errors.Is(observed.err, context.Canceled) {
		t.Fatalf("cancelled progress result: %+v error=%v", observed.result, observed.err)
	}
	if !workflow.verification {
		if before, clean := observed.result.CleanCancellation(observed.err); !clean || before != (cut == "before_progress") {
			t.Fatalf("native cleanup disposition: before=%t clean=%t cut=%s err=%v", before, clean, cut, observed.err)
		}
	}
	if !workflow.verification && cut != "before_progress" {
		if !observed.rollbackJoinedAtReturn {
			t.Fatal("native progress returned before its physical rollback joined")
		}
		if err := barrier.joinRollback(ctx); err != nil {
			t.Fatalf("exact physical rollback did not join: %v", err)
		}
	} else if barrier.matchedStatement() {
		t.Fatal("before-entry control executed an attachment UPDATE")
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
	if err != nil || !found || row.Phase != wantPhase || row.AttemptState != "aborted" {
		t.Fatalf("exact cancelled attachment evidence: %+v found=%t err=%v", row, found, err)
	}
	t.Logf("shutdown progress pin: backend=%s cut=%s acknowledgement=%t phase=%s attempt=%s matched_statement=%t applied_rows=%d physical_rollback_joined_at_return=%t abandonment=acknowledged", backend, cut, observed.result.Acknowledged, row.Phase, row.AttemptState, barrier.matchedStatement(), barrier.updatedRows(), observed.rollbackJoinedAtReturn)
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
	// A fresh Manager/reader and generation must reattach, not reconstruct or
	// republish the immutable construction abandoned by the stopped occurrence.
	restarted := f.restartedReceiverConfigManager(t, nil)
	restartBinding, err := restarted.grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	restartAdmission, err := managedexecution.New(managedexecution.KindNormalRuntime, restartBinding.RuntimeInstanceID, restartBinding.RuntimeGeneration, "", "shutdown-progress-restart", restartBinding.BundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.manager.Run(managedexecution.WithAdmission(restarted.ctx, restartAdmission)); err != nil {
		t.Fatal(err)
	}
	if created, err := restarted.manager.EnsureFlowInstance(restarted.ctx, req); err != nil || created {
		t.Fatalf("fresh Manager did not reuse construction: created=%t err=%v", created, err)
	}
	current, found, err := restarted.workflows.Load(restarted.ctx, owner)
	if err != nil || !found || !reflect.DeepEqual(current, initial) {
		t.Fatalf("reattachment changed immutable construction: before=%+v after=%+v found=%t err=%v", initial, current, found, err)
	}
	restarted.bus.mu.Lock()
	published := len(restarted.bus.published)
	restarted.bus.mu.Unlock()
	if published != 0 {
		t.Fatalf("restart republished constructor events: %d", published)
	}
	if err := restarted.manager.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("fresh readiness work did not join: %v", err)
	}
	recovered, found, err := restarted.store.LoadDynamicFlowRuntimeReadiness(restarted.ctx, owner.RunID, owner.Route)
	if err != nil || !found || recovered.AttemptOrdinal <= row.AttemptOrdinal || recovered.Phase != pipeline.FlowAttachmentReady || recovered.AttemptState != "accepted" {
		t.Fatalf("fresh Manager did not acknowledge successor readiness: before=%+v after=%+v found=%t err=%v", row, recovered, found, err)
	}
	assertConstructorRows(t, restarted, backend, 1)
	if err := restarted.manager.Shutdown(); err != nil {
		t.Fatalf("fresh Manager cleanup: %v", err)
	}
}
