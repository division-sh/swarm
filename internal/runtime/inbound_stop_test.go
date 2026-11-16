package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type heldInboundLandingRuns struct {
	runlifecycle.LandingRunReader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *heldInboundLandingRuns) LoadLandingRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LandingRun, bool, error) {
	landing, found, err := r.LandingRunReader.LoadLandingRun(ctx, scope)
	r.once.Do(func() { close(r.entered) })
	select {
	case <-ctx.Done():
		return runlifecycle.LandingRun{}, false, ctx.Err()
	case <-r.release:
		return landing, found, err
	}
}

func TestStoredInboundSelectionRechecksStopAfterNativeLandingRead(t *testing.T) {
	catalog, source := runtimeAdmissionTestCatalog(t, "stop-read", "inbound")
	definition := runtimeAdmissionTestContext(t, source, catalog, eventtest.UUID(t.Name()))
	manager, err := newTestRuntimeContextManager(t, nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	reader := &heldInboundLandingRuns{LandingRunReader: manager.landingRuns, entered: make(chan struct{}), release: make(chan struct{})}
	manager.landingRuns = reader
	requestCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	target := definition.StandingTargets[0]
	nextRunID := eventtest.UUID(t.Name() + "-next-landing")
	selected := make(chan struct{})
	result := make(chan *InboundPublicationUse, 1)
	publicationErr := make(chan error, 1)
	go func() {
		use, _, err := manager.AcquireInboundPublication(requestCtx, target.Alias, target.Provider)
		publicationErr <- err
		result <- use
	}()
	defer func() {
		if t.Failed() {
			cancel()
		}
		close(reader.release)
		if err := <-publicationErr; err != nil {
			t.Errorf("complete held publication: %v", err)
		}
		use := <-result
		if use == nil || use.executed != nil {
			t.Errorf("held read did not switch to publication-only custody: %v", use)
		}
		if use != nil {
			if use.context.StandingTargets[0].RunID != nextRunID {
				t.Errorf("held read minted stale landing custody: got %s want %s", use.context.StandingTargets[0].RunID, nextRunID)
			}
			if manager.liveSelectionMu.TryLock() {
				manager.liveSelectionMu.Unlock()
				t.Error("admitted publication no longer excludes live selection")
			}
			if err := use.Done(); err != nil {
				t.Error(err)
			}
		}
		<-selected
	}()
	<-reader.entered
	go func() {
		release := manager.BeginLiveRunSelection()
		landing := reader.LandingRunReader.(runtimeTestLandingRuns)[target.FlowPath]
		landing.RunID = nextRunID
		reader.LandingRunReader.(runtimeTestLandingRuns)[target.FlowPath] = landing
		manager.PrepareInboundStop()
		release()
		close(selected)
	}()
	select {
	case <-selected:
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive live selection waited for an unadmitted native landing read")
	}
	if err := manager.SealInboundExecution(t.Context()); err != nil {
		t.Fatal(err)
	}
	definition.Runtime.CloseAdmission()
	if err := manager.OpenStoredInbound(); err != nil {
		t.Fatal(err)
	}
}

func TestStoredInboundPreparationWaitCancelsBeforeRecoveryJoin(t *testing.T) {
	catalog, source := runtimeAdmissionTestCatalog(t, "stop-wait", "inbound")
	definition := runtimeAdmissionTestContext(t, source, catalog, eventtest.UUID(t.Name()))
	manager, err := newTestRuntimeContextManager(t, nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	originalBus := definition.Runtime.Bus
	definition.Runtime.Bus = &runtimebus.EventBus{}
	t.Cleanup(func() { definition.Runtime.Bus = originalBus })
	selected, err := definition.Runtime.Bus.BeginPipelineParentTransition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	barrier := &startupClaimBarrierContext{Context: t.Context(), entered: make(chan struct{}), release: make(chan struct{})}
	sealed := make(chan error, 1)
	go func() { sealed <- manager.SealInboundExecution(barrier) }()
	defer func() {
		close(barrier.release)
		selected.Done()
		if err := <-sealed; err != nil {
			t.Errorf("join recovery seal: %v", err)
		}
	}()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("seal did not reach the existing recovery owner")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	publication := make(chan error, 1)
	go func() {
		target := definition.StandingTargets[0]
		use, _, err := manager.AcquireInboundPublication(canceled, target.Alias, target.Provider)
		if use != nil {
			err = errors.Join(err, use.Done(), errors.New("canceled preparation minted publication custody"))
		}
		publication <- err
	}()
	select {
	case err := <-publication:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled preparation result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publication cancellation waited for the recovery join")
	}
	select {
	case err := <-sealed:
		sealed <- err
		t.Fatal("seal completed before the held recovery owner joined")
	default:
	}
}

func TestStoredInboundCustodyDoesNotMintExecutionAndJoinsEnteredRequests(t *testing.T) {
	catalog, source := runtimeAdmissionTestCatalog(t, "stop", "inbound")
	definition := runtimeAdmissionTestContext(t, source, catalog, eventtest.UUID(t.Name()))
	manager, err := newTestRuntimeContextManager(t, nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	target := definition.StandingTargets[0]
	requestCtx, finishRequest, admitted := manager.BeginInboundRequest(t.Context())
	if !admitted || !manager.InboundClaimAdmissionOpen() {
		t.Fatal("ordinary request admission refused")
	}
	t.Cleanup(finishRequest)
	ordinary, _, err := manager.AcquireInboundPublication(requestCtx, target.Alias, target.Provider)
	if err != nil || ordinary == nil || ordinary.executed == nil {
		t.Fatalf("ordinary execution custody: use=%v err=%v", ordinary, err)
	}
	if err := ordinary.Done(); err != nil {
		t.Fatal(err)
	}
	if err := manager.SealInboundExecution(t.Context()); err != nil {
		t.Fatal(err)
	}
	if manager.InboundClaimAdmissionOpen() {
		t.Fatal("claim completion remained admitted during stop")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if use, _, err := manager.AcquireInboundPublication(canceled, target.Alias, target.Provider); use != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("preparation barrier bypass: use=%v err=%v", use, err)
	}
	if err := manager.OpenStoredInbound(); err != nil {
		t.Fatal(err)
	}
	definition.Runtime.CloseAdmission()
	stored, lookup, err := manager.AcquireInboundPublication(requestCtx, target.Alias, target.Provider)
	if err != nil || stored == nil || stored.executed != nil || !lookup.Loaded() || lookup.Target.RunID != target.RunID {
		t.Fatalf("publication-only custody: use=%v lookup=%+v err=%v", stored, lookup, err)
	}
	if definition.WorkOwner.ActiveCount() != 0 {
		t.Fatal("store-only custody minted an execution lease")
	}
	if _, rooted, err := runtimecorrelation.RootAdmissionFromContext(stored.ctx); !rooted || err != nil {
		t.Fatal("publication custody lost its original rooted source")
	}
	if err := stored.Done(); err != nil {
		t.Fatal(err)
	}
	foreignFact, err := runtimecorrelation.NewSourceArtifactFact(runtimeContextTestHashC)
	if err != nil {
		t.Fatal(err)
	}
	foreign := runtimecorrelation.WithSourceArtifactFact(requestCtx, foreignFact)
	if use, _, err := manager.AcquireInboundPublication(foreign, target.Alias, target.Provider); use != nil || err == nil {
		t.Fatal("store-only admission accepted another source")
	}
	if use, _, err := manager.AcquireIngress(requestCtx, target.Alias, target.Provider); use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("publication phase reopened execution: %v", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- manager.CloseInboundRequests(t.Context()) }()
	finishRequest()
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	if _, release, admitted := manager.BeginInboundRequest(t.Context()); admitted || release != nil {
		t.Fatal("request admitted after listener cutoff")
	}
	if err := manager.OpenStoredInbound(); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("closed intake reopened: %v", err)
	}
}
