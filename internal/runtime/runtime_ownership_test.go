package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type fakeRuntimeStartupOwnershipStore struct {
	acquire func(context.Context, string) (runtimestartupownership.Lease, error)
}

func (f fakeRuntimeStartupOwnershipStore) AcquireRuntimeStartupOwnership(ctx context.Context, ownerID string) (runtimestartupownership.Lease, error) {
	if f.acquire == nil {
		return nil, nil
	}
	return f.acquire(ctx, ownerID)
}

type fakeRuntimeStartupOwnershipLease struct {
	released atomic.Int32
}

func (f *fakeRuntimeStartupOwnershipLease) Release(context.Context) error {
	f.released.Add(1)
	return nil
}

func TestRuntimeStart_FailsWhenSharedStoreOwnershipAlreadyHeld(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)

	rt1, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{
		StartupOwnership: fakeRuntimeStartupOwnershipStore{
			acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
				return &fakeRuntimeStartupOwnershipLease{}, nil
			},
		},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime(rt1): %v", err)
	}
	if err := rt1.Start(context.Background()); err != nil {
		t.Fatalf("Start(rt1): %v", err)
	}
	t.Cleanup(func() { _ = rt1.Shutdown() })

	rt2, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{
		StartupOwnership: fakeRuntimeStartupOwnershipStore{
			acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
				return nil, fmt.Errorf("shared runtime store already owned by another runtime instance")
			},
		},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime(rt2): %v", err)
	}
	err = rt2.Start(context.Background())
	if err == nil {
		t.Fatal("expected second runtime start to fail when shared-store ownership is already held")
	}
	if !strings.Contains(err.Error(), "shared runtime store already owned by another runtime instance") {
		t.Fatalf("Start(rt2) error = %v, want explicit shared-store ownership denial", err)
	}
}

func TestRuntimeShutdown_ReleasesSharedStoreOwnership(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	lease := &fakeRuntimeStartupOwnershipLease{}

	rt1, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{
		StartupOwnership: fakeRuntimeStartupOwnershipStore{
			acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
				return lease, nil
			},
		},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime(rt1): %v", err)
	}
	if err := rt1.Start(context.Background()); err != nil {
		t.Fatalf("Start(rt1): %v", err)
	}
	if err := rt1.Shutdown(); err != nil {
		t.Fatalf("Shutdown(rt1): %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("startup ownership lease release count = %d, want 1", got)
	}

	rt2, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{
		StartupOwnership: fakeRuntimeStartupOwnershipStore{
			acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
				return &fakeRuntimeStartupOwnershipLease{}, nil
			},
		},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime(rt2): %v", err)
	}
	if err := rt2.Start(context.Background()); err != nil {
		t.Fatalf("Start(rt2) after shutdown release: %v", err)
	}
	if err := rt2.Shutdown(); err != nil {
		t.Fatalf("Shutdown(rt2): %v", err)
	}
}

func TestRuntimePreparedStartupOwnershipIsConsumedWithoutReacquire(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	lease := &fakeRuntimeStartupOwnershipLease{}
	var acquires atomic.Int32
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{
		StartupOwnership: fakeRuntimeStartupOwnershipStore{
			acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
				acquires.Add(1)
				return lease, nil
			},
		},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.PrepareInitialStartupOwnership(context.Background()); err != nil {
		t.Fatalf("PrepareInitialStartupOwnership: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := acquires.Load(); got != 1 {
		t.Fatalf("startup ownership acquires = %d, want one prepared acquire", got)
	}
	if err := rt.ReleasePreparedStartupOwnership(context.Background()); err != nil {
		t.Fatalf("ReleasePreparedStartupOwnership after Start: %v", err)
	}
	if got := lease.released.Load(); got != 0 {
		t.Fatalf("consumed prepared lease released before Shutdown %d time(s)", got)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("startup ownership lease release count = %d, want one", got)
	}
}

func TestRuntimePreparedStartupOwnershipCanBeReleasedBeforeStart(t *testing.T) {
	lease := &fakeRuntimeStartupOwnershipLease{}
	rt := &Runtime{
		Stores: Stores{StartupOwnership: fakeRuntimeStartupOwnershipStore{
			acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
				return lease, nil
			},
		}},
		ownerID: "runtime-owner",
	}
	if err := rt.PrepareInitialStartupOwnership(context.Background()); err != nil {
		t.Fatalf("PrepareInitialStartupOwnership: %v", err)
	}
	if err := rt.ReleasePreparedStartupOwnership(context.Background()); err != nil {
		t.Fatalf("ReleasePreparedStartupOwnership: %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("startup ownership lease release count = %d, want one", got)
	}
	if err := rt.ReleasePreparedStartupOwnership(context.Background()); err != nil {
		t.Fatalf("second ReleasePreparedStartupOwnership: %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("second release changed lease count to %d", got)
	}
}

func TestRuntimeCleanupStartFailure_ReleasesSharedStoreOwnership(t *testing.T) {
	lease := &fakeRuntimeStartupOwnershipLease{}
	ctx, cancel := context.WithCancel(context.Background())
	rt := &Runtime{
		startCtx:       ctx,
		cancelStart:    cancel,
		ownershipLease: lease,
	}

	rt.cleanupStartFailure()

	if got := lease.released.Load(); got != 1 {
		t.Fatalf("startup ownership lease release count = %d, want 1", got)
	}
	if rt.cancelStart != nil {
		t.Fatal("cancelStart was not cleared")
	}
	if rt.startCtx != nil {
		t.Fatal("startCtx was not cleared")
	}
	if rt.ownershipLease != nil {
		t.Fatal("ownershipLease was not cleared")
	}
}

func TestRuntimeReplacementBorrowsAndCommitsStartupOwnershipWithoutReacquire(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	lease := &fakeRuntimeStartupOwnershipLease{}
	var acquires atomic.Int32
	ownership := fakeRuntimeStartupOwnershipStore{acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
		acquires.Add(1)
		return lease, nil
	}}
	newRuntime := func() *Runtime {
		rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{StartupOwnership: ownership}, Options: RuntimeOptions{
			SelfCheck: false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, DisablePersistentStartupRecovery: true,
		}})
		if err != nil {
			t.Fatalf("NewRuntime: %v", err)
		}
		return rt
	}
	predecessor := newRuntime()
	if err := predecessor.Start(context.Background()); err != nil {
		t.Fatalf("start predecessor: %v", err)
	}
	candidate := newRuntime()
	if _, err := candidate.PrepareStartupOwnershipHandoff(predecessor); err == nil || !strings.Contains(err.Error(), "must quiesce") {
		t.Fatalf("handoff before predecessor quiescence error = %v", err)
	}
	if err := predecessor.QuiesceForReplacement(DefaultShutdownOptions()); err != nil {
		t.Fatalf("quiesce predecessor: %v", err)
	}
	handoff, err := candidate.PrepareStartupOwnershipHandoff(predecessor)
	if err != nil {
		t.Fatalf("PrepareStartupOwnershipHandoff: %v", err)
	}
	if err := candidate.Start(context.Background()); err != nil {
		t.Fatalf("start candidate under handoff: %v", err)
	}
	if got := acquires.Load(); got != 1 {
		t.Fatalf("startup ownership acquires = %d, want one predecessor acquire", got)
	}
	if err := handoff.Commit(); err != nil {
		t.Fatalf("commit handoff: %v", err)
	}
	if err := predecessor.Shutdown(); err == nil || !strings.Contains(err.Error(), "handoff is pending") {
		t.Fatalf("predecessor shutdown during visibility commit error = %v", err)
	}
	handoff.Finalize()
	if err := predecessor.Shutdown(); err != nil {
		t.Fatalf("shutdown predecessor: %v", err)
	}
	if got := lease.released.Load(); got != 0 {
		t.Fatalf("predecessor released transferred lease %d time(s)", got)
	}
	if err := candidate.Shutdown(); err != nil {
		t.Fatalf("shutdown candidate: %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("candidate lease releases = %d, want one", got)
	}
}

func TestRuntimeReplacementStartupRollbackPreservesPredecessorOwnership(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	lease := &fakeRuntimeStartupOwnershipLease{}
	ownership := fakeRuntimeStartupOwnershipStore{acquire: func(context.Context, string) (runtimestartupownership.Lease, error) { return lease, nil }}
	newRuntime := func() *Runtime {
		rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{StartupOwnership: ownership}, Options: RuntimeOptions{
			SelfCheck: false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, DisablePersistentStartupRecovery: true,
		}})
		if err != nil {
			t.Fatalf("NewRuntime: %v", err)
		}
		return rt
	}
	predecessor := newRuntime()
	if err := predecessor.Start(context.Background()); err != nil {
		t.Fatalf("start predecessor: %v", err)
	}
	candidate := newRuntime()
	if err := predecessor.QuiesceForReplacement(DefaultShutdownOptions()); err != nil {
		t.Fatalf("quiesce predecessor: %v", err)
	}
	handoff, err := candidate.PrepareStartupOwnershipHandoff(predecessor)
	if err != nil {
		t.Fatalf("PrepareStartupOwnershipHandoff: %v", err)
	}
	if err := candidate.Start(context.Background()); err != nil {
		t.Fatalf("start candidate: %v", err)
	}
	if err := candidate.Shutdown(); err != nil {
		t.Fatalf("shutdown rejected candidate: %v", err)
	}
	if err := handoff.Rollback(); err != nil {
		t.Fatalf("rollback startup ownership: %v", err)
	}
	if got := lease.released.Load(); got != 0 {
		t.Fatalf("rejected candidate released predecessor lease %d time(s)", got)
	}
	if err := predecessor.Shutdown(); err != nil {
		t.Fatalf("shutdown predecessor: %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("predecessor lease releases = %d, want one", got)
	}
}

func TestRuntimeReplacementPostCommitRollbackRestoresPredecessorOwnership(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	lease := &fakeRuntimeStartupOwnershipLease{}
	ownership := fakeRuntimeStartupOwnershipStore{acquire: func(context.Context, string) (runtimestartupownership.Lease, error) { return lease, nil }}
	newRuntime := func() *Runtime {
		rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{StartupOwnership: ownership}, Options: RuntimeOptions{
			SelfCheck: false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, DisablePersistentStartupRecovery: true,
		}})
		if err != nil {
			t.Fatalf("NewRuntime: %v", err)
		}
		return rt
	}
	predecessor := newRuntime()
	if err := predecessor.Start(context.Background()); err != nil {
		t.Fatalf("start predecessor: %v", err)
	}
	candidate := newRuntime()
	if err := predecessor.QuiesceForReplacement(DefaultShutdownOptions()); err != nil {
		t.Fatalf("quiesce predecessor: %v", err)
	}
	handoff, err := candidate.PrepareStartupOwnershipHandoff(predecessor)
	if err != nil {
		t.Fatalf("PrepareStartupOwnershipHandoff: %v", err)
	}
	if err := candidate.Start(context.Background()); err != nil {
		t.Fatalf("start candidate: %v", err)
	}
	if err := handoff.Commit(); err != nil {
		t.Fatalf("commit handoff: %v", err)
	}
	if err := candidate.QuiesceForReplacement(DefaultShutdownOptions()); err != nil {
		t.Fatalf("quiesce committed candidate: %v", err)
	}
	if err := handoff.Rollback(); err != nil {
		t.Fatalf("rollback committed ownership: %v", err)
	}
	if got := lease.released.Load(); got != 0 {
		t.Fatalf("rolled-back candidate released predecessor lease %d time(s)", got)
	}
	if err := predecessor.Shutdown(); err != nil {
		t.Fatalf("shutdown predecessor: %v", err)
	}
	if got := lease.released.Load(); got != 1 {
		t.Fatalf("restored predecessor lease releases = %d, want one", got)
	}
}

func loadRuntimeOwnershipWorkflowModule(t *testing.T) semanticOnlyWorkflowRuntime {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	return semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)}
}
