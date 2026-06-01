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
