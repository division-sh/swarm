package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/config"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
)

func TestRuntimeStart_FailsWhenSharedStoreOwnershipAlreadyHeld(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	module := loadRuntimeOwnershipWorkflowModule(t)

	rt1, err := NewRuntime(context.Background(), &config.Config{}, Stores{SQLDB: db}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime(rt1): %v", err)
	}
	if err := rt1.Start(context.Background()); err != nil {
		t.Fatalf("Start(rt1): %v", err)
	}
	t.Cleanup(func() { _ = rt1.Shutdown() })

	rt2, err := NewRuntime(context.Background(), &config.Config{}, Stores{SQLDB: db}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
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
	_, db, _ := testutil.StartPostgres(t)
	module := loadRuntimeOwnershipWorkflowModule(t)

	rt1, err := NewRuntime(context.Background(), &config.Config{}, Stores{SQLDB: db}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime(rt1): %v", err)
	}
	if err := rt1.Start(context.Background()); err != nil {
		t.Fatalf("Start(rt1): %v", err)
	}
	if err := rt1.Shutdown(); err != nil {
		t.Fatalf("Shutdown(rt1): %v", err)
	}

	rt2, err := NewRuntime(context.Background(), &config.Config{}, Stores{SQLDB: db}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
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

func loadRuntimeOwnershipWorkflowModule(t *testing.T) semanticOnlyWorkflowRuntime {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	return semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)}
}
