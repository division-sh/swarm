package serveapp

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestBundleDeleteRuntimeQuiescenceRestoresExactRunningContextOnBothStores(t *testing.T) {
	backends := []struct {
		name string
		open func(*testing.T) *selectedStoreOwner
	}{
		{name: "sqlite", open: func(t *testing.T) *selectedStoreOwner {
			stores, err := buildStores(context.Background(), storebackend.Selection{
				Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite"),
			}, &config.Config{})
			if err != nil {
				t.Fatalf("build SQLite stores: %v", err)
			}
			t.Cleanup(func() { _ = stores.CloseUnactivated() })
			return stores
		}},
		{name: "postgres", open: func(t *testing.T) *selectedStoreOwner {
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			selected, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			db := storetest.DatabaseForTest(selected)
			t.Cleanup(func() { _ = db.Close() })
			return openSelectedPostgresOwner(t, dsn, db, &config.Config{})
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			stores := backend.open(t)
			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
			if _, err := initializeStateStores(context.Background(), stores.Schema(), bundle); err != nil {
				t.Fatalf("initialize state stores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			fact := mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("d"))
			runtimeInstanceID := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
			processWorkOwner := worklifetime.NewProcess()
			var capability runtimestartupownership.ProcessCapability
			var runtimes []*runtimepkg.Runtime
			t.Cleanup(func() {
				shutdownFailed := false
				for i := len(runtimes) - 1; i >= 0; i-- {
					if err := runtimes[i].ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second}); err != nil {
						t.Errorf("shutdown bundle-delete runtime: %v", err)
						shutdownFailed = true
					}
				}
				if shutdownFailed {
					return
				}
				if err := closeSelectedStoreTestProcess(processWorkOwner, capability); err != nil {
					t.Errorf("close bundle-delete selected-store generation: %v", err)
				}
			})
			providerCatalog := testProviderTriggerCatalog(t)
			newRuntime := func() *runtimepkg.Runtime {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, stores, &config.Config{}, runtimepkg.RuntimeOptions{
					SelfCheck: false, WorkflowModule: stubWorkflowModule{source: source},
					LLMRuntime: servedNoopLLMRuntime{}, DisablePersistentStartupRecovery: true,
					ProviderTriggerCatalog: providerCatalog, ProcessWorkOwner: processWorkOwner,
					BundleSourceFact: fact, RuntimeInstanceID: runtimeInstanceID,
				}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				runtimes = append(runtimes, rt)
				return rt
			}
			predecessor := newRuntime()
			capability, _ = installSelectedStoreTestProcessTopology(t, stores, predecessor, source, fact, runtimeInstanceID)
			if err := predecessor.Start(context.Background()); err != nil {
				t.Fatalf("start predecessor: %v", err)
			}
			manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
				BundleSourceFact: fact, Source: source, Runtime: predecessor, WorkOwner: predecessor.WorkOccurrence(),
			}))
			if err != nil {
				t.Fatalf("NewRuntimeContextManager: %v", err)
			}
			restored := newRuntime()
			supervisor := newProcessLifecycleSupervisor(projectServeRuntimePersistence(stores), nil, predecessor)
			supervisor.SetProcessCapability(capability)
			supervisor.SetRuntimeContextManager(manager, fact)
			supervisor.shutdownOptions = runtimepkg.ShutdownOptions{Grace: 5 * time.Second}
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return restored, restored.WorkOccurrence(), nil
			}
			owner := bundleDeleteRuntimeQuiescer{contexts: manager, supervisor: supervisor}
			quiescence, err := owner.QuiesceBundleRuntime(context.Background(), fact.BundleHash())
			if err != nil {
				t.Fatalf("quiesce bundle runtime: %v", err)
			}
			if lookup := manager.LookupBundleHashStatus(fact.BundleHash()); lookup.Loaded() {
				t.Fatalf("runtime remained loaded during quiescence: %#v", lookup)
			}
			if supervisor.operationMu.TryLock() {
				supervisor.operationMu.Unlock()
				t.Fatal("bundle delete released supervisor operation ownership before restoration")
			}
			if err := quiescence.Restore(context.Background()); err != nil {
				t.Fatalf("restore bundle runtime: %v", err)
			}
			if !supervisor.operationMu.TryLock() {
				t.Fatal("bundle delete retained supervisor operation ownership after restoration")
			}
			supervisor.operationMu.Unlock()
			use, _, err := manager.AcquireBundleHash(context.Background(), fact.BundleHash())
			if err != nil || use == nil || use.Runtime() != restored {
				t.Fatalf("restored runtime authority = use:%#v err:%v", use, err)
			}
			if err := use.Done(); err != nil {
				t.Fatalf("release restored runtime authority: %v", err)
			}
			if supervisor.CurrentRuntime() != restored || !restored.Manager.IsRunning() {
				t.Fatalf("restored process runtime = current:%p running:%v", supervisor.CurrentRuntime(), restored.Manager.IsRunning())
			}
			terminal, err := owner.QuiesceBundleRuntime(context.Background(), fact.BundleHash())
			if err != nil {
				t.Fatalf("quiesce restored bundle runtime: %v", err)
			}
			terminal.Commit()
			if lookup := manager.LookupBundleHashStatus(fact.BundleHash()); lookup.Found || lookup.Loaded() {
				t.Fatalf("committed deletion retained runtime context: %#v", lookup)
			}
			if supervisor.CurrentRuntime() != nil {
				t.Fatalf("committed deletion retained process runtime %p", supervisor.CurrentRuntime())
			}
		})
	}
}
