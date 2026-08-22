package serveapp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestBundleDeleteRuntimeQuiescenceRestoresExactRunningContextOnBothStores(t *testing.T) {
	backends := []struct {
		name string
		open func(*testing.T) storeBundle
	}{
		{name: "sqlite", open: func(t *testing.T) storeBundle {
			stores, err := buildStores(context.Background(), storebackend.Selection{
				Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite"),
			}, &config.Config{})
			if err != nil {
				t.Fatalf("build SQLite stores: %v", err)
			}
			t.Cleanup(func() { _ = stores.SQLDB.Close() })
			return stores
		}},
		{name: "postgres", open: func(t *testing.T) storeBundle {
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			selected, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			db := storetest.DatabaseForTest(selected)
			t.Cleanup(func() { _ = db.Close() })
			return selectedPostgresStoreBundle(selected, db, &config.Config{})
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			stores := backend.open(t)
			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
			if _, err := initializeStateStores(context.Background(), stores, bundle); err != nil {
				t.Fatalf("initialize state stores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			fact := mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("d"))
			runtimeInstanceID := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
			processWorkOwner := newSupervisorTestProcessOwner(t)
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
				t.Cleanup(func() { _ = rt.Shutdown() })
				return rt
			}
			predecessor := newRuntime()
			capability, _ := installSelectedStoreTestProcessTopology(t, stores, predecessor, source, fact, runtimeInstanceID)
			t.Cleanup(func() { _ = capability.Release(context.Background()) })
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
			supervisor := &runtimeProjectSupervisor{
				currentSource: source, currentBundle: bundle, currentRT: predecessor,
				currentBundleSourceFact: fact, runtimeContexts: manager,
				executionPosture: executionposture.Live, runtimeInstanceID: runtimeInstanceID, runtimeGeneration: 1,
				processCapability: capability,
			}
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
			if lookup := manager.LookupBundleHashStatus(fact.BundleHash()); lookup.Loaded() {
				t.Fatalf("committed deletion quiescence republished runtime: %#v", lookup)
			}
		})
	}
}

func TestBundleDeleteRuntimeQuiescenceFinalizesRetainedRecoveryBeforeLookup(t *testing.T) {
	t.Run("replacement_rollback", func(t *testing.T) {
		contexts := &runtimepkg.RuntimeContextManager{}
		publication := &recordingRuntimeReplacementPublication{}
		pendingPublication := &pendingRuntimeReplacement{
			publication: publication, freeze: &startupHandoffFreeze{}, retainCurrentProject: true,
		}
		pendingRollback := &pendingRuntimeReplacementRollback{
			phase: runtimeReplacementRollbackPredecessor, predecessorPublication: pendingPublication,
		}
		supervisor := &runtimeProjectSupervisor{
			pendingReplacement: pendingPublication, pendingReplacementRollback: pendingRollback,
		}
		quiescence, err := (bundleDeleteRuntimeQuiescer{contexts: contexts, supervisor: supervisor}).QuiesceBundleRuntime(context.Background(), "not-loaded")
		if err != nil {
			t.Fatalf("finalize replacement rollback before bundle lookup: %v", err)
		}
		if publication.publishAttempts.Load() != 1 {
			t.Fatalf("retained replacement predecessor publication attempts = %d, want 1", publication.publishAttempts.Load())
		}
		if supervisor.pendingReplacementRollback != nil || supervisor.pendingReplacement != nil {
			t.Fatalf("bundle lookup preceded retained replacement rollback finalization: rollback:%p publication:%p", supervisor.pendingReplacementRollback, supervisor.pendingReplacement)
		}
		if supervisor.operationMu.TryLock() {
			supervisor.operationMu.Unlock()
			t.Fatal("inert bundle delete token did not retain supervisor operation ownership")
		}
		quiescence.Commit()
		if !supervisor.operationMu.TryLock() {
			t.Fatal("inert bundle delete commit did not release supervisor operation ownership")
		}
		supervisor.operationMu.Unlock()
	})

	t.Run("source_set_rollback", func(t *testing.T) {
		contexts := &runtimepkg.RuntimeContextManager{}
		publication := &recordingRuntimeReplacementPublication{}
		pendingPublication := &pendingRuntimeReplacement{
			publication: publication, freeze: &startupHandoffFreeze{}, retainCurrentProject: true,
		}
		pendingRollback := &pendingRuntimeSourceSetRollback{
			sourceSetRestored: true, survivorsRecovered: true, predecessorPublication: pendingPublication,
		}
		supervisor := &runtimeProjectSupervisor{
			pendingReplacement: pendingPublication, pendingSourceSetRollback: pendingRollback,
		}
		quiescence, err := (bundleDeleteRuntimeQuiescer{contexts: contexts, supervisor: supervisor}).QuiesceBundleRuntime(context.Background(), "not-loaded")
		if err != nil {
			t.Fatalf("finalize source-set rollback before bundle lookup: %v", err)
		}
		if publication.publishAttempts.Load() != 1 {
			t.Fatalf("retained predecessor publication attempts = %d, want 1", publication.publishAttempts.Load())
		}
		if supervisor.pendingSourceSetRollback != nil || supervisor.pendingReplacement != nil {
			t.Fatalf("bundle lookup preceded retained source-set recovery: rollback:%p publication:%p", supervisor.pendingSourceSetRollback, supervisor.pendingReplacement)
		}
		if err := quiescence.Restore(context.Background()); err != nil {
			t.Fatalf("release inert bundle delete operation: %v", err)
		}
		if !supervisor.operationMu.TryLock() {
			t.Fatal("inert bundle delete restore did not release supervisor operation ownership")
		}
		supervisor.operationMu.Unlock()
	})
}

func TestRuntimeReplacementPublicationCanRetainPrimaryProject(t *testing.T) {
	primary := &runtimepkg.Runtime{}
	restoredSecondary := &runtimepkg.Runtime{}
	publication := &recordingRuntimeReplacementPublication{}
	supervisor := &runtimeProjectSupervisor{
		currentRoot: "primary-root",
		currentRT:   primary,
		pendingReplacement: &pendingRuntimeReplacement{
			publication:          publication,
			root:                 "secondary-root",
			runtime:              restoredSecondary,
			retainCurrentProject: true,
		},
	}
	before := supervisor.CurrentRuntime()
	if err := supervisor.completePendingReplacement(); err != nil {
		t.Fatalf("complete secondary runtime publication: %v", err)
	}
	if publication.publishAttempts.Load() != 1 {
		t.Fatalf("secondary runtime publication attempts = %d, want 1", publication.publishAttempts.Load())
	}
	if supervisor.CurrentRuntime() != before || supervisor.CurrentRuntime() != primary {
		t.Fatalf("secondary runtime publication replaced primary runtime: got %p want %p", supervisor.CurrentRuntime(), primary)
	}
	if supervisor.currentRoot != "primary-root" {
		t.Fatalf("secondary runtime publication replaced primary root = %q", supervisor.currentRoot)
	}
	if supervisor.pendingReplacement != nil {
		t.Fatal("completed secondary runtime publication remained pending")
	}
}
