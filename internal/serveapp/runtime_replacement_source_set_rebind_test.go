package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRuntimeProjectSupervisorReplacementRefreshesSurvivingGenerationsOnBothStores(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) storeBundle
	}
	backends := []backend{
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
			ctx := context.Background()
			stores := backend.open(t)
			cfg := &config.Config{}
			processWorkOwner := newSupervisorTestProcessOwner(t)
			runtimeInstanceID := uuid.NewString()
			providerCatalog := testProviderTriggerCatalog(t)

			type runtimeFixture struct {
				root   string
				hash   string
				bundle *runtimecontracts.WorkflowContractBundle
				source semanticview.Source
				rt     *runtimepkg.Runtime
			}
			newRuntime := func(root, hash string) runtimeFixture {
				bundle := loadWorkflowValidationBundleAt(t, root)
				if _, err := initializeStateStores(ctx, stores, bundle); err != nil {
					t.Fatalf("initialize state stores for %s: %v", hash, err)
				}
				source := semanticview.Wrap(bundle)
				fact := mustServeTestEphemeralBundleSourceFact(hash)
				rt, err := runtimepkg.NewRuntime(ctx, runtimeDepsForServeTest(stores, cfg, runtimepkg.RuntimeOptions{
					SelfCheck: false, WorkflowModule: stubWorkflowModule{source: source},
					LLMRuntime: runtimellm.NoopRuntime{}, DisablePersistentStartupRecovery: true,
					ProviderTriggerCatalog: providerCatalog, ProcessWorkOwner: processWorkOwner,
					BundleSourceFact: fact, RuntimeInstanceID: runtimeInstanceID,
				}))
				if err != nil {
					t.Fatalf("NewRuntime(%s): %v", hash, err)
				}
				t.Cleanup(func() {
					_ = rt.ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second})
				})
				return runtimeFixture{root: root, hash: hash, bundle: bundle, source: source, rt: rt}
			}

			predecessor := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-predecessor", "alpha-worker"), runtimeContextTestHash("a"))
			survivor := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-survivor", "beta-worker"), runtimeContextTestHash("b"))
			candidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-candidate", "gamma-worker"), runtimeContextTestHash("c"))
			restoredCandidate := newRuntime(candidate.root, candidate.hash)
			failedCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-failed-candidate", "delta-worker"), runtimeContextTestHash("d"))
			blockedCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-blocked-candidate", "epsilon-worker"), runtimeContextTestHash("e"))

			coordinates := []runtimeagenttopology.SourceCoordinate{
				{BundleHash: predecessor.hash, BundleSource: "ephemeral"},
				{BundleHash: survivor.hash, BundleSource: "ephemeral"},
			}
			var desired []runtimeagenttopology.DesiredAgent
			for i, fixture := range []runtimeFixture{predecessor, survivor} {
				compiled, err := fixture.rt.Manager.CompileStaticTopologyDesiredAgents(fixture.source, coordinates[i])
				if err != nil {
					t.Fatalf("compile initial desired agents for %s: %v", fixture.hash, err)
				}
				desired = append(desired, compiled...)
			}
			initialPlan, err := runtimeagenttopology.NewSourceSetPlan(coordinates, desired)
			if err != nil {
				t.Fatalf("construct initial complete source set: %v", err)
			}
			capability, err := stores.StartupOwnership.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
				OwnerID: "replacement-survivor-test", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
			})
			if err != nil {
				t.Fatalf("acquire process capability: %v", err)
			}
			if err := installServeSourceSet(ctx, capability, initialPlan); err != nil {
				t.Fatalf("install initial complete source set: %v", err)
			}
			installSelectedStoreTestGeneration(t, capability, predecessor.rt, initialPlan, 1)
			installSelectedStoreTestGeneration(t, capability, survivor.rt, initialPlan, 1)
			if err := predecessor.rt.Start(ctx); err != nil {
				t.Fatalf("start predecessor: %v", err)
			}
			if err := survivor.rt.Start(ctx); err != nil {
				t.Fatalf("start survivor: %v", err)
			}

			contexts, err := runtimepkg.NewRuntimeContextManager(nil,
				runtimepkg.BundleContext{
					BundleSourceFact: predecessor.rt.Options.BundleSourceFact, Source: predecessor.source,
					Runtime: predecessor.rt, WorkOwner: predecessor.rt.WorkOccurrence(),
				},
				runtimepkg.BundleContext{
					BundleSourceFact: survivor.rt.Options.BundleSourceFact, Source: survivor.source,
					Runtime: survivor.rt, WorkOwner: survivor.rt.WorkOccurrence(),
				},
			)
			if err != nil {
				t.Fatalf("NewRuntimeContextManager: %v", err)
			}
			t.Cleanup(func() {
				for _, result := range contexts.DeactivateAll(runtimepkg.RuntimeContextCauseUnloaded) {
					if result.ShutdownErr != nil {
						t.Errorf("deactivate runtime context %s: %v", result.BundleHash, result.ShutdownErr)
					}
				}
				if err := capability.Release(context.Background()); err != nil {
					t.Errorf("release process capability: %v", err)
				}
			})

			var ready atomic.Bool
			ready.Store(true)
			supervisor := &runtimeProjectSupervisor{
				ready: &ready, currentRoot: predecessor.root, currentSource: predecessor.source,
				currentBundle: predecessor.bundle, currentRT: predecessor.rt,
				currentBundleSourceFact: predecessor.rt.Options.BundleSourceFact,
				runtimeContexts:         contexts, executionPosture: executionposture.Live,
				runtimeInstanceID: runtimeInstanceID, runtimeGeneration: 1,
				replacementShutdown: runtimepkg.ShutdownOptions{Grace: 5 * time.Second},
			}
			supervisor.SetProcessCapability(capability)
			status, err := supervisor.replaceCurrentRuntimeWithSource(
				ctx, candidate.root, candidate.source, candidate.bundle, candidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: candidate.hash}, candidate.rt, candidate.rt.WorkOccurrence(),
			)
			if err != nil {
				t.Fatalf("replace predecessor while survivor remains loaded: %v", err)
			}
			if !status.Loaded || !ready.Load() || supervisor.CurrentRuntime() != candidate.rt {
				t.Fatalf("replacement publication = status:%#v ready:%v runtime:%p, want candidate %p", status, ready.Load(), supervisor.CurrentRuntime(), candidate.rt)
			}

			currentPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists {
				t.Fatalf("load replacement source set: exists=%v err=%v", exists, err)
			}
			evidence, err := survivor.rt.CurrentStartupGrantEvidence()
			if err != nil {
				t.Fatalf("load survivor generation evidence: %v", err)
			}
			if evidence.SourceSetRevision != currentPlan.Revision || evidence.RuntimeGeneration != 2 {
				t.Fatalf("survivor generation = %#v, want generation 2 at source set %s", evidence, currentPlan.Revision)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor context after replacement = %#v, want loaded", lookup)
			}

			agents, err := stores.ManagerStore.LoadAgents(ctx)
			if err != nil {
				t.Fatalf("read persisted replacement topology: %v", err)
			}
			var persistedSurvivor bool
			for _, agent := range agents {
				if agent.Config.ID != "beta-worker" {
					continue
				}
				persistedSurvivor = true
				if agent.LifecyclePhase != "running" || agent.LifecycleGeneration != 2 ||
					agent.Topology.Authority.Static == nil || agent.Topology.Authority.Static.SourceSetRevision != currentPlan.Revision {
					t.Fatalf("persisted survivor lifecycle = %#v, want running generation 2 at source set %s", agent, currentPlan.Revision)
				}
			}
			if !persistedSurvivor {
				t.Fatal("persisted survivor lifecycle row was not found")
			}

			var publicationHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				if publicationHooks.Add(1) == 1 {
					return errors.New("public ingress reconciliation failed")
				}
				return nil
			})
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return restoredCandidate.rt, restoredCandidate.rt.WorkOccurrence(), nil
			}
			_, replacementErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, failedCandidate.root, failedCandidate.source, failedCandidate.bundle, failedCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: failedCandidate.hash}, failedCandidate.rt, failedCandidate.rt.WorkOccurrence(),
			)
			if replacementErr == nil || !strings.Contains(replacementErr.Error(), "public ingress reconciliation failed") {
				t.Fatalf("post-publication replacement error = %v, want public ingress failure", replacementErr)
			}
			if publicationHooks.Load() != 2 {
				t.Fatalf("publication hooks = %d, want failed candidate plus restored predecessor", publicationHooks.Load())
			}
			if !ready.Load() || supervisor.CurrentRuntime() != restoredCandidate.rt || supervisor.CurrentProject().ProjectDir != candidate.root {
				t.Fatalf("restored supervisor state = ready:%v runtime:%p project:%#v", ready.Load(), supervisor.CurrentRuntime(), supervisor.CurrentProject())
			}
			if lookup := contexts.LookupBundleHashStatus(candidate.hash); !lookup.Loaded() {
				t.Fatalf("restored predecessor context = %#v, want loaded", lookup)
			}
			if lookup := contexts.LookupBundleHashStatus(failedCandidate.hash); lookup.Found {
				t.Fatalf("failed published candidate context = %#v, want withdrawn", lookup)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor after post-publication rollback = %#v, want loaded", lookup)
			}
			restoredPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || restoredPlan.Revision != currentPlan.Revision {
				t.Fatalf("restored source set = exists:%v revision:%s want:%s err:%v", exists, restoredPlan.Revision, currentPlan.Revision, err)
			}
			agents, err = stores.ManagerStore.LoadAgents(ctx)
			if err != nil {
				t.Fatalf("read topology after post-publication rollback: %v", err)
			}
			var restoredGamma, restoredBeta bool
			for _, agent := range agents {
				switch agent.Config.ID {
				case "gamma-worker":
					restoredGamma = agent.LifecyclePhase == "running"
				case "beta-worker":
					restoredBeta = agent.LifecyclePhase == "running"
				case "delta-worker":
					t.Fatalf("failed candidate topology survived rollback: %#v", agent)
				}
			}
			if !restoredGamma || !restoredBeta {
				t.Fatalf("restored topology = gamma:%v beta:%v", restoredGamma, restoredBeta)
			}

			survivorManager := survivor.rt.Manager
			var blockedHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				blockedHooks.Add(1)
				survivor.rt.Manager = nil
				return errors.New("public ingress reconciliation failed before survivor restore")
			})
			_, blockedErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, blockedCandidate.root, blockedCandidate.source, blockedCandidate.bundle, blockedCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: blockedCandidate.hash}, blockedCandidate.rt, blockedCandidate.rt.WorkOccurrence(),
			)
			survivor.rt.Manager = survivorManager
			if blockedErr == nil || !strings.Contains(blockedErr.Error(), "prepare predecessor source-set survivor restoration") {
				t.Fatalf("blocked predecessor restoration error = %v", blockedErr)
			}
			if blockedHooks.Load() != 1 {
				t.Fatalf("blocked publication hooks = %d, want no predecessor publication after preparation failure", blockedHooks.Load())
			}
			if ready.Load() {
				t.Fatal("supervisor became ready after predecessor survivor preparation failure")
			}
			blockedPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || blockedPlan.Revision == currentPlan.Revision {
				t.Fatalf("source set after predecessor preparation failure = exists:%v revision:%s previous:%s err:%v", exists, blockedPlan.Revision, currentPlan.Revision, err)
			}
			var blockedSource, restoredSource bool
			for _, source := range blockedPlan.Sources {
				blockedSource = blockedSource || source.BundleHash == blockedCandidate.hash
				restoredSource = restoredSource || source.BundleHash == candidate.hash
			}
			if !blockedSource || restoredSource {
				t.Fatalf("source set changed before predecessor survivor preparation: %#v", blockedPlan.Sources)
			}
			if lookup := contexts.LookupBundleHashStatus(candidate.hash); lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseReplacing {
				t.Fatalf("predecessor after failed survivor preparation = %#v, want unavailable replacing", lookup)
			}
			if lookup := contexts.LookupBundleHashStatus(blockedCandidate.hash); lookup.Found {
				t.Fatalf("withdrawn blocked candidate context = %#v, want absent", lookup)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor under retained candidate head = %#v, want loaded", lookup)
			}
			evidence, err = survivor.rt.CurrentStartupGrantEvidence()
			if err != nil || evidence.SourceSetRevision != blockedPlan.Revision {
				t.Fatalf("survivor authority after failed predecessor preparation = %#v, head:%s err:%v", evidence, blockedPlan.Revision, err)
			}

			deactivated := contexts.DeactivateBundleHashWithOptions(
				survivor.hash, runtimepkg.RuntimeContextCauseUnloaded, runtimepkg.ShutdownOptions{Grace: 5 * time.Second},
			)
			if deactivated.ShutdownErr != nil {
				t.Fatalf("shutdown rebound survivor: %v", deactivated.ShutdownErr)
			}
		})
	}
}
