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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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

type failSourceSetGrantCapability struct {
	runtimestartupownership.ProcessCapability
	revision          string
	bundleHash        string
	onFailure         func()
	attempts          atomic.Int32
	failuresRemaining atomic.Int32
	failures          atomic.Int32
}

type failEarlyTopologyRollbackCapability struct {
	runtimestartupownership.ProcessCapability
	candidateBundleHash      string
	grantFailuresRemaining   atomic.Int32
	restoreFailuresRemaining atomic.Int32
}

type failOnceRuntimeReplacementPublication struct {
	delegate         runtimeReplacementPublication
	attempts         atomic.Int32
	discardAttempts  atomic.Int32
	withdrawAttempts atomic.Int32
}

type failOnceRuntimeReplacementRollback struct {
	delegate         runtimeReplacementPublication
	withdrawAttempts atomic.Int32
}

func (p *failOnceRuntimeReplacementRollback) Publish() error {
	return p.delegate.Publish()
}

func (p *failOnceRuntimeReplacementRollback) Discard() error {
	return p.delegate.Discard()
}

func (p *failOnceRuntimeReplacementRollback) Withdraw(ctx context.Context) error {
	if p.withdrawAttempts.Add(1) == 1 {
		return errors.New("injected transient replacement withdrawal failure")
	}
	return p.delegate.Withdraw(ctx)
}

func (p *failOnceRuntimeReplacementPublication) Publish() error {
	if p.attempts.Add(1) == 1 {
		return errors.New("injected retained predecessor publication failure")
	}
	return p.delegate.Publish()
}

func (p *failOnceRuntimeReplacementPublication) Discard() error {
	p.discardAttempts.Add(1)
	return p.delegate.Discard()
}

func (p *failOnceRuntimeReplacementPublication) Withdraw(ctx context.Context) error {
	p.withdrawAttempts.Add(1)
	return p.delegate.Withdraw(ctx)
}

type recordingRuntimeReplacementPublication struct {
	publishAttempts  atomic.Int32
	discardAttempts  atomic.Int32
	withdrawAttempts atomic.Int32
	publishErr       error
	discardErr       error
	withdrawErr      error
}

func (p *recordingRuntimeReplacementPublication) Publish() error {
	p.publishAttempts.Add(1)
	return p.publishErr
}

func (p *recordingRuntimeReplacementPublication) Discard() error {
	p.discardAttempts.Add(1)
	return p.discardErr
}

func (p *recordingRuntimeReplacementPublication) Withdraw(context.Context) error {
	p.withdrawAttempts.Add(1)
	return p.withdrawErr
}

func TestRuntimeProjectSupervisorFailedRollbackRetainsExactTerminalOperation(t *testing.T) {
	publication := &recordingRuntimeReplacementPublication{withdrawErr: errors.New("injected withdrawal failure")}
	pending := &pendingRuntimeReplacement{publication: publication, phase: runtimeReplacementPublished}
	supervisor := &runtimeProjectSupervisor{pendingReplacement: pending}
	if err := supervisor.rollbackPendingReplacement(context.Background(), pending); err == nil || !strings.Contains(err.Error(), "injected withdrawal failure") {
		t.Fatalf("rollback pending replacement error = %v, want injected withdrawal failure", err)
	}
	if pending.phase != runtimeReplacementRollbackPublished || supervisor.pendingReplacement != pending {
		t.Fatalf("failed rollback did not retain terminal rollback phase: phase=%d pending=%p want=%p", pending.phase, supervisor.pendingReplacement, pending)
	}
	if err := supervisor.completePendingReplacement(); err == nil || !strings.Contains(err.Error(), "rollback is incomplete") {
		t.Fatalf("forward completion after failed rollback = %v, want fail-closed phase rejection", err)
	}
	if publication.publishAttempts.Load() != 0 {
		t.Fatalf("forward publication attempts after failed rollback = %d, want 0", publication.publishAttempts.Load())
	}
	publication.withdrawErr = nil
	if err := supervisor.rollbackPendingReplacement(context.Background(), pending); err != nil {
		t.Fatalf("retry exact retained withdrawal: %v", err)
	}
	if publication.withdrawAttempts.Load() != 2 || supervisor.pendingReplacement != nil {
		t.Fatalf("retained withdrawal retry = attempts:%d pending:%p, want 2/nil", publication.withdrawAttempts.Load(), supervisor.pendingReplacement)
	}
}

func TestRuntimeProjectSupervisorOperationBoundariesFinalizeRetainedReplacementRollback(t *testing.T) {
	tests := []struct {
		name string
		run  func(*runtimeProjectSupervisor) error
	}{
		{
			name: "close",
			run: func(supervisor *runtimeProjectSupervisor) error {
				_, err := supervisor.CloseProject(context.Background())
				return err
			},
		},
		{
			name: "load",
			run: func(supervisor *runtimeProjectSupervisor) error {
				supervisor.DisableSourceReplacement("test boundary")
				_, err := supervisor.loadProject(context.Background(), t.TempDir())
				if err == nil || !strings.Contains(err.Error(), "test boundary") {
					t.Fatalf("load boundary error = %v, want post-finalization source-replacement refusal", err)
				}
				return nil
			},
		},
		{
			name: "replace",
			run: func(supervisor *runtimeProjectSupervisor) error {
				_, err := supervisor.replaceCurrentRuntimeWithSource(
					context.Background(), "", nil, nil, runtimecorrelation.BundleSourceFact{}, runtimecontracts.BundleIdentity{}, nil, nil,
				)
				if err == nil || !strings.Contains(err.Error(), "runtime replacement requires a candidate runtime") {
					t.Fatalf("replace boundary error = %v, want post-finalization candidate validation", err)
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			supervisor := &runtimeProjectSupervisor{
				pendingReplacementRollback: &pendingRuntimeReplacementRollback{phase: runtimeReplacementRollbackComplete},
			}
			if err := tt.run(supervisor); err != nil {
				t.Fatalf("operation boundary: %v", err)
			}
			if supervisor.pendingReplacementRollback != nil {
				t.Fatal("operation boundary did not finalize retained replacement rollback")
			}
		})
	}
}

func (c *failSourceSetGrantCapability) IssueGenerationGrant(
	ctx context.Context,
	req runtimestartupownership.GrantRequest,
) (runtimestartupownership.GenerationGrant, error) {
	if (c.revision == "" || req.SourceSetRevision == c.revision) && (c.bundleHash == "" || req.BundleHash == c.bundleHash) {
		c.attempts.Add(1)
		for {
			remaining := c.failuresRemaining.Load()
			if remaining <= 0 {
				break
			}
			if c.failuresRemaining.CompareAndSwap(remaining, remaining-1) {
				c.failures.Add(1)
				if c.onFailure != nil {
					c.onFailure()
				}
				return nil, errors.New("injected predecessor survivor grant failure")
			}
		}
	}
	return c.ProcessCapability.IssueGenerationGrant(ctx, req)
}

func (c *failEarlyTopologyRollbackCapability) IssueGenerationGrant(
	ctx context.Context,
	req runtimestartupownership.GrantRequest,
) (runtimestartupownership.GenerationGrant, error) {
	if req.BundleHash == c.candidateBundleHash && c.grantFailuresRemaining.CompareAndSwap(1, 0) {
		return nil, errors.New("injected early candidate generation failure")
	}
	return c.ProcessCapability.IssueGenerationGrant(ctx, req)
}

func (c *failEarlyTopologyRollbackCapability) RestoreSourceSet(
	ctx context.Context,
	req runtimeagenttopology.SourceSetCommitRequest,
) (runtimeagenttopology.SourceSetCommitResult, error) {
	if c.restoreFailuresRemaining.CompareAndSwap(1, 0) {
		return runtimeagenttopology.SourceSetCommitResult{}, errors.New("injected early source-set restoration failure")
	}
	return c.ProcessCapability.RestoreSourceSet(ctx, req)
}

func TestRuntimeProjectSupervisorRollbackPendingReplacementUsesExactPublicationPhase(t *testing.T) {
	tests := []struct {
		name         string
		phase        runtimeReplacementPhase
		publishFails bool
		wantDiscard  int32
		wantWithdraw int32
	}{
		{name: "failed preparation discards", phase: runtimeReplacementPrepared, publishFails: true, wantDiscard: 1},
		{name: "published withdraws", phase: runtimeReplacementPublished, wantWithdraw: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publication := &recordingRuntimeReplacementPublication{}
			if tt.publishFails {
				publication.publishErr = errors.New("injected pre-publication failure")
			}
			pending := &pendingRuntimeReplacement{publication: publication, phase: tt.phase}
			supervisor := &runtimeProjectSupervisor{pendingReplacement: pending}
			if tt.publishFails {
				if err := supervisor.completePendingReplacement(); err == nil || !strings.Contains(err.Error(), "injected pre-publication failure") {
					t.Fatalf("complete pending replacement error = %v, want injected pre-publication failure", err)
				}
				if pending.phase != runtimeReplacementPrepared || supervisor.pendingReplacement != pending {
					t.Fatalf("failed publication advanced or lost prepared continuation: phase=%d pending=%p want=%p", pending.phase, supervisor.pendingReplacement, pending)
				}
			}
			if err := supervisor.rollbackPendingReplacement(context.Background(), pending); err != nil {
				t.Fatalf("rollback pending replacement: %v", err)
			}
			if got := publication.discardAttempts.Load(); got != tt.wantDiscard {
				t.Fatalf("discard attempts = %d, want %d", got, tt.wantDiscard)
			}
			if got := publication.withdrawAttempts.Load(); got != tt.wantWithdraw {
				t.Fatalf("withdraw attempts = %d, want %d", got, tt.wantWithdraw)
			}
			if supervisor.pendingReplacement != nil {
				t.Fatal("settled replacement rollback remained pending")
			}
		})
	}
}

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
			commitFailureCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-commit-failure-candidate", "zeta-worker"), runtimeContextTestHash("f"))
			commitFailureRestored := newRuntime(candidate.root, candidate.hash)
			retainedFailureCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-retained-failure-candidate", "eta-worker"), runtimeContextTestHash("0"))
			retainedFailureRestored := newRuntime(candidate.root, candidate.hash)
			recoveryCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-recovery-candidate", "theta-worker"), runtimeContextTestHash("1"))
			blockedCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-blocked-candidate", "iota-worker"), runtimeContextTestHash("2"))
			blockedRestored := newRuntime(recoveryCandidate.root, recoveryCandidate.hash)
			directFailureCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-direct-failure-candidate", "kappa-worker"), runtimeContextTestHash("3"))
			directFailureRestored := newRuntime(recoveryCandidate.root, recoveryCandidate.hash)
			finalCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-final-candidate", "lambda-worker"), runtimeContextTestHash("4"))
			earlyFailureCandidate := newRuntime(writeServeRuntimeAgentSlugFixture(t, "replacement-early-failure-candidate", "mu-worker"), runtimeContextTestHash("5"))
			earlyFailureRestored := newRuntime(finalCandidate.root, finalCandidate.hash)

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
			var rollbackPublication *failOnceRuntimeReplacementRollback
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				if publicationHooks.Add(1) == 1 {
					supervisor.mu.RLock()
					pending := supervisor.pendingReplacement
					supervisor.mu.RUnlock()
					if pending == nil {
						t.Fatal("published candidate replacement was not retained")
					}
					rollbackPublication = &failOnceRuntimeReplacementRollback{delegate: pending.publication}
					pending.publication = rollbackPublication
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
			if replacementErr == nil || !strings.Contains(replacementErr.Error(), "public ingress reconciliation failed") ||
				!strings.Contains(replacementErr.Error(), "injected transient replacement withdrawal failure") {
				t.Fatalf("post-publication replacement error = %v, want public ingress and retained withdrawal failures", replacementErr)
			}
			if rollbackPublication == nil || rollbackPublication.withdrawAttempts.Load() != 1 {
				t.Fatalf("retained withdrawal attempts = %v, want one failed attempt", rollbackPublication)
			}
			if ready.Load() {
				t.Fatal("supervisor became ready while replacement rollback remained pending")
			}
			supervisor.mu.RLock()
			retainedReplacementRollback := supervisor.pendingReplacementRollback
			retainedRollbackPublication := supervisor.pendingReplacement
			supervisor.mu.RUnlock()
			if retainedReplacementRollback == nil || retainedRollbackPublication == nil || retainedRollbackPublication.phase != runtimeReplacementRollbackPublished {
				phase := runtimeReplacementPrepared
				if retainedRollbackPublication != nil {
					phase = retainedRollbackPublication.phase
				}
				t.Fatalf("failed withdrawal continuation = rollback:%p publication:%p phase:%d", retainedReplacementRollback, retainedRollbackPublication, phase)
			}
			_, boundaryErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, candidate.root, candidate.source, candidate.bundle, candidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: candidate.hash}, nil, nil,
			)
			if boundaryErr == nil || !strings.Contains(boundaryErr.Error(), "runtime replacement requires a candidate runtime") {
				t.Fatalf("replacement boundary after retained withdrawal = %v, want candidate validation after recovery", boundaryErr)
			}
			if rollbackPublication.withdrawAttempts.Load() != 2 {
				t.Fatalf("retained withdrawal attempts after replacement boundary = %d, want 2", rollbackPublication.withdrawAttempts.Load())
			}
			if publicationHooks.Load() != 2 {
				t.Fatalf("publication hooks = %d, want failed candidate plus restored predecessor", publicationHooks.Load())
			}
			if !ready.Load() || supervisor.CurrentRuntime() != restoredCandidate.rt || supervisor.CurrentProject().ProjectDir != candidate.root {
				t.Fatalf("restored supervisor state = ready:%v runtime:%p project:%#v", ready.Load(), supervisor.CurrentRuntime(), supervisor.CurrentProject())
			}
			supervisor.mu.RLock()
			retainedReplacementRollback = supervisor.pendingReplacementRollback
			retainedRollbackPublication = supervisor.pendingReplacement
			supervisor.mu.RUnlock()
			if retainedReplacementRollback != nil || retainedRollbackPublication != nil {
				t.Fatalf("replacement boundary retained settled rollback: rollback:%p publication:%p", retainedReplacementRollback, retainedRollbackPublication)
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

			failingCapability := &failSourceSetGrantCapability{
				ProcessCapability: capability,
				revision:          currentPlan.Revision,
			}
			failingCapability.failuresRemaining.Store(1)
			supervisor.SetProcessCapability(failingCapability)
			var commitFailureHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				if commitFailureHooks.Add(1) == 1 {
					return errors.New("public ingress reconciliation failed before survivor commit retry")
				}
				return nil
			})
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return commitFailureRestored.rt, commitFailureRestored.rt.WorkOccurrence(), nil
			}
			_, commitFailureErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, commitFailureCandidate.root, commitFailureCandidate.source, commitFailureCandidate.bundle, commitFailureCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: commitFailureCandidate.hash}, commitFailureCandidate.rt, commitFailureCandidate.rt.WorkOccurrence(),
			)
			if commitFailureErr == nil || !strings.Contains(commitFailureErr.Error(), "public ingress reconciliation failed before survivor commit retry") {
				t.Fatalf("survivor commit retry replacement error = %v", commitFailureErr)
			}
			if failingCapability.failures.Load() != 1 || failingCapability.attempts.Load() < 2 {
				t.Fatalf("predecessor survivor grant attempts = %d failures:%d, want failed commit plus immediate adopted retry", failingCapability.attempts.Load(), failingCapability.failures.Load())
			}
			if commitFailureHooks.Load() != 2 {
				t.Fatalf("survivor commit retry publication hooks = %d, want failed candidate plus restored predecessor", commitFailureHooks.Load())
			}
			if !ready.Load() || supervisor.CurrentRuntime() != commitFailureRestored.rt {
				t.Fatalf("survivor commit retry supervisor state = ready:%v runtime:%p want:%p", ready.Load(), supervisor.CurrentRuntime(), commitFailureRestored.rt)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor after adopted predecessor retry = %#v, want loaded", lookup)
			}
			retriedPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || retriedPlan.Revision != currentPlan.Revision {
				t.Fatalf("source set after adopted predecessor retry = exists:%v revision:%s want:%s err:%v", exists, retriedPlan.Revision, currentPlan.Revision, err)
			}
			evidence, err = survivor.rt.CurrentStartupGrantEvidence()
			if err != nil || evidence.SourceSetRevision != retriedPlan.Revision {
				t.Fatalf("survivor authority after adopted predecessor retry = %#v head:%s err:%v", evidence, retriedPlan.Revision, err)
			}

			retainedCapability := &failSourceSetGrantCapability{
				ProcessCapability: capability,
				revision:          currentPlan.Revision,
			}
			retainedCapability.failuresRemaining.Store(2)
			supervisor.SetProcessCapability(retainedCapability)
			var retainedFailureHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				switch retainedFailureHooks.Add(1) {
				case 1:
					return errors.New("public ingress reconciliation failed before retained survivor recovery")
				case 2:
					return errors.New("retained predecessor public ingress reconciliation failed")
				}
				return nil
			})
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return retainedFailureRestored.rt, retainedFailureRestored.rt.WorkOccurrence(), nil
			}
			_, retainedFailureErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, retainedFailureCandidate.root, retainedFailureCandidate.source, retainedFailureCandidate.bundle, retainedFailureCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: retainedFailureCandidate.hash}, retainedFailureCandidate.rt, retainedFailureCandidate.rt.WorkOccurrence(),
			)
			if retainedFailureErr == nil || !strings.Contains(retainedFailureErr.Error(), "recover pending predecessor source-set survivors") {
				t.Fatalf("retained survivor recovery error = %v, want second adopted commit failure", retainedFailureErr)
			}
			if retainedCapability.failures.Load() != 2 || retainedCapability.attempts.Load() != 2 {
				t.Fatalf("retained predecessor survivor grant attempts = %d failures:%d, want two failed commits", retainedCapability.attempts.Load(), retainedCapability.failures.Load())
			}
			if retainedFailureHooks.Load() != 1 {
				t.Fatalf("retained survivor publication hooks = %d, want failed candidate only", retainedFailureHooks.Load())
			}
			if ready.Load() {
				t.Fatal("supervisor became ready while predecessor survivor recovery remained pending")
			}
			supervisor.mu.RLock()
			pendingRollback := supervisor.pendingSourceSetRollback
			supervisor.mu.RUnlock()
			if pendingRollback == nil {
				t.Fatal("failed predecessor survivor recovery was not retained")
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseSourceSetTransition {
				t.Fatalf("survivor after repeated predecessor commit failure = %#v, want retained source-set fence", lookup)
			}
			recovered, err := supervisor.completePendingSourceSetSurvivors(context.Background(), pendingRollback.manager)
			if err != nil || !recovered {
				t.Fatalf("prepare retained predecessor publication survivor recovery = recovered:%v err:%v", recovered, err)
			}
			pendingRollback.survivorsRecovered = true
			restoredContext := pendingRollback.predecessorContext
			restoredContext.Runtime = retainedFailureRestored.rt
			restoredContext.WorkOwner = retainedFailureRestored.rt.WorkOccurrence()
			restoredPlan, err = replacementSourceSetPlan(contexts, "", restoredContext)
			if err != nil {
				t.Fatalf("compile retained predecessor publication source set: %v", err)
			}
			if err := supervisor.installProcessGenerationGrant(ctx, retainedFailureRestored.rt, restoredPlan); err != nil {
				t.Fatalf("install retained predecessor publication grant: %v", err)
			}
			if err := supervisor.startCurrentRuntime(ctx, retainedFailureRestored.rt); err != nil {
				t.Fatalf("start retained predecessor publication runtime: %v", err)
			}
			targets, _, err := retainedFailureRestored.rt.EnsureStandingTargets(ctx)
			if err != nil {
				t.Fatalf("prepare retained predecessor publication standing targets: %v", err)
			}
			restoredContext.StandingTargets = targets
			publication, err := contexts.PrepareRestoredBundleHashReplacementPublication(restoredContext.BundleHash(), restoredContext)
			if err != nil {
				t.Fatalf("prepare retained predecessor publication: %v", err)
			}
			failingPublication := &failOnceRuntimeReplacementPublication{delegate: publication}
			retainedPublication := &pendingRuntimeReplacement{
				publication: failingPublication,
				root:        pendingRollback.predecessorContext.ContractsRoot, source: restoredContext.Source,
				bundle: supervisor.currentBundle, fact: restoredContext.BundleSourceFact,
				identity: restoredContext.BundleIdentity, runtime: retainedFailureRestored.rt,
				freeze: pendingRollback.freeze,
			}
			pendingRollback.predecessorPublication = retainedPublication
			supervisor.mu.Lock()
			supervisor.pendingReplacement = retainedPublication
			supervisor.mu.Unlock()
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				t.Fatal("retained predecessor publication was reconstructed instead of resumed")
				return nil, nil, nil
			}
			_, publicationFailureErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, recoveryCandidate.root, recoveryCandidate.source, recoveryCandidate.bundle, recoveryCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: recoveryCandidate.hash}, recoveryCandidate.rt, recoveryCandidate.rt.WorkOccurrence(),
			)
			if publicationFailureErr == nil || !strings.Contains(publicationFailureErr.Error(), "injected retained predecessor publication failure") {
				t.Fatalf("retained predecessor publication failure = %v", publicationFailureErr)
			}
			if failingPublication.attempts.Load() != 1 || ready.Load() {
				t.Fatalf("retained predecessor publication after transient failure = attempts:%d ready:%v, want 1/false", failingPublication.attempts.Load(), ready.Load())
			}
			supervisor.mu.RLock()
			stillPendingRollback := supervisor.pendingSourceSetRollback
			stillPendingPublication := supervisor.pendingReplacement
			supervisor.mu.RUnlock()
			if stillPendingRollback != pendingRollback || stillPendingPublication != retainedPublication || pendingRollback.predecessorPublication != retainedPublication {
				t.Fatalf("transient predecessor publication lost exact continuation: rollback:%p/%p publication:%p/%p retained:%p", stillPendingRollback, pendingRollback, stillPendingPublication, retainedPublication, pendingRollback.predecessorPublication)
			}

			_, predecessorIngressErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, recoveryCandidate.root, recoveryCandidate.source, recoveryCandidate.bundle, recoveryCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: recoveryCandidate.hash}, recoveryCandidate.rt, recoveryCandidate.rt.WorkOccurrence(),
			)
			if predecessorIngressErr == nil || !strings.Contains(predecessorIngressErr.Error(), "retained predecessor public ingress reconciliation failed") {
				t.Fatalf("retained predecessor ingress reconciliation failure = %v", predecessorIngressErr)
			}
			if failingPublication.attempts.Load() != 2 {
				t.Fatalf("retained predecessor publication attempts after ingress failure = %d, want one failed and one successful publication attempt", failingPublication.attempts.Load())
			}
			if retainedFailureHooks.Load() != 2 || ready.Load() {
				t.Fatalf("retained predecessor ingress phase = attempts:%d ready:%v, want 2/false", retainedFailureHooks.Load(), ready.Load())
			}
			supervisor.mu.RLock()
			stillPendingRollback = supervisor.pendingSourceSetRollback
			stillPendingPublication = supervisor.pendingReplacement
			supervisor.mu.RUnlock()
			if stillPendingRollback != pendingRollback || stillPendingPublication != retainedPublication || retainedPublication.phase != runtimeReplacementPublished {
				t.Fatalf("published predecessor ingress continuation was not retained: rollback:%p/%p publication:%p/%p phase:%d", stillPendingRollback, pendingRollback, stillPendingPublication, retainedPublication, retainedPublication.phase)
			}
			var successorHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				successorHooks.Add(1)
				return nil
			})

			recoveryStatus, recoveryErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, recoveryCandidate.root, recoveryCandidate.source, recoveryCandidate.bundle, recoveryCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: recoveryCandidate.hash}, recoveryCandidate.rt, recoveryCandidate.rt.WorkOccurrence(),
			)
			if recoveryErr != nil {
				t.Fatalf("replacement boundary did not finalize retained predecessor ingress recovery: %v", recoveryErr)
			}
			if failingPublication.attempts.Load() != 2 {
				t.Fatalf("retained predecessor publication was repeated after ingress failure: attempts=%d want 2", failingPublication.attempts.Load())
			}
			if failingPublication.discardAttempts.Load() != 0 || failingPublication.withdrawAttempts.Load() != 0 {
				t.Fatalf("retained predecessor publication was rolled back: discard=%d withdraw=%d", failingPublication.discardAttempts.Load(), failingPublication.withdrawAttempts.Load())
			}
			if !recoveryStatus.Loaded || !ready.Load() || supervisor.CurrentRuntime() != recoveryCandidate.rt {
				t.Fatalf("replacement after retained recovery = status:%#v ready:%v runtime:%p want:%p", recoveryStatus, ready.Load(), supervisor.CurrentRuntime(), recoveryCandidate.rt)
			}
			if retainedCapability.attempts.Load() < 3 || retainedCapability.failures.Load() != 2 {
				t.Fatalf("finalized predecessor survivor grant attempts = %d failures:%d, want retained third attempt", retainedCapability.attempts.Load(), retainedCapability.failures.Load())
			}
			if retainedFailureHooks.Load() != 3 || successorHooks.Load() != 1 {
				t.Fatalf("captured predecessor/successor publication hooks = %d/%d, want 3/1", retainedFailureHooks.Load(), successorHooks.Load())
			}
			supervisor.mu.RLock()
			pendingRollback = supervisor.pendingSourceSetRollback
			supervisor.mu.RUnlock()
			if pendingRollback != nil {
				t.Fatal("retained predecessor survivor recovery was not cleared")
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor after retained recovery replacement = %#v, want loaded", lookup)
			}
			recoveredPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists {
				t.Fatalf("load source set after retained recovery replacement: exists=%v err=%v", exists, err)
			}
			evidence, err = survivor.rt.CurrentStartupGrantEvidence()
			if err != nil || evidence.SourceSetRevision != recoveredPlan.Revision {
				t.Fatalf("survivor authority after retained recovery replacement = %#v head:%s err:%v", evidence, recoveredPlan.Revision, err)
			}

			survivorManager := survivor.rt.Manager
			var blockedHooks atomic.Int32
			supervisor.SetProcessCapability(capability)
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				blockedHooks.Add(1)
				survivor.rt.Manager = nil
				return errors.New("public ingress reconciliation failed before survivor restore")
			})
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return blockedRestored.rt, blockedRestored.rt.WorkOccurrence(), nil
			}
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
			if err != nil || !exists || blockedPlan.Revision == recoveredPlan.Revision {
				t.Fatalf("source set after predecessor preparation failure = exists:%v revision:%s previous:%s err:%v", exists, blockedPlan.Revision, recoveredPlan.Revision, err)
			}
			var blockedSource, restoredSource bool
			for _, source := range blockedPlan.Sources {
				blockedSource = blockedSource || source.BundleHash == blockedCandidate.hash
				restoredSource = restoredSource || source.BundleHash == recoveryCandidate.hash
			}
			if !blockedSource || restoredSource {
				t.Fatalf("source set changed before predecessor survivor preparation: %#v", blockedPlan.Sources)
			}
			if lookup := contexts.LookupBundleHashStatus(recoveryCandidate.hash); lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseReplacing {
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
			supervisor.mu.RLock()
			blockedReplacementRollback := supervisor.pendingReplacementRollback
			blockedSourceSetRollback := supervisor.pendingSourceSetRollback
			supervisor.mu.RUnlock()
			if blockedReplacementRollback == nil || blockedSourceSetRollback != nil {
				t.Fatalf("blocked publication rollback owner = replacement:%p source-set:%p, want retained replacement only", blockedReplacementRollback, blockedSourceSetRollback)
			}

			var blockedRecoveryHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				blockedRecoveryHooks.Add(1)
				return nil
			})
			_, boundaryErr = supervisor.replaceCurrentRuntimeWithSource(
				ctx, directFailureCandidate.root, directFailureCandidate.source, directFailureCandidate.bundle,
				directFailureCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: directFailureCandidate.hash}, nil, nil,
			)
			if boundaryErr == nil || !strings.Contains(boundaryErr.Error(), "runtime replacement requires a candidate runtime") {
				t.Fatalf("replacement boundary after predecessor preparation recovery = %v, want candidate validation", boundaryErr)
			}
			if blockedRecoveryHooks.Load() != 1 || !ready.Load() || supervisor.CurrentRuntime() != blockedRestored.rt {
				t.Fatalf("preparation recovery state = hooks:%d ready:%v runtime:%p want:%p", blockedRecoveryHooks.Load(), ready.Load(), supervisor.CurrentRuntime(), blockedRestored.rt)
			}
			supervisor.mu.RLock()
			blockedReplacementRollback = supervisor.pendingReplacementRollback
			supervisor.mu.RUnlock()
			if blockedReplacementRollback != nil {
				t.Fatal("replacement boundary did not clear retained preparation rollback")
			}
			restoredAfterBlocked, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || restoredAfterBlocked.Revision != recoveredPlan.Revision {
				t.Fatalf("source set after predecessor preparation recovery = exists:%v revision:%s want:%s err:%v", exists, restoredAfterBlocked.Revision, recoveredPlan.Revision, err)
			}

			directFailureCapability := &failSourceSetGrantCapability{
				ProcessCapability: capability,
				bundleHash:        survivor.hash,
			}
			directFailureCapability.failuresRemaining.Store(1)
			directFailureCapability.onFailure = func() { survivor.rt.Manager = nil }
			supervisor.SetProcessCapability(directFailureCapability)
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return directFailureRestored.rt, directFailureRestored.rt.WorkOccurrence(), nil
			}
			_, directFailureErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, directFailureCandidate.root, directFailureCandidate.source, directFailureCandidate.bundle,
				directFailureCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: directFailureCandidate.hash}, directFailureCandidate.rt, directFailureCandidate.rt.WorkOccurrence(),
			)
			survivor.rt.Manager = survivorManager
			if directFailureErr == nil || !strings.Contains(directFailureErr.Error(), "injected predecessor survivor grant failure") ||
				!strings.Contains(directFailureErr.Error(), "prepare predecessor source-set survivor restoration") {
				t.Fatalf("direct survivor rollback preparation error = %v", directFailureErr)
			}
			if directFailureCapability.failures.Load() != 1 || ready.Load() {
				t.Fatalf("direct survivor rollback state = failures:%d ready:%v, want 1/false", directFailureCapability.failures.Load(), ready.Load())
			}
			supervisor.mu.RLock()
			directRollback := supervisor.pendingSourceSetRollback
			directReplacementRollback := supervisor.pendingReplacementRollback
			supervisor.mu.RUnlock()
			if directRollback == nil || directRollback.sourceSetRestored || directReplacementRollback != nil {
				t.Fatalf("direct survivor rollback owner = source-set:%p restored:%v replacement:%p", directRollback, directRollback != nil && directRollback.sourceSetRestored, directReplacementRollback)
			}

			directPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || directPlan.Revision == recoveredPlan.Revision {
				t.Fatalf("source set while direct rollback is retained = exists:%v revision:%s previous:%s err:%v", exists, directPlan.Revision, recoveredPlan.Revision, err)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseSourceSetTransition {
				t.Fatalf("survivor while direct rollback is retained = %#v, want source-set fence", lookup)
			}

			supervisor.SetProcessCapability(capability)
			var finalHooks atomic.Int32
			supervisor.SetRuntimePublishedHook(func(context.Context) error {
				finalHooks.Add(1)
				return nil
			})
			finalStatus, finalErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, finalCandidate.root, finalCandidate.source, finalCandidate.bundle, finalCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: finalCandidate.hash}, finalCandidate.rt, finalCandidate.rt.WorkOccurrence(),
			)
			if finalErr != nil {
				t.Fatalf("replacement boundary did not recover direct survivor rollback: %v", finalErr)
			}
			if !finalStatus.Loaded || !ready.Load() || supervisor.CurrentRuntime() != finalCandidate.rt || finalHooks.Load() != 2 {
				t.Fatalf("final replacement state = status:%#v ready:%v runtime:%p want:%p hooks:%d", finalStatus, ready.Load(), supervisor.CurrentRuntime(), finalCandidate.rt, finalHooks.Load())
			}
			supervisor.mu.RLock()
			directRollback = supervisor.pendingSourceSetRollback
			directReplacementRollback = supervisor.pendingReplacementRollback
			supervisor.mu.RUnlock()
			if directRollback != nil || directReplacementRollback != nil {
				t.Fatalf("final replacement retained rollback owner: source-set:%p replacement:%p", directRollback, directReplacementRollback)
			}
			finalPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || finalPlan.Revision == directPlan.Revision {
				t.Fatalf("final source set = exists:%v revision:%s retained:%s err:%v", exists, finalPlan.Revision, directPlan.Revision, err)
			}
			if lookup := contexts.LookupBundleHashStatus(finalCandidate.hash); !lookup.Loaded() {
				t.Fatalf("final candidate context = %#v, want loaded", lookup)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor after direct rollback recovery = %#v, want loaded", lookup)
			}
			evidence, err = survivor.rt.CurrentStartupGrantEvidence()
			if err != nil || evidence.SourceSetRevision != finalPlan.Revision {
				t.Fatalf("survivor authority after direct rollback recovery = %#v head:%s err:%v", evidence, finalPlan.Revision, err)
			}

			earlyFailureCapability := &failEarlyTopologyRollbackCapability{
				ProcessCapability:   capability,
				candidateBundleHash: earlyFailureCandidate.hash,
			}
			earlyFailureCapability.grantFailuresRemaining.Store(1)
			earlyFailureCapability.restoreFailuresRemaining.Store(1)
			supervisor.SetProcessCapability(earlyFailureCapability)
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return earlyFailureRestored.rt, earlyFailureRestored.rt.WorkOccurrence(), nil
			}
			_, earlyFailureErr := supervisor.replaceCurrentRuntimeWithSource(
				ctx, earlyFailureCandidate.root, earlyFailureCandidate.source, earlyFailureCandidate.bundle,
				earlyFailureCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: earlyFailureCandidate.hash}, earlyFailureCandidate.rt, earlyFailureCandidate.rt.WorkOccurrence(),
			)
			if earlyFailureErr == nil || !strings.Contains(earlyFailureErr.Error(), "injected early candidate generation failure") ||
				!strings.Contains(earlyFailureErr.Error(), "injected early source-set restoration failure") {
				t.Fatalf("early topology rollback error = %v", earlyFailureErr)
			}
			if ready.Load() {
				t.Fatal("supervisor became ready while early topology rollback remained pending")
			}
			supervisor.mu.RLock()
			earlyRollback := supervisor.pendingSourceSetRollback
			supervisor.mu.RUnlock()
			if earlyRollback == nil || earlyRollback.sourceSetRestored {
				t.Fatalf("early topology rollback owner = %p restored:%v, want retained pre-restore phase", earlyRollback, earlyRollback != nil && earlyRollback.sourceSetRestored)
			}
			earlyPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || earlyPlan.Revision == finalPlan.Revision {
				t.Fatalf("source set during early topology rollback = exists:%v revision:%s previous:%s err:%v", exists, earlyPlan.Revision, finalPlan.Revision, err)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor under retained early candidate head = %#v, want loaded candidate authority", lookup)
			}

			supervisor.SetProcessCapability(capability)
			_, boundaryErr = supervisor.replaceCurrentRuntimeWithSource(
				ctx, earlyFailureCandidate.root, earlyFailureCandidate.source, earlyFailureCandidate.bundle,
				earlyFailureCandidate.rt.Options.BundleSourceFact,
				runtimecontracts.BundleIdentity{BundleHash: earlyFailureCandidate.hash}, nil, nil,
			)
			if boundaryErr == nil || !strings.Contains(boundaryErr.Error(), "runtime replacement requires a candidate runtime") {
				t.Fatalf("replacement boundary after early topology rollback = %v, want candidate validation", boundaryErr)
			}
			if !ready.Load() || supervisor.CurrentRuntime() != earlyFailureRestored.rt {
				t.Fatalf("early topology recovery state = ready:%v runtime:%p want:%p", ready.Load(), supervisor.CurrentRuntime(), earlyFailureRestored.rt)
			}
			supervisor.mu.RLock()
			earlyRollback = supervisor.pendingSourceSetRollback
			supervisor.mu.RUnlock()
			if earlyRollback != nil {
				t.Fatal("replacement boundary did not clear early topology rollback")
			}
			recoveredEarlyPlan, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || recoveredEarlyPlan.Revision != finalPlan.Revision {
				t.Fatalf("source set after early topology recovery = exists:%v revision:%s want:%s err:%v", exists, recoveredEarlyPlan.Revision, finalPlan.Revision, err)
			}
			if lookup := contexts.LookupBundleHashStatus(survivor.hash); !lookup.Loaded() {
				t.Fatalf("survivor after early topology recovery = %#v, want loaded", lookup)
			}
			evidence, err = survivor.rt.CurrentStartupGrantEvidence()
			if err != nil || evidence.SourceSetRevision != recoveredEarlyPlan.Revision {
				t.Fatalf("survivor authority after early topology recovery = %#v head:%s err:%v", evidence, recoveredEarlyPlan.Revision, err)
			}
		})
	}
}
