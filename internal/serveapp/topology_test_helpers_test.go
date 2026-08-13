package serveapp

import (
	"context"
	"errors"
	"sync"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type supervisorTestRetainedSession struct {
	mu                       sync.Mutex
	authority                runtimestartupownership.Authority
	plan                     runtimeagenttopology.SourceSetPlan
	sourceSetRequests        []runtimeagenttopology.SourceSetCommitRequest
	bundleDeleteRequests     []runtimebundledelete.FinalMutationRequest
	destructiveResetRequests []runtimedestructivereset.CleanupRequest
	callback                 func()
	released                 bool
}

func (s *supervisorTestRetainedSession) Authority() (runtimestartupownership.Authority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return runtimestartupownership.Authority{}, errors.New("supervisor test retained session is released")
	}
	return s.authority, nil
}

func (s *supervisorTestRetainedSession) ProveCurrent(context.Context) error {
	_, err := s.Authority()
	return err
}

func (s *supervisorTestRetainedSession) InstallTerminalOwner(owner runtimestartupownership.SessionTerminalOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner == nil || s.callback != nil {
		return errors.New("supervisor test retained session terminal callback is invalid")
	}
	s.callback = owner.SelectedStoreSessionTerminal
	return nil
}

func (*supervisorTestRetainedSession) RecordGenerationGrantTransition(context.Context, *runtimestartupownership.GrantEvidence, runtimestartupownership.GrantEvidence) error {
	return nil
}

func (s *supervisorTestRetainedSession) LoadSourceSet(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plan, s.plan.Revision != "", nil
}

func (s *supervisorTestRetainedSession) CommitSourceSet(_ context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.plan.Revision
	s.sourceSetRequests = append(s.sourceSetRequests, req)
	s.plan = req.Plan
	return runtimeagenttopology.SourceSetCommitResult{
		Operation: req.Operation, OperationID: req.OperationID,
		PreviousRevision: previous, CurrentRevision: req.Plan.Revision,
	}, nil
}

func (s *supervisorTestRetainedSession) ApplyBundleDeleteFinalMutation(_ context.Context, req runtimebundledelete.FinalMutationRequest, topology *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bundleDeleteRequests = append(s.bundleDeleteRequests, req)
	if topology != nil {
		s.sourceSetRequests = append(s.sourceSetRequests, *topology)
		s.plan = topology.Plan
	}
	return runtimebundledelete.FinalMutationResult{OperationName: req.OperationName, BundleHash: req.BundleHash, AppliedAt: req.RequestedAt, Deleted: true}, nil
}

func (s *supervisorTestRetainedSession) ApplyDestructiveResetCleanup(_ context.Context, req runtimedestructivereset.CleanupRequest, topology *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destructiveResetRequests = append(s.destructiveResetRequests, req)
	if topology != nil {
		s.sourceSetRequests = append(s.sourceSetRequests, *topology)
		s.plan = topology.Plan
	}
	return runtimedestructivereset.CleanupResult{OperationName: req.Result.OperationName, IncludeBundles: req.Result.IncludeBundles, AppliedAt: req.RequestedAt}, nil
}

func (*supervisorTestRetainedSession) CommitAgentLifecycleTransition(context.Context, runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return runtimemanager.AgentLifecycleTransitionResult{}, errors.New("supervisor test session has no agent lifecycle rows")
}

func (s *supervisorTestRetainedSession) Release(context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.released = true
	callback := s.callback
	s.mu.Unlock()
	if callback != nil {
		callback()
	}
	return nil
}

func bindSupervisorTestRuntimeTopology(
	t testing.TB,
	rt *runtimepkg.Runtime,
	source semanticview.Source,
	fact runtimecorrelation.BundleSourceFact,
	workOwner *worklifetime.RuntimeOccurrence,
	runtimeInstanceID string,
) {
	t.Helper()
	if rt == nil || rt.Bus == nil {
		t.Fatal("supervisor topology test runtime and event bus are required")
	}
	rt.Options.WorkflowModule = stubWorkflowModule{source: source}
	rt.Options.BundleSourceFact = fact
	rt.Options.RuntimeInstanceID = runtimeInstanceID
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		SemanticSource:    source,
		WorkOwner:         workOwner,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
}

func installSupervisorTestProcessCapability(
	t testing.TB,
	supervisor *runtimeProjectSupervisor,
	manager *runtimemanager.AgentManager,
	source semanticview.Source,
	fact runtimecorrelation.BundleSourceFact,
	runtimeInstanceID string,
) runtimeagenttopology.SourceSetPlan {
	t.Helper()
	bundleHash, bundleSource := fact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := manager.CompileStaticTopologyDesiredAgents(source, coordinate)
	if err != nil {
		t.Fatalf("compile supervisor test source set: %v", err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct supervisor test source set: %v", err)
	}
	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "supervisor-test-process", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	}, "supervisor_test")
	if err != nil {
		t.Fatalf("construct supervisor test process authority: %v", err)
	}
	capability, err := runtimestartupownership.NewProcessCapability(&supervisorTestRetainedSession{authority: authority})
	if err != nil {
		t.Fatalf("construct supervisor test process capability: %v", err)
	}
	if _, err := capability.InstallCompleteSourceSet(context.Background(), runtimeagenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), Plan: plan,
	}); err != nil {
		t.Fatalf("install supervisor test source set: %v", err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	supervisor.runtimeInstanceID = runtimeInstanceID
	supervisor.SetProcessCapability(capability)
	return plan
}

func installSelectedStoreTestProcessTopology(
	t testing.TB,
	stores storeBundle,
	rt *runtimepkg.Runtime,
	source semanticview.Source,
	fact runtimecorrelation.BundleSourceFact,
	runtimeInstanceID string,
) (runtimestartupownership.ProcessCapability, runtimeagenttopology.SourceSetPlan) {
	t.Helper()
	if stores.StartupOwnership == nil || rt == nil || rt.Manager == nil {
		t.Fatal("selected-store topology test requires startup ownership and a runtime manager")
	}
	bundleHash, bundleSource := fact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := rt.Manager.CompileStaticTopologyDesiredAgents(source, coordinate)
	if err != nil {
		t.Fatalf("compile selected-store test source set: %v", err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct selected-store test source set: %v", err)
	}
	capability, err := stores.StartupOwnership.AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{
		OwnerID: "serve-topology-test:" + runtimeInstanceID, BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		t.Fatalf("acquire selected-store test process capability: %v", err)
	}
	if err := installServeSourceSet(context.Background(), capability, plan); err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("install selected-store test source set: %v", err)
	}
	installSelectedStoreTestGeneration(t, capability, rt, plan, 1)
	return capability, plan
}

func installSelectedStoreTestGeneration(
	t testing.TB,
	capability runtimestartupownership.ProcessCapability,
	rt *runtimepkg.Runtime,
	plan runtimeagenttopology.SourceSetPlan,
	generation uint64,
) {
	t.Helper()
	if capability == nil || rt == nil {
		t.Fatal("selected-store test generation requires a capability and runtime")
	}
	_, bundleSource := rt.Options.BundleSourceFact.StorageValues()
	grant, err := capability.IssueGenerationGrant(context.Background(), runtimestartupownership.GrantRequest{
		BundleHash: rt.Options.BundleSourceFact.BundleHash(), BundleSource: bundleSource,
		RuntimeInstanceID: rt.Options.RuntimeInstanceID, RuntimeGeneration: generation,
		SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue selected-store test generation grant: %v", err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		_ = grant.Retire(context.Background())
		t.Fatalf("install selected-store test generation grant: %v", err)
	}
}

func registerServeTestEphemeralAgent(t testing.TB, manager *runtimemanager.AgentManager, cfg runtimeactors.AgentConfig) {
	t.Helper()
	admission, err := runtimeagenttopology.NewEphemeralAdmission(uuid.NewString(), "runtime_shard")
	if err != nil {
		t.Fatalf("construct serve test ephemeral topology: %v", err)
	}
	if err := manager.MaterializeAdmittedAgentForExecution(context.Background(), runtimemanager.PersistedAgent{
		Config: cfg, Status: "ephemeral", HiredBy: "serve-test", Topology: admission,
	}); err != nil {
		t.Fatalf("register serve test ephemeral agent: %v", err)
	}
}

func registerServeTestDurableAgent(
	t testing.TB,
	selected storetest.AgentFixtureStore,
	manager *runtimemanager.AgentManager,
	cfg runtimeactors.AgentConfig,
) {
	t.Helper()
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		t.Fatalf("resolve serve test durable agent identity: %v", err)
	}
	const bundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ctx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		bundleHash,
	))
	rec := runtimemanager.PersistedAgent{
		Config: cfg, Status: "active", HiredBy: "serve-test-declaration",
	}
	if err := storetest.UpsertAgentFixture(ctx, selected, rec); err != nil {
		t.Fatalf("persist serve test durable agent: %v", err)
	}
	committed, err := selected.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("load committed serve test durable agent: %v", err)
	}
	for _, candidate := range committed {
		candidateIdentity, identityErr := candidate.Config.ConcreteIdentity()
		if identityErr != nil {
			t.Fatalf("read committed serve test durable identity: %v", identityErr)
		}
		if candidateIdentity.Normalize() != identity.Normalize() {
			continue
		}
		candidate.Config = cfg
		if err := manager.MaterializeAdmittedAgentForExecution(ctx, candidate); err != nil {
			t.Fatalf("materialize committed serve test durable agent: %v", err)
		}
		return
	}
	t.Fatalf("committed serve test durable agent %s was not readable", identity.Description())
}
