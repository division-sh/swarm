package startupownership

import (
	"context"
	"errors"
	"sync"
	"testing"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

const startupBundleHashA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type retainedSessionProbe struct {
	mu             sync.Mutex
	authority      Authority
	plan           runtimeagenttopology.SourceSetPlan
	callback       func()
	released       bool
	proveErr       error
	records        []GrantEvidence
	lifecycleScope runtimeauthoractivity.Scope
}

func (s *retainedSessionProbe) Authority() (Authority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return Authority{}, errors.New("retained session released")
	}
	return s.authority, nil
}

func (s *retainedSessionProbe) ProveCurrent(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return errors.New("retained session released")
	}
	return s.proveErr
}

func (s *retainedSessionProbe) InstallTerminalOwner(owner SessionTerminalOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner == nil || s.callback != nil {
		return errors.New("terminal callback invalid")
	}
	s.callback = owner.SelectedStoreSessionTerminal
	return nil
}

func (s *retainedSessionProbe) RecordGenerationGrantTransition(_ context.Context, _ *GrantEvidence, next GrantEvidence) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, next)
	return nil
}

func (s *retainedSessionProbe) LoadSourceSet(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plan, s.plan.Revision != "", nil
}

func (s *retainedSessionProbe) CommitSourceSet(_ context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.plan.Revision
	s.plan = req.Plan
	return runtimeagenttopology.SourceSetCommitResult{Operation: req.Operation, OperationID: req.OperationID, PreviousRevision: previous, CurrentRevision: req.Plan.Revision}, nil
}

func (*retainedSessionProbe) ApplyBundleDeleteFinalMutation(context.Context, runtimebundledelete.FinalMutationRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error) {
	return runtimebundledelete.FinalMutationResult{}, errors.New("not implemented")
}

func (*retainedSessionProbe) ApplyDestructiveResetCleanup(context.Context, runtimedestructivereset.CleanupRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error) {
	return runtimedestructivereset.CleanupResult{}, errors.New("not implemented")
}

func (s *retainedSessionProbe) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	s.mu.Lock()
	s.lifecycleScope, _ = runtimeauthoractivity.ScopeFromContext(ctx)
	s.mu.Unlock()
	return runtimemanager.AgentLifecycleTransitionResult{OperationID: req.OperationID, Identity: req.Identity, AgentID: req.AgentID, RuntimeEpoch: req.TargetEpoch, Generation: req.TargetGeneration, Phase: req.TargetPhase, Topology: req.Topology}, nil
}

func (s *retainedSessionProbe) Release(context.Context) error {
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

func testCapability(t *testing.T) (ProcessCapability, *retainedSessionProbe, runtimeagenttopology.SourceSetPlan) {
	t.Helper()
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{BundleHash: startupBundleHashA, BundleSource: "ephemeral"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := NewColdAuthority(AcquireRequest{OwnerID: "owner-a", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}, "test")
	if err != nil {
		t.Fatal(err)
	}
	session := &retainedSessionProbe{authority: authority, plan: plan}
	capability, err := NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	return capability, session, plan
}

func TestAcquireRequestRequiresTypedProcessIdentity(t *testing.T) {
	if _, err := NewColdAuthority(AcquireRequest{OwnerID: "owner", BootID: uuid.NewString()}, "test"); err == nil {
		t.Fatal("NewColdAuthority accepted a missing runtime instance")
	}
	if _, err := NewColdAuthority(AcquireRequest{OwnerID: "owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}, "test"); err != nil {
		t.Fatalf("NewColdAuthority: %v", err)
	}
}

func TestProcessCapabilityIssuesExactRevocableGenerationGrant(t *testing.T) {
	capability, session, plan := testCapability(t)
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, BundleSource: "ephemeral", RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 7, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("IssueGenerationGrant: %v", err)
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatalf("Evidence: %v", err)
	}
	if evidence.ProcessAuthorityID != session.authority.AuthorityID || evidence.RuntimeGeneration != 7 || evidence.SourceSetRevision != plan.Revision {
		t.Fatalf("grant evidence = %#v", evidence)
	}
	if err := grant.Retire(context.Background()); err != nil {
		t.Fatalf("Retire: %v", err)
	}
	select {
	case <-grant.Done():
	default:
		t.Fatal("retired grant remained open")
	}
}

func TestGenerationGrantProjectsExactBundleScopeForLifecycleCommit(t *testing.T) {
	capability, session, plan := testCapability(t)
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, BundleSource: "ephemeral", RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("IssueGenerationGrant: %v", err)
	}
	topology, err := runtimeagenttopology.StaticAdmission(plan.Revision, startupBundleHashA, "ephemeral", runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatalf("StaticAdmission: %v", err)
	}
	if _, err := grant.CommitAgentLifecycleTransition(context.Background(), runtimemanager.AgentLifecycleTransition{Topology: topology}); err != nil {
		t.Fatalf("CommitAgentLifecycleTransition: %v", err)
	}
	session.mu.Lock()
	scope := session.lifecycleScope
	session.mu.Unlock()
	want := runtimeauthoractivity.BundleScope(session.authority.RuntimeInstanceID, startupBundleHashA)
	if scope != want {
		t.Fatalf("lifecycle scope = %#v, want %#v", scope, want)
	}
}

func TestRetainedSessionLossRetiresCapabilityAndAllGrantsBeforeReturn(t *testing.T) {
	capability, session, plan := testCapability(t)
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, BundleSource: "ephemeral", RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.proveErr = errors.New("session lost")
	session.mu.Unlock()
	if err := grant.ProveCurrent(context.Background()); err == nil {
		t.Fatal("ProveCurrent accepted a lost retained session")
	}
	select {
	case <-capability.Done():
	default:
		t.Fatal("capability was not terminal before ProveCurrent returned")
	}
	select {
	case <-grant.Done():
	default:
		t.Fatal("grant was not terminal before ProveCurrent returned")
	}
}

func TestProcessCapabilityReleaseIsTerminal(t *testing.T) {
	capability, _, _ := testCapability(t)
	if err := capability.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case <-capability.Done():
	default:
		t.Fatal("released capability remained open")
	}
	if _, err := capability.Evidence(); err == nil {
		t.Fatal("released capability returned authority evidence")
	}
}
