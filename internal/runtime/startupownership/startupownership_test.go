package startupownership

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

const startupBundleHashA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const startupBundleHashB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

type retainedSessionProbe struct {
	mu             sync.Mutex
	authority      Authority
	plan           runtimeagenttopology.SourceSetPlan
	callback       func(TerminalResult)
	released       bool
	proveErr       error
	records        []GrantEvidence
	lifecycleScope runtimeauthoractivity.Scope
	monitorProve   func(context.Context, time.Duration) error
	terminalRecord bool
	recordErr      error
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

func (s *retainedSessionProbe) MonitorProveCurrent(ctx context.Context, deadline time.Duration) error {
	s.mu.Lock()
	monitorProve := s.monitorProve
	s.mu.Unlock()
	if monitorProve != nil {
		return monitorProve(ctx, deadline)
	}
	return s.ProveCurrent(ctx)
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
	s.records = append(s.records, next)
	callback := s.callback
	terminal := s.terminalRecord
	err := s.recordErr
	s.mu.Unlock()
	if terminal {
		callback(TerminalResult{Cause: TerminalOwnershipUnprovable})
	}
	return err
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

func (*retainedSessionProbe) ReplayBundleDeleteResult(context.Context, runtimebundledelete.FinalMutationRequest) (runtimebundledelete.Result, error) {
	return runtimebundledelete.Result{}, errors.New("not implemented")
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
	s.mu.Unlock()
	return nil
}

func testCapability(t *testing.T) (ProcessCapability, *retainedSessionProbe, runtimeagenttopology.SourceSetPlan) {
	t.Helper()
	session, plan := testRetainedSession(t)
	capability, err := NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	return capability, session, plan
}

func testRetainedSession(t *testing.T) (*retainedSessionProbe, runtimeagenttopology.SourceSetPlan) {
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
	return session, plan
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

func TestGenerationGrantRejectsEveryAuthorityUseAfterSourceSetChanges(t *testing.T) {
	capability, session, plan := testCapability(t)
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, BundleSource: "ephemeral", RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("IssueGenerationGrant: %v", err)
	}
	grantToAdmit, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, BundleSource: "ephemeral", RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 2, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("IssueGenerationGrant for admission: %v", err)
	}
	if _, err := grantToAdmit.MarkProbesSettled(context.Background(), []string{"startup"}); err != nil {
		t.Fatalf("MarkProbesSettled before source-set replacement: %v", err)
	}
	replacement, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{
		{BundleHash: startupBundleHashA, BundleSource: "ephemeral"},
		{BundleHash: startupBundleHashB, BundleSource: "ephemeral"},
	}, nil)
	if err != nil {
		t.Fatalf("NewSourceSetPlan: %v", err)
	}
	if _, err := capability.ReplaceSourceSet(context.Background(), runtimeagenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), ExpectedRevision: plan.Revision, Plan: replacement,
	}); err != nil {
		t.Fatalf("ReplaceSourceSet: %v", err)
	}

	if err := grant.ProveCurrent(context.Background()); err == nil {
		t.Fatal("ProveCurrent accepted a superseded source-set revision")
	}
	if _, err := grant.SourceSetPlan(context.Background()); err == nil {
		t.Fatal("SourceSetPlan returned a superseded source-set revision")
	}
	if _, err := grant.MarkProbesSettled(context.Background(), nil); err == nil {
		t.Fatal("MarkProbesSettled admitted a superseded source-set revision")
	}
	if _, err := grantToAdmit.AdmitExecution(context.Background()); err == nil {
		t.Fatal("AdmitExecution admitted a superseded source-set revision")
	}
	flowTopology, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "flow/instance", "plan-v1")
	if err != nil {
		t.Fatalf("FlowReadinessAdmission: %v", err)
	}
	if _, err := grant.CommitAgentLifecycleTransition(context.Background(), runtimemanager.AgentLifecycleTransition{Topology: flowTopology}); err == nil {
		t.Fatal("CommitAgentLifecycleTransition accepted flow readiness through a superseded grant")
	}
	if _, err := capability.RestoreSourceSet(context.Background(), runtimeagenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), ExpectedRevision: replacement.Revision, Plan: plan,
	}); err != nil {
		t.Fatalf("RestoreSourceSet: %v", err)
	}
	if err := grant.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("ProveCurrent after restoring exact source-set revision: %v", err)
	}
	if _, err := grant.MarkProbesSettled(context.Background(), []string{"startup"}); err != nil {
		t.Fatalf("MarkProbesSettled after restoring exact source-set revision: %v", err)
	}
	if _, err := grantToAdmit.AdmitExecution(context.Background()); err != nil {
		t.Fatalf("AdmitExecution after restoring exact source-set revision: %v", err)
	}
	if err := grant.Retire(context.Background()); err != nil {
		t.Fatalf("Retire restored prepared grant: %v", err)
	}
	if err := grantToAdmit.Retire(context.Background()); err != nil {
		t.Fatalf("Retire restored admitted grant: %v", err)
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

func TestProcessCapabilityReleaseDoesNotDeadlockWhenGrantPersistenceTerminalizesSession(t *testing.T) {
	capability, session, plan := testCapability(t)
	if _, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, BundleSource: "ephemeral", RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	}); err != nil {
		t.Fatalf("IssueGenerationGrant: %v", err)
	}
	session.mu.Lock()
	session.terminalRecord = true
	session.recordErr = errors.New("selected-store session lost during grant retirement")
	session.mu.Unlock()

	released := make(chan error, 1)
	go func() { released <- capability.Release(context.Background()) }()
	select {
	case err := <-released:
		if err == nil || !strings.Contains(err.Error(), "session lost") {
			t.Fatalf("Release error = %v, want exact session loss", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Release deadlocked while the retained session terminalized the grant")
	}
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != TerminalOwnershipUnprovable {
		t.Fatalf("terminal result = %#v ok=%v, want ownership_unprovable", result, ok)
	}
}

func TestProcessCapabilityMonitorTerminalizesIdleBackendFailure(t *testing.T) {
	session, _ := testRetainedSession(t)
	entered := make(chan struct{})
	var once sync.Once
	session.monitorProve = func(context.Context, time.Duration) error {
		once.Do(func() { close(entered) })
		return errors.New("retained backend session disappeared")
	}
	capability, err := newProcessCapability(session, time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("newProcessCapability: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("possession monitor did not run")
	}
	select {
	case <-capability.Done():
	case <-time.After(time.Second):
		t.Fatal("idle possession loss did not terminalize capability")
	}
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
		t.Fatalf("terminal result=%#v ok=%v, want ownership_unprovable", result, ok)
	}
}

func TestProcessCapabilityMonitorReleaseRace(t *testing.T) {
	t.Run("release before probe", func(t *testing.T) {
		session, _ := testRetainedSession(t)
		var calls int
		session.monitorProve = func(context.Context, time.Duration) error {
			calls++
			return nil
		}
		capability, err := newProcessCapability(session, time.Hour, time.Second)
		if err != nil {
			t.Fatalf("newProcessCapability: %v", err)
		}
		if err := capability.Release(context.Background()); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if calls != 0 {
			t.Fatalf("backend probes = %d, want zero after release-before-probe", calls)
		}
		result, ok := capability.TerminalResult()
		if !ok || result.Cause != TerminalReleased {
			t.Fatalf("release terminal result=%#v ok=%v, want released", result, ok)
		}
	})

	t.Run("release during probe", func(t *testing.T) {
		session, _ := testRetainedSession(t)
		entered := make(chan struct{})
		var once sync.Once
		session.monitorProve = func(ctx context.Context, _ time.Duration) error {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return ctx.Err()
		}
		capability, err := newProcessCapability(session, time.Millisecond, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("newProcessCapability: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("possession monitor did not enter backend proof")
		}
		released := make(chan error, 1)
		go func() { released <- capability.Release(context.Background()) }()
		select {
		case err := <-released:
			if err != nil {
				t.Fatalf("Release: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Release did not cancel and join backend proof")
		}
		result, ok := capability.TerminalResult()
		if !ok || result.Cause != TerminalReleased {
			t.Fatalf("release terminal result=%#v ok=%v, want released", result, ok)
		}
	})
}

func TestProcessCapabilityMonitorTakeoverRace(t *testing.T) {
	session, _ := testRetainedSession(t)
	entered := make(chan struct{})
	var once sync.Once
	session.monitorProve = func(ctx context.Context, _ time.Duration) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return errors.New("monitor probe cancelled by terminal callback")
	}
	capability, err := newProcessCapability(session, time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("newProcessCapability: %v", err)
	}
	concrete := capability.(*processCapability)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("possession monitor did not enter backend proof")
	}
	successorID := uuid.NewString()
	session.callback(TerminalResult{Cause: TerminalOwnershipSuperseded, SuccessorAuthorityID: successorID})
	select {
	case <-concrete.monitorDone:
	case <-time.After(time.Second):
		t.Fatal("terminal callback did not cancel the monitor")
	}
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != TerminalOwnershipSuperseded || result.SuccessorAuthorityID != successorID {
		t.Fatalf("terminal result=%#v ok=%v, want exact successor", result, ok)
	}
	capability.(*processCapability).terminalize(TerminalResult{Cause: TerminalOwnershipUnprovable})
	result, _ = capability.TerminalResult()
	if result.Cause != TerminalOwnershipSuperseded || result.SuccessorAuthorityID != successorID {
		t.Fatalf("second terminalization replaced exact successor: %#v", result)
	}
}

func TestProcessCapabilityMonitorJoinsEveryTerminalPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		terminate func(ProcessCapability, *retainedSessionProbe)
	}{
		{name: "release", terminate: func(capability ProcessCapability, _ *retainedSessionProbe) {
			if err := capability.Release(context.Background()); err != nil {
				t.Fatalf("Release: %v", err)
			}
		}},
		{name: "backend failure", terminate: func(_ ProcessCapability, _ *retainedSessionProbe) {}},
		{name: "exact takeover", terminate: func(_ ProcessCapability, session *retainedSessionProbe) {
			session.callback(TerminalResult{Cause: TerminalOwnershipSuperseded, SuccessorAuthorityID: uuid.NewString()})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			session, _ := testRetainedSession(t)
			entered := make(chan struct{})
			var once sync.Once
			session.monitorProve = func(ctx context.Context, _ time.Duration) error {
				once.Do(func() { close(entered) })
				if test.name == "backend failure" {
					return errors.New("retained backend session disappeared")
				}
				<-ctx.Done()
				return ctx.Err()
			}
			capability, err := newProcessCapability(session, time.Millisecond, 10*time.Millisecond)
			if err != nil {
				t.Fatalf("newProcessCapability: %v", err)
			}
			concrete := capability.(*processCapability)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("possession monitor did not enter backend proof")
			}
			test.terminate(capability, session)
			select {
			case <-concrete.monitorDone:
			case <-time.After(time.Second):
				t.Fatal("terminal path did not join possession monitor")
			}
			select {
			case <-capability.Done():
			default:
				t.Fatal("terminal path joined monitor without terminalizing capability")
			}
		})
	}
}

func TestProcessCapabilityMonitorDoesNotClassifyOperationContentionAsLoss(t *testing.T) {
	session, _ := testRetainedSession(t)
	entered := make(chan struct{})
	session.monitorProve = func(context.Context, time.Duration) error {
		close(entered)
		return nil
	}
	capability, err := newProcessCapability(session, time.Hour, 2*time.Millisecond)
	if err != nil {
		t.Fatalf("newProcessCapability: %v", err)
	}
	concrete := capability.(*processCapability)
	concrete.opMu.Lock()
	attempted := make(chan struct{})
	probeDone := make(chan error, 1)
	go func() {
		probeDone <- concrete.monitorPossessionOnce(context.Background(), func() { close(attempted) })
	}()
	select {
	case <-attempted:
	case <-time.After(time.Second):
		concrete.opMu.Unlock()
		t.Fatal("monitor did not reach the canonical operation boundary")
	}
	select {
	case <-capability.Done():
		concrete.opMu.Unlock()
		t.Fatal("operation contention terminalized process capability")
	default:
	}
	concrete.opMu.Unlock()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("monitor did not resume after operation boundary released")
	}
	if err := <-probeDone; err != nil {
		t.Fatalf("monitor proof after operation boundary: %v", err)
	}
	if err := capability.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestProcessCapabilityPreservesOnlyExactSuccessorTerminalEvidence(t *testing.T) {
	capability, session, _ := testCapability(t)
	successor := uuid.NewString()
	session.callback(TerminalResult{Cause: TerminalOwnershipSuperseded, SuccessorAuthorityID: successor})
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != TerminalOwnershipSuperseded || result.SuccessorAuthorityID != successor {
		t.Fatalf("exact successor terminal result=%#v ok=%v", result, ok)
	}

	capability, session, _ = testCapability(t)
	session.callback(TerminalResult{Cause: TerminalOwnershipSuperseded})
	result, ok = capability.TerminalResult()
	if !ok || result.Cause != TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
		t.Fatalf("unproved successor terminal result=%#v ok=%v", result, ok)
	}
}
