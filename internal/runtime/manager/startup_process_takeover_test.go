package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/google/uuid"
)

type startupProcessTakeoverStore struct {
	ManagerPersistence
	target        ProcessExecutionBinding
	agents        []PersistedAgent
	states        map[string]AgentLifecycleState
	requests      []AgentLifecycleTransition
	failCommitAt  int
	commitAttempt int
}

func (s *startupProcessTakeoverStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return append([]PersistedAgent(nil), s.agents...), nil
}

func (s *startupProcessTakeoverStore) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	return s.target, s.target.Validate()
}

func (s *startupProcessTakeoverStore) LoadAgentLifecycleState(_ context.Context, identity runtimeagentidentity.Identity) (AgentLifecycleState, bool, error) {
	key, err := identity.Fingerprint()
	if err != nil {
		return AgentLifecycleState{}, false, err
	}
	state, ok := s.states[key]
	return state, ok, nil
}

func (s *startupProcessTakeoverStore) ListDurableAgentLifecycleStates(context.Context) ([]AgentLifecycleState, error) {
	states := make([]AgentLifecycleState, 0, len(s.states))
	for _, state := range s.states {
		states = append(states, state)
	}
	return states, nil
}

func (s *startupProcessTakeoverStore) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	s.commitAttempt++
	s.requests = append(s.requests, req)
	if s.commitAttempt == s.failCommitAt {
		return AgentLifecycleTransitionResult{}, errors.New("injected process takeover interruption")
	}
	key, err := req.Identity.Fingerprint()
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	state, ok := s.states[key]
	if !ok {
		return AgentLifecycleTransitionResult{}, errors.New("lifecycle state is missing")
	}
	result := AgentLifecycleTransitionResult{
		OperationID: req.OperationID, Identity: req.Identity, AgentID: req.AgentID,
		PreviousEpoch: state.RuntimeEpoch, PreviousGeneration: state.Generation, PreviousPhase: state.Phase,
		RuntimeEpoch: req.TargetEpoch, Generation: req.TargetGeneration, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode, Topology: req.Topology,
		ProcessBinding: s.target,
	}
	state.RuntimeEpoch = result.RuntimeEpoch
	state.Generation = result.Generation
	state.Phase = result.Phase
	state.ProcessBinding = result.ProcessBinding
	s.states[key] = state
	for i := range s.agents {
		candidate, identityErr := s.agents[i].Config.ConcreteIdentity()
		if identityErr == nil && candidate.Normalize() == req.Identity.Normalize() {
			s.agents[i].LifecycleEpoch = result.RuntimeEpoch
			s.agents[i].LifecycleGeneration = result.Generation
			s.agents[i].LifecyclePhase = result.Phase
			s.agents[i].ProcessBinding = result.ProcessBinding
		}
	}
	return result, nil
}

func TestStartupProcessTakeoverRebindsStaticAndReadinessCellsBeforeHydrationAndResumes(t *testing.T) {
	staticTopology := managerTestTopologyAdmission(t)
	readinessTopology, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "review/instance-1", "plan-v1")
	if err != nil {
		t.Fatalf("FlowReadinessAdmission: %v", err)
	}
	otherTopology, err := runtimeagenttopology.StaticAdmission(
		"other-source-revision",
		"bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"persisted",
		runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("StaticAdmission for unrelated source: %v", err)
	}
	predecessor := startupProcessBinding(
		managerTestTopologyBundleHash, "ephemeral",
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		"cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		"dddddddd-dddd-4ddd-8ddd-dddddddddddd", 1,
	)
	successor := startupProcessBinding(
		managerTestTopologyBundleHash, "ephemeral",
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
		"44444444-4444-4444-8444-444444444444", 2,
	)
	unrelated := startupProcessBinding(
		otherTopology.Authority.Static.BundleHash, otherTopology.Authority.Static.BundleSource,
		"55555555-5555-4555-8555-555555555555",
		"66666666-6666-4666-8666-666666666666",
		"77777777-7777-4777-8777-777777777777",
		"88888888-8888-4888-8888-888888888888", 1,
	)
	records := []PersistedAgent{
		startupTakeoverAgent(t, "static-agent", staticTopology, predecessor),
		startupTakeoverAgent(t, "readiness-agent", readinessTopology, predecessor),
		startupTakeoverAgent(t, "terminated-static-agent", staticTopology, predecessor),
		startupTakeoverAgent(t, "other-agent", otherTopology, unrelated),
	}
	records[2].Status = "terminated"
	records[2].LifecyclePhase = AgentLifecycleTerminated
	records[2].LifecycleRunMode = AgentRunModeStopped
	store := &startupProcessTakeoverStore{
		target: successor, agents: append([]PersistedAgent(nil), records...), states: map[string]AgentLifecycleState{}, failCommitAt: 2,
	}
	for _, record := range records {
		identity, identityErr := record.Config.ConcreteIdentity()
		if identityErr != nil {
			t.Fatalf("ConcreteIdentity: %v", identityErr)
		}
		key, _ := identity.Fingerprint()
		store.states[key] = AgentLifecycleState{
			Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: record.LifecycleEpoch,
			Generation: record.LifecycleGeneration, Phase: record.LifecyclePhase,
			ConfigRevision: "config-v1", RunMode: record.LifecycleRunMode,
			Topology: record.Topology, ProcessBinding: record.ProcessBinding,
		}
	}
	manager := &AgentManager{
		store:     store,
		lifecycle: newAgentLifecycleCoordinator(store, nil, nil, store, nil),
		roles:     PersistenceRoles{LifecycleCensus: store, LifecycleState: store},
	}

	if err := manager.RebindLifecycleExecutionForStartup(context.Background()); err == nil || !strings.Contains(err.Error(), "injected process takeover interruption") {
		t.Fatalf("first takeover error = %v, want injected interruption", err)
	}
	if got := len(manager.lifecycle.cells); got != 0 {
		t.Fatalf("process-local lifecycle cells = %d before durable takeover completed, want zero", got)
	}
	store.failCommitAt = 0
	if err := manager.RebindLifecycleExecutionForStartup(context.Background()); err != nil {
		t.Fatalf("resume process takeover: %v", err)
	}
	if got := len(store.requests); got != 4 {
		t.Fatalf("process takeover attempts = %d, want one success, one interrupted attempt, and two resumed successes", got)
	}
	for _, request := range store.requests {
		if request.OperationKind != "process_takeover" {
			t.Fatalf("operation kind = %q, want process_takeover", request.OperationKind)
		}
	}
	for i, record := range store.agents {
		if i < 3 {
			if !record.ProcessBinding.Equal(successor) || record.LifecycleGeneration != records[i].LifecycleGeneration+1 {
				t.Fatalf("rebound record %d = %#v generation=%d", i, record.ProcessBinding, record.LifecycleGeneration)
			}
			continue
		}
		if !record.ProcessBinding.Equal(unrelated) || record.LifecycleGeneration != records[i].LifecycleGeneration {
			t.Fatalf("unrelated source record changed: %#v generation=%d", record.ProcessBinding, record.LifecycleGeneration)
		}
	}
}

func startupTakeoverAgent(t testing.TB, id string, topology runtimeagenttopology.Admission, binding ProcessExecutionBinding) PersistedAgent {
	t.Helper()
	return PersistedAgent{
		Config: managerTestAgentConfig(runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: id, Role: "worker", Type: "sonnet", Model: "regular", FlowID: "global",
			Identity: runtimeagentidentitytest.RootRuntime(t, id, "startup-process-takeover"),
		}),
		Status: "active", LifecycleEpoch: 7, LifecycleGeneration: 11,
		LifecyclePhase: AgentLifecycleRunning, LifecycleRunMode: AgentRunModeStandard,
		Topology: topology, ProcessBinding: binding,
	}
}

func startupProcessBinding(bundleHash, bundleSource, authorityID, bootID, grantID, runtimeID string, generation uint64) ProcessExecutionBinding {
	return ProcessExecutionBinding{
		ProcessAuthorityID: authorityID, ProcessOwnerID: "startup-process-owner",
		ProcessBootID: bootID, GenerationGrantID: grantID,
		BundleHash: bundleHash, BundleSource: bundleSource,
		RuntimeInstanceID: runtimeID, RuntimeGeneration: generation,
	}
}
