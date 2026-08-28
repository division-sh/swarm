package agentfixture

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

const agentFixtureBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type Store interface {
	runtimestartupownership.Store
	runtimemanager.AgentLifecycleStateReader
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
}

type lifecycleStore struct {
	t        testing.TB
	selected Store
	session  *fixtureSession
}

func (s *lifecycleStore) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return Commit(s.t, ctx, s.selected, req)
}

func Lifecycle(t testing.TB, selected Store) runtimemanager.AgentLifecyclePersistence {
	t.Helper()
	session, err := fixtureSessionFor(t, context.Background(), selected)
	if err != nil {
		t.Fatalf("acquire agent lifecycle fixture process capability: %v", err)
	}
	return &lifecycleStore{t: t, selected: selected, session: session}
}

func (s *lifecycleStore) ProcessExecutionBinding() (runtimemanager.ProcessExecutionBinding, error) {
	if s == nil || s.session == nil {
		return runtimemanager.ProcessExecutionBinding{}, fmt.Errorf("agent lifecycle fixture process capability is required")
	}
	s.session.mu.Lock()
	defer s.session.mu.Unlock()
	if s.session.grant == nil {
		return runtimemanager.ProcessExecutionBinding{}, fmt.Errorf("agent lifecycle fixture generation grant is required")
	}
	return s.session.grant.ProcessExecutionBinding()
}

type fixtureSession struct {
	mu                sync.Mutex
	capability        runtimestartupownership.ProcessCapability
	runtimeInstanceID string
	grant             runtimestartupownership.GenerationGrant
	grantRevision     string
}

var fixtureSessions sync.Map

func fixtureSessionFor(t testing.TB, ctx context.Context, selected Store) (*fixtureSession, error) {
	t.Helper()
	key := fmt.Sprintf("%p:%p", t, selected)
	if existing, ok := fixtureSessions.Load(key); ok {
		return existing.(*fixtureSession), nil
	}
	runtimeInstanceID := uuid.NewString()
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "storetest-agent-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		return nil, err
	}
	created := &fixtureSession{capability: capability, runtimeInstanceID: runtimeInstanceID}
	actual, loaded := fixtureSessions.LoadOrStore(key, created)
	if loaded {
		_ = capability.Release(context.Background())
		return actual.(*fixtureSession), nil
	}
	t.Cleanup(func() {
		fixtureSessions.Delete(key)
		_ = capability.Release(context.Background())
	})
	return created, nil
}

// ProcessCapability returns the retained capability that owns this test's
// admitted agent fixtures. Startup tests consume this owner instead of
// acquiring a competing selected-store process.
func ProcessCapability(t testing.TB, ctx context.Context, selected Store) (runtimestartupownership.ProcessCapability, error) {
	t.Helper()
	session, err := fixtureSessionFor(t, ctx, selected)
	if err != nil {
		return nil, err
	}
	return session.capability, nil
}

func validateFixtureStaticSourceSetRebind(ctx context.Context, selected Store, skipIdentityKey string) error {
	agents, err := selected.LoadAgents(ctx)
	if err != nil {
		return err
	}
	for _, rec := range agents {
		if rec.ProcessBinding.BundleHash != agentFixtureBundleHash || rec.ProcessBinding.BundleSource != "ephemeral" {
			continue
		}
		identity, identityErr := rec.Config.ConcreteIdentity()
		if identityErr != nil {
			return identityErr
		}
		identityKey, identityErr := identity.Fingerprint()
		if identityErr != nil {
			return identityErr
		}
		if identityKey == skipIdentityKey {
			continue
		}
		if err := rec.Topology.Validate(); err != nil {
			return fmt.Errorf("agent fixture source-set rebind found invalid topology for %s: %w", identity.Description(), err)
		}
		if rec.Topology.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan {
			return fmt.Errorf(
				"agent fixture cannot change static source-set authority while %s retains %s topology",
				identity.Description(), rec.Topology.Authority.Kind,
			)
		}
	}
	return nil
}

func (s *fixtureSession) grantForPlan(ctx context.Context, selected Store, plan runtimeagenttopology.SourceSetPlan, skipIdentityKey string) (runtimestartupownership.GenerationGrant, error) {
	if s.grant != nil && s.grantRevision == plan.Revision {
		if _, err := s.grant.Evidence(); err == nil {
			return s.grant, nil
		}
	}
	grant, err := s.capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: agentFixtureBundleHash, BundleSource: "ephemeral", RuntimeInstanceID: s.runtimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		return nil, err
	}
	target, err := grant.ProcessExecutionBinding()
	if err != nil {
		return nil, err
	}
	agents, err := selected.LoadAgents(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(agents, func(i, j int) bool {
		left, _ := agents[i].Config.ConcreteIdentity()
		right, _ := agents[j].Config.ConcreteIdentity()
		leftKey, _ := left.Fingerprint()
		rightKey, _ := right.Fingerprint()
		return leftKey < rightKey
	})
	for _, rec := range agents {
		if rec.ProcessBinding.BundleHash != agentFixtureBundleHash || rec.ProcessBinding.BundleSource != "ephemeral" || rec.ProcessBinding.Equal(target) {
			continue
		}
		identity, identityErr := rec.Config.ConcreteIdentity()
		if identityErr != nil {
			return nil, identityErr
		}
		identityKey, identityErr := identity.Fingerprint()
		if identityErr != nil {
			return nil, identityErr
		}
		if identityKey == skipIdentityKey {
			continue
		}
		if err := rec.Topology.Validate(); err != nil {
			return nil, fmt.Errorf("agent fixture source-set rebind found invalid topology for %s: %w", identity.Description(), err)
		}
		if rec.Topology.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan {
			return nil, fmt.Errorf(
				"agent fixture cannot change static source-set authority while %s retains %s topology",
				identity.Description(), rec.Topology.Authority.Kind,
			)
		}
		state, found, stateErr := selected.LoadAgentLifecycleState(ctx, identity)
		if stateErr != nil {
			return nil, stateErr
		}
		if !found {
			return nil, fmt.Errorf("agent fixture lifecycle state disappeared for %s", identity.Description())
		}
		kind := "source_set_rebind"
		targetEpoch, targetGeneration := state.RuntimeEpoch, state.Generation
		if state.ProcessBinding.ProcessAuthorityID != target.ProcessAuthorityID ||
			state.ProcessBinding.ProcessOwnerID != target.ProcessOwnerID ||
			state.ProcessBinding.ProcessBootID != target.ProcessBootID {
			kind = "process_takeover"
			targetEpoch++
			targetGeneration++
		}
		topology, topologyErr := runtimeagenttopology.StaticAdmission(
			plan.Revision, agentFixtureBundleHash, "ephemeral", runtimeagenttopology.LifetimeDurableManaged,
		)
		if topologyErr != nil {
			return nil, topologyErr
		}
		if _, commitErr := grant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: kind, RequestHash: uuid.NewString(),
			Identity: identity, AgentID: identity.AgentID(), Trigger: kind,
			ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
			TargetEpoch: targetEpoch, TargetGeneration: targetGeneration, TargetPhase: state.Phase,
			ConfigRevision: state.ConfigRevision, RunMode: state.RunMode, Topology: topology, Now: time.Now().UTC(),
		}); commitErr != nil {
			return nil, commitErr
		}
	}
	s.grant = grant
	s.grantRevision = plan.Revision
	return grant, nil
}

// Upsert admits a durable agent fixture through the same complete source-set
// and generation-grant boundary as production startup.
func Upsert(t testing.TB, ctx context.Context, selected Store, rec runtimemanager.PersistedAgent) error {
	t.Helper()
	if selected == nil {
		return fmt.Errorf("agent fixture selected store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	configRevision, err := canonicaljson.Hash(rec.Config)
	if err != nil {
		return err
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: agentFixtureBundleHash, BundleSource: "ephemeral"}
	session, err := fixtureSessionFor(t, ctx, selected)
	if err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	current, exists, err := session.capability.CurrentSourceSet(ctx)
	if err != nil {
		return err
	}
	sources := append([]runtimeagenttopology.SourceCoordinate(nil), current.Sources...)
	foundSource := false
	for _, source := range sources {
		if source.Normalize().Key() == coordinate.Key() {
			foundSource = true
			break
		}
	}
	if !foundSource {
		sources = append(sources, coordinate)
	}
	agents := append([]runtimeagenttopology.DesiredAgent(nil), current.Agents...)
	key, err := identity.Normalize().Fingerprint()
	if err != nil {
		return err
	}
	replaced := false
	for i := range agents {
		candidate, keyErr := agents[i].Identity.Normalize().Fingerprint()
		if keyErr != nil {
			return keyErr
		}
		if candidate == key {
			agents[i] = runtimeagenttopology.DesiredAgent{Identity: identity, Source: coordinate, ConfigRevision: configRevision}
			replaced = true
			break
		}
	}
	if !replaced {
		agents = append(agents, runtimeagenttopology.DesiredAgent{Identity: identity, Source: coordinate, ConfigRevision: configRevision})
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan(sources, agents)
	if err != nil {
		return err
	}
	skipRebindKey := ""
	if replaced {
		skipRebindKey = key
	}
	if !exists || current.Revision != plan.Revision {
		if err := validateFixtureStaticSourceSetRebind(ctx, selected, skipRebindKey); err != nil {
			return err
		}
		commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
		if exists {
			commit.ExpectedRevision = current.Revision
			_, err = session.capability.ReplaceSourceSet(ctx, commit)
		} else {
			_, err = session.capability.InstallCompleteSourceSet(ctx, commit)
		}
		if err != nil {
			return err
		}
	}
	grant, err := session.grantForPlan(ctx, selected, plan, skipRebindKey)
	if err != nil {
		return err
	}
	topology, err := runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		return err
	}
	rec.Topology = topology
	state, found, err := loadState(ctx, selected, identity)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	transition := runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(), Identity: identity, AgentID: identity.AgentID(),
		Trigger: "storetest_fixture", TargetEpoch: 1, TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
		ConfigRevision: configRevision, RunMode: runtimemanager.AgentRunModeStopped, Agent: &rec, Topology: topology, Now: now,
	}
	if found {
		transition.OperationKind = "reconfigure"
		transition.ExpectedEpoch = state.RuntimeEpoch
		transition.ExpectedGeneration = state.Generation
		transition.ExpectedPhase = state.Phase
		transition.TargetEpoch = state.RuntimeEpoch
		transition.TargetGeneration = state.Generation + 1
		transition.TargetPhase = state.Phase
		transition.RunMode = state.RunMode
		binding, bindingErr := grant.ProcessExecutionBinding()
		if bindingErr != nil {
			return bindingErr
		}
		classifyFixtureTransition(&transition, state, binding)
	}
	result, err := grant.CommitAgentLifecycleTransition(ctx, transition)
	if err != nil {
		return err
	}
	if !found && rec.Status != "terminated" {
		_, err = grant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: "start", RequestHash: uuid.NewString(), Identity: identity, AgentID: identity.AgentID(),
			Trigger: "storetest_fixture", ExpectedEpoch: result.RuntimeEpoch, ExpectedGeneration: result.Generation, ExpectedPhase: result.Phase,
			TargetEpoch: result.RuntimeEpoch, TargetGeneration: result.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
			ConfigRevision: configRevision, RunMode: runtimemanager.AgentRunModeStandard, Topology: topology, Now: now.Add(time.Nanosecond),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Commit admits an exact lifecycle fixture through a retained process
// capability. Static transitions update the complete fixture source set;
// flow-readiness transitions retain that set and its current generation grant.
func Commit(t testing.TB, ctx context.Context, selected Store, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	t.Helper()
	if selected == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle fixture selected store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session, err := fixtureSessionFor(t, ctx, selected)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	current, exists, err := session.capability.CurrentSourceSet(ctx)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: agentFixtureBundleHash, BundleSource: "ephemeral"}
	sources := append([]runtimeagenttopology.SourceCoordinate(nil), current.Sources...)
	foundSource := false
	for _, source := range sources {
		if source.Normalize().Key() == coordinate.Key() {
			foundSource = true
			break
		}
	}
	if !foundSource {
		sources = append(sources, coordinate)
	}
	agents := append([]runtimeagenttopology.DesiredAgent(nil), current.Agents...)
	identity := req.Identity.Normalize()
	key, err := identity.Fingerprint()
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	stateBeforePlan, stateBeforePlanFound, err := loadState(ctx, selected, identity)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if stateBeforePlanFound && req.OperationKind != "spawn" && req.OperationKind != "reconfigure" {
		req.ConfigRevision = stateBeforePlan.ConfigRevision
	}
	flowReadiness := req.Topology.Validate() == nil && req.Topology.Authority.Kind == runtimeagenttopology.AuthorityFlowReadinessPlan
	if flowReadiness {
		for _, agent := range agents {
			candidate, keyErr := agent.Identity.Normalize().Fingerprint()
			if keyErr != nil {
				return runtimemanager.AgentLifecycleTransitionResult{}, keyErr
			}
			if candidate == key {
				return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf(
					"flow-readiness fixture %s conflicts with static desired-agent authority", identity.Description(),
				)
			}
		}
	} else {
		filtered := agents[:0]
		for _, agent := range agents {
			candidate, keyErr := agent.Identity.Normalize().Fingerprint()
			if keyErr != nil {
				return runtimemanager.AgentLifecycleTransitionResult{}, keyErr
			}
			if candidate != key {
				filtered = append(filtered, agent)
			}
		}
		agents = filtered
		if req.TargetPhase != runtimemanager.AgentLifecycleTerminated {
			agents = append(agents, runtimeagenttopology.DesiredAgent{Identity: identity, Source: coordinate, ConfigRevision: req.ConfigRevision})
		}
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan(sources, agents)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	skipRebindKey := ""
	if stateBeforePlanFound {
		skipRebindKey = key
	}
	if !exists || current.Revision != plan.Revision {
		if err := validateFixtureStaticSourceSetRebind(ctx, selected, skipRebindKey); err != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, err
		}
		commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
		if exists {
			commit.ExpectedRevision = current.Revision
			_, err = session.capability.ReplaceSourceSet(ctx, commit)
		} else {
			_, err = session.capability.InstallCompleteSourceSet(ctx, commit)
		}
		if err != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, err
		}
	}
	grant, err := session.grantForPlan(ctx, selected, plan, skipRebindKey)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if req.Topology.Validate() != nil || req.Topology.Authority.Kind == runtimeagenttopology.AuthorityStaticDeclarationPlan {
		req.Topology, err = runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
		if err != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, err
		}
		if req.Agent != nil {
			req.Agent.Topology = req.Topology
		}
	}
	if stateBeforePlanFound {
		stateAfterPlan, found, stateErr := loadState(ctx, selected, identity)
		if stateErr != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, stateErr
		}
		if !found {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent fixture lifecycle state disappeared for %s", identity.Description())
		}
		if req.ExpectedEpoch == stateBeforePlan.RuntimeEpoch && req.ExpectedGeneration == stateBeforePlan.Generation && req.ExpectedPhase == stateBeforePlan.Phase {
			req.ExpectedEpoch = stateAfterPlan.RuntimeEpoch
			req.ExpectedGeneration = stateAfterPlan.Generation
			req.ExpectedPhase = stateAfterPlan.Phase
		}
		binding, bindingErr := grant.ProcessExecutionBinding()
		if bindingErr != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, bindingErr
		}
		classifyFixtureTransition(&req, stateAfterPlan, binding)
	}
	return grant.CommitAgentLifecycleTransition(ctx, req)
}

func classifyFixtureTransition(req *runtimemanager.AgentLifecycleTransition, previous runtimemanager.AgentLifecycleState, target runtimemanager.ProcessExecutionBinding) {
	if req == nil || previous.ProcessBinding.Equal(target) {
		return
	}
	if previous.ProcessBinding.ProcessAuthorityID == target.ProcessAuthorityID &&
		previous.ProcessBinding.ProcessOwnerID == target.ProcessOwnerID &&
		previous.ProcessBinding.ProcessBootID == target.ProcessBootID {
		if req.TargetPhase == runtimemanager.AgentLifecycleTerminated {
			req.OperationKind = "source_set_retire"
		} else {
			req.OperationKind = "source_set_rebind"
		}
		return
	}
	req.OperationKind = "process_takeover"
	req.TargetEpoch = previous.RuntimeEpoch + 1
	req.TargetGeneration = previous.Generation + 1
}

func loadState(ctx context.Context, selected Store, identity runtimeagentidentity.Identity) (runtimemanager.AgentLifecycleState, bool, error) {
	if reader, ok := selected.(runtimemanager.AgentLifecycleStateReader); ok {
		return reader.LoadAgentLifecycleState(ctx, identity)
	}
	return runtimemanager.AgentLifecycleState{}, false, nil
}
