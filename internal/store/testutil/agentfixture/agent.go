package agentfixture

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	runtimemanager.AgentLifecycleCellCensus
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
}

type lifecycleStore struct {
	t        testing.TB
	selected Store
}

func (s *lifecycleStore) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return CommitExact(s.t, ctx, s.selected, req)
}

func Lifecycle(t testing.TB, selected Store) runtimemanager.AgentLifecyclePersistence {
	t.Helper()
	return &lifecycleStore{t: t, selected: selected}
}

func (s *lifecycleStore) ProcessExecutionBinding() (runtimemanager.ProcessExecutionBinding, error) {
	if s == nil || s.selected == nil {
		return runtimemanager.ProcessExecutionBinding{}, fmt.Errorf("agent lifecycle fixture process capability is required")
	}
	session, err := fixtureSessionFor(s.t, context.Background(), s.selected)
	if err != nil {
		return runtimemanager.ProcessExecutionBinding{}, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.grant == nil {
		return runtimemanager.ProcessExecutionBinding{}, fmt.Errorf("agent lifecycle fixture generation grant is required")
	}
	return session.grant.ProcessExecutionBinding()
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

func validateFixtureStaticSourceSetRebind(ctx context.Context, selected Store) ([]runtimemanager.AgentLifecycleState, error) {
	states, err := selected.ListDurableAgentLifecycleStates(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(states, func(i, j int) bool {
		left, _ := states[i].Identity.Normalize().Fingerprint()
		right, _ := states[j].Identity.Normalize().Fingerprint()
		return left < right
	})
	for _, state := range states {
		if state.Phase == runtimemanager.AgentLifecycleTerminated ||
			state.ProcessBinding.BundleHash != agentFixtureBundleHash || state.ProcessBinding.BundleSource != "ephemeral" {
			continue
		}
		identity := state.Identity.Normalize()
		if err := identity.Validate(); err != nil {
			return nil, fmt.Errorf("agent fixture source-set rebind found invalid identity: %w", err)
		}
		if err := state.Topology.Validate(); err != nil {
			return nil, fmt.Errorf("agent fixture source-set rebind found invalid topology for %s: %w", identity.Description(), err)
		}
		if state.Topology.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan {
			return nil, fmt.Errorf(
				"agent fixture cannot change static source-set authority while %s retains %s topology",
				identity.Description(), state.Topology.Authority.Kind,
			)
		}
	}
	return states, nil
}

func (s *fixtureSession) grantForStaticPlan(ctx context.Context, selected Store, plan runtimeagenttopology.SourceSetPlan, skipIdentityKey string) (runtimestartupownership.GenerationGrant, error) {
	states, err := validateFixtureStaticSourceSetRebind(ctx, selected)
	if err != nil {
		return nil, err
	}
	if s.grant != nil && s.grantRevision == plan.Revision {
		if _, err := s.grant.Evidence(); err == nil {
			return s.grant, nil
		}
	}
	grant, err := s.issueGrant(ctx, plan)
	if err != nil {
		return nil, err
	}
	target, err := grant.ProcessExecutionBinding()
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if state.Phase == runtimemanager.AgentLifecycleTerminated ||
			state.ProcessBinding.BundleHash != agentFixtureBundleHash || state.ProcessBinding.BundleSource != "ephemeral" ||
			state.ProcessBinding.Equal(target) {
			continue
		}
		identity := state.Identity.Normalize()
		identityKey, identityErr := identity.Fingerprint()
		if identityErr != nil {
			return nil, identityErr
		}
		if identityKey == skipIdentityKey {
			continue
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

func (s *fixtureSession) grantForExactPlan(ctx context.Context, plan runtimeagenttopology.SourceSetPlan) (runtimestartupownership.GenerationGrant, error) {
	if s.grant != nil && s.grantRevision == plan.Revision {
		if _, err := s.grant.Evidence(); err == nil {
			return s.grant, nil
		}
	}
	grant, err := s.issueGrant(ctx, plan)
	if err != nil {
		return nil, err
	}
	s.grant = grant
	s.grantRevision = plan.Revision
	return grant, nil
}

func (s *fixtureSession) issueGrant(ctx context.Context, plan runtimeagenttopology.SourceSetPlan) (runtimestartupownership.GenerationGrant, error) {
	return s.capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: agentFixtureBundleHash, BundleSource: "ephemeral", RuntimeInstanceID: s.runtimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
}

func admissionIsZero(admission runtimeagenttopology.Admission) bool {
	return admission.Lifetime == "" && admission.Authority.Kind == "" &&
		admission.Authority.Static == nil && admission.Authority.Readiness == nil && admission.Authority.Ephemeral == nil
}

func validateSyntheticStaticAdmission(admission runtimeagenttopology.Admission, field string) error {
	if !admissionIsZero(admission) {
		return fmt.Errorf("%s must not carry topology; use the exact durable fixture operation", field)
	}
	return nil
}

func validateSyntheticStaticTransition(req runtimemanager.AgentLifecycleTransition) (runtimeagentidentity.Identity, string, error) {
	if err := validateSyntheticStaticAdmission(req.Topology, "synthetic static lifecycle transition"); err != nil {
		return runtimeagentidentity.Identity{}, "", err
	}
	identity := req.Identity.Normalize()
	key, err := identity.Fingerprint()
	if err != nil {
		return runtimeagentidentity.Identity{}, "", err
	}
	if req.Agent == nil {
		return identity, key, nil
	}
	if err := validateSyntheticStaticAdmission(req.Agent.Topology, "synthetic static lifecycle agent"); err != nil {
		return runtimeagentidentity.Identity{}, "", err
	}
	agentIdentity, err := req.Agent.Config.ConcreteIdentity()
	if err != nil {
		return runtimeagentidentity.Identity{}, "", err
	}
	if agentIdentity.Normalize() != identity {
		return runtimeagentidentity.Identity{}, "", fmt.Errorf("synthetic static lifecycle agent identity does not match transition identity")
	}
	return identity, key, nil
}

func validateExactDurableTransition(req runtimemanager.AgentLifecycleTransition) (runtimeagentidentity.Identity, string, runtimeagenttopology.AuthorityKind, error) {
	if err := req.Topology.Validate(); err != nil {
		return runtimeagentidentity.Identity{}, "", "", fmt.Errorf("exact lifecycle fixture topology: %w", err)
	}
	if req.Topology.Lifetime != runtimeagenttopology.LifetimeDurableManaged {
		return runtimeagentidentity.Identity{}, "", "", fmt.Errorf("exact lifecycle fixture requires durable managed topology")
	}
	switch req.Topology.Authority.Kind {
	case runtimeagenttopology.AuthorityStaticDeclarationPlan, runtimeagenttopology.AuthorityFlowReadinessPlan:
	case runtimeagenttopology.AuthorityEphemeralExecution:
		return runtimeagentidentity.Identity{}, "", "", fmt.Errorf("exact lifecycle fixture cannot persist ephemeral execution topology")
	default:
		return runtimeagentidentity.Identity{}, "", "", fmt.Errorf("exact lifecycle fixture topology kind %q is unsupported", req.Topology.Authority.Kind)
	}
	identity := req.Identity.Normalize()
	key, err := identity.Fingerprint()
	if err != nil {
		return runtimeagentidentity.Identity{}, "", "", err
	}
	if req.Agent != nil {
		if !req.Agent.Topology.Equal(req.Topology) {
			return runtimeagentidentity.Identity{}, "", "", fmt.Errorf("exact lifecycle fixture agent topology does not match transition topology")
		}
		agentIdentity, identityErr := req.Agent.Config.ConcreteIdentity()
		if identityErr != nil {
			return runtimeagentidentity.Identity{}, "", "", identityErr
		}
		if agentIdentity.Normalize() != identity {
			return runtimeagentidentity.Identity{}, "", "", fmt.Errorf("exact lifecycle fixture agent identity does not match transition identity")
		}
	}
	return identity, key, req.Topology.Authority.Kind, nil
}

// UpsertStatic admits a synthetic static durable agent through the complete
// source-set and generation-grant boundary. Caller-provided topology is never
// interpreted or overwritten.
func UpsertStatic(t testing.TB, ctx context.Context, selected Store, rec runtimemanager.PersistedAgent) error {
	t.Helper()
	if selected == nil {
		return fmt.Errorf("agent fixture selected store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSyntheticStaticAdmission(rec.Topology, "synthetic static agent"); err != nil {
		return err
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	configRevision, err := canonicaljson.Hash(rec.Config)
	if err != nil {
		return err
	}
	configRevision = strings.TrimPrefix(configRevision, "sha256:")
	if _, err := validateFixtureStaticSourceSetRebind(ctx, selected); err != nil {
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
	if _, err := validateFixtureStaticSourceSetRebind(ctx, selected); err != nil {
		return err
	}
	if !exists || current.Revision != plan.Revision {
		commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
		if exists {
			commit.ExpectedRevision = current.Revision
			_, err = session.capability.RestoreSourceSet(ctx, commit)
		} else {
			_, err = session.capability.InstallCompleteSourceSet(ctx, commit)
		}
		if err != nil {
			return err
		}
	}
	grant, err := session.grantForStaticPlan(ctx, selected, plan, skipRebindKey)
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

// CommitStatic applies one synthetic static lifecycle transition. It is the
// only direct lifecycle fixture operation allowed to derive source-set
// membership and static admission.
func CommitStatic(t testing.TB, ctx context.Context, selected Store, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	t.Helper()
	if selected == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle fixture selected store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	identity, key, err := validateSyntheticStaticTransition(req)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if _, err := validateFixtureStaticSourceSetRebind(ctx, selected); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
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
	stateBeforePlan, stateBeforePlanFound, err := loadState(ctx, selected, identity)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if stateBeforePlanFound && req.OperationKind != "spawn" && req.OperationKind != "reconfigure" {
		req.ConfigRevision = stateBeforePlan.ConfigRevision
	}
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
	plan, err := runtimeagenttopology.NewSourceSetPlan(sources, agents)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	skipRebindKey := ""
	if stateBeforePlanFound {
		skipRebindKey = key
	}
	if _, err := validateFixtureStaticSourceSetRebind(ctx, selected); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if !exists || current.Revision != plan.Revision {
		commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
		if exists {
			commit.ExpectedRevision = current.Revision
			_, err = session.capability.RestoreSourceSet(ctx, commit)
		} else {
			_, err = session.capability.InstallCompleteSourceSet(ctx, commit)
		}
		if err != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, err
		}
	}
	grant, err := session.grantForStaticPlan(ctx, selected, plan, skipRebindKey)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	req.Topology, err = runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if req.Agent != nil {
		req.Agent.Topology = req.Topology
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

// CommitExact persists one caller-supplied durable sealed topology. Flow
// readiness never enters the static desired-agent set, and exact static
// authority must already agree with that complete set.
func CommitExact(t testing.TB, ctx context.Context, selected Store, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	t.Helper()
	if selected == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle fixture selected store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	identity, key, kind, err := validateExactDurableTransition(req)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
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
	plan := current
	switch kind {
	case runtimeagenttopology.AuthorityFlowReadinessPlan:
		for _, agent := range current.Agents {
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
			plan, err = runtimeagenttopology.NewSourceSetPlan(sources, current.Agents)
			if err != nil {
				return runtimemanager.AgentLifecycleTransitionResult{}, err
			}
			states, censusErr := selected.ListDurableAgentLifecycleStates(ctx)
			if censusErr != nil {
				return runtimemanager.AgentLifecycleTransitionResult{}, censusErr
			}
			for _, state := range states {
				if state.Phase != runtimemanager.AgentLifecycleTerminated &&
					state.ProcessBinding.BundleHash == agentFixtureBundleHash && state.ProcessBinding.BundleSource == "ephemeral" {
					return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent fixture source coordinate is missing while durable fixture lifecycle cells remain")
				}
			}
			commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
			if exists {
				commit.ExpectedRevision = current.Revision
				_, err = session.capability.RestoreSourceSet(ctx, commit)
			} else {
				_, err = session.capability.InstallCompleteSourceSet(ctx, commit)
			}
			if err != nil {
				return runtimemanager.AgentLifecycleTransitionResult{}, err
			}
		}
	case runtimeagenttopology.AuthorityStaticDeclarationPlan:
		static := req.Topology.Authority.Static
		if !exists || static == nil || static.SourceSetRevision != current.Revision ||
			static.BundleHash != coordinate.BundleHash || static.BundleSource != coordinate.BundleSource {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("exact static lifecycle topology does not match the current fixture source set")
		}
		matched := false
		desiredRevision := ""
		desiredSource := runtimeagenttopology.SourceCoordinate{}
		for _, agent := range current.Agents {
			candidate, keyErr := agent.Identity.Normalize().Fingerprint()
			if keyErr != nil {
				return runtimemanager.AgentLifecycleTransitionResult{}, keyErr
			}
			if candidate == key {
				desiredRevision = agent.ConfigRevision
				desiredSource = agent.Source.Normalize()
				matched = agent.ConfigRevision == req.ConfigRevision && agent.Source.Normalize().Key() == coordinate.Key()
				break
			}
		}
		if !matched {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf(
				"exact static lifecycle topology is not present in the current fixture source set: desired_revision=%q requested_revision=%q desired_source=%q requested_source=%q",
				desiredRevision, req.ConfigRevision, desiredSource.Key(), coordinate.Key(),
			)
		}
	}
	grant, err := session.grantForExactPlan(ctx, plan)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	state, stateFound, err := loadState(ctx, selected, identity)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if stateFound {
		binding, bindingErr := grant.ProcessExecutionBinding()
		if bindingErr != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, bindingErr
		}
		classifyFixtureTransition(&req, state, binding)
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
