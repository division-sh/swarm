package agentfixture

import (
	"context"
	"fmt"
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
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
}

type lifecycleStore struct {
	selected Store
}

func (s lifecycleStore) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return Commit(ctx, s.selected, req)
}

func Lifecycle(selected Store) runtimemanager.AgentLifecyclePersistence {
	return lifecycleStore{selected: selected}
}

// Upsert admits a durable agent fixture through the same complete source-set
// and generation-grant boundary as production startup.
func Upsert(ctx context.Context, selected Store, rec runtimemanager.PersistedAgent) error {
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
	runtimeInstanceID := uuid.NewString()
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "storetest-agent-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		return err
	}
	release := true
	defer func() {
		if release {
			_ = capability.Release(context.Background())
		}
	}()
	current, exists, err := capability.CurrentSourceSet(ctx)
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
	commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
	if exists {
		commit.ExpectedRevision = current.Revision
		_, err = capability.ReplaceSourceSet(ctx, commit)
	} else {
		_, err = capability.InstallCompleteSourceSet(ctx, commit)
	}
	if err != nil {
		return err
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource, RuntimeInstanceID: runtimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
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
	if err := capability.Release(ctx); err != nil {
		return err
	}
	release = false
	return nil
}

// Commit admits an exact lifecycle fixture through a retained process
// capability. Invalid/absent topology in legacy fixtures is replaced by a
// complete static declaration plan. Flow-readiness topology remains valid only
// when the caller supplied its matching retained grant.
func Commit(ctx context.Context, selected Store, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	if selected == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle fixture selected store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runtimeInstanceID := uuid.NewString()
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "storetest-agent-lifecycle-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	defer func() { _ = capability.Release(context.Background()) }()
	current, exists, err := capability.CurrentSourceSet(ctx)
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
	commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
	if exists {
		commit.ExpectedRevision = current.Revision
		_, err = capability.ReplaceSourceSet(ctx, commit)
	} else {
		_, err = capability.InstallCompleteSourceSet(ctx, commit)
	}
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource, RuntimeInstanceID: runtimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
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
	return grant.CommitAgentLifecycleTransition(ctx, req)
}

func loadState(ctx context.Context, selected Store, identity runtimeagentidentity.Identity) (runtimemanager.AgentLifecycleState, bool, error) {
	if reader, ok := selected.(runtimemanager.AgentLifecycleStateReader); ok {
		return reader.LoadAgentLifecycleState(ctx, identity)
	}
	return runtimemanager.AgentLifecycleState{}, false, nil
}
