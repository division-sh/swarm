package manager

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// Recovery hydrates the exact child's existing declaration. Authoritative
// manager start owns its fenced takeover before route publication; hydration
// cannot create a second durable actor or adopt another run's declaration.
func (am *AgentManager) MaterializeSelectedDeclarationAgentForRecovery(ctx context.Context, desired PersistedAgent) error {
	if am == nil || am.lifecycle == nil || am.lifecycle.stateReader == nil {
		return fmt.Errorf("selected declaration recovery requires its native lifecycle owner")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireAdmittedAgentIdentity(ctx, desired.Config); err != nil {
		return err
	}
	identity, _ := desired.Config.ConcreteIdentity()
	if err := desired.Topology.Validate(); err != nil {
		return err
	}
	if identity.Route.Presence != agentidentity.RouteRoot || desired.Topology.Authority.Kind != agenttopology.AuthoritySelectedForkDeclarationPlan ||
		desired.Topology.Authority.Selected.RunID != identity.RunID {
		return fmt.Errorf("selected declaration recovery requires its exact child root declaration")
	}
	if err := am.requireRunExecutionOwnership(ctx, identity.RunID); err != nil {
		return err
	}
	previous, found, err := am.lifecycle.stateReader.LoadAgentLifecycleState(ctx, identity)
	if err != nil {
		return err
	}
	if !found || previous.Phase == AgentLifecycleTerminated {
		return am.MaterializeAdmittedAgent(ctx, desired)
	}
	if _, exists := am.lifecycle.executionSnapshotByIdentity(identity); exists {
		return fmt.Errorf("%w: %s", ErrAgentAlreadyExists, identity.AgentID())
	}
	rebound, err := selectedDeclarationRecoveryRecord(identity, previous, desired)
	if err != nil {
		return err
	}
	return am.MaterializeAdmittedAgentForExecution(ctx, rebound)
}

func selectedDeclarationRecoveryRecord(identity agentidentity.Identity, previous AgentLifecycleState, desired PersistedAgent) (PersistedAgent, error) {
	revision, err := lifecycleConfigRevision(desired)
	if err != nil {
		return PersistedAgent{}, err
	}
	if previous.Identity != identity || previous.AgentID != identity.AgentID() || previous.ConfigRevision != revision ||
		!previous.Topology.Equal(desired.Topology) || previous.RuntimeEpoch <= 0 || previous.Generation == 0 || previous.Generation == ^uint64(0) ||
		(previous.Phase != AgentLifecycleRegistered && previous.Phase != AgentLifecycleRunning) {
		return PersistedAgent{}, fmt.Errorf("selected declaration recovery differs from retained actor identity, configuration or lifecycle")
	}
	if err := previous.ProcessBinding.Validate(); err != nil {
		return PersistedAgent{}, err
	}
	desired.LifecycleEpoch, desired.LifecycleGeneration = previous.RuntimeEpoch, previous.Generation
	desired.LifecyclePhase, desired.LifecycleRunMode, desired.ProcessBinding = previous.Phase, previous.RunMode, previous.ProcessBinding
	return desired, nil
}
