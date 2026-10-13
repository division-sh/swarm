package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type unacknowledgedAgentReplacement struct {
	request AgentLifecycleTransition
	outcome AgentLifecycleTransitionResult
}

func sameAgentRowObservation(left, right AgentLifecycleState) bool {
	if !left.Topology.Equal(right.Topology) {
		return false
	}
	// Admission contains pointer arms; equality is over their typed facts.
	left.Topology = right.Topology
	return left == right
}

func (c *agentLifecycleCoordinator) loadAgentView(ctx context.Context, identity agentidentity.Identity) (AgentLifecycleState, bool, error) {
	if c == nil || c.persistence() == nil || c.stateReader == nil {
		return AgentLifecycleState{}, false, errors.New("agent lifecycle requires its durable row owner")
	}
	view, found, err := c.stateReader.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found {
		return AgentLifecycleState{}, found, err
	}
	if view.Identity != identity.Normalize() || view.AgentID != identity.AgentID() {
		return AgentLifecycleState{}, false, errors.New("durable agent row returned a different identity")
	}
	return view, true, nil
}

func (c *agentLifecycleCoordinator) committedAgentView(ctx context.Context, result AgentLifecycleTransitionResult) (AgentLifecycleState, error) {
	view, found, err := c.loadAgentView(ctx, result.Identity)
	if err != nil {
		return AgentLifecycleState{}, err
	}
	if !found || result.TransitionID == "" || view.LastTransitionID != result.TransitionID {
		return AgentLifecycleState{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_outcome_not_current", "agent-lifecycle", "adopt_committed_view", map[string]any{"agent_identity": result.Identity, "operation_id": result.OperationID})
	}
	return view, nil
}

// The operation lock serializes candidate derivation with this row observation.
// A changed durable view never grants the old executable projection new rights.
func (c *agentLifecycleCoordinator) refreshAgentViewLocked(ctx context.Context, cell *agentLifecycleCell) error {
	if pending := cell.unacknowledged; pending != nil {
		if c.outcomeReader == nil {
			return errors.New("agent lifecycle outcome owner is required to resolve an unacknowledged replacement")
		}
		outcome, found, err := c.outcomeReader.ReadAgentLifecycleOutcome(ctx, pending.request)
		if err != nil {
			return err
		}
		if found {
			view, err := c.committedAgentView(ctx, outcome)
			if err != nil {
				return err
			}
			pending.outcome = outcome
			if cell.execution != nil && (cell.execution.token != lifecycleToken(cell.identity, view.RuntimeEpoch, view.Generation) ||
				!lifecycleProjectionEqual(view.ConfigRevision, view.Topology, cell.view.ConfigRevision, cell.view.Topology)) {
				cell.execution.fenced = true
			}
			return nil
		}
		cell.unacknowledged = nil
	}
	view, found, err := c.loadAgentView(ctx, cell.identity)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: durable row for %s", ErrAgentNotFound, cell.identity.Description())
	}
	if cell.execution != nil && !lifecycleProjectionEqual(view.ConfigRevision, view.Topology, cell.view.ConfigRevision, cell.view.Topology) {
		cell.execution.fenced = true
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_view_stale", "agent-lifecycle", "refresh_agent_view", map[string]any{"agent_identity": cell.identity})
	}
	if cell.execution != nil && cell.execution.token != lifecycleToken(cell.identity, view.RuntimeEpoch, view.Generation) {
		cell.execution.fenced = true
	}
	cell.view = view
	return nil
}
