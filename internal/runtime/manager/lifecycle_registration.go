package manager

import (
	"context"
	"errors"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Retains prepared code and the exact creator request, never current-row truth.
type agentRegistration struct {
	request       AgentLifecycleTransition
	agent         Agent
	binding       ProcessExecutionBinding
	phase         runtimeLifecyclePhase
	run           uint64
	admission     semanticview.FlowOwnedAgentSubscriptionAdmission
	standingOwner *worklifetime.StandingOccurrence
}

func registrationProcessBinding(store AgentLifecyclePersistence) (ProcessExecutionBinding, error) {
	owner, ok := store.(processExecutionBindingProvider)
	if !ok {
		return ProcessExecutionBinding{}, errors.New("agent registration requires its process binding owner")
	}
	return owner.ProcessExecutionBinding()
}

func (c *agentLifecycleCoordinator) resumeAgentRegistrationLocked(ctx context.Context, rec PersistedAgent, revision string, pending *agentRegistration, readinessOwner **agentLifecycleCell) (bool, error) {
	identity := pending.request.Identity
	if c.outcomeReader == nil {
		return true, errors.New("agent registration requires its immutable outcome owner")
	}
	readCtx := context.WithoutCancel(ctx)
	outcome, found, err := c.outcomeReader.ReadAgentLifecycleOutcome(readCtx, pending.request)
	if err != nil {
		return true, err
	}
	if !found {
		// The native transaction rolled back: there is no accepted creator to
		// adopt. Normal registration must validate a fresh candidate and row.
		delete(c.registrations, identity)
		return false, nil
	}
	if revision != pending.request.ConfigRevision || !rec.Topology.Equal(pending.request.Topology) || c.phase != pending.phase || c.runGeneration != pending.run {
		return true, runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "lifecycle_unacknowledged_request_conflict", "agent-lifecycle", "register_execution", map[string]any{"agent_identity": identity})
	}
	store := c.persistence()
	binding, err := registrationProcessBinding(store)
	if err != nil {
		return true, err
	}
	if !pending.binding.Equal(binding) {
		return true, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_registration_owner_changed", "agent-lifecycle", "register_execution", map[string]any{"agent_identity": identity})
	}
	if _, err := c.committedAgentView(readCtx, outcome); err != nil {
		return true, err
	}
	// Exact replay revalidates native source/process authority; it cannot mint a
	// second transition.
	result, err := store.CommitAgentLifecycleTransition(readCtx, pending.request)
	if err != nil {
		return true, err
	}
	view, err := c.committedAgentView(readCtx, result)
	if err != nil {
		return true, err
	}
	c.publishRegisteredExecutionLocked(*pending.request.Agent, pending.agent, pending.admission, pending.standingOwner, view, readinessOwner)
	return true, nil
}

func registrationStandingOwner(ctx context.Context) *worklifetime.StandingOccurrence {
	var standingOwner *worklifetime.StandingOccurrence
	if owner, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		standingOwner, _ = worklifetime.StandingProjection(owner)
	}
	return standingOwner
}

func (c *agentLifecycleCoordinator) publishRegisteredExecutionLocked(rec PersistedAgent, agent Agent, admission semanticview.FlowOwnedAgentSubscriptionAdmission, standingOwner *worklifetime.StandingOccurrence, view AgentLifecycleState, readinessOwner **agentLifecycleCell) {
	identity := view.Identity
	generationCtx, cancelGeneration := context.WithCancel(c.context())
	startedAt := rec.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	execution := &agentExecutionProjection{agent: agent, config: rec.Config, admission: admission, startedAt: startedAt,
		token: lifecycleToken(identity, view.RuntimeEpoch, view.Generation), standingOwner: standingOwner,
		generationCtx: generationCtx, cancelGeneration: cancelGeneration, subscriptions: admittedSubscriptionEventTypes(admission)}
	cell := &agentLifecycleCell{identity: identity, view: view, execution: execution}
	c.cells[identity] = cell
	delete(c.registrations, identity)
	if readinessOwner != nil {
		cell.routeTransitions++
		*readinessOwner = cell
	}
}
