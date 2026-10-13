package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

// AgentExecutionObservation is row evidence, not process possession or permission
// to settle a captured predecessor attempt.
type AgentExecutionObservation struct {
	State   manager.AgentLifecycleState
	Found   bool
	Current bool
}

func ReadAgentExecution(ctx context.Context, q rowQueryer, token effects.LifecycleToken, sqlite bool) (AgentExecutionObservation, error) {
	return observeAgentExecution(ctx, q, token.Identity, token, sqlite, false)
}

func LockAgentExecutionTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, token effects.LifecycleToken, sqlite bool) (AgentExecutionObservation, error) {
	if tx == nil {
		return AgentExecutionObservation{}, fmt.Errorf("agent execution requires the original mutation transaction")
	}
	return observeAgentExecution(ctx, tx, identity, token, sqlite, true)
}

func observeAgentExecution(ctx context.Context, q rowQueryer, identity agentidentity.Identity, token effects.LifecycleToken, sqlite, lock bool) (AgentExecutionObservation, error) {
	state, _, found, err := loadAgentLifecycleState(ctx, q, identity, sqlite, lock)
	if err != nil {
		return AgentExecutionObservation{}, err
	}
	return AgentExecutionObservation{
		State: state, Found: found, Current: found && agentExecutionMatches(state, token),
	}, nil
}

func agentExecutionMatches(state manager.AgentLifecycleState, token effects.LifecycleToken) bool {
	return token.Valid() && token.Identity.Normalize() == state.Identity.Normalize() &&
		token.RuntimeEpoch == state.RuntimeEpoch && token.Generation == state.Generation &&
		state.Phase == manager.AgentLifecycleRunning
}
