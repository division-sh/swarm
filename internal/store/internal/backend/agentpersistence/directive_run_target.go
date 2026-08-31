package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type directiveRunTargetQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *AgentPostgresOwner) ResolveAgentDirectiveRunTarget(ctx context.Context, identity runtimeagentidentity.Identity) (runtimeagentcontrol.RunTargetResolution, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if s == nil || s.backend == nil {
		return runtimeagentcontrol.RunTargetResolution{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if err := validateDirectiveRunTarget(ctx, s.backend, identity, identity.RunID); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	return runtimeagentcontrol.RunTargetResolution{RunID: identity.RunID, Mode: runtimeagentcontrol.RunResolutionSpecified}, nil
}

func (s *AgentSQLiteOwner) ResolveAgentDirectiveRunTarget(ctx context.Context, identity runtimeagentidentity.Identity) (runtimeagentcontrol.RunTargetResolution, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if s == nil || s.backend == nil {
		return runtimeagentcontrol.RunTargetResolution{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if err := validateSQLiteDirectiveRunTarget(ctx, s.backend, identity, identity.RunID); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	return runtimeagentcontrol.RunTargetResolution{RunID: identity.RunID, Mode: runtimeagentcontrol.RunResolutionSpecified}, nil
}

func validateDirectiveRunTarget(ctx context.Context, db directiveRunTargetQueryer, identity runtimeagentidentity.Identity, runID string) error {
	return validateRunTarget(ctx, db, identity, runID, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid LIMIT 1`)
}

func validateSQLiteDirectiveRunTarget(ctx context.Context, db directiveRunTargetQueryer, identity runtimeagentidentity.Identity, runID string) error {
	return validateRunTarget(ctx, db, identity, runID, `SELECT COALESCE(status, '') FROM runs WHERE run_id = ? LIMIT 1`)
}

func validateRunTarget(ctx context.Context, db directiveRunTargetQueryer, identity runtimeagentidentity.Identity, runID, query string) error {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return runTargetStateError(runtimeagentcontrol.ErrRunNotFound, identity, runID, "")
	}
	var status string
	err := db.QueryRowContext(ctx, query, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return runTargetStateError(runtimeagentcontrol.ErrRunNotFound, identity, runID, "")
	}
	if err != nil {
		return fmt.Errorf("load directive run target: %w", err)
	}
	status = strings.TrimSpace(status)
	state, parseErr := runtimerunlifecycle.ParseState(status)
	if parseErr == nil && state.Active() {
		return nil
	}
	return runTargetStateError(runtimeagentcontrol.ErrRunAlreadyTerminal, identity, runID, status)
}

func runTargetStateError(err error, identity runtimeagentidentity.Identity, runID, status string) error {
	return &runtimeagentcontrol.StateError{
		Err: err, AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(),
		RunID: runID, CurrentStatus: status,
	}
}
