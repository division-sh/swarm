package runtimepersistence

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

type agentDirectiveActiveSession struct {
	SessionID string
	RunID     string
}

type directiveRunTargetQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PostgresStore) ResolveAgentDirectiveRunTarget(ctx context.Context, identity runtimeagentidentity.Identity, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error) {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	explicitRunID = strings.TrimSpace(explicitRunID)
	if s == nil || s.backend == nil {
		return runtimeagentcontrol.RunTargetResolution{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if explicitRunID != "" {
		if err := validateDirectiveRunTarget(ctx, s.backend, identity, explicitRunID); err != nil {
			return runtimeagentcontrol.RunTargetResolution{}, err
		}
		return runtimeagentcontrol.RunTargetResolution{
			RunID: explicitRunID,
			Mode:  runtimeagentcontrol.RunResolutionSpecified,
		}, nil
	}
	sessions, err := listActiveDirectiveSessions(ctx, s.backend, fields)
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	switch len(sessions) {
	case 0:
		return runtimeagentcontrol.RunTargetResolution{
			RunID: uuid.NewString(),
			Mode:  runtimeagentcontrol.RunResolutionNewRunAllocated,
		}, nil
	case 1:
		session := sessions[0]
		if strings.TrimSpace(session.RunID) == "" {
			return runtimeagentcontrol.RunTargetResolution{}, ambiguousDirectiveRunTarget(identity, sessions)
		}
		if err := validateDirectiveRunTarget(ctx, s.backend, identity, session.RunID); err != nil {
			return runtimeagentcontrol.RunTargetResolution{}, err
		}
		return runtimeagentcontrol.RunTargetResolution{
			RunID: session.RunID,
			Mode:  runtimeagentcontrol.RunResolutionActiveSession,
			ActiveSessions: []runtimeagentcontrol.ActiveSessionTarget{{
				SessionID: session.SessionID,
				RunID:     session.RunID,
			}},
		}, nil
	default:
		return runtimeagentcontrol.RunTargetResolution{}, ambiguousDirectiveRunTarget(identity, sessions)
	}
}

func (s *SQLiteRuntimeStore) ResolveAgentDirectiveRunTarget(ctx context.Context, identity runtimeagentidentity.Identity, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error) {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	explicitRunID = strings.TrimSpace(explicitRunID)
	if s == nil || s.backend == nil {
		return runtimeagentcontrol.RunTargetResolution{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if explicitRunID != "" {
		if err := validateSQLiteDirectiveRunTarget(ctx, s.backend, identity, explicitRunID); err != nil {
			return runtimeagentcontrol.RunTargetResolution{}, err
		}
		return runtimeagentcontrol.RunTargetResolution{
			RunID: explicitRunID,
			Mode:  runtimeagentcontrol.RunResolutionSpecified,
		}, nil
	}
	sessions, err := listActiveSQLiteDirectiveSessions(ctx, s.backend, fields)
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	switch len(sessions) {
	case 0:
		return runtimeagentcontrol.RunTargetResolution{
			RunID: uuid.NewString(),
			Mode:  runtimeagentcontrol.RunResolutionNewRunAllocated,
		}, nil
	case 1:
		session := sessions[0]
		if strings.TrimSpace(session.RunID) == "" {
			return runtimeagentcontrol.RunTargetResolution{}, ambiguousDirectiveRunTarget(identity, sessions)
		}
		if err := validateSQLiteDirectiveRunTarget(ctx, s.backend, identity, session.RunID); err != nil {
			return runtimeagentcontrol.RunTargetResolution{}, err
		}
		return runtimeagentcontrol.RunTargetResolution{
			RunID: session.RunID,
			Mode:  runtimeagentcontrol.RunResolutionActiveSession,
			ActiveSessions: []runtimeagentcontrol.ActiveSessionTarget{{
				SessionID: session.SessionID,
				RunID:     session.RunID,
			}},
		}, nil
	default:
		return runtimeagentcontrol.RunTargetResolution{}, ambiguousDirectiveRunTarget(identity, sessions)
	}
}

func listActiveDirectiveSessions(ctx context.Context, db directiveRunTargetQueryer, fields runtimeagentidentity.StorageFields) ([]agentDirectiveActiveSession, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			session_id::text,
			COALESCE(run_id::text, '')
		FROM agent_sessions
		WHERE agent_id = $1
		  AND agent_name_owner = $2
		  AND agent_name_source = $3
		  AND agent_route_presence = $4
		  AND flow_scope_key = $5
		  AND flow_instance_id = $6
		  AND flow_instance = $7
		  AND status = 'active'
		ORDER BY updated_at DESC, created_at DESC, session_id ASC
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return nil, fmt.Errorf("list active directive sessions: %w", err)
	}
	defer rows.Close()

	out := []agentDirectiveActiveSession{}
	for rows.Next() {
		var rec agentDirectiveActiveSession
		if err := rows.Scan(&rec.SessionID, &rec.RunID); err != nil {
			return nil, fmt.Errorf("scan active directive session: %w", err)
		}
		rec.SessionID = strings.TrimSpace(rec.SessionID)
		rec.RunID = strings.TrimSpace(rec.RunID)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active directive sessions: %w", err)
	}
	return out, nil
}

func listActiveSQLiteDirectiveSessions(ctx context.Context, db directiveRunTargetQueryer, fields runtimeagentidentity.StorageFields) ([]agentDirectiveActiveSession, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			session_id,
			COALESCE(run_id, '')
		FROM agent_sessions
		WHERE agent_id = ?
		  AND agent_name_owner = ?
		  AND agent_name_source = ?
		  AND agent_route_presence = ?
		  AND flow_scope_key = ?
		  AND flow_instance_id = ?
		  AND flow_instance = ?
		  AND status = 'active'
		ORDER BY updated_at DESC, created_at DESC, session_id ASC
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return nil, fmt.Errorf("list active directive sessions: %w", err)
	}
	defer rows.Close()

	out := []agentDirectiveActiveSession{}
	for rows.Next() {
		var rec agentDirectiveActiveSession
		if err := rows.Scan(&rec.SessionID, &rec.RunID); err != nil {
			return nil, fmt.Errorf("scan active directive session: %w", err)
		}
		rec.SessionID = strings.TrimSpace(rec.SessionID)
		rec.RunID = strings.TrimSpace(rec.RunID)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active directive sessions: %w", err)
	}
	return out, nil
}

func validateDirectiveRunTarget(ctx context.Context, db directiveRunTargetQueryer, identity runtimeagentidentity.Identity, runID string) error {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return &runtimeagentcontrol.StateError{
			Err:          runtimeagentcontrol.ErrRunNotFound,
			AgentID:      identity.AgentID(),
			FlowInstance: identity.FlowInstance(),
			RunID:        runID,
		}
	}
	var status string
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM runs
		WHERE run_id = $1::uuid
		LIMIT 1
	`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimeagentcontrol.StateError{
			Err:          runtimeagentcontrol.ErrRunNotFound,
			AgentID:      identity.AgentID(),
			FlowInstance: identity.FlowInstance(),
			RunID:        runID,
		}
	}
	if err != nil {
		return fmt.Errorf("load directive run target: %w", err)
	}
	status = strings.TrimSpace(status)
	state, parseErr := runtimerunlifecycle.ParseState(status)
	if parseErr == nil && state.Active() {
		return nil
	}
	return &runtimeagentcontrol.StateError{
		Err:           runtimeagentcontrol.ErrRunAlreadyTerminal,
		AgentID:       identity.AgentID(),
		FlowInstance:  identity.FlowInstance(),
		RunID:         runID,
		CurrentStatus: status,
	}
}

func validateSQLiteDirectiveRunTarget(ctx context.Context, db directiveRunTargetQueryer, identity runtimeagentidentity.Identity, runID string) error {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return &runtimeagentcontrol.StateError{
			Err:          runtimeagentcontrol.ErrRunNotFound,
			AgentID:      identity.AgentID(),
			FlowInstance: identity.FlowInstance(),
			RunID:        runID,
		}
	}
	var status string
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM runs
		WHERE run_id = ?
		LIMIT 1
	`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimeagentcontrol.StateError{
			Err:          runtimeagentcontrol.ErrRunNotFound,
			AgentID:      identity.AgentID(),
			FlowInstance: identity.FlowInstance(),
			RunID:        runID,
		}
	}
	if err != nil {
		return fmt.Errorf("load directive run target: %w", err)
	}
	status = strings.TrimSpace(status)
	state, parseErr := runtimerunlifecycle.ParseState(status)
	if parseErr == nil && state.Active() {
		return nil
	}
	return &runtimeagentcontrol.StateError{
		Err:           runtimeagentcontrol.ErrRunAlreadyTerminal,
		AgentID:       identity.AgentID(),
		FlowInstance:  identity.FlowInstance(),
		RunID:         runID,
		CurrentStatus: status,
	}
}

func ambiguousDirectiveRunTarget(identity runtimeagentidentity.Identity, sessions []agentDirectiveActiveSession) error {
	targets := make([]runtimeagentcontrol.ActiveSessionTarget, 0, len(sessions))
	for _, session := range sessions {
		targets = append(targets, runtimeagentcontrol.ActiveSessionTarget{
			SessionID: session.SessionID,
			RunID:     session.RunID,
		})
	}
	return &runtimeagentcontrol.StateError{
		Err:            runtimeagentcontrol.ErrAmbiguousRunTarget,
		AgentID:        identity.AgentID(),
		FlowInstance:   identity.FlowInstance(),
		ActiveSessions: targets,
	}
}
