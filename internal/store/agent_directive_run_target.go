package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
)

type agentDirectiveActiveSession struct {
	SessionID string
	RunID     string
}

func (s *PostgresStore) ResolveAgentDirectiveRunTarget(ctx context.Context, agentID, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error) {
	agentID = strings.TrimSpace(agentID)
	explicitRunID = strings.TrimSpace(explicitRunID)
	if s == nil || s.DB == nil {
		return runtimeagentcontrol.RunTargetResolution{}, fmt.Errorf("postgres store is required")
	}
	if agentID == "" {
		return runtimeagentcontrol.RunTargetResolution{}, fmt.Errorf("agent_id is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if !caps.Events.HasRuns {
		return runtimeagentcontrol.RunTargetResolution{}, unsupportedSchemaCapability("runs", SchemaFlavorUnavailable)
	}
	if explicitRunID != "" {
		if err := validateDirectiveRunTarget(ctx, s.DB, agentID, explicitRunID); err != nil {
			return runtimeagentcontrol.RunTargetResolution{}, err
		}
		return runtimeagentcontrol.RunTargetResolution{
			RunID: explicitRunID,
			Mode:  runtimeagentcontrol.RunResolutionSpecified,
		}, nil
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical || !caps.Conversations.SessionRunID {
		return runtimeagentcontrol.RunTargetResolution{}, unsupportedSchemaCapability("agent_sessions.run_id", caps.Conversations.Sessions)
	}
	sessions, err := listActiveDirectiveSessions(ctx, s.DB, agentID)
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
			return runtimeagentcontrol.RunTargetResolution{}, ambiguousDirectiveRunTarget(agentID, sessions)
		}
		if err := validateDirectiveRunTarget(ctx, s.DB, agentID, session.RunID); err != nil {
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
		return runtimeagentcontrol.RunTargetResolution{}, ambiguousDirectiveRunTarget(agentID, sessions)
	}
}

func listActiveDirectiveSessions(ctx context.Context, db *sql.DB, agentID string) ([]agentDirectiveActiveSession, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			session_id::text,
			COALESCE(run_id::text, '')
		FROM agent_sessions
		WHERE agent_id = $1
		  AND status = 'active'
		ORDER BY updated_at DESC, created_at DESC, session_id ASC
	`, agentID)
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

func validateDirectiveRunTarget(ctx context.Context, db *sql.DB, agentID, runID string) error {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return &runtimeagentcontrol.StateError{
			Err:     runtimeagentcontrol.ErrRunNotFound,
			AgentID: agentID,
			RunID:   runID,
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
			Err:     runtimeagentcontrol.ErrRunNotFound,
			AgentID: agentID,
			RunID:   runID,
		}
	}
	if err != nil {
		return fmt.Errorf("load directive run target: %w", err)
	}
	status = strings.TrimSpace(strings.ToLower(status))
	switch status {
	case "running", "paused":
		return nil
	default:
		return &runtimeagentcontrol.StateError{
			Err:           runtimeagentcontrol.ErrRunAlreadyTerminal,
			AgentID:       agentID,
			RunID:         runID,
			CurrentStatus: status,
		}
	}
}

func ambiguousDirectiveRunTarget(agentID string, sessions []agentDirectiveActiveSession) error {
	targets := make([]runtimeagentcontrol.ActiveSessionTarget, 0, len(sessions))
	for _, session := range sessions {
		targets = append(targets, runtimeagentcontrol.ActiveSessionTarget{
			SessionID: session.SessionID,
			RunID:     session.RunID,
		})
	}
	return &runtimeagentcontrol.StateError{
		Err:            runtimeagentcontrol.ErrAmbiguousRunTarget,
		AgentID:        strings.TrimSpace(agentID),
		ActiveSessions: targets,
	}
}
