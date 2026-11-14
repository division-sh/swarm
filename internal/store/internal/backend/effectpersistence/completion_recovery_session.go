package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/effects"
)

// Inspection must not take the settlement path's session write lock.
func completionRecoverySessionAbandoned(ctx context.Context, tx *sql.Tx, attempt effects.Attempt, sqlite bool) (bool, error) {
	target := attempt.Authority.Target
	if target.Kind != effects.UsageTargetAgentTurn || !target.Memory.Enabled {
		return false, nil
	}
	if strings.TrimSpace(attempt.SessionGrantID) == "" || strings.TrimSpace(attempt.SessionLockOwner) == "" {
		return false, fmt.Errorf("completion recovery requires its admitted session grant")
	}
	fields, err := agentIdentityFields(target.AgentIdentity)
	if err != nil {
		return false, err
	}
	query := `SELECT status,lease_grant_id,lease_holder,lease_expires_at FROM agent_sessions
		WHERE session_id=$1 AND run_id=$2 AND agent_id=$3 AND agent_name_owner=$4
		AND agent_name_source=$5 AND agent_route_presence=$6 AND flow_scope_key=$7
		AND flow_instance_id=$8 AND flow_instance=$9`
	if !sqlite {
		query = strings.ReplaceAll(strings.ReplaceAll(query, "session_id=$1", "session_id=$1::uuid"), "run_id=$2", "run_id=$2::uuid")
	}
	var status string
	var grant, holder sql.NullString
	var expiryRaw any
	if err := tx.QueryRowContext(ctx, query, target.SessionID, target.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&status, &grant, &holder, &expiryRaw); err != nil {
		return false, fmt.Errorf("completion recovery exact session: %w", err)
	}
	switch status {
	case "terminated", "suspended":
		return true, nil
	case "active":
		return completionRecoverySessionGrantAbandoned(ctx, tx, attempt, grant, holder, expiryRaw, sqlite)
	default:
		return false, fmt.Errorf("completion recovery session has invalid status %q", status)
	}
}

func completionRecoverySessionGrantAbandoned(ctx context.Context, tx *sql.Tx, attempt effects.Attempt, grant, holder sql.NullString, expiryRaw any, sqlite bool) (bool, error) {
	if !grant.Valid && !holder.Valid && expiryRaw == nil {
		return true, nil // The session owner released this grant.
	}
	expiry, valid, err := sqliteTimeValue(expiryRaw)
	if err != nil {
		return false, fmt.Errorf("completion recovery session expiry: %w", err)
	}
	if !valid || !grant.Valid || !holder.Valid || strings.TrimSpace(grant.String) == "" || strings.TrimSpace(holder.String) == "" {
		return false, fmt.Errorf("completion recovery session has incomplete grant evidence")
	}
	now, err := selectedStoreGrantDecisionNowTx(ctx, tx, !sqlite)
	if err != nil {
		return false, err
	}
	return grant.String != attempt.SessionGrantID || holder.String != attempt.SessionLockOwner || !expiry.After(now), nil
}
