package llmpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

func resetSessionsTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt *mutationprotocol.Attempt, lifecycle completionCandidateOwner, source string, now time.Time) (runtimesessions.ResetSummary, error) {
	if postgres {
		if err := tx.QueryRowContext(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&now); err != nil {
			return runtimesessions.ResetSummary{}, err
		}
	}
	// Discovery fixes the reset's exact footprint before any session lock.
	// New sessions outside this snapshot cannot be swept by a later bulk UPDATE.
	rows, err := tx.QueryContext(ctx, `SELECT CAST(session_id AS TEXT), CAST(run_id AS TEXT), agent_id, flow_instance
		FROM agent_sessions WHERE status IN ('active','suspended') ORDER BY run_id, session_id`)
	if err != nil {
		return runtimesessions.ResetSummary{}, err
	}
	var captured []runtimesessions.ResetDisposition
	for rows.Next() {
		var d runtimesessions.ResetDisposition
		if err := rows.Scan(&d.SessionID, &d.RunID, &d.AgentID, &d.FlowInstance); err != nil {
			return runtimesessions.ResetSummary{}, errors.Join(err, rows.Close())
		}
		captured = append(captured, d)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return runtimesessions.ResetSummary{}, err
	}
	seen := map[string]bool{}
	for _, d := range captured {
		if seen[d.RunID] {
			continue
		}
		seen[d.RunID] = true
		if postgres {
			_, err = sourceadmission.LoadPostgresTx(ctx, tx, d.RunID, false, false)
		} else {
			_, err = sourceadmission.LoadSQLiteTx(ctx, tx, d.RunID, false, false)
		}
		if err != nil {
			return runtimesessions.ResetSummary{}, err
		}
	}
	var summary runtimesessions.ResetSummary
	for _, d := range captured {
		query := `SELECT CAST(run_id AS TEXT), agent_id, flow_instance, status FROM agent_sessions
			WHERE session_id=$1 AND status IN ('active','suspended')`
		if postgres {
			query += ` FOR UPDATE`
		}
		var runID, agentID, flowInstance string
		err := tx.QueryRowContext(ctx, query, d.SessionID).Scan(&runID, &agentID, &flowInstance, &d.PreviousStatus)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return runtimesessions.ResetSummary{}, err
		}
		if runID != d.RunID || agentID != d.AgentID || flowInstance != d.FlowInstance {
			return runtimesessions.ResetSummary{}, fmt.Errorf("reset session %s changed its admitted run footprint", d.SessionID)
		}
		d.TerminationReason, d.TerminationDetail = runtimesessions.TerminationReasonOrphaned.String(), source
		result, err := tx.ExecContext(ctx, `UPDATE agent_sessions
			SET status='terminated', termination_reason='orphaned', termination_detail=NULLIF($1,''),
			terminated_at=COALESCE(terminated_at,$2), lease_holder=NULL, lease_grant_id=NULL, lease_expires_at=NULL, updated_at=$2
			WHERE session_id=$3 AND run_id=$4 AND status IN ('active','suspended')`, source, now, d.SessionID, d.RunID)
		if err != nil {
			return runtimesessions.ResetSummary{}, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return runtimesessions.ResetSummary{}, err
		}
		if changed != 1 {
			return runtimesessions.ResetSummary{}, fmt.Errorf("reset session %s lost its locked row: %d", d.SessionID, changed)
		}
		if err := addAgentSessionFacts(attempt, d.RunID, d.SessionID); err != nil {
			return runtimesessions.ResetSummary{}, err
		}
		summary.OrphanedSessions = append(summary.OrphanedSessions, d)
	}
	clear(seen)
	for _, d := range summary.OrphanedSessions {
		if seen[d.RunID] {
			continue
		}
		seen[d.RunID] = true
		if _, err := attempt.RequestCompletion(ctx, lifecycle, d.RunID, nil); err != nil {
			return runtimesessions.ResetSummary{}, err
		}
	}
	return summary, nil
}
