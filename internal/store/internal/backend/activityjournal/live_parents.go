package activityjournal

import (
	"context"
	"database/sql"
	"fmt"
)

const activityLiveParentMatch = `p.request_event_id=activity_attempts.request_event_id
	AND p.run_id=activity_attempts.run_id AND p.reply_context_present=activity_attempts.reply_context_present
	AND COALESCE(p.reply_context_id,'')=COALESCE(activity_attempts.reply_context_id,'')
	AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_activity_detachments retained
	 WHERE retained.request_event_id=activity_attempts.request_event_id)`

func pairActivityLiveParentsTx(ctx context.Context, tx *sql.Tx, requestID string, copied bool) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("activity live-parent pairing requires its native transaction and context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	query := `INSERT INTO activity_attempt_live_parents (request_event_id,run_id,reply_context_id,reply_context_present)
		SELECT request_event_id,run_id,reply_context_id,reply_context_present
		FROM activity_attempts WHERE request_event_id=$1
		AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_activity_detachments retained
		 WHERE retained.request_event_id=activity_attempts.request_event_id)`
	if copied {
		query += ` AND execution_binding IS NULL AND status IN ('succeeded','failed','uncertain')`
	}
	result, err := tx.ExecContext(ctx, query, requestID)
	if err != nil {
		return fmt.Errorf("pair exact activity live parents: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read activity live-parent insert count: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("activity live-parent pairing requires one exact canonical row: count=%d", count)
	}
	return nil
}

// RequireLiveAttemptParentsTx validates live evidence, never detached completion
// or execution permission. A missing pair is corruption, not ordinary absence.
func RequireLiveAttemptParentsTx(ctx context.Context, tx *sql.Tx, requestID string) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("activity live-parent read requires its native transaction and context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var matches bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM activity_attempts
		JOIN activity_attempt_live_parents p ON `+activityLiveParentMatch+`
		WHERE activity_attempts.request_event_id=$1)`, requestID).Scan(&matches); err != nil {
		return fmt.Errorf("read exact activity live parents: %w", err)
	}
	if !matches {
		return fmt.Errorf("activity attempt lacks its exact live-parent association")
	}
	return nil
}
