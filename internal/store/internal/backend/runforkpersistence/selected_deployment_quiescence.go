package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

// requireSelectedDeploymentDrainedTx is the durable completion gate for an
// admitted selected deployment feed. Cursor completion alone cannot prove that
// the event pipeline or its receiver continuations have finished.
func requireSelectedDeploymentDrainedTx(ctx context.Context, tx *sql.Tx, forkRunID string) error {
	if tx == nil || forkRunID == "" {
		return fmt.Errorf("selected deployment quiescence requires transaction and fork run")
	}
	var feeds int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, forkRunID).Scan(&feeds); err != nil {
		return fmt.Errorf("read selected deployment feed inventory: %w", err)
	}
	if feeds == 0 {
		return nil
	}
	checks := []struct {
		name  string
		query string
	}{
		{"unfinished feed", `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' AND status<>'closed'`},
		{"missing ordinal evidence", `SELECT COUNT(*) FROM fan_out_intents i
			WHERE i.run_id=$1 AND i.origin_kind='deployment' AND i.cursor<>
			(SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.deployment_feed_id=i.deployment_feed_id)`},
		{"noncommitted ordinal", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND deployment_feed_id IS NOT NULL AND outcome_kind<>'committed'`},
		{"committed ordinal without event scope", `SELECT COUNT(*) FROM fan_out_outcomes o
			LEFT JOIN committed_replay_scopes s ON s.run_id=o.run_id AND s.event_id=o.event_id
			WHERE o.run_id=$1 AND o.deployment_feed_id IS NOT NULL AND o.outcome_kind='committed'
			AND o.event_id IS NOT NULL AND s.event_id IS NULL`},
		{"unfinished event pipeline", `SELECT COUNT(*) FROM committed_replay_scopes s
			LEFT JOIN event_receipts r ON r.event_id=s.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
			WHERE s.run_id=$1 AND COALESCE(r.outcome,'')<>'success'`},
		{"unfinished receiver delivery", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND (status<>'delivered' OR continuation_handoff_at IS NULL)`},
	}
	for _, check := range checks {
		var count int
		if err := tx.QueryRowContext(ctx, check.query, forkRunID).Scan(&count); err != nil {
			return fmt.Errorf("read selected deployment %s: %w", check.name, err)
		}
		if count != 0 {
			if check.name == "unfinished feed" {
				var status string
				var cursor, cardinality int
				if err := tx.QueryRowContext(ctx, `SELECT status,cursor,cardinality FROM fan_out_intents
					WHERE run_id=$1 AND origin_kind='deployment' AND status<>'closed'
					ORDER BY CAST(deployment_feed_id AS TEXT) LIMIT 1`, forkRunID).Scan(&status, &cursor, &cardinality); err == nil {
					return fmt.Errorf("selected deployment %s: %d remaining (first status=%s cursor=%d cardinality=%d)", check.name, count, status, cursor, cardinality)
				}
			}
			if check.name == "unfinished receiver delivery" {
				var subscriberType, subscriberID, status, eventID string
				var handedOff bool
				if err := tx.QueryRowContext(ctx, `SELECT subscriber_type,subscriber_id,status,event_id,
					CASE WHEN continuation_handoff_at IS NOT NULL THEN 1 ELSE 0 END
					FROM event_deliveries WHERE run_id=$1 AND (status<>'delivered' OR continuation_handoff_at IS NULL)
					ORDER BY CAST(delivery_id AS TEXT) LIMIT 1`, forkRunID).
					Scan(&subscriberType, &subscriberID, &status, &eventID, &handedOff); err == nil {
					var controlStatus sql.NullString
					_ = tx.QueryRowContext(ctx, `SELECT control_status FROM run_control_state WHERE run_id=$1`, forkRunID).Scan(&controlStatus)
					var pipelineReceipts int
					_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&pipelineReceipts)
					return fmt.Errorf("selected deployment %s: %d remaining (first event=%s subscriber=%s/%s status=%s handed_off=%t control=%s pipeline_receipts=%d)",
						check.name, count, eventID, subscriberType, subscriberID, status, handedOff, controlStatus.String, pipelineReceipts)
				}
			}
			return fmt.Errorf("selected deployment %s: %d remaining", check.name, count)
		}
	}
	return nil
}
