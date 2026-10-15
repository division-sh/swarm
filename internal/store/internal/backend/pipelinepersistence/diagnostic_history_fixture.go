package pipelinepersistence

import (
	"context"
	"database/sql"
	"time"
)

// Historical diagnostic receipts remain lineage-only and grant no dispatch or
// settlement authority to a live pipeline occurrence.
func InsertDiagnosticDeadLetterHistoryFixtureTx(ctx context.Context, tx *sql.Tx, eventID string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO event_receipts
		(event_id,subscriber_type,subscriber_id,entity_id,flow_instance,outcome,reason_code,side_effects,processed_at)
		VALUES ($1,'platform','pipeline',NULL,NULL,'dead_letter','runtime_log_pipeline_dead_letter','{}',$2)`, eventID, at)
	return err
}
