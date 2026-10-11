package eventrecord

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// GenericSchedulePublicationModes is physical evidence, not schedule authority.
type GenericSchedulePublicationModes struct {
	Count   int
	MinMode string
	MaxMode string
}

func ReadGenericSchedulePublicationModesTx(ctx context.Context, tx *sql.Tx, taskID string) (GenericSchedulePublicationModes, error) {
	var out GenericSchedulePublicationModes
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(execution_mode), ''), COALESCE(MAX(execution_mode), '') FROM events WHERE task_id=$1 AND produced_by=$2 AND produced_by_type='platform'`, taskID, genericschedule.OccurrenceProducerID()).Scan(&out.Count, &out.MinMode, &out.MaxMode)
	return out, err
}

func ReadGenericScheduleDistinctPublicationModesTx(ctx context.Context, tx *sql.Tx, taskID string) (GenericSchedulePublicationModes, error) {
	var out GenericSchedulePublicationModes
	err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT event_id), COALESCE(MIN(execution_mode), ''), COALESCE(MAX(execution_mode), '') FROM events WHERE task_id=$1 AND produced_by=$2 AND produced_by_type='platform'`, taskID, genericschedule.OccurrenceProducerID()).Scan(&out.Count, &out.MinMode, &out.MaxMode)
	return out, err
}

func CountGenericSchedulePublicationsTx(ctx context.Context, tx *sql.Tx, taskID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE task_id=$1 AND produced_by=$2 AND produced_by_type='platform'`, taskID, genericschedule.OccurrenceProducerID()).Scan(&out)
	return out, err
}
