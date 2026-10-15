package mutationlog

import (
	"context"
	"database/sql"
)

type ReceiverFieldMutationStorage struct{ Old, New, WriterType, WriterID, Step string }

func ReadLoopObservedRevisionMutationStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	var out ReceiverFieldMutationStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='observed_revision'`, runID, entityID, eventID).Scan(&out.New, &out.WriterType, &out.WriterID, &out.Step)
	return out, err
}

func CountLoopObservedRevisionMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='observed_revision'`, runID, entityID, eventID).Scan(&count)
	return count, err
}

func ReadReceiverBusinessTokenMutationStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	var out ReceiverFieldMutationStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(old_value AS TEXT),CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_token'`, runID, entityID, eventID).Scan(&out.Old, &out.New, &out.WriterType, &out.WriterID, &out.Step)
	return out, err
}

func ReadReceiverProcessedCountMutationStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) (ReceiverFieldMutationStorage, error) {
	var out ReceiverFieldMutationStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(old_value AS TEXT),'null'),CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_count' AND writer_id='workflow_engine'`, runID, entityID, eventID).Scan(&out.Old, &out.New, &out.WriterType, &out.WriterID, &out.Step)
	return out, err
}

func ReadReceiverMarkerMutationStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) ([]ReceiverFieldMutationStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT COALESCE(CAST(old_value AS TEXT),'null'),CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='marker'`, runID, entityID, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiverFieldMutationStorage
	for rows.Next() {
		var row ReceiverFieldMutationStorage
		if err := rows.Scan(&row.Old, &row.New, &row.WriterType, &row.WriterID, &row.Step); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountReceiverBusinessTokenMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='processed_token'`, runID, eventID).Scan(&count)
	return count, err
}

func CountReceiverMarkerMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='marker'`, runID, eventID).Scan(&count)
	return count, err
}

func CountReceiverProcessedCountMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_count'`, runID, entityID, eventID).Scan(&count)
	return count, err
}

func CountReceiverEntityCausedMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3`, runID, entityID, eventID).Scan(&count)
	return count, err
}
