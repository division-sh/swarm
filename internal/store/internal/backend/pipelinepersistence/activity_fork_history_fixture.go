package pipelinepersistence

import (
	"context"
	"database/sql"
	"time"
)

func SetHistoricalActivityForkAccumulatorTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, accumulator string) error {
	query := `UPDATE entity_state SET accumulator=?3 WHERE run_id=?1 AND entity_id=?2`
	if postgres {
		query = `UPDATE entity_state SET accumulator=$3::jsonb WHERE run_id=$1::uuid AND entity_id=$2::uuid`
	}
	_, err := tx.ExecContext(ctx, query, runID, entityID, accumulator)
	return err
}

func InsertHistoricalActivityForkLoopMutationTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, requestID, loops string, at time.Time) error {
	query := `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at)
		VALUES (?1,?2,'accumulator','handler_loops','null',?3,?4,'platform','activity-fork-proof','seed',?5)`
	if postgres {
		query = `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at)
			VALUES ($1::uuid,$2::uuid,'accumulator','handler_loops','null'::jsonb,$3::jsonb,$4::uuid,'platform','activity-fork-proof','seed',$5)`
	}
	_, err := tx.ExecContext(ctx, query, runID, entityID, loops, requestID, at)
	return err
}
