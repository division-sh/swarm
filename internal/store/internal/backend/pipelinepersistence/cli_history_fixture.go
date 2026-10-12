package pipelinepersistence

import (
	"context"
	"database/sql"
	"time"
)

func InsertCLIMaterializationHistoryMutationsTx(ctx context.Context, tx *sql.Tx, run, entity, event string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations (
		run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
	) VALUES
		($1,$2,'lifecycle_state','','null','"ready"',$3,'platform','cli-test','seed',$4),
		($1,$2,'authored_field','name','null','"CLI Entity"',$3,'platform','cli-test','seed',$4)`, run, entity, event, at)
	return err
}

func InsertCLIMaterializationHistoryStateTx(ctx context.Context, tx *sql.Tx, run, entity string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_state (
		run_id, entity_id, flow_instance, entity_type, name, current_state, gates, fields, accumulator, revision,
		entered_state_at, created_at, updated_at
	) VALUES ($1,$2,'flow-a/1','default','CLI Entity','ready','{}','{"name":"CLI Entity"}','{}',1,$3,$3,$3)`, run, entity, at)
	return err
}

func ReadCLIMaterializedEntityStateTx(ctx context.Context, tx *sql.Tx, run, entity string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).Scan(&state)
	return state, err
}
