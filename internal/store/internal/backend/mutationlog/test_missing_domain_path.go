package mutationlog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Probe the schema constraint below semantic validation, on one already
// constructed entity. Even a broken constraint must not leave the bad row.
func ProbeMissingDomainPathForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, entity string) error {
	var present int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).Scan(&present); err != nil {
		return err
	}
	query := `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at) VALUES ($1,$2,$3,'accumulator','','null','{"bad":true}','platform','constraint-probe','seed',$4)`
	if postgres {
		query = `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at) VALUES ($1::uuid,$2::uuid,$3::uuid,'accumulator','','null'::jsonb,'{"bad":true}'::jsonb,'platform','constraint-probe','seed',$4)`
	}
	if _, err := tx.ExecContext(ctx, query, uuid.NewString(), run, entity, time.Now().UTC()); err != nil {
		return err
	}
	return fmt.Errorf("malformed mutation was accepted by the schema")
}
