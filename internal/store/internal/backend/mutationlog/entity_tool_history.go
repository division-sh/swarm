package mutationlog

import (
	"context"
	"database/sql"
	"time"
)

func InsertEntityToolHistoricalReadOnlyMutationsTx(ctx context.Context, tx *sql.Tx, runID, entityID, eventID string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations
		(run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at)
		VALUES ($1::uuid,$2::uuid,'lifecycle_state','','null'::jsonb,'"queued"'::jsonb,$3::uuid,'platform','revision-test','seed',$4),
		($1::uuid,$2::uuid,'authored_field','status','null'::jsonb,'"open"'::jsonb,$3::uuid,'platform','revision-test','seed',$4)`, runID, entityID, eventID, at)
	return err
}
