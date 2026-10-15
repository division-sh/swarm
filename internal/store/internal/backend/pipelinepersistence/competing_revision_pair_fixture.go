package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func AdvanceSelectionRevisionPairTx(ctx context.Context, tx *sql.Tx, state pipeline.WorkflowEngineStateRecord) error {
	result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET revision=revision+1 WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, state.Identity.RunID, state.EntityID, state.ExpectedRevision)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("competing writer rows=%d: %v", n, err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE entity_state SET revision=revision+1, fields='{"marker":"second"}' WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, state.Identity.RunID, state.EntityID, state.ExpectedRevision)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("competing field writer rows=%d: %v", n, err)
	}
	return nil
}

func AdvanceProspectiveRevisionPairTx(ctx context.Context, tx *sql.Tx, state pipeline.WorkflowEngineStateRecord) error {
	result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET revision=revision+1 WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3 AND revision=$4`, state.Identity.RunID, state.Identity.Route.InstancePath, state.EntityID, state.ExpectedRevision)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("competing revision cut rows=%d err=%v", count, err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE entity_state SET revision=revision+1 WHERE run_id=$1 AND flow_instance=$2 AND entity_id=$3 AND revision=$4`, state.Identity.RunID, state.Identity.Route.InstancePath, state.EntityID, state.ExpectedRevision)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("competing field revision cut rows=%d err=%v", count, err)
	}
	return nil
}
