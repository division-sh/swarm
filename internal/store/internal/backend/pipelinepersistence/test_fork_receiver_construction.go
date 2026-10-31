package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func RemoveForkReceiverFieldCompanionTxForTest(ctx context.Context, tx *sql.Tx, runID, entityID, instance string) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance=$3`, runID, entityID, instance)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("receiver field-companion fault requires one exact row, got %d", count)
	}
	return nil
}
