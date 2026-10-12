package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func SetRetainedFanOutCapsuleWireForTest(ctx context.Context, tx *sql.Tx, runID string, raw []byte) (int64, error) {
	return retainedFanOutMetadataFaultResult(tx.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(raw), runID))
}

func SetRetainedFanOutCardinalityForTest(ctx context.Context, tx *sql.Tx, runID string, cardinality int) (int64, error) {
	return retainedFanOutMetadataFaultResult(tx.ExecContext(ctx, `UPDATE fan_out_intents SET cardinality=$1 WHERE run_id=$2`, cardinality, runID))
}

func retainedFanOutMetadataFaultResult(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if changed != 1 {
		return 0, fmt.Errorf("retained fan-out metadata fault requires exactly one existing row, got %d", changed)
	}
	return changed, nil
}
