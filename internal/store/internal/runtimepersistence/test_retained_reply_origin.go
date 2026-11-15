package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
)

func CorruptRetainedReplyOriginForTest(ctx context.Context, selected any, runID, contextID string, origin json.RawMessage) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.CorruptRetainedReplyOriginForTest(ctx, runID, contextID, origin)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.CorruptRetainedReplyOriginForTest(ctx, runID, contextID, origin)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
