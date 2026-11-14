package runtimepersistence

import (
	"context"
	"fmt"
)

// SetMailboxCompletionInsertFaultForTest owns the named response-insert fault's
// DDL through the selected writer, not an unfenced fixture connection.
func SetMailboxCompletionInsertFaultForTest(ctx context.Context, selected any, key, witness string, enabled bool) error {
	if enabled && (key == "" || witness == "") {
		return fmt.Errorf("mailbox completion fault requires a key and witness")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil || store.postgresOwner == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.postgresOwner.SetMailboxCompletionInsertFaultForTest(ctx, key, witness, enabled)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || store.sQLiteOwner == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.sQLiteOwner.SetMailboxCompletionInsertFaultForTest(ctx, key, witness, enabled)
	default:
		return fmt.Errorf("unsupported mailbox completion fault store %T", selected)
	}
}
