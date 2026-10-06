package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
)

func TestSQLiteInspectionWriteControlAttemptsRealCreation(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*SQLiteRuntimeStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}, &sql.DB{}} {
		if err := AttemptSQLiteInspectionTableCreationForTest(ctx, owner); err == nil {
			t.Fatalf("invalid write-control owner accepted: %T", owner)
		}
	}
	writer, _ := decisionCardTestStore(t, "sqlite")
	if err := AttemptSQLiteInspectionTableCreationForTest(ctx, writer); err != nil {
		t.Fatal(err)
	}
	snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, writer)
	if err != nil || len(snapshot["snapshot_forbidden_writer"].Columns) != 1 || snapshot["snapshot_forbidden_writer"].Columns[0] != "id" {
		t.Fatalf("control did not execute its exact CREATE on the passed backend: %v", err)
	}
}
