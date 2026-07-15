package serveapp

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
)

func bootstrapSQLiteSchemaForTest(t testing.TB, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, plans []store.SchemaTableDDL) {
	t.Helper()
	if err := sqliteStore.BootstrapSchema(ctx, store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion:    "cmd-test",
			PlatformVersion: "test",
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
}
