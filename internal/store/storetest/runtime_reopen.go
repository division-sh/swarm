package storetest

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
)

// Each reopen constructs a native owner from the fixture's original location.
// No running owner's pool or coordinator is recovered.
func StartPostgresRuntimeStoreWithReopen(t *testing.T) (*store.PostgresStore, func() *store.PostgresStore) {
	t.Helper()
	dsn, _, _ := testutil.StartPostgres(t)
	open := func() *store.PostgresStore {
		t.Helper()
		selected, err := store.NewPostgresStore(dsn)
		if err != nil {
			t.Fatalf("construct postgres runtime store: %v", err)
		}
		t.Cleanup(func() {
			if err := selected.Close(); err != nil {
				t.Errorf("close postgres runtime store: %v", err)
			}
		})
		BootstrapPostgresRuntimeStore(t, selected)
		bindTestPayloadAdmitter(selected)
		return selected
	}
	return open(), open
}

func StartSQLiteRuntimeStoreWithReopen(t testing.TB, ctx context.Context) (*store.SQLiteRuntimeStore, func() *store.SQLiteRuntimeStore) {
	t.Helper()
	spec, plans := canonicalPlatformPlans(t)
	path := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	request := store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion: "storetest", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
		},
	}
	open := func() *store.SQLiteRuntimeStore {
		t.Helper()
		selected, err := store.NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatalf("construct sqlite runtime store: %v", err)
		}
		t.Cleanup(func() {
			if err := selected.Close(); err != nil {
				t.Errorf("close sqlite runtime store: %v", err)
			}
		})
		if err := selected.BootstrapSchema(ctx, request); err != nil {
			t.Fatalf("bootstrap sqlite runtime store: %v", err)
		}
		bindTestPayloadAdmitter(selected)
		return selected
	}
	return open(), open
}
