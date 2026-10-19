package storetest

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
)

// StartPostgresRuntimeStore constructs one admitted native owner without
// exposing its pool to the caller.
func StartPostgresRuntimeStore(t *testing.T) *store.PostgresStore {
	t.Helper()
	selected, _ := StartPostgresRuntimeStoreWithReopen(t)
	return selected
}

// Each reopen constructs a native owner from the fixture's original location.
// No running owner's pool or coordinator is recovered.
// A supplied sandbox location permits an independently constructed process peer.
func StartPostgresRuntimeStoreWithReopen(t *testing.T, locations ...string) (*store.PostgresStore, func() *store.PostgresStore) {
	t.Helper()
	if len(locations) > 1 || (len(locations) == 1 && locations[0] == "") {
		t.Fatal("native postgres fixture requires at most one nonempty sandbox location")
	}
	var dsn string
	if len(locations) == 1 {
		dsn = locations[0]
	} else {
		dsn = testutil.StartPostgresDSN(t)
	}
	var ownersMu sync.Mutex
	var owners []*store.PostgresStore
	// Register once, before consumers. Later peers must not jump ahead of
	// the consumer's join in the testing cleanup stack.
	t.Cleanup(func() {
		ownersMu.Lock()
		defer ownersMu.Unlock()
		for i := len(owners) - 1; i >= 0; i-- {
			if err := owners[i].Close(); err != nil {
				t.Errorf("close postgres runtime store: %v", err)
			}
		}
	})
	open := func() *store.PostgresStore {
		t.Helper()
		selected, err := store.NewPostgresStore(dsn)
		if err != nil {
			t.Fatalf("construct postgres runtime store: %v", err)
		}
		ownersMu.Lock()
		owners = append(owners, selected)
		ownersMu.Unlock()
		BootstrapPostgresRuntimeStore(t, selected)
		bindTestPayloadAdmitter(selected)
		return selected
	}
	return open(), open
}

func StartSQLiteRuntimeStoreWithReopen(t testing.TB, ctx context.Context, locations ...string) (*store.SQLiteRuntimeStore, func() *store.SQLiteRuntimeStore) {
	t.Helper()
	if len(locations) > 1 || (len(locations) == 1 && locations[0] == "") {
		t.Fatal("native sqlite fixture requires at most one nonempty file location")
	}
	spec, plans := canonicalPlatformPlans(t)
	var path string
	if len(locations) == 1 {
		path = locations[0]
	} else {
		path = filepath.Join(t.TempDir(), ".swarm", "dev.db")
	}
	request := store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion: "storetest", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
		},
	}
	var ownersMu sync.Mutex
	var owners []*store.SQLiteRuntimeStore
	t.Cleanup(func() {
		ownersMu.Lock()
		defer ownersMu.Unlock()
		for i := len(owners) - 1; i >= 0; i-- {
			if err := owners[i].Close(); err != nil {
				t.Errorf("close sqlite runtime store: %v", err)
			}
		}
	})
	open := func() *store.SQLiteRuntimeStore {
		t.Helper()
		selected, err := store.NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatalf("construct sqlite runtime store: %v", err)
		}
		ownersMu.Lock()
		owners = append(owners, selected)
		ownersMu.Unlock()
		if err := selected.BootstrapSchema(ctx, request); err != nil {
			t.Fatalf("bootstrap sqlite runtime store: %v", err)
		}
		bindTestPayloadAdmitter(selected)
		return selected
	}
	return open(), open
}
