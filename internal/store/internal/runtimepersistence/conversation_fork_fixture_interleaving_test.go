package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

type forkFixtureAdmissionContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *forkFixtureAdmissionContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestConversationForkRawSeedOverlapsRuntimeDiagnostic(t *testing.T) {
	ctx := testAuthorActivityContext()
	entered, release := make(chan struct{}), make(chan struct{})
	var armed, held atomic.Bool
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	db := sql.OpenDB(diagnosticSQLConnector{
		Connector: diagnosticSQLiteConnector{filepath.Join(t.TempDir(), "fork.db") + "?_pragma=journal_mode(WAL)"},
		afterExec: func(ctx context.Context, query string) error {
			if !armed.Load() || !strings.Contains(strings.ToLower(query), diagnosticEventInsertSQL) || !held.CompareAndSwap(false, true) {
				return nil
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	t.Cleanup(func() { _ = db.Close() })
	store := NewSQLiteRuntimeStoreForTest(db)
	if err := store.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
		t.Fatal(err)
	}
	item := createLifecycleDiagnostic(t, ctx, store)
	identity, err := item.Identity.StorageFields()
	if err != nil {
		t.Fatal(err)
	}
	// Bound the held-writer experiment, not schema construction under -race.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	armed.Store(true)
	go func() {
		done <- runtimepkg.NewRuntimeLogger(store, executionposture.Live, nil).ProjectLifecycleDiagnostic(ctx, item)
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("real diagnostic missed write barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// This is the original served fixture's first write, with valid admitted
	// agent coordinates. The lock holder is a real selected-store diagnostic.
	now := time.Now().UTC()
	_, err = db.ExecContext(ctx, `INSERT INTO agent_sessions (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		flow_scope_key, flow_instance_id, flow_instance,
		memory_enabled, memory_source, status, created_at, updated_at
	) VALUES (?,?,?,?,?,?,?,?,?,1,'authored','active',?,?)`,
		uuid.NewString(), identity.RunID, identity.AgentID, identity.NameOwner, identity.NameSource,
		identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID, identity.FlowInstancePath, now, now)
	if err == nil || !strings.Contains(err.Error(), "SQLITE_BUSY") {
		t.Fatalf("raw fixture write under real diagnostic lock = %v, want SQLITE_BUSY", err)
	}
	t.Logf("reproduced raw session seed failure with real diagnostic lock owner: %v", err)
	fixture := ConversationForkSourceFixture{
		Identity: item.Identity, RunID: identity.RunID, SessionID: uuid.NewString(), EntityID: uuid.NewString(),
		Event1ID: uuid.NewString(), Event2ID: uuid.NewString(), CreatedAt: now,
		Turn1At: now.Add(-2 * time.Minute), Turn2At: now.Add(-time.Minute),
	}
	queued, cancelQueued := context.WithCancel(ctx)
	defer cancelQueued()
	observed := &forkFixtureAdmissionContext{Context: queued, entered: make(chan struct{})}
	seedDone := make(chan error, 1)
	go func() { _, err := SeedConversationForkSourceForTest(observed, store, fixture); seedDone <- err }()
	select {
	case <-observed.entered:
	case err := <-seedDone:
		t.Fatalf("fixture bypassed cancellable admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The writer is still held. Cancellation must leave no fixture writes,
	// rather than surface SQLITE_BUSY or wait out a database retry budget.
	cancelQueued()
	if err := <-seedDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued fixture cancellation = %v", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatalf("diagnostic settlement: %v", err)
	}
	if _, err := SeedConversationForkSourceForTest(ctx, store, fixture); err != nil {
		t.Fatalf("fixture after diagnostic settlement: %v", err)
	}
}

func TestConversationForkSourceFixtureAtomicBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		name := "postgres"
		if sqlite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store, db := newLifecycleDiagnosticTestStore(t, sqlite)
			ctx := testAuthorActivityContext()
			item := createLifecycleDiagnostic(t, ctx, store)
			now := time.Now().UTC().Truncate(time.Second)
			fixture := ConversationForkSourceFixture{
				Identity: item.Identity, RunID: item.Identity.RunID, SessionID: uuid.NewString(), EntityID: uuid.NewString(),
				Event1ID: uuid.NewString(), CreatedAt: now, Turn1At: now.Add(-2 * time.Minute), Turn2At: now.Add(-time.Minute),
			}
			fixture.Event2ID = fixture.Event1ID
			// Event2 conflicts after the session, entity, mutations and first
			// event have been written. None may survive the failed transaction.
			if _, err := SeedConversationForkSourceForTest(ctx, store, fixture); err == nil {
				t.Fatal("invalid second event accepted")
			}
			for _, table := range []string{"agent_sessions", "entity_state", "entity_mutations", "events"} {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("%s contains %d rows after rollback", table, count)
				}
			}
			fixture.Event2ID = uuid.NewString()
			result, err := SeedConversationForkSourceForTest(ctx, store, fixture)
			if err != nil {
				t.Fatal(err)
			}
			if result[0].ID() != fixture.Event1ID || result[1].ID() != fixture.Event2ID || !result[0].CreatedAt().Equal(fixture.Turn1At) || !result[1].CreatedAt().Equal(fixture.Turn2At) {
				t.Fatal("historical event identities/timestamps changed")
			}
		})
	}
}
