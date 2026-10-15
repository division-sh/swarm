package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store"
)

func TestOperatorChannelStorageObservationRejectsInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			var nilPostgres *store.PostgresStore
			var nilSQLite *store.SQLiteRuntimeStore
			reads := []struct {
				name   string
				read   func(context.Context, any) (bool, error)
				absent bool
			}{
				{"all_actions", func(ctx context.Context, owner any) (bool, error) {
					count, err := CountOperatorChannelActionIntentStorage(ctx, owner)
					return count == 0, err
				}, false},
				{"provider_event_text", func(ctx context.Context, owner any) (bool, error) {
					fact, err := ReadOperatorChannelTextIntentFactStorage(ctx, owner, "absent")
					return fact == nil, err
				}, true},
			}
			for _, read := range reads {
				t.Run(read.name, func(t *testing.T) {
					zero, err := read.read(context.Background(), selected)
					if !zero || (read.absent && !errors.Is(err, sql.ErrNoRows)) || (!read.absent && err != nil) {
						t.Fatalf("empty physical witness changed: zero=%t error=%v", zero, err)
					}
					for _, owner := range []any{nil, nilPostgres, nilSQLite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
						if zero, err := read.read(context.Background(), owner); err == nil || !zero {
							t.Fatalf("foreign %T supplied successful/provisional evidence: zero=%t error=%v", owner, zero, err)
						}
					}
					cancelled, cancel := context.WithCancel(context.Background())
					cancel()
					if zero, err := read.read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled read supplied evidence: zero=%t error=%v", zero, err)
					}
				})
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range reads {
				if zero, err := read.read(context.Background(), selected); !zero || err == nil {
					t.Fatalf("closed %s read supplied evidence: zero=%t error=%v", read.name, zero, err)
				}
			}
		})
	}
}
