package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestBusRoutingStorageObservationsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			reads := []func(context.Context, any) (bool, error){
				func(ctx context.Context, owner any) (bool, error) {
					rows, err := ReadBusRoutingEventNames(ctx, owner)
					return rows == nil, err
				},
				func(ctx context.Context, owner any) (bool, error) {
					rows, err := ReadBusAncestorDeliveryDiagnostics(ctx, owner)
					return rows == nil, err
				},
			}
			for _, read := range reads {
				if zero, err := read(context.Background(), selected); !zero || err != nil {
					t.Fatalf("empty physical observation changed: zero=%t error=%v", zero, err)
				}
				for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
					if zero, err := read(context.Background(), owner); !zero || err == nil {
						t.Fatalf("foreign %T supplied provisional/successful evidence: zero=%t error=%v", owner, zero, err)
					}
				}
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				if zero, err := read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled observation supplied evidence: zero=%t error=%v", zero, err)
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range reads {
				if zero, err := read(context.Background(), selected); !zero || err == nil {
					t.Fatalf("closed observation supplied evidence: zero=%t error=%v", zero, err)
				}
			}
		})
	}
}
