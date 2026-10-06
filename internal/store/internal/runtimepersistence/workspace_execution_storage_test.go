package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestWorkspaceExecutionObserversRefuseNonOwnersCancelledAndClosedBothStores(t *testing.T) {
	// The result must be completely empty on error, including after an earlier
	// query in the same read snapshot succeeded.
	observers := []struct {
		name string
		read func(context.Context, any, string) (any, error)
		zero any
	}{
		{"turns", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadManagedAgentTurnStorageForTest(ctx, owner, id, "actor")
		}, []ManagedAgentTurnStorageRow(nil)},
		{"turn-effects", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadManagedTurnEffectStorageForTest(ctx, owner, id, "actor")
		}, ManagedTurnEffectStorage{}},
		{"deliveries", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadManagedDeliveryStorageForTest(ctx, owner, id, "actor")
		}, ManagedDeliveryStorage{}},
		{"selected-execution", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedExecutionStorageForTest(ctx, owner, id)
		}, SelectedExecutionStorage{}},
		{"fork-counts", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadConversationForkStorageForTest(ctx, owner, id)
		}, ConversationForkStorage{}},
		{"fork-turn", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadConversationForkTurnStorageForTest(ctx, owner, id, "exact-key")
		}, ConversationForkTurnStorage{}},
		{"fork-domain", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadConversationForkDomainStorageForTest(ctx, owner, id)
		}, ConversationForkDomainStorage{}},
		{"fork-diagnostics", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadConversationForkTurnDiagnosticsForTest(ctx, owner, id)
		}, []ConversationForkTurnDiagnostic(nil)},
		{"http-effects", func(ctx context.Context, owner any, _ string) (any, error) {
			return ReadAuthoredHTTPToolEffectStorageForTest(ctx, owner)
		}, []AuthoredHTTPToolEffectStorage(nil)},
		{"workspace-invocation", func(ctx context.Context, owner any, _ string) (any, error) {
			return ReadWorkspaceMockInvocationStorageForTest(ctx, owner)
		}, WorkspaceMockInvocationStorage{}},
		{"source-domain", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedForkSourceDomainForTest(ctx, owner, id)
		}, map[string]SelectedForkStorageTableSnapshot(nil)},
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := context.Background()
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertRefused := func(name string, ctx context.Context, owner any, cause error) {
				t.Helper()
				for _, observer := range observers {
					t.Run(name+"/"+observer.name, func(t *testing.T) {
						value, err := observer.read(ctx, owner, uuid.NewString())
						if err == nil || (cause != nil && !errors.Is(err, cause)) || !reflect.DeepEqual(value, observer.zero) {
							t.Fatalf("refusal returned partial/foreign evidence: value=%+v error=%v", value, err)
						}
					})
				}
			}
			assertRefused("nil", ctx, nil, nil)
			assertRefused("raw-pool", ctx, &sql.DB{}, nil)
			assertRefused("uninitialized-sqlite", ctx, &SQLiteRuntimeStore{}, nil)
			assertRefused("uninitialized-postgres", ctx, &PostgresStore{}, nil)
			assertRefused("cancelled", cancelled, fixture.store, context.Canceled)
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			assertRefused("closed", ctx, fixture.store, nil)
		})
	}
}
