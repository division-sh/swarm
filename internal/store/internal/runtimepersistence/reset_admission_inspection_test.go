package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type resetAdmissionInspectionStore interface {
	startupownership.Store
	InspectSnapshot(context.Context, func(context.Context) error) error
	InspectPendingResetOperations(context.Context) ([]destructivereset.Operation, error)
}

type resetAdmissionInspectionReader struct{ store resetAdmissionInspectionStore }

func (r resetAdmissionInspectionReader) PendingResetOperations(ctx context.Context) ([]destructivereset.Operation, error) {
	return r.store.InspectPendingResetOperations(ctx)
}

func TestPendingResetAdmissionReadOnlyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected resetAdmissionInspectionStore
			var db *sql.DB
			if backend == "sqlite" {
				s := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = s, s.backend.ConstructionHandle()
			} else {
				_, handle, _ := testutil.StartPostgres(t)
				selected, db = admitTestPostgresStore(t, handle), handle
			}
			ctx := context.Background()
			// Seed through the production authority/writer, then release it.
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-admission-inspection"))
			if err != nil {
				t.Fatal(err)
			}
			operation, err := capability.AdmitResetOperation(ctx, destructivereset.Request{
				OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "read-only-proof", RequestedAt: time.Now().UTC(),
			})
			if releaseErr := capability.Release(ctx); err != nil || releaseErr != nil {
				t.Fatalf("seed/release: %v, %v", err, releaseErr)
			}
			var before int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_startup_authority_facts`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			var expired context.Context
			if err := selected.InspectSnapshot(ctx, func(scoped context.Context) error {
				expired = scoped
				pending, err := destructivereset.InspectPendingOperations(scoped, resetAdmissionInspectionReader{selected})
				if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], operation) {
					t.Fatalf("inspected pending evidence changed: %+v err=%v", pending, err)
				}
				canceled, cancel := context.WithCancel(scoped)
				cancel()
				if _, err := selected.InspectPendingResetOperations(canceled); !errors.Is(err, context.Canceled) {
					t.Fatalf("required read lost cancellation: %v", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.InspectPendingResetOperations(expired); err == nil {
				t.Fatal("expired selected snapshot escaped to the ledger")
			}
			pending, err := selected.InspectPendingResetOperations(ctx)
			if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], operation) {
				t.Fatalf("inspection advanced or settled reset evidence: %+v err=%v", pending, err)
			}
			var after int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_startup_authority_facts`).Scan(&after); err != nil || after != before {
				t.Fatalf("inspection changed authority lineage: before=%d after=%d err=%v", before, after, err)
			}
		})
	}
}
