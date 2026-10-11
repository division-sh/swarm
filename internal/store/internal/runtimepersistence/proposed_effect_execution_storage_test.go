package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestProposedEffectExecutionObservationRefusesEscapesAndUsesOriginalReadsBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadProposedEffectRunExecutionStorageForTest(context.Background(), owner, uuid.NewString()); err == nil || got != (ProposedEffectRunExecutionStorage{}) {
			t.Fatalf("foreign owner %T supplied evidence: %+v/%v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadProposedEffectRunExecutionStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || got != (ProposedEffectRunExecutionStorage{}) {
				t.Fatalf("absent exact run evidence=%+v/%v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("execution counts escaped original read owner: %+v", counts)
			}
			for _, run := range []string{"", "bad", uuid.Nil.String(), " " + uuid.NewString()} {
				if got, err := ReadProposedEffectRunExecutionStorageForTest(ctx, fixture.store, run); err == nil || got != (ProposedEffectRunExecutionStorage{}) {
					t.Fatalf("invalid exact run %q supplied evidence: %+v/%v", run, got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadProposedEffectRunExecutionStorageForTest(cancelled, fixture.store, uuid.NewString()); err == nil || got != (ProposedEffectRunExecutionStorage{}) {
				t.Fatalf("cancelled read retained evidence: %+v/%v", got, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadProposedEffectRunExecutionStorageForTest(ctx, fixture.store, uuid.NewString()); err == nil || got != (ProposedEffectRunExecutionStorage{}) {
				t.Fatalf("closed read retained evidence: %+v/%v", got, err)
			}
		})
	}
}
