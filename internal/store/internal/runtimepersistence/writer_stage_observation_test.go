package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWriterStageObservationPreservesExactEntityAndEarliestMatchingStateBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runID, sibling := seedReceiverJoinedInventoryStorage(t, fixture, ctx)
			for _, cut := range []struct {
				run, entity, stage, wantEntity, wantState string
			}{
				{runID, "10000000-1111-4111-8111-111111111111", "not-the-filter", "10000000-1111-4111-8111-111111111111", "active"},
				{runID, "20000000-1111-4111-8111-111111111111", "active", "20000000-1111-4111-8111-111111111111", "done"},
				{runID, "", "active", "10000000-1111-4111-8111-111111111111", "active"},
				{runID, "", "done", "20000000-1111-4111-8111-111111111111", "done"},
				{runID, "", "Active", "", ""},
				{runID, "", "not-a-state", "", ""},
				{runID, "abcdef01-1111-4111-8111-111111111111", "active", "abcdef01-1111-4111-8111-111111111111", ""},
				{sibling, "", "active", "70000000-1111-4111-8111-111111111111", "active"},
				{uuid.NewString(), "", "active", "", ""},
			} {
				got, err := ObserveWriterStageForTest(ctx, fixture.store, cut.run, cut.entity, cut.stage)
				want := WriterStageEvidence{EntityID: cut.wantEntity, State: cut.wantState}
				if err != nil || got != want {
					t.Fatalf("exact-ID/earliest matching state/no-row/run/case policy: cut=%+v got=%+v want=%+v err=%v", cut, got, want, err)
				}
			}
			requireWriterStageReadOnlyCounter(t, ctx, fixture.store, runID)
		})
	}
}

func requireWriterStageReadOnlyCounter(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	if got, err := ObserveWriterStageForTest(ctx, selected, runID, "", "active"); err != nil || got.EntityID == "" || got.State != "active" {
		t.Fatalf("native stage observation: %+v %v", got, err)
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
		t.Fatalf("stage evidence left the original read owner: %+v", counts)
	}
	restore()
	if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("stage observation changed whole storage: %v", err)
	}
}

func TestWriterStageObservationRefusesRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	ctx, id := context.Background(), "abcdef01-1111-4111-8111-111111111111"
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ObserveWriterStageForTest(ctx, selected, id, "", "active"); err == nil || got != (WriterStageEvidence{}) {
			t.Fatalf("raw/uninitialized stage owner returned evidence: %+v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, cut := range [][2]string{{"", ""}, {"bad", ""}, {id, "bad"}} {
				if got, err := ObserveWriterStageForTest(ctx, fixture.store, cut[0], cut[1], "active"); err == nil || got != (WriterStageEvidence{}) {
					t.Fatalf("invalid stage identity returned evidence: %+v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ObserveWriterStageForTest(cancelled, fixture.store, id, "", "active"); !errors.Is(err, context.Canceled) || got != (WriterStageEvidence{}) {
				t.Fatalf("cancelled stage returned evidence: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ObserveWriterStageForTest(ctx, fixture.store, id, "", "active"); err == nil || got != (WriterStageEvidence{}) {
				t.Fatalf("closed stage returned evidence: %+v %v", got, err)
			}
		})
	}
}
