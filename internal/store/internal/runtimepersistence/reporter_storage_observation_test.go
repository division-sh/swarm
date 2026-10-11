package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunEventCounterPhysicalConsistencyPreservesZeroAndExactScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			runID, sibling := uuid.NewString(), uuid.NewString()
			for _, run := range []string{runID, sibling} {
				requireRunningRunForTest(t, ctx, selected, run, time.Now().UTC())
			}
			if counts, err := ReadRunEventCounterStorageConsistencyForTest(ctx, selected, runID); err != nil || counts != (RunEventCounterStorageConsistency{}) {
				t.Fatalf("genuine zero counter was inferred or replaced: %+v/%v", counts, err)
			}
			for _, run := range []string{runID, runID, runID, sibling} {
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "first.event", "fixture", "", []byte(`{}`), 0, run,
					events.EventEnvelope{}, eventtest.RootRoutingSource(run), time.Now().UTC())
				if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			counts, err := ReadRunEventCounterStorageConsistencyForTest(ctx, selected, runID)
			if err != nil || counts.Stored != 3 || counts.Actual != 3 {
				t.Fatalf("counter or physical count lost exact run scope: %+v/%v", counts, err)
			}
			if observation := probe.Snapshot(); observation.Total.ReadCommits != 1 || observation.Total.WriteCommits != 0 || observation.Active != 0 {
				t.Fatalf("counter evidence escaped original read coordinator: %+v", observation)
			}
			if counts, err := ReadRunEventCounterStorageConsistencyForTest(ctx, selected, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || counts != (RunEventCounterStorageConsistency{}) {
				t.Fatalf("absent run fabricated a zero counter receipt: %+v/%v", counts, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if counts, err := ReadRunEventCounterStorageConsistencyForTest(cancelled, selected, runID); !errors.Is(err, context.Canceled) || counts != (RunEventCounterStorageConsistency{}) {
				t.Fatalf("cancelled counter retained evidence: %+v/%v", counts, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if counts, err := ReadRunEventCounterStorageConsistencyForTest(ctx, selected, runID); err == nil || counts != (RunEventCounterStorageConsistency{}) {
				t.Fatalf("closed counter fabricated evidence: %+v/%v", counts, err)
			}
		})
	}
}

func TestReporterStorageObservationsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, runID string) {
		t.Helper()
		if counts, err := ReadRunEventCounterStorageConsistencyForTest(ctx, owner, runID); err == nil || counts != (RunEventCounterStorageConsistency{}) {
			t.Fatalf("refused counter retained evidence: %+v/%v", counts, err)
		}
		if rows, err := ReadFanOutSemanticRejectionStorageForTest(ctx, owner, runID); err == nil || rows != nil {
			t.Fatalf("refused rejections retained evidence: %+v/%v", rows, err)
		}
		if rows, err := ReadFanOutObligationHistoryStorageForTest(ctx, owner, runID); err == nil || rows != nil {
			t.Fatalf("refused history retained evidence: %+v/%v", rows, err)
		}
	}
	for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		refuse(context.Background(), owner, uuid.NewString())
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			owner, ctx := backend.open(t).store, testAuthorActivityContext()
			runID := uuid.NewString()
			for _, invalid := range []string{"", "bad", uuid.Nil.String()} {
				refuse(ctx, owner, invalid)
			}
			if rows, err := ReadFanOutSemanticRejectionStorageForTest(ctx, owner, runID); err != nil || len(rows) != 0 {
				t.Fatalf("empty rejection query fabricated rows: %+v/%v", rows, err)
			}
			if rows, err := ReadFanOutObligationHistoryStorageForTest(ctx, owner, runID); err != nil || len(rows) != 0 {
				t.Fatalf("empty history query fabricated rows: %+v/%v", rows, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			refuse(cancelled, owner, runID)
			if err := owner.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(ctx, owner, runID)
		})
	}
}
