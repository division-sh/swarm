package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestRetainedReplyOriginFaultRefusesAbsentRowWithoutPartialHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			if err := CorruptOpenReplyOriginForTest(f.ctx, f.store, state.Identity.RunID, "absent-context", `{"foreign":"origin"}`); err == nil || !strings.Contains(err.Error(), "corrupt exact retained reply entry: rows=0") {
				t.Fatalf("absent exact reply origin did not refuse: %v", err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("absent reply mutation committed partial origin, revision or Story evidence")
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := CorruptOpenReplyOriginForTest(cancelled, f.store, state.Identity.RunID, "absent-context", `{}`); !errors.Is(err, context.Canceled) {
				t.Fatalf("reply origin fault ignored cancellation: %v", err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("cancelled reply mutation changed persisted evidence")
			}
		})
	}
}

func TestRetainedReplyOriginFaultUsesOriginalSQLiteWriterAdmission(t *testing.T) {
	assertGateFaultWriterAdmission(t, func(ctx context.Context, selected any, run string) error {
		return CorruptOpenReplyOriginForTest(ctx, selected, run, "absent-context", `{}`)
	})
}

func TestRetainedReplyOriginFaultRefusesMissingRawAndClosedOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if err := CorruptOpenReplyOriginForTest(context.Background(), selected, "run", "context", `{}`); err == nil {
			t.Fatalf("reply origin fault accepted non-native owner %T", selected)
		}
	}
	f, state := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	if err := CorruptOpenReplyOriginForTest(f.ctx, f.store, state.Identity.RunID, "context", `{}`); err == nil {
		t.Fatal("reply origin fault accepted a closed owner")
	}
}
