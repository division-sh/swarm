package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type a2CreationFaultCase struct {
	name string
	set  func(context.Context, any, bool) error
}

func a2CreationFaultCases() []a2CreationFaultCase {
	return []a2CreationFaultCase{
		{"a2_activation_join_failure", SetA2ActivationJoinScheduleFailureForTest},
		{"a2_join_close_failure", SetA2JoinCloseScheduleFailureForTest},
		{"a2_accumulator_publication_failure", SetA2AccumulatorPublicationFailureForTest},
	}
}

func TestA2CreationFaultsUseOriginalCoordinatorAndLeaveDataHistoryUnchangedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newWorkflowTargetConstructionFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
			if backend == "postgres" {
				query = `SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1 AND NOT tgisinternal`
			}
			for index, fault := range a2CreationFaultCases() {
				for _, enabled := range []bool{true, false} {
					if err := fault.set(f.ctx, f.store, enabled); err != nil {
						t.Fatal(err)
					}
					var count int
					if err := f.db.QueryRowContext(f.ctx, query, fault.name).Scan(&count); err != nil || (count == 1) != enabled || count > 1 {
						t.Fatalf("%s trigger lifetime changed: enabled=%v count=%d err=%v", fault.name, enabled, count, err)
					}
				}
				if counters := probe.Snapshot(); counters.Active != 0 || counters.Total.Begun != uint64((index+1)*2) || counters.Total.WriteCommits != uint64((index+1)*2) || counters.Total.ReadCommits != 0 {
					t.Fatalf("%s bypassed original coordinator or split native installation: %+v", fault.name, counters)
				}
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("creation trigger wiring changed application data, revisions, Story history or clocks")
			}
		})
	}
}

func TestA2CreationFaultsRespectOriginalSQLiteWriterAdmissionAndCancellation(t *testing.T) {
	for _, fault := range a2CreationFaultCases() {
		for _, enabled := range []bool{true, false} {
			name := fault.name + "/remove"
			if enabled {
				name = fault.name + "/install"
			}
			t.Run(name, func(t *testing.T) {
				assertGateFaultWriterAdmission(t, func(ctx context.Context, selected any, _ string) error {
					return fault.set(ctx, selected, enabled)
				})
			})
		}
	}
}

func TestA2CreationFaultsRefuseMissingRawAndClosedOwners(t *testing.T) {
	for _, fault := range a2CreationFaultCases() {
		for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
			if err := fault.set(context.Background(), selected, true); err == nil {
				t.Fatalf("%s accepted non-native owner %T", fault.name, selected)
			}
		}
	}
	f, _ := newWorkflowTargetConstructionFixture(t, "sqlite")
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	for _, fault := range a2CreationFaultCases() {
		if err := fault.set(f.ctx, f.store, true); err == nil {
			t.Fatalf("%s accepted closed owner", fault.name)
		}
	}
}
