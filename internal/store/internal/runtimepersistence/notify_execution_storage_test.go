package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNotifyExecutionStorageKeepsExactScopeAndOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run, _ := seedWorkflowProjectionFaultCut(t, fixture, ctx)
			if err := commitSemanticParentFixture(ctx, fixture.store, run, uuid.NewString(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			var turns NotifyCompletedTurnsStorage
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT flow_instance) FROM agent_turns WHERE run_id=$1 AND agent_id='observer' AND failure IS NULL`, run).Scan(&turns.Turns, &turns.Instances); err != nil {
				t.Fatal(err)
			}
			var cursor NotifyFanOutCursorStorage
			if err := fixture.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents WHERE run_id=$1`, run).Scan(&cursor.Total, &cursor.Cursor, &cursor.Owed, &cursor.Blocked); err != nil {
				t.Fatal(err)
			}
			var work NotifyFanOutWorkStorage
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN next_chunk_size=1 THEN 1 ELSE 0 END),0),COALESCE(MIN(next_chunk_size),0),COALESCE(MAX(next_chunk_size),0) FROM fan_out_intents WHERE run_id=$1`, run).Scan(&work.Intents, &work.FloorOne, &work.Minimum, &work.Maximum); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, run).Scan(&work.Revisions); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1`, run).Scan(&work.Facts); err != nil {
				t.Fatal(err)
			}
			if work.Revisions == 0 || work.Facts == 0 {
				t.Fatalf("ledger witness requires real committed revisions and facts: %+v", work)
			}
			if got, err := ReadNotifyFanOutWorkForTest(ctx, fixture.store, uuid.NewString()); err != nil || got != (NotifyFanOutWorkStorage{}) {
				t.Fatalf("foreign run gained diagnostic evidence: %+v %v", got, err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadNotifyRunPresenceForTest(ctx, fixture.store, run); err != nil || got != 1 {
				t.Fatalf("run cardinality %d %v", got, err)
			}
			if got, err := ReadNotifyRunPresenceForTest(ctx, fixture.store, uuid.NewString()); err != nil || got != 0 {
				t.Fatalf("foreign run evidence %d %v", got, err)
			}
			if got, err := ReadNotifyFlowInstanceCountForTest(ctx, fixture.store, "projection-flow"); err != nil || got != 1 {
				t.Fatalf("header cardinality %d %v", got, err)
			}
			if got, err := ReadNotifyCompletedTurnsForTest(ctx, fixture.store, run, "observer"); err != nil || got != turns {
				t.Fatalf("turn predicate changed %+v %+v %v", got, turns, err)
			}
			if got, err := ReadNotifyLifecycleTransitionCountForTest(ctx, fixture.store, "observer", "new", "active"); err != nil || got != 0 {
				t.Fatalf("phase count %d %v", got, err)
			}
			if got, err := ReadNotifyFanOutCursorForTest(ctx, fixture.store, run); err != nil || got != cursor {
				t.Fatalf("cursor changed %+v %+v %v", got, cursor, err)
			}
			if got, err := ReadNotifyFanOutWorkForTest(ctx, fixture.store, run); err != nil || got != work {
				t.Fatalf("diagnostic changed %+v %+v %v", got, work, err)
			}
			if _, err := ReadNotifyLatestEventFailureForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("absent failure became evidence: %v", err)
			}
			if _, err := ReadNotifyFirstRunFailureForTest(ctx, fixture.store, run); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("absent run failure became evidence: %v", err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 9 || counts.Total.WriteCommits != 0 || counts.Total.ReadCommits != 7 || counts.Active != 0 {
				t.Fatalf("readback escaped original coordinator: %+v", counts)
			}
		})
	}
}

func TestNotifyExecutionStorageRefusesRawCancelledClosedAndUnavailableOwnersBothStores(t *testing.T) {
	run := uuid.NewString()
	readers := []struct {
		name, table string
		read        func(context.Context, any) (any, error)
		zero        any
	}{
		{"turns", "agent_turns", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyCompletedTurnsForTest(ctx, owner, run, "observer")
		}, NotifyCompletedTurnsStorage{}},
		{"phases", "agent_lifecycle_transition_facts", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyLifecycleTransitionCountForTest(ctx, owner, "observer", "new", "active")
		}, 0},
		{"latest-failure", "dead_letters", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyLatestEventFailureForTest(ctx, owner, uuid.NewString())
		}, ""},
		{"flows", "flow_instances", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyFlowInstanceCountForTest(ctx, owner, "projection-flow")
		}, 0},
		{"runs", "runs", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyRunPresenceForTest(ctx, owner, run)
		}, 0},
		{"cursor", "fan_out_intents", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyFanOutCursorForTest(ctx, owner, run)
		}, NotifyFanOutCursorStorage{}},
		{"first-failure", "dead_letters", func(ctx context.Context, owner any) (any, error) {
			return ReadNotifyFirstRunFailureForTest(ctx, owner, run)
		}, ""},
		{"work", "fan_out_intents", func(ctx context.Context, owner any) (any, error) { return ReadNotifyFanOutWorkForTest(ctx, owner, run) }, NotifyFanOutWorkStorage{}},
		{"work-revisions", "run_fork_revisions", func(ctx context.Context, owner any) (any, error) { return ReadNotifyFanOutWorkForTest(ctx, owner, run) }, NotifyFanOutWorkStorage{}},
		{"work-facts", "run_fork_fact_revisions", func(ctx context.Context, owner any) (any, error) { return ReadNotifyFanOutWorkForTest(ctx, owner, run) }, NotifyFanOutWorkStorage{}},
	}
	refuse := func(t *testing.T, ctx context.Context, owner any) {
		t.Helper()
		for _, reader := range readers {
			if got, err := reader.read(ctx, owner); err == nil || errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, reader.zero) {
				t.Fatalf("%s invalid owner became evidence: %+v %v", reader.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(t, context.Background(), owner)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, ctx, fixture.store)
			for _, reader := range readers {
				if err := runUnrevisionedEventFixtureTransactionForTest(context.Background(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "ALTER TABLE "+reader.table+" RENAME TO unavailable_notify_observation")
					return err
				}); err != nil {
					t.Fatal(err)
				}
				got, readErr := reader.read(context.Background(), fixture.store)
				if err := runUnrevisionedEventFixtureTransactionForTest(context.Background(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "ALTER TABLE unavailable_notify_observation RENAME TO "+reader.table)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if readErr == nil || errors.Is(readErr, sql.ErrNoRows) || !reflect.DeepEqual(got, reader.zero) {
					t.Fatalf("%s schema failure became evidence: %+v %v", reader.name, got, readErr)
				}
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), fixture.store)
		})
	}
}
