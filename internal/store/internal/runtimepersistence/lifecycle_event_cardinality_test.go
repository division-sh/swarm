package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestLifecycleEventCardinalityPreservesDuplicatesNamesAndClassesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, runID, sibling := testAuthorActivityContext(), uuid.NewString(), uuid.NewString()
			at := time.Date(2026, 10, 4, 17, 0, 0, 0, time.UTC)
			for _, run := range []string{runID, sibling} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, run)
				for _, name := range []string{"work.completed", "work.completed", "concrete/work.completed", "contract.diagnostic"} {
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(name), "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, at)
					if err := commitSemanticEventFixture(ctx, fixture.store.(semanticEventFixtureStore), event); err != nil {
						t.Fatal(err)
					}
				}
				for _, event := range []events.Event{
					eventtest.RuntimeDiagnostic(uuid.NewString(), "contract.diagnostic", "fixture", "", []byte(`{}`), 0, run, "", events.EventEnvelope{}, at),
					eventtest.DiagnosticDirect(uuid.NewString(), "platform.runtime_log", "runtime", "", []byte(`{"message":"cardinality-first"}`), 0, run, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, at),
					eventtest.DiagnosticDirect(uuid.NewString(), "platform.runtime_log", "runtime", "", []byte(`{"message":"cardinality-second"}`), 0, run, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, at),
				} {
					var err error
					if event.AdmissionClass() == events.EventAdmissionDiagnosticDirect {
						err = commitDiagnosticRuntimeLogFixture(ctx, fixture.store.(diagnosticRuntimeLogFixtureStore), event)
					} else {
						err = commitSemanticEventFixture(ctx, fixture.store.(semanticEventFixtureStore), event)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, check := range []struct {
				name string
				want int
			}{{"work.completed", 2}, {"concrete/work.completed", 1}, {"platform.runtime_log", 2}, {"contract.diagnostic", 2}, {"Work.completed", 0}, {"template/work.completed", 0}, {"missing", 0}} {
				var physical int
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, runID, check.name).Scan(&physical); err != nil {
					t.Fatal(err)
				}
				got, err := ReadLifecycleEventCardinalityForTest(ctx, fixture.store, runID, check.name)
				if err != nil || got != check.want || got != physical {
					t.Fatalf("exact event name %q scope/class/cardinality changed: got=%d physical=%d want=%d err=%v", check.name, got, physical, check.want, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 7 || counts.Total.ReadCommits != 7 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("cardinality bypassed original read coordinator: %+v", counts)
			}
			if got, err := ReadLifecycleEventCardinalityForTest(ctx, fixture.store, uuid.NewString(), "work.completed"); err != nil || got != 0 {
				t.Fatalf("absence fabricated rows: %d %v", got, err)
			}
			if got, err := ReadLifecycleEventCardinalityForTest(ctx, fixture.store, sibling, "work.completed"); err != nil || got != 2 {
				t.Fatalf("sibling run lost exact scope: %d %v", got, err)
			}
		})
	}
}

func TestLifecycleEventCardinalityRefusesInvalidCancelledAndUnavailableOwnershipBothStores(t *testing.T) {
	ctx, runID := context.Background(), "abcdef01-1111-4111-8111-111111111111"
	refuse := func(t *testing.T, ctx context.Context, owner any, runID, name string) {
		t.Helper()
		if got, err := ReadLifecycleEventCardinalityForTest(ctx, owner, runID, name); err == nil || got != 0 {
			t.Fatalf("refusal returned fabricated count: %d %v", got, err)
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(t, ctx, owner, runID, "work.completed")
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), strings.ToUpper(runID)} {
				refuse(t, ctx, fixture.store, invalid, "work.completed")
			}
			refuse(t, ctx, fixture.store, runID, "")
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadLifecycleEventCardinalityForTest(cancelled, fixture.store, runID, "work.completed"); !errors.Is(err, context.Canceled) || got != 0 {
				t.Fatalf("cancelled evidence lost refusal: %d %v", got, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(testAuthorActivityContext(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_lifecycle_events`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			restore := func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(testAuthorActivityContext(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_lifecycle_events RENAME TO events`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}
			defer func() {
				if !restored {
					restore()
				}
			}()
			refuse(t, ctx, fixture.store, runID, "work.completed")
			restore()
			restored = true
			if got, err := ReadLifecycleEventCardinalityForTest(ctx, fixture.store, runID, "work.completed"); err != nil || got != 0 {
				t.Fatalf("restored empty storage unavailable: %d %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, ctx, fixture.store, runID, "work.completed")
		})
	}
}
