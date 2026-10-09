package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestReviewer2589TerminalSnapshotUUIDAlias(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := "abcdefab-cdef-4abc-8def-abcdefabcdef"
			started := time.Now().UTC().Round(time.Microsecond)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, started)
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "revision.spelling", "gateway", "counter-visibility", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, started)
			event, err := eventtest.AdmitPayload(event, "", "revision.spelling")
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
			if err != nil {
				t.Fatal(err)
			}
			readID := runID
			if fixture.postgres {
				readID = strings.ToUpper(runID)
			}
			err = runSelectedFixtureMutation(ctx, fixture.store, "review counter UUID identity", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				inserted, err := insertExactSpellingEvent(txctx, fixture.postgres, attempt, record)
				if err != nil {
					return err
				}
				if !inserted {
					return fmt.Errorf("expected physical event insert")
				}
				snapshot, _, err := writer.MarkTerminalTx(txctx, attempt, runtimerunlifecycle.TerminalRequest{RunID: readID, State: runtimerunlifecycle.StateCancelled, EndedAt: started.Add(time.Second)})
				if err != nil {
					return err
				}
				if snapshot.EventCount != 1 {
					return fmt.Errorf("terminal snapshot returned count=%d for physical event count=1", snapshot.EventCount)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIssue2589StagedEventFixturesMaintainPhysicalCountersBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newExactFactFixture(t, s)
		ctx := testAuthorActivityContext()
		dialect := authoractivityfixture.DialectSQLite
		if s.postgres {
			dialect = authoractivityfixture.DialectPostgres
		}
		recordFor := func(event events.Event) eventrecord.Record {
			t.Helper()
			bound, err := eventfixture.BindPayload(event)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
			if err != nil {
				t.Fatal(err)
			}
			return record
		}
		record := recordFor(eventtest.ExistingRunRootIngress(f.eventID, "matrix.event", "staged", "", []byte(`{"value":1}`), 0, f.runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, f.at))
		readCount := func(want int64) {
			t.Helper()
			snapshot, err := s.selected.(runLifecycleCandidateParityStore).LoadRunLifecycleSnapshot(ctx, f.runID)
			if err != nil || int64(snapshot.EventCount) != want {
				t.Fatalf("staged event counter=%d, want %d; err=%v", snapshot.EventCount, want, err)
			}
		}
		rollback := errors.New("roll back staged physical event and counter")
		err := runSelectedFixtureMutation(ctx, s.selected, "staged rollback", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
			return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				inserted, err := eventfixture.InsertUnrevisioned(txctx, tx, dialect, record)
				if err != nil || !inserted {
					return fmt.Errorf("staged rollback insert=%v err=%v", inserted, err)
				}
				return rollback
			})
		})
		if !errors.Is(err, rollback) {
			t.Fatal(err)
		}
		readCount(0)
		exactTransaction(t, s, func(txctx context.Context, tx *sql.Tx) {
			for _, want := range []bool{true, false} {
				inserted, err := eventfixture.InsertUnrevisioned(txctx, tx, dialect, record)
				if err != nil || inserted != want {
					t.Fatalf("staged event insert=%v, want %v; err=%v", inserted, want, err)
				}
			}
		})
		readCount(1)
		conflict := recordFor(eventtest.ExistingRunRootIngress(f.eventID, "matrix.event", "staged", "", []byte(`{"value":2}`), 0, f.runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, f.at))
		exactTransaction(t, s, func(txctx context.Context, tx *sql.Tx) {
			if inserted, err := eventfixture.InsertUnrevisioned(txctx, tx, dialect, conflict); err == nil || inserted {
				t.Fatalf("conflicting staged event admitted: inserted=%v err=%v", inserted, err)
			}
			runless := recordFor(eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", []byte(`{"log_level":"warn","message":"counter control"}`), 0, "", "", events.EventEnvelope{}, f.at))
			if inserted, err := eventfixture.InsertUnrevisioned(txctx, tx, dialect, runless); err != nil || !inserted {
				t.Fatalf("runless staged event insert=%v err=%v", inserted, err)
			}
		})
		readCount(1)
	})
}
