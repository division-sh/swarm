package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunForkExactEventWriterPreservesAdmittedUUIDSpellingsBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		for _, spelling := range []string{"canonical", "uppercase", "compact", "run_uppercase", "run_compact"} {
			t.Run(spelling, func(t *testing.T) {
				f := newRunForkRevisionMatrixFixture()
				id := uuid.NewString()
				switch spelling {
				case "uppercase":
					id = strings.ToUpper(id)
				case "compact":
					id = strings.ReplaceAll(id, "-", "")
				case "run_uppercase":
					f.runID = strings.ToUpper(f.runID)
				case "run_compact":
					f.runID = strings.ReplaceAll(f.runID, "-", "")
				}
				requireRunFixtureForTest(t, testAuthorActivityContext(), s.selected, semanticRunFixture{
					Origin: semanticScenarioSetupRunOriginForTest(), RunID: f.runID, StartedAt: f.at,
				})
				event := eventtest.RunCreatingRootIngress(id, "revision.spelling", "gateway", "uuid-spelling", []byte(`{"value":1}`), 0, f.runID, "", events.EventEnvelope{}, f.at)
				event, err := eventtest.AdmitPayload(event, "", "revision.spelling")
				if err != nil {
					t.Fatal(err)
				}
				admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatalf("existing event admission rejected %s: %v", spelling, err)
				}
				record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
				if err != nil {
					t.Fatal(err)
				}
				if record.EventID != id || record.RunID != f.runID {
					t.Fatalf("admission rewrote input UUID: event=%q/%q run=%q/%q", record.EventID, id, record.RunID, f.runID)
				}
				exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					effects := runforkrevision.NewEffects()
					var inserted bool
					var err error
					if s.postgres {
						inserted, err = eventrecordpostgres.Insert(ctx, tx, effects, record)
					} else {
						inserted, err = eventrecordsqlite.Insert(ctx, tx, effects, record)
					}
					if err != nil || !inserted {
						t.Fatalf("actual event-record insert: inserted=%t err=%v", inserted, err)
					}
					var storedID, storedRunID string
					if err := tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT), CAST(run_id AS TEXT) FROM events WHERE event_id=$1`, id).Scan(&storedID, &storedRunID); err != nil {
						t.Fatal(err)
					}
					wantStored, wantStoredRun := id, f.runID
					if s.postgres {
						wantStored = uuid.MustParse(id).String()
						wantStoredRun = uuid.MustParse(f.runID).String()
					}
					if storedID != wantStored || storedRunID != wantStoredRun {
						t.Fatalf("unexpected persisted UUID: event=%q/%q run=%q/%q", storedID, wantStored, storedRunID, wantStoredRun)
					}
					whole, err := runforkrevision.ForRun(storedRunID, runforkrevision.FamilyEvents)
					if err != nil {
						t.Fatal(err)
					}
					// Same inserted row and prior ledger; roll back only the oracle's
					// finalization so the actual writer's effects remain untouched.
					mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT spelling_oracle`)
					want, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole)
					if err != nil {
						t.Fatalf("whole-capture control: %v", err)
					}
					wantLedger := exactLedger(t, ctx, tx, storedRunID)
					if len(wantLedger) != 1 || wantLedger[0].Key != storedID || !wantLedger[0].Present {
						t.Fatalf("whole control did not capture actual persisted event: %+v", wantLedger)
					}
					t.Logf("admission+insert+whole capture PASS: event_input=%q stored=%q run_input=%q stored=%q ledger_key=%q", id, storedID, f.runID, storedRunID, wantLedger[0].Key)
					mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT spelling_oracle`)
					mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT spelling_oracle`)
					got, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, effects)
					if err != nil {
						t.Fatalf("actual event writer exact capture rejected admitted UUID input=%q stored=%q after whole control passed: %v", id, storedID, err)
					}
					if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(exactLedger(t, ctx, tx, storedRunID), wantLedger) {
						t.Fatalf("exact writer capture differs from whole control: exact=%+v whole=%+v", got, want)
					}
					duplicateEffects := runforkrevision.NewEffects()
					if s.postgres {
						inserted, err = eventrecordpostgres.Insert(ctx, tx, duplicateEffects, record)
					} else {
						inserted, err = eventrecordsqlite.Insert(ctx, tx, duplicateEffects, record)
					}
					if err != nil || inserted {
						t.Fatalf("duplicate insert: inserted=%t err=%v", inserted, err)
					}
					duplicateResults, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, duplicateEffects)
					if err != nil || len(duplicateResults) != 0 || !reflect.DeepEqual(exactLedger(t, ctx, tx, storedRunID), wantLedger) {
						t.Fatalf("duplicate insert contributed effects: results=%+v err=%v", duplicateResults, err)
					}
				})
			})
		}
	})
}
