package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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
				ctx := testAuthorActivityContext()
				requireRunFixtureForTest(t, ctx, s.selected, semanticRunFixture{
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
				type proof struct {
					storedID, storedRunID string
					oracleLedger          []exactLedgerRow
				}
				committed := runExactFactProtocol(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) (proof, error) {
					inserted, err := insertExactSpellingEvent(ctx, s.postgres, attempt, record)
					if err != nil {
						return proof{}, err
					}
					if !inserted {
						return proof{}, fmt.Errorf("actual event-record insert was a no-op")
					}
					var out proof
					err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						if err := tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT), CAST(run_id AS TEXT) FROM events WHERE event_id=$1`, id).Scan(&out.storedID, &out.storedRunID); err != nil {
							return err
						}
						wantID, wantRunID := id, f.runID
						if s.postgres {
							wantID, wantRunID = uuid.MustParse(id).String(), uuid.MustParse(f.runID).String()
						}
						if out.storedID != wantID || out.storedRunID != wantRunID {
							return fmt.Errorf("persisted UUID event=%q/%q run=%q/%q", out.storedID, wantID, out.storedRunID, wantRunID)
						}
						whole, err := runforkrevision.ForRun(out.storedRunID, runforkrevision.FamilyEvents)
						if err != nil {
							return err
						}
						mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT spelling_oracle`)
						if _, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, whole); err != nil {
							return fmt.Errorf("whole-capture control: %w", err)
						}
						out.oracleLedger = exactLedger(t, ctx, tx, out.storedRunID)
						mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT spelling_oracle`)
						mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT spelling_oracle`)
						if len(out.oracleLedger) != 1 || out.oracleLedger[0].Key != out.storedID || !out.oracleLedger[0].Present {
							return fmt.Errorf("whole control did not capture actual event: %+v", out.oracleLedger)
						}
						return nil
					})
					return out, err
				})
				out, acknowledged := committed.Value()
				if !acknowledged || committed.Err() != nil {
					t.Fatalf("exact event write: acknowledged=%v err=%v", acknowledged, committed.Err())
				}
				assertExactSpellingLedger(t, s, out.storedRunID, out.oracleLedger)
				duplicate := runExactFactProtocol(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) (bool, error) {
					return insertExactSpellingEvent(ctx, s.postgres, attempt, record)
				})
				inserted, acknowledged := duplicate.Value()
				if !acknowledged || duplicate.Err() != nil || inserted {
					t.Fatalf("duplicate insert: acknowledged=%v inserted=%v err=%v", acknowledged, inserted, duplicate.Err())
				}
				assertExactSpellingLedger(t, s, out.storedRunID, out.oracleLedger)
			})
		}
	})
}

func insertExactSpellingEvent(ctx context.Context, postgres bool, attempt *mutationprotocol.Attempt, record eventrecord.Record) (bool, error) {
	if postgres {
		return eventrecordpostgres.Insert(ctx, attempt, record)
	}
	return eventrecordsqlite.Insert(ctx, attempt, record)
}

func assertExactSpellingLedger(t *testing.T, s exactFactStore, runID string, want []exactLedgerRow) {
	t.Helper()
	exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
		if got := exactLedger(t, ctx, tx, runID); !reflect.DeepEqual(got, want) {
			t.Fatalf("actual protocol capture differs from whole control: exact=%+v whole=%+v", got, want)
		}
	})
}
