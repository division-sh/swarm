package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// This is physical schema admission, not a substitute for the actual selected
// publication/recovery journey. Use the complete authoritative generated schema.
func TestIssue642PendingJoinCauseLineageConstraintsBothStores(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			f := openTimerHistoryNativeFixture(t, dialect)
			now := time.Now().UTC()
			ancestor, parent, child := uuid.NewString(), uuid.NewString(), uuid.NewString()
			f.runContext(t, ancestor, now)
			f.runContext(t, parent, now)
			ctx := f.runContext(t, child, now)
			cause := seedPendingLineageConstraintEvent(t, f, ctx, ancestor, now)
			physicalSource := seedPendingLineageConstraintEvent(t, f, ctx, parent, now)
			output := seedPendingLineageConstraintEvent(t, f, ctx, child, now)
			pending := uuid.NewString()
			insert := func(ctx context.Context, tx *sql.Tx, source string, coordinate, causeRun any, result string) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_executions
					(execution_id,fork_run_id,source_run_id,source_event_id,source_obligation_event_id,committed_cause_run_id,fork_event_id,event_name,selection_authority,created_at)
					VALUES ($1,$2,$3,$4,$5,$6,$7,'platform.join_complete',$8,$9)`,
					uuid.NewString(), child, parent, source, coordinate, causeRun, result, "store.run_fork.selected_contract_execution", now)
				return err
			}
			write := func(source string, coordinate, causeRun any, result string) error {
				outcome := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (admission runfork.RunForkReplayResumeAdmission, err error) {
					err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						return insert(ctx, tx, source, coordinate, causeRun, result)
					})
					return
				})
				return outcome.Err()
			}
			if err := write(cause, pending, ancestor, output); err != nil {
				t.Fatalf("valid pending cause/obligation pair: %v", err)
			}
			physicalOutput := seedPendingLineageConstraintEvent(t, f, ctx, child, now)
			if err := write(physicalSource, nil, nil, physicalOutput); err != nil {
				t.Fatalf("ordinary physical lineage was not preserved: %v", err)
			}
			for _, fault := range []string{"missing_cause_run", "missing_obligation", "obligation_equals_cause", "wrong_cause_run", "missing_physical_cause", "null_cause_does_not_bypass_event_fk", "duplicate_obligation", "duplicate_physical_input", "duplicate_output"} {
				t.Run(fault, func(t *testing.T) {
					result := seedPendingLineageConstraintEvent(t, f, ctx, child, now)
					source, coordinate, causeRun := cause, any(uuid.NewString()), any(ancestor)
					switch fault {
					case "missing_cause_run":
						causeRun = nil
					case "missing_obligation":
						coordinate = nil
					case "obligation_equals_cause":
						coordinate = cause
					case "wrong_cause_run":
						causeRun = parent
					case "missing_physical_cause":
						source = uuid.NewString()
					case "null_cause_does_not_bypass_event_fk":
						source, coordinate, causeRun = uuid.NewString(), nil, nil
					case "duplicate_obligation":
						coordinate = pending
					case "duplicate_physical_input":
						source, coordinate, causeRun = physicalSource, nil, nil
					case "duplicate_output":
						result = output
					}
					if err := write(source, coordinate, causeRun, result); err == nil {
						t.Fatal("hostile/partial physical lineage was admitted")
					}
					var count int
					if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions`).Scan(&count); err != nil || count != 2 {
						t.Fatalf("refused lineage changed committed rows: count=%d error=%v", count, err)
					}
				})
			}
			outcome := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (admission runfork.RunForkReplayResumeAdmission, err error) {
				err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `DELETE FROM events WHERE event_id=$1`, cause)
					return err
				})
				return
			})
			if outcome.Err() == nil || outcome.Acknowledged() {
				t.Fatal("physical cause FK allowed purging the referenced ancestor event")
			}
		})
	}
}

func seedPendingLineageConstraintEvent(t *testing.T, f *timerHistoryNativeFixture, ctx context.Context, runID string, at time.Time) string {
	t.Helper()
	id := uuid.NewString()
	event := eventtest.PersistedProjectionForProducer(id, "platform.join_complete", eventtest.Producer(events.EventProducerPlatform, "test"), "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
	event, err := eventtest.AdmitPayload(event, ".", "platform.join_complete")
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (admission runfork.RunForkReplayResumeAdmission, err error) {
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if f.postgres != nil {
				_, err := eventpostgres.InsertUnrevisionedFixtureRecord(ctx, tx, record)
				return err
			}
			_, err := eventsqlite.InsertUnrevisionedFixtureRecord(ctx, tx, record)
			return err
		})
		return
	})
	if err := result.Err(); err != nil || !result.Acknowledged() {
		t.Fatalf("seed canonical schema-only event: %v", err)
	}
	return id
}
