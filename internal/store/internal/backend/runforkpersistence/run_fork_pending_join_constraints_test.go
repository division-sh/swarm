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
