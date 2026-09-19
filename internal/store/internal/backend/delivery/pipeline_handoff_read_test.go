package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestPipelineHandoffObservationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, adapter, _ := selectionWriterFixture(t, backend)
			// Isolate the scalar read contract. Group integration tests use the
			// complete selected-store schema and canonical delivery admission.
			if _, err := db.Exec(`CREATE TABLE event_deliveries (event_id TEXT NOT NULL, continuation_handoff_at TIMESTAMP)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			eventID, otherID := uuid.NewString(), uuid.NewString()
			assert := func(q queryer, id string, want bool) {
				t.Helper()
				got, err := adapter.PipelineHandoffIncomplete(ctx, q, id)
				if err != nil || got != want {
					t.Fatalf("handoff incomplete=%v want=%v err=%v", got, want, err)
				}
			}
			assert(db, eventID, false)
			if _, err := db.Exec(`INSERT INTO event_deliveries (event_id) VALUES ($1),($2),($2)`, otherID, eventID); err != nil {
				t.Fatal(err)
			}
			assert(db, eventID, true)
			if _, err := db.Exec(`UPDATE event_deliveries SET continuation_handoff_at=CURRENT_TIMESTAMP WHERE event_id=$1`, eventID); err != nil {
				t.Fatal(err)
			}
			assert(db, eventID, false)
			assert(db, otherID, true)
			if _, err := db.Exec(`INSERT INTO event_deliveries (event_id) VALUES ($1)`, eventID); err != nil {
				t.Fatal(err)
			}
			assert(db, eventID, true)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`UPDATE event_deliveries SET continuation_handoff_at=CURRENT_TIMESTAMP WHERE event_id=$1`, eventID); err != nil {
				t.Fatal(err)
			}
			assert(tx, eventID, false)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assert(db, eventID, true)
			for _, invalid := range []string{"", " " + eventID} {
				if got, err := adapter.PipelineHandoffIncomplete(ctx, db, invalid); err == nil || got {
					t.Fatalf("invalid identity accepted: %q %v %v", invalid, got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := adapter.PipelineHandoffIncomplete(cancelled, db, eventID); !errors.Is(err, context.Canceled) || got {
				t.Fatalf("cancelled observation=%v err=%v", got, err)
			}
			if _, err := db.Exec(`DROP TABLE event_deliveries`); err != nil {
				t.Fatal(err)
			}
			if got, err := adapter.PipelineHandoffIncomplete(ctx, db, eventID); err == nil || got {
				t.Fatalf("failed read returned evidence: %v %v", got, err)
			}
		})
	}
}
