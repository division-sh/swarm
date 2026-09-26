package pipelinepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestFanOutProducerFairnessIgnoresCallerAuditTimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			seedFanOutReadbackClaim(t, db)
			intent, err := scanFanOutIntent(db.QueryRow(`SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents`))
			if err != nil {
				t.Fatal(err)
			}
			request := intent.Request
			request.Capsule.EntityID = uuid.NewString()
			request.Source = fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEntityField,
				RunID: request.Key.RunID, EntityID: request.Capsule.EntityID, Field: "items"}
			valueType := "TEXT"
			if backend == "postgres" {
				valueType = "JSONB"
			}
			if _, err := db.Exec(`CREATE TABLE entity_mutations (
				mutation_id TEXT, run_id TEXT, entity_id TEXT, domain TEXT, path TEXT,
				old_value ` + valueType + `, new_value ` + valueType + `, caused_by_event TEXT,
				writer_type TEXT, writer_id TEXT, handler_step TEXT, created_at TIMESTAMP)`); err != nil {
				t.Fatal(err)
			}
			stateFields := json.RawMessage(`{"items":[{"value":1},{"value":2}]}`)
			for _, offset := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
				request.Key.TriggeringDeliveryID = uuid.NewString()
				before := time.Now().UTC().Add(-time.Millisecond)
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := request.Validate(); err != nil {
					t.Fatal(err)
				}
				if err := insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", nil, runforkrevision.NewEffects(), request, stateFields, request.Capsule.Lineage.ParentEventID, before.Add(offset)); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				after := time.Now().UTC().Add(time.Millisecond)
				actual, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE triggering_delivery_id=$1`, request.Key.TriggeringDeliveryID))
				if err != nil {
					t.Fatal(err)
				}
				if actual.CreatedAt.Before(before) || actual.CreatedAt.After(after) || !actual.LastServedAt.IsZero() || !actual.UpdatedAt.Equal(actual.CreatedAt) {
					t.Fatalf("caller audit offset %v changed finite creation position: created=%v updated=%v served=%v admission=[%v,%v]", offset, actual.CreatedAt, actual.UpdatedAt, actual.LastServedAt, before, after)
				}
			}
		})
	}
}
