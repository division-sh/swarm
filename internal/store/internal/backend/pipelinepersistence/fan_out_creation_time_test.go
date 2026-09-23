package pipelinepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
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
			if _, err := db.Exec(`CREATE TABLE resource_version_pins (run_id TEXT,flow_path TEXT,event_name TEXT,version_id TEXT)`); err != nil {
				t.Fatal(err)
			}
			request := intent.Request
			request.Source = fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion,
				Declaration: durabledata.DeclarationRef{FlowPath: "root", EventName: "items"}, VersionID: durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("a", 64))}
			if _, err := db.Exec(`INSERT INTO resource_version_pins VALUES ($1,$2,$3,$4)`, request.Key.RunID, request.Source.Declaration.FlowPath, request.Source.Declaration.EventName, request.Source.VersionID); err != nil {
				t.Fatal(err)
			}
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
				if err := insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", runforkrevision.NewEffects(), request, nil, request.Capsule.Lineage.ParentEventID, before.Add(offset)); err != nil {
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
