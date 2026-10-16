package pipelinepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func Test2293FanOutEvaluationSourceSurvivesLaterWritesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, final := range []string{`{"items":["new","tail"]}`, `{"items":["new"]}`, `{"items":["new","extra","tail"]}`, `{}`} {
			t.Run(backend+"/"+final, func(t *testing.T) {
				db := a2CollectionDatabase(t, backend)
				r := a2CollectionRequest(t, fanoutobligation.SourceEntityField, rc.CatalogTypeReference{Type: "[text]"}, 2)
				a2CollectionSeedRun(t, db, r)
				a2CollectionTrigger(t, db, r, []byte(`{}`))
				old := []any{"old", "tail"}
				var err error
				r, err = r.CaptureEntitySource(old)
				if err != nil {
					t.Fatal(err)
				}
				old[0] = "caller mutation"
				if _, err := db.Exec(`INSERT INTO entity_state (run_id,entity_id,fields) VALUES ($1,$2,$3)`, r.Key.RunID, r.Source.EntityID, final); err != nil {
					t.Fatal(err)
				}
				intent := a2PersistCapturedCollection(t, db, backend, r, []byte(final))
				// Reload after the producer returns, and resume at a nonzero ordinal.
				resumed, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1`, r.Key.RunID))
				if err != nil {
					t.Fatal(err)
				}
				resumed.Cursor = 1
				page, err := loadFanOutEvaluation(context.Background(), db, backend == "postgres", nil, resumed)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(page.Items, []any{"tail"}) {
					t.Fatalf("resumed ordinal source=%v", page.Items)
				}
				all, err := loadFanOutEvaluation(context.Background(), db, backend == "postgres", nil, intent)
				if err != nil || !reflect.DeepEqual(all.Items, []any{"old", "tail"}) {
					t.Fatalf("evaluation source=%v error=%v", all.Items, err)
				}
				var raw []byte
				if err := db.QueryRow(`SELECT fields FROM entity_state WHERE run_id=$1`, r.Key.RunID).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var got, want any
				if err := json.Unmarshal(raw, &got); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(final), &want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("final business state changed: %s", raw)
				}
			})
		}
	}
}

func Test2293FanOutEvaluationSourceCannotFallBackToFinalStateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := a2CollectionDatabase(t, backend)
			r := a2CollectionRequest(t, fanoutobligation.SourceEntityField, rc.CatalogTypeReference{Type: "[text]"}, 1)
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			_, err = bindFanOutSourceTx(context.Background(), tx, backend == "postgres", runforkrevision.NewEffects(), r, []byte(`{"items":["looks-valid"]}`), r.Capsule.Lineage.ParentEventID, time.Now().UTC())
			if err == nil || !strings.Contains(err.Error(), "evaluation-point capture") {
				t.Fatalf("uncaptured source borrowed final state: %v", err)
			}
			var count int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM entity_mutations`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("refusal wrote an immutable revision: %d %v", count, err)
			}
		})
	}
}
