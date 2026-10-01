package pipelinepersistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

// This complements the state-map lineage proof using the actual retained reader
// over the existing reduced SQL fixture, not fork materialization or execution.
func TestA2RetainedPayloadMapSourceLineageOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			db := a2CollectionDatabase(t, backend)
			ref := rc.CatalogTypeReference{Type: "map[text][integer]"}
			ancestor := a2CollectionRequest(t, fanoutobligation.SourceEventPayloadField, ref, 3)
			original := []byte(`{"other":{"other-z":[90],"other-a":[80],"other-mid":[70]},"items":{"z":[9,9]," a ":[2],"a":[3]}}`)
			a2CollectionTrigger(t, db, ancestor, original)
			a2PersistCollection(t, db, backend, ancestor, nil)
			child := a2CollectionRequest(t, fanoutobligation.SourceEventPayloadField, ref, 3)
			a2CollectionTrigger(t, db, child, []byte(`{"items":{"child-z":[19],"child-a":[12],"child-mid":[13]}}`))
			a2PersistCollection(t, db, backend, child, nil)
			foreign := uuid.NewString()
			if _, err := db.Exec(`INSERT INTO runs (run_id,forked_from_run_id) VALUES ($1,NULL),($2,$1),($3,NULL)`, ancestor.Key.RunID, child.Key.RunID, foreign); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO entity_state (run_id,entity_id,fields) VALUES ($1,$1,$2)`, child.Key.RunID, `{"items":{"live-z":[99],"live-a":[98],"live-mid":[97]}}`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE fan_out_intents SET cursor=1,next_chunk_size=2 WHERE triggering_delivery_id=$1`, ancestor.Key.TriggeringDeliveryID); err != nil {
				t.Fatal(err)
			}
			loadInherited := func() fanoutobligation.Intent {
				t.Helper()
				intent, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE triggering_delivery_id=$1`, ancestor.Key.TriggeringDeliveryID))
				if err != nil {
					t.Fatal(err)
				}
				// As in the state-map control, supply the inherited reader context;
				// this is not an admitted/materialized executable fork intent.
				intent.Request.Key.RunID = child.Key.RunID
				intent.Request.Capsule.EntityID = child.Key.RunID
				return intent
			}
			snapshot := func() [][][]string {
				t.Helper()
				var evidence [][][]string
				for _, query := range []string{
					`SELECT * FROM fan_out_intents ORDER BY run_id,triggering_delivery_id`,
					`SELECT * FROM fan_out_outcomes ORDER BY run_id,triggering_delivery_id,ordinal`,
					`SELECT * FROM events ORDER BY event_id`,
					`SELECT * FROM entity_mutations ORDER BY mutation_id`,
					`SELECT * FROM entity_state ORDER BY run_id,entity_id`,
					`SELECT * FROM runs ORDER BY run_id`,
					`SELECT * FROM run_fork_selected_contract_executions ORDER BY fork_event_id`,
					`SELECT * FROM run_fork_delivery_event_replays ORDER BY fork_event_id`,
				} {
					rows, err := db.Query(query)
					if err != nil {
						t.Fatal(err)
					}
					columns, err := rows.Columns()
					if err != nil {
						rows.Close()
						t.Fatal(err)
					}
					var table [][]string
					for rows.Next() {
						values, destinations := make([]any, len(columns)), make([]any, len(columns))
						for i := range values {
							destinations[i] = &values[i]
						}
						if err := rows.Scan(destinations...); err != nil {
							rows.Close()
							t.Fatal(err)
						}
						row := make([]string, len(values))
						for i, value := range values {
							if raw, ok := value.([]byte); ok {
								value = string(raw)
							}
							row[i] = fmt.Sprintf("%T:%v", value, value)
						}
						table = append(table, row)
					}
					if err := rows.Err(); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					if err := rows.Close(); err != nil {
						t.Fatal(err)
					}
					evidence = append(evidence, table)
				}
				return evidence
			}
			before := snapshot()
			requireUnchanged := func() {
				t.Helper()
				if after := snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatalf("retained reader changed cursor/source/effects: before=%#v after=%#v", before, after)
				}
			}
			read := func() {
				t.Helper()
				intent := loadInherited()
				if intent.Source != ancestor.Source || intent.Request.Source != ancestor.Source || intent.Source.Field != "items" || intent.Cursor != 1 || intent.ChunkEndOrdinal() != 3 {
					t.Fatalf("inherited source/field/range changed: %#v", intent)
				}
				input, err := loadFanOutEvaluation(ctx, db, backend == "postgres", nil, intent)
				if err != nil {
					t.Fatal(err)
				}
				if input.Trigger.ID() != ancestor.Source.EventID || input.Trigger.RunID() != ancestor.Key.RunID || input.StartOrdinal != 1 || !reflect.DeepEqual(input.Items, []any{"a", "z"}) {
					t.Fatalf("inherited payload event/ordinal/suffix changed: %#v", input)
				}
				var want, got any
				if err := canonicaljson.DecodePreservingNumberLexemes(original, &want); err != nil {
					t.Fatal(err)
				}
				if err := canonicaljson.DecodePreservingNumberLexemes(input.Trigger.Payload(), &got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("ancestor payload was rebound to child/live state: %#v", got)
				}
				requireUnchanged()
			}
			read()
			read()
			for _, tc := range []struct {
				name, errorContains string
				change              func(*fanoutobligation.Intent)
			}{
				{"foreign_lineage", "outside intent run", func(intent *fanoutobligation.Intent) { intent.Request.Key.RunID = foreign }},
				{"missing_event", "event record missing", func(intent *fanoutobligation.Intent) {
					missing := uuid.NewString()
					intent.Source.EventID, intent.Request.Source.EventID, intent.Request.Capsule.Lineage.ParentEventID = missing, missing, missing
				}},
				{"missing_field", "source field absent is absent", func(intent *fanoutobligation.Intent) {
					intent.Source.Field, intent.Request.Source.Field = "absent", "absent"
					intent.Request.Capsule.SourceProjection = intent.Request.Capsule.SourceProjection.Clone()
					intent.Request.Capsule.SourceProjection.ItemsFrom = "payload.absent"
					var err error
					intent.Request.PlanRef.SemanticDigest, err = canonicaljson.Hash(intent.Request.Capsule.SourceProjection)
					if err != nil {
						t.Fatal(err)
					}
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					intent := loadInherited()
					tc.change(&intent)
					if err := intent.Validate(); err != nil {
						t.Fatalf("hostile reader input failed before its source gate: %v", err)
					}
					_, err := loadFanOutEvaluation(ctx, db, backend == "postgres", nil, intent)
					if err == nil || !strings.Contains(err.Error(), tc.errorContains) {
						t.Fatalf("%s source refusal = %v, want %q", tc.name, err, tc.errorContains)
					}
					if tc.name == "missing_event" {
						var missing *eventrecord.MissingError
						if !errors.Is(err, eventrecord.ErrMissing) || !errors.As(err, &missing) || missing.EventID != intent.Source.EventID {
							t.Fatalf("missing event lost exact owner/identity: %v", err)
						}
					}
					requireUnchanged()
				})
				read()
			}
		})
	}
}
