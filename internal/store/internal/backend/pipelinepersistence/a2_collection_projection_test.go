package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// These are writer/codec/retained-reader controls over reduced SQL fixtures,
// not a claim of full boot, publication, lifecycle, or schema integration.
func a2CollectionDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	db := fanOutReadbackTestDB(t, backend)
	idType, jsonType, bytesType, timeType := "TEXT", "TEXT", "BLOB", "TIMESTAMP"
	if backend == "postgres" {
		idType, jsonType, bytesType, timeType = "UUID", "JSONB", "BYTEA", "TIMESTAMPTZ"
	}
	columns := []string{}
	for _, name := range strings.Fields("event_class event_name task_id flow_instance scope payload_schema_bundle_hash payload_schema_flow_id payload_schema_event_key payload_schema_digest payload_schema_class execution_mode produced_by produced_by_type routing_source_kind routing_source_authority") {
		columns = append(columns, name+" TEXT")
	}
	for _, name := range strings.Fields("event_id run_id entity_id source_event_id operator_reference_event_id") {
		columns = append(columns, name+" "+idType)
	}
	for _, name := range strings.Fields("payload source_route target_route target_set route_settlement inherited_fan_out_origin") {
		columns = append(columns, name+" "+jsonType)
	}
	columns = append(columns, "payload_bytes "+bytesType, "chain_depth INTEGER", "created_at "+timeType, "UNIQUE(event_id)")
	for _, ddl := range []string{
		"CREATE TABLE events (" + strings.Join(columns, ",") + ")",
		"CREATE TABLE runs (run_id " + idType + " PRIMARY KEY, forked_from_run_id " + idType + ")",
		"CREATE TABLE run_fork_selected_contract_executions (fork_event_id " + idType + ", source_run_id " + idType + ", source_event_id " + idType + ", selection_authority TEXT)",
		"CREATE TABLE run_fork_delivery_event_replays (fork_event_id " + idType + ", source_run_id " + idType + ", source_event_id " + idType + ", selection_authority TEXT)",
		"CREATE TABLE entity_mutations (mutation_id TEXT,run_id TEXT,entity_id TEXT,domain TEXT,path TEXT,old_value " + jsonType + ",new_value " + jsonType + ",caused_by_event TEXT,writer_type TEXT,writer_id TEXT,handler_step TEXT,created_at TIMESTAMP)",
		"CREATE TABLE entity_state (run_id TEXT, entity_id TEXT, fields " + jsonType + ")",
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func a2CollectionRequest(t *testing.T, kind fanoutobligation.SourceKind, ref rc.CatalogTypeReference, cardinality int) fanoutobligation.IntentRequest {
	t.Helper()
	run, event, delivery := uuid.NewString(), uuid.NewString(), uuid.NewString()
	element := rc.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: `handlers["batch.ready"].fan_out`}
	p, err := rc.AdmitCollectionProjection(ref)
	if err != nil {
		t.Fatal(err)
	}
	path := "payload.items"
	source := fanoutobligation.SourceRef{Kind: kind, Field: "items", EventID: event}
	if kind == fanoutobligation.SourceEntityField {
		path = "entity.items"
		source.EventID = ""
		source.RunID = run
		source.EntityID = run
	}
	s := rc.FanOutPlanSemantics{ElementRef: element, ItemsFrom: path, CollectionType: ref, CollectionProjection: p, ItemType: p.ItemType(), ItemAlias: "entry", Identity: "entry", IdentityDerived: true, MaxItems: rc.DefaultFanOutMaxItems, Emit: rc.EmitSpec{Event: "item.ready"}}
	digest, err := canonicaljson.Hash(s)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := events.NewRootRoutingSource(run)
	if err != nil {
		t.Fatal(err)
	}
	request := fanoutobligation.IntentRequest{
		Key:     fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: delivery, ElementRef: element},
		PlanRef: rc.FanOutPlanRef{BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), ElementRef: element, SemanticDigest: digest}, Source: source, Cardinality: cardinality,
		Capsule: fanoutobligation.Capsule{SourceProjection: s, NodeKey: "root.scatter", ExecutionFlowID: ".", EntityID: run, HandlerEventKey: "batch.ready", Route: flowidentity.StoredRoute(".", run, run), ProducerSource: producer, Lineage: events.EventLineage{RunID: run, ParentEventID: event, ExecutionMode: executionmode.Live}},
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}

func a2CollectionFixtureEvidence(t *testing.T, element rc.FanOutElementRef, path string, ref rc.CatalogTypeReference) (rc.FanOutPlanSemantics, string) {
	t.Helper()
	projection, err := rc.AdmitCollectionProjection(ref)
	if err != nil {
		t.Fatal(err)
	}
	evidence := rc.FanOutPlanSemantics{ElementRef: element, ItemsFrom: path, CollectionType: ref, CollectionProjection: projection,
		ItemType: projection.ItemType(), ItemAlias: "entry", Identity: "entry", IdentityDerived: true,
		MaxItems: rc.DefaultFanOutMaxItems, Emit: rc.EmitSpec{Event: "item.ready"}}
	digest, err := canonicaljson.Hash(evidence)
	if err != nil {
		t.Fatal(err)
	}
	return evidence, digest
}

func a2CollectionTrigger(t *testing.T, db *sql.DB, r fanoutobligation.IntentRequest, payload []byte) {
	t.Helper()
	event := eventtest.RunCreatingRootIngress(r.Capsule.Lineage.ParentEventID, "batch.ready", "gateway", "", payload, 0, r.Capsule.Lineage.RunID, "", events.EventEnvelope{}, time.Now().UTC())
	event, err := eventtest.AdmitPayload(event, "", "batch.ready")
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
	if err := insertPublicationBatchEventFixture(context.Background(), db, record); err != nil {
		t.Fatal(err)
	}
}

func a2PersistCollection(t *testing.T, db *sql.DB, backend string, request fanoutobligation.IntentRequest, state []byte) fanoutobligation.Intent {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", nil, runforkrevision.NewEffects(), request, state, request.Capsule.Lineage.ParentEventID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	intent, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE triggering_delivery_id=$1`, request.Key.TriggeringDeliveryID))
	if err != nil {
		t.Fatal(err)
	}
	if !intent.Request.Capsule.Equal(request.Capsule) || intent.Request.PlanRef != request.PlanRef {
		t.Fatal("source evidence changed across writer/scan")
	}
	return intent
}

func TestA2PersistedCollectionProjectionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := a2CollectionDatabase(t, backend)
			for _, tc := range []struct {
				name, typ, raw string
				want           []any
			}{
				{"scalar_map", "map[text]numeric", `{"z":75.0," a ":2,"a":3}`, []any{" a ", "a", "z"}},
				{"record_map", "map[text]Record", `{"z":{"values":[1,1]},"a":{"values":[2]}}`, []any{"a", "z"}},
				{"list_map", "map[text][integer]", `{"z":[1,1],"a":[2]}`, []any{"a", "z"}},
				{"empty_map", "map[text]integer", `{}`, []any{}},
				{"empty_list", "[text]", `[]`, []any{}},
				{"list_order_multiplicity", "[text]", `["z","a","z"]`, []any{"z", "a", "z"}},
			} {
				for _, kind := range []fanoutobligation.SourceKind{fanoutobligation.SourceEventPayloadField, fanoutobligation.SourceEntityField} {
					t.Run(tc.name+"/"+string(kind), func(t *testing.T) {
						ref := rc.CatalogTypeReference{Type: tc.typ, Catalog: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Record": {Fields: map[string]rc.TypeFieldSpec{"values": {Type: "[integer]"}}}}}}
						r := a2CollectionRequest(t, kind, ref, len(tc.want))
						if kind == fanoutobligation.SourceEntityField {
							r.Capsule.SourceProjection.SourceAfterWrites = true
							var err error
							r.PlanRef.SemanticDigest, err = canonicaljson.Hash(r.Capsule.SourceProjection)
							if err != nil {
								t.Fatal(err)
							}
						}
						field := []byte(`{"items":` + tc.raw + `}`)
						a2CollectionTrigger(t, db, r, field)
						intent := a2PersistCollection(t, db, backend, r, field)
						var raw []byte
						if kind == fanoutobligation.SourceEntityField {
							if intent.Source.MutationID == "" {
								t.Fatal("state source not pinned")
							}
							if err := db.QueryRow(`SELECT new_value FROM entity_mutations WHERE mutation_id=$1`, intent.Source.MutationID).Scan(&raw); err != nil {
								t.Fatal(err)
							}
							var before, after any
							if err := canonicaljson.DecodePreservingNumberLexemes([]byte(tc.raw), &before); err != nil {
								t.Fatal(err)
							}
							if err := canonicaljson.DecodePreservingNumberLexemes(raw, &after); err != nil {
								t.Fatal(err)
							}
							if !reflect.DeepEqual(before, after) {
								t.Fatalf("meaningful values/number kinds lost: %s", raw)
							}
						} else {
							raw = []byte(tc.raw)
						}
						got, err := collectionRangeFromJSON(raw, intent.Request, 0, len(tc.want))
						if err != nil || !reflect.DeepEqual(got, tc.want) {
							t.Fatalf("retained projection: %#v %v", got, err)
						}
						if len(tc.want) == 0 && intent.Status != fanoutobligation.StatusClosed {
							t.Fatal("empty collection owes work")
						}
					})
				}
			}
		})
	}
}

func TestA2DeferredCollectionRangeOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := a2CollectionDatabase(t, backend)
			value := map[string]any{}
			for i := 39; i >= 0; i-- {
				value[fmt.Sprintf("k%02d", i)] = i
			}
			r := a2CollectionRequest(t, fanoutobligation.SourceEntityField, rc.CatalogTypeReference{Type: "map[text]integer"}, 40)
			raw, err := json.Marshal(map[string]any{"items": value})
			if err != nil {
				t.Fatal(err)
			}
			a2CollectionTrigger(t, db, r, []byte(`{"items":{}}`))
			intent := a2PersistCollection(t, db, backend, r, raw)
			if _, err := db.Exec(`INSERT INTO runs (run_id) VALUES ($1)`, r.Key.RunID); err != nil {
				t.Fatal(err)
			}
			live := map[string]any{}
			for i := 0; i < 40; i++ {
				live[fmt.Sprintf("live%02d", i)] = i + 100
			}
			liveRaw, err := json.Marshal(map[string]any{"items": live})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO entity_state (run_id,entity_id,fields) VALUES ($1,$1,$2)`, r.Key.RunID, string(liveRaw)); err != nil {
				t.Fatal(err)
			}
			for _, chunk := range []struct{ cursor, size int }{{3, 32}, {35, 2}, {37, 3}} {
				intent.Cursor, intent.NextChunkSize = chunk.cursor, chunk.size
				if _, err := db.Exec(`UPDATE fan_out_intents SET cursor=$1,next_chunk_size=$2 WHERE run_id=$3`, chunk.cursor, chunk.size, r.Key.RunID); err != nil {
					t.Fatal(err)
				}
				for retry := 0; retry < 2; retry++ {
					// Restart uses a fresh strict SQL scan, not an in-memory projection.
					loaded, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1`, r.Key.RunID))
					if err != nil {
						t.Fatal(err)
					}
					input, err := loadFanOutEvaluation(context.Background(), db, backend == "postgres", nil, loaded)
					if err != nil {
						t.Fatal(err)
					}
					if input.StartOrdinal != chunk.cursor || len(input.Items) != chunk.size {
						t.Fatalf("wrong bounded range %#v", input)
					}
					for offset, key := range input.Items {
						if key != fmt.Sprintf("k%02d", chunk.cursor+offset) {
							t.Fatalf("live source/order drift: %#v", input.Items)
						}
					}
				}
			}
		})
	}
}

func TestA2CollectionProjectionEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := a2CollectionDatabase(t, backend)
			r := a2CollectionRequest(t, fanoutobligation.SourceEntityField, rc.CatalogTypeReference{Type: "map[text]integer"}, 2)
			a2CollectionTrigger(t, db, r, []byte(`{"items":{}}`))
			intent := a2PersistCollection(t, db, backend, r, []byte(`{"items":{"z":1,"a":2}}`))
			for _, tc := range []struct {
				name, column string
				value        any
			}{
				{"missing_evidence", "capsule", func() string {
					c := r.Capsule
					c.SourceProjection = rc.FanOutPlanSemantics{}
					raw, _ := fanoutobligation.MarshalCapsule(c)
					return string(raw)
				}()},
				{"projection_mode", "capsule", func() string {
					c := r.Capsule
					c.SourceProjection = c.SourceProjection.Clone()
					c.SourceProjection.CollectionProjection.Kind = rc.CollectionListItems
					raw, _ := fanoutobligation.MarshalCapsule(c)
					return string(raw)
				}()},
				{"plan_digest", "semantic_digest", "sha256:" + strings.Repeat("9", 64)},
				{"source_field", "source_field", "other"},
				{"missing_revision", "source_mutation_id", nil},
				{"cardinality", "cardinality", 1001},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tx, err := db.BeginTx(context.Background(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if _, err := tx.Exec(`UPDATE fan_out_intents SET `+tc.column+`=$1 WHERE run_id=$2`, tc.value, r.Key.RunID); err != nil {
						t.Fatal(err)
					}
					if _, err := scanFanOutIntent(tx.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1`, r.Key.RunID)); err == nil {
						t.Fatal("corrupt evidence survived scan")
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				})
			}
			for _, raw := range []string{`{}`, `{"items":null}`, `{"items":["a","z"]}`, `{"items":{"a":1}}`} {
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				err = insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", nil, runforkrevision.NewEffects(), r, []byte(raw), r.Capsule.Lineage.ParentEventID, time.Now().UTC())
				if err == nil {
					t.Fatalf("invalid state source accepted: %s", raw)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			for _, bad := range []func(*fanoutobligation.IntentRequest){
				func(r *fanoutobligation.IntentRequest) { r.Capsule.SourceProjection = rc.FanOutPlanSemantics{} },
				func(r *fanoutobligation.IntentRequest) { r.PlanRef.SemanticDigest = "wrong" },
				func(r *fanoutobligation.IntentRequest) { r.Source.Field = "wrong" },
				func(r *fanoutobligation.IntentRequest) { r.Cardinality = 1001 },
			} {
				request := r
				request.Capsule.SourceProjection = r.Capsule.SourceProjection.Clone()
				bad(&request)
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				err = insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", nil, runforkrevision.NewEffects(), request, []byte(`{"items":{"a":1,"z":2}}`), r.Capsule.Lineage.ParentEventID, time.Now().UTC())
				if err == nil {
					t.Fatal("contradictory source evidence accepted at insert")
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			// Source revision, intent and earlier state effects share the caller's
			// transaction; a later failure must leave none of this candidate.
			candidate := r
			candidate.Key.TriggeringDeliveryID = uuid.NewString()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO entity_state (run_id,entity_id,fields) VALUES ($1,$1,$2)`, r.Key.RunID, `{"items":{"a":1,"z":2}}`); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", nil, runforkrevision.NewEffects(), candidate, []byte(`{"items":{"a":1,"z":2}}`), r.Capsule.Lineage.ParentEventID, time.Now().UTC()); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_mutations`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("refusal leaked source revisions: %d", count)
			}
			loaded, err := scanFanOutIntent(db.QueryRow(`SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents`))
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Cursor != 0 || loaded.Source != intent.Source {
				t.Fatal("refusal changed cursor or revision binding")
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM fan_out_intents`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("rollback leaked candidate intent: %d %v", count, err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rollback leaked earlier state effects: %d %v", count, err)
			}
		})
	}
}

func TestA2RetainedMapSourceLineageOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := a2CollectionDatabase(t, backend)
			r := a2CollectionRequest(t, fanoutobligation.SourceEntityField, rc.CatalogTypeReference{Type: "map[text]integer"}, 3)
			a2CollectionTrigger(t, db, r, []byte(`{"items":{}}`))
			intent := a2PersistCollection(t, db, backend, r, []byte(`{"items":{"z":1," a ":2,"a":3}}`))
			child, foreign := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO runs (run_id,forked_from_run_id) VALUES ($1,NULL),($2,$1),($3,NULL)`, r.Key.RunID, child, foreign); err != nil {
				t.Fatal(err)
			}
			intent.Request.Key.RunID = child
			intent.Request.Capsule.EntityID = child
			intent.Cursor = 1
			intent.NextChunkSize = 2
			input, err := loadFanOutEvaluation(context.Background(), db, backend == "postgres", nil, intent)
			if err != nil || !reflect.DeepEqual(input.Items, []any{"a", "z"}) {
				t.Fatalf("retained ancestor map = %#v, %v", input.Items, err)
			}
			intent.Request.Key.RunID = foreign
			if _, err := loadFanOutEvaluation(context.Background(), db, backend == "postgres", nil, intent); err == nil {
				t.Fatal("foreign lineage source accepted")
			}
			intent.Request.Key.RunID = child
			intent.Source.MutationID = uuid.NewString()
			if _, err := loadFanOutEvaluation(context.Background(), db, backend == "postgres", nil, intent); err == nil {
				t.Fatal("nonexistent immutable revision accepted")
			}
		})
	}
}
