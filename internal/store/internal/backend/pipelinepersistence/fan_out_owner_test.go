package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestFanOutIntentSQLArgsEncodeClosedSourceUnionWithExplicitAbsence(t *testing.T) {
	request := fanoutobligation.IntentRequest{
		PlanRef: runtimecontracts.FanOutPlanRef{BundleHash: "bundle", SemanticDigest: "digest"},
	}
	now := time.Now().UTC()
	for _, test := range []struct {
		name   string
		source fanoutobligation.SourceRef
		want   []any
	}{
		{
			name: "event payload field",
			source: fanoutobligation.SourceRef{
				Kind: fanoutobligation.SourceEventPayloadField, EventID: "event-id", Field: "items",
			},
			want: []any{"event_payload_field", "event-id", nil, nil, "items", nil, nil, nil, nil},
		},
		{
			name: "entity field revision",
			source: fanoutobligation.SourceRef{
				Kind: fanoutobligation.SourceEntityField, RunID: "run-id", EntityID: "entity-id", Field: "items", MutationID: "mutation-id",
			},
			want: []any{"entity_field_revision", nil, "run-id", "entity-id", "items", "mutation-id", nil, nil, nil},
		},
		{
			name: "resource version",
			source: fanoutobligation.SourceRef{
				Kind:        fanoutobligation.SourceResourceVersion,
				Declaration: durabledata.DeclarationRef{FlowPath: "root", EventName: "records"},
				VersionID:   durabledata.VersionID("resource-version"),
			},
			want: []any{"resource_version", nil, nil, nil, nil, nil, "root", "records", "resource-version"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := fanOutIntentSQLArgs(request, test.source, []byte(`{}`), fanoutobligation.StatusOpen, now)[7:16]
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("source SQL args = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestScanFanOutIntentPreservesCapsuleNumberLexemesOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			command := seedFanOutReadbackClaim(t, db)

			var capsuleRaw []byte
			if err := db.QueryRow(`SELECT capsule FROM fan_out_intents WHERE run_id=$1`, command.Claim.Key.RunID).Scan(&capsuleRaw); err != nil {
				t.Fatal(err)
			}
			var capsule fanoutobligation.Capsule
			if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
				t.Fatal(err)
			}
			capsule.StateFields = map[string]any{
				"integer": json.Number("75"),
				"decimal": json.Number("75.0"),
			}
			capsuleRaw, err := json.Marshal(capsule)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(capsuleRaw), command.Claim.Key.RunID); err != nil {
				t.Fatal(err)
			}

			intent, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1`, command.Claim.Key.RunID))
			if err != nil {
				t.Fatal(err)
			}
			integer, integerOK := intent.Request.Capsule.StateFields["integer"].(json.Number)
			decimal, decimalOK := intent.Request.Capsule.StateFields["decimal"].(json.Number)
			if !integerOK || integer.String() != "75" || !decimalOK || decimal.String() != "75.0" {
				t.Fatalf("hydrated capsule numerics = integer:%#v decimal:%#v, want lexical json.Number carriers", intent.Request.Capsule.StateFields["integer"], intent.Request.Capsule.StateFields["decimal"])
			}
		})
	}
}

func TestFanOutEntitySourceRevisionWriterPreservesNumberKindsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			newValueType := "TEXT"
			if backend == "postgres" {
				newValueType = "JSONB"
			}
			if _, err := db.Exec(`CREATE TABLE entity_mutations (
				mutation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, entity_id TEXT NOT NULL,
				domain TEXT NOT NULL, path TEXT NOT NULL, old_value ` + newValueType + ` NOT NULL,
				new_value ` + newValueType + ` NOT NULL, caused_by_event TEXT,
				writer_type TEXT NOT NULL, writer_id TEXT NOT NULL, handler_step TEXT NOT NULL,
				created_at TIMESTAMP NOT NULL
			)`); err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			var raw []byte
			rollback := errors.New("rollback isolated encoding test")
			write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					mutationID, err := insertFanOutEntitySourceRevisionTx(
						ctx, tx, backend == "postgres", attempt,
						runID, uuid.NewString(), "items",
						[]any{map[string]any{"integer": int64(75), "double": float64(75), "exponent": json.Number("75e0")}},
						uuid.NewString(), time.Now().UTC(),
					)
					if err != nil {
						return err
					}
					if err := tx.QueryRowContext(ctx, `SELECT new_value FROM entity_mutations WHERE mutation_id=$1`, mutationID).Scan(&raw); err != nil {
						return err
					}
					return rollback
				})
				return struct{}{}, err
			}
			var result mutationprotocol.Result[struct{}]
			if backend == "postgres" {
				store, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				result = mutationprotocol.RunPostgres(context.Background(), store, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
			} else {
				store, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				result = mutationprotocol.RunSQLite(context.Background(), store, "fan-out source encoding", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
			}
			if !errors.Is(result.Err(), rollback) {
				t.Fatalf("source revision attempt = %v, want isolated rollback", result.Err())
			}
			var items []map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(raw, &items); err != nil {
				t.Fatal(err)
			}
			for field, want := range map[string]string{"integer": "75", "double": "75.0", "exponent": "75.0"} {
				got, ok := items[0][field].(json.Number)
				if !ok || got.String() != want {
					t.Fatalf("persisted entity revision %s = %#v, want %q", field, items[0][field], want)
				}
			}
		})
	}
}

func TestFanOutChunkReadbackOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"committed", "not_committed", "contradictory"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				db := fanOutReadbackTestDB(t, backend)
				command := seedFanOutReadbackClaim(t, db)
				if outcome != "not_committed" {
					persistFanOutReadbackChunk(t, db, command)
				}
				if outcome == "contradictory" {
					if _, err := db.Exec(`DELETE FROM fan_out_outcomes`); err != nil {
						t.Fatal(err)
					}
				}
				committed, err := reconcileFanOutChunk(context.Background(), db, backend == "postgres", command)
				switch outcome {
				case "committed":
					if !committed || err != nil {
						t.Fatalf("exact commit = %t, %v", committed, err)
					}
				case "not_committed":
					if committed || err != nil {
						t.Fatalf("exact no-commit = %t, %v", committed, err)
					}
				case "contradictory":
					if committed || err == nil {
						t.Fatalf("contradictory readback = %t, %v", committed, err)
					}
				}
			})
		}
	}
}

func persistFanOutReadbackChunk(t *testing.T, db *sql.DB, command runtimepipeline.FanOutChunkCommand) {
	t.Helper()
	key := command.Claim.Key
	if _, err := db.Exec(`UPDATE fan_out_intents SET cursor=cursor+1,claim_owner=NULL,lease_expires_at=NULL,last_served_at=$1,next_chunk_size=$2 WHERE run_id=$3`, command.Now, fanoutobligation.MaxChunkSize, key.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,failure,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, command.Outcomes[0].Ordinal, string(fanoutobligation.OutcomeSemanticRejected), string(command.Outcomes[0].Failure), command.Now); err != nil {
		t.Fatal(err)
	}
}

func fanOutReadbackTestDB(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
	} else {
		var err error
		db, err = sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "fanout-readback.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	statements := []string{
		`CREATE TABLE fan_out_intents (
			run_id TEXT NOT NULL, triggering_delivery_id TEXT NOT NULL, flow_path TEXT NOT NULL, declaration_family TEXT NOT NULL, semantic_path TEXT NOT NULL,
			bundle_hash TEXT NOT NULL, semantic_digest TEXT NOT NULL, source_kind TEXT NOT NULL,
			source_event_id TEXT, source_run_id TEXT, source_entity_id TEXT, source_field TEXT, source_mutation_id TEXT,
			source_resource_flow_path TEXT, source_resource_event_name TEXT, source_resource_version_id TEXT,
			cardinality INTEGER NOT NULL, cursor INTEGER NOT NULL, status TEXT NOT NULL, next_chunk_size INTEGER NOT NULL,
			last_served_at TIMESTAMP, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
			claim_owner TEXT, claim_generation BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMP, blocked_reason TEXT, capsule TEXT NOT NULL,
			retry_ready_at TIMESTAMP, retry_failure TEXT,
			PRIMARY KEY (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path))`,
		`CREATE TABLE fan_out_outcomes (
			run_id TEXT NOT NULL, triggering_delivery_id TEXT NOT NULL, flow_path TEXT NOT NULL, declaration_family TEXT NOT NULL, semantic_path TEXT NOT NULL,
			ordinal INTEGER NOT NULL, outcome_kind TEXT NOT NULL, event_id TEXT, source_event_id TEXT, inherited_disposition TEXT, failure TEXT, created_at TIMESTAMP NOT NULL,
			PRIMARY KEY (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal))`,
	}
	if backend == "postgres" {
		statements[1] = strings.ReplaceAll(statements[1], "event_id TEXT", "event_id UUID")
		statements[1] = strings.ReplaceAll(statements[1], "source_event_id TEXT", "source_event_id UUID")
		statements[1] = strings.ReplaceAll(statements[1], "failure TEXT", "failure JSONB")
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func seedFanOutReadbackClaim(t *testing.T, db *sql.DB) runtimepipeline.FanOutChunkCommand {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	runID, eventID, deliveryID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	elementRef := runtimecontracts.FanOutElementRef{FlowPath: "root", Family: "handler_rule", SemanticPath: `handlers["items.ready"].rules[0]`}
	producer, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	capsule, err := fanoutobligation.MarshalCapsule(fanoutobligation.Capsule{
		NodeKey: "root.fan-out", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
		HandlerEventKey: "items.ready", ProducerSource: producer,
		Lineage:     events.EventLineage{RunID: runID, ParentEventID: eventID, ExecutionMode: executionmode.Live},
		StateFields: map[string]any{"integer": int64(75), "double": float64(75)},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := fanoutobligation.Claim{
		Key:   fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: elementRef},
		Owner: "readback-worker", Generation: 1, LeaseUntil: now.Add(time.Minute),
	}
	if _, err := db.Exec(`INSERT INTO fan_out_intents (
		run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,
		cardinality,cursor,status,next_chunk_size,created_at,updated_at,claim_owner,claim_generation,lease_expires_at,capsule
	) VALUES ($1,$2,$3,$4,$5,$6,$7,'event_payload_field',$8,'items',2,0,'open',4,$9,$9,$10,1,$11,$12)`,
		runID, deliveryID, elementRef.FlowPath, elementRef.Family, elementRef.SemanticPath, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), eventID, now, claim.Owner, claim.LeaseUntil, string(capsule)); err != nil {
		t.Fatal(err)
	}
	failure := runtimeengine.NormalizeFailure(&runtimeengine.EmitPayloadContractError{
		Event: "fan-out.test", Kind: runtimeengine.EmitPayloadSchemaMismatch,
		Path: "$.item", Constraint: "type", Expected: "declared item", Actual: "invalid item", Detail: "fan-out test item is invalid",
	}, "test", "commit")
	failureJSON, err := runtimefailures.MarshalEnvelope(failure.Failure)
	if err != nil {
		t.Fatal(err)
	}
	return runtimepipeline.FanOutChunkCommand{Claim: claim, Outcomes: []runtimepipeline.FanOutChunkOutcome{{Ordinal: 0, Failure: failureJSON}}, Now: now.Add(time.Second)}
}
