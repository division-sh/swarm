package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestAdaptiveFanOutChunkUsesExactPlatformBands(t *testing.T) {
	for _, tc := range []struct {
		name     string
		current  int
		duration time.Duration
		want     int
	}{
		{name: "fast lower", current: 4, duration: 0, want: 5},
		{name: "fast boundary", current: 4, duration: 250 * time.Millisecond, want: 5},
		{name: "fast ceiling", current: 32, duration: time.Millisecond, want: 32},
		{name: "neutral lower", current: 4, duration: 250*time.Millisecond + time.Nanosecond, want: 4},
		{name: "neutral upper", current: 4, duration: time.Second, want: 4},
		{name: "slow even", current: 4, duration: time.Second + time.Nanosecond, want: 2},
		{name: "slow odd rounds up", current: 5, duration: 2 * time.Second, want: 3},
		{name: "slow floor", current: 1, duration: 2 * time.Second, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := adaptiveFanOutChunk(tc.current, tc.duration); got != tc.want {
				t.Fatalf("adaptiveFanOutChunk(%d, %s) = %d, want %d", tc.current, tc.duration, got, tc.want)
			}
		})
	}
}

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
			want: []any{"event-id", nil, nil, "items", nil, nil, nil, nil},
		},
		{
			name: "entity field revision",
			source: fanoutobligation.SourceRef{
				Kind: fanoutobligation.SourceEntityField, RunID: "run-id", EntityID: "entity-id", Field: "items", MutationID: "mutation-id",
			},
			want: []any{nil, "run-id", "entity-id", "items", "mutation-id", nil, nil, nil},
		},
		{
			name: "resource version",
			source: fanoutobligation.SourceRef{
				Kind:        fanoutobligation.SourceResourceVersion,
				Declaration: durabledata.DeclarationRef{PackageKey: "root", EventName: "records"},
				VersionID:   durabledata.VersionID("resource-version"),
			},
			want: []any{nil, nil, nil, nil, nil, "root", "records", "resource-version"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := fanOutIntentSQLArgs(request, test.source, []byte(`{}`), fanoutobligation.StatusOpen, now)[7:15]
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

func TestFanOutChunkCommitAcknowledgementReadbackOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"committed", "rolled_back", "contradictory"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				db := fanOutReadbackTestDB(t, backend)
				command := seedFanOutReadbackClaim(t, db)
				ackLost := errors.New("injected fan-out commit acknowledgement loss")
				run := func(ctx context.Context, effects *revisionEffects, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						return err
					}
					if err := operation(ctx, tx, nil); err != nil {
						_ = tx.Rollback()
						return err
					}
					if outcome == "rolled_back" {
						if err := tx.Rollback(); err != nil {
							return err
						}
						return ackLost
					}
					if err := tx.Commit(); err != nil {
						return err
					}
					if outcome == "committed" {
						time.Sleep(1100 * time.Millisecond)
					}
					if outcome == "contradictory" {
						if _, err := db.ExecContext(ctx, `DELETE FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.PackageKey, command.Claim.Key.ElementRef.ElementID); err != nil {
							return err
						}
					}
					return ackLost
				}
				committed, err := commitFanOutChunk(
					context.Background(), nil, backend == "postgres", run,
					func(ctx context.Context, claim fanoutobligation.Claim, status fanoutobligation.Status, nextChunk int, lastChunkMS int64, observedAt time.Time) error {
						query := `UPDATE fan_out_intents SET next_chunk_size=$1,last_chunk_ms=$2,last_served_at=$3,updated_at=$3,claim_owner=NULL,lease_expires_at=NULL WHERE run_id=$4 AND triggering_delivery_id=$5 AND package_key=$6 AND element_id=$7 AND status=$8 AND claim_generation=$9 AND ((status='open' AND claim_owner=$10) OR (status='closed' AND claim_owner IS NULL))`
						result, updateErr := db.ExecContext(ctx, query, nextChunk, lastChunkMS, observedAt, claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.PackageKey, claim.Key.ElementRef.ElementID, string(status), claim.Generation, claim.Owner)
						if updateErr != nil {
							return updateErr
						}
						rows, updateErr := result.RowsAffected()
						if updateErr != nil || rows != 1 {
							return errors.Join(updateErr, fanoutobligation.ErrStaleClaim)
						}
						return nil
					}, time.Now,
					func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error {
						return nil
					},
					func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error) {
						return runtimerunlifecycle.CandidateRequestResult{}, fmt.Errorf("non-terminal chunk must not request completion")
					},
					db, command,
				)
				var cursor, count, nextChunk int
				var lastChunkMS int64
				if queryErr := db.QueryRow(`SELECT cursor,next_chunk_size,last_chunk_ms,(SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id AND o.package_key=i.package_key AND o.element_id=i.element_id) FROM fan_out_intents i`).Scan(&cursor, &nextChunk, &lastChunkMS, &count); queryErr != nil {
					t.Fatal(queryErr)
				}
				switch outcome {
				case "committed":
					if err != nil || cursor != 1 || count != 1 || nextChunk != 2 || lastChunkMS < 1000 || committed.Intent.Cursor != 1 || committed.Intent.Status != fanoutobligation.StatusOpen || committed.Intent.NextChunkSize != 2 {
						t.Fatalf("committed readback = cursor:%d outcomes:%d next:%d last_ms:%d result:%#v err:%v", cursor, count, nextChunk, lastChunkMS, committed.Intent, err)
					}
				case "rolled_back":
					if !errors.Is(err, ackLost) || cursor != 0 || count != 0 || committed.Intent.Cursor != 0 {
						t.Fatalf("no-commit readback = cursor:%d outcomes:%d result:%#v err:%v", cursor, count, committed.Intent, err)
					}
				case "contradictory":
					failure, ok := runtimefailures.EnvelopeFromError(err)
					if !ok || failure.Class != runtimefailures.ClassOutcomeUncertain || cursor != 1 || count != 0 || committed.Intent.Cursor != 0 {
						t.Fatalf("contradictory readback = cursor:%d outcomes:%d result:%#v failure:%#v err:%v", cursor, count, committed.Intent, failure, err)
					}
				}
			})
		}
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
			run_id TEXT NOT NULL, triggering_delivery_id TEXT NOT NULL, package_key TEXT NOT NULL, element_id TEXT NOT NULL,
			bundle_hash TEXT NOT NULL, semantic_digest TEXT NOT NULL, source_kind TEXT NOT NULL,
			source_event_id TEXT, source_run_id TEXT, source_entity_id TEXT, source_field TEXT, source_mutation_id TEXT,
			source_resource_package_key TEXT, source_resource_event_name TEXT, source_resource_version_id TEXT,
			cardinality INTEGER NOT NULL, cursor INTEGER NOT NULL, status TEXT NOT NULL, next_chunk_size INTEGER NOT NULL,
			last_chunk_ms BIGINT NOT NULL DEFAULT 0, last_served_at TIMESTAMP, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
			claim_owner TEXT, claim_generation BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMP, blocked_reason TEXT, capsule TEXT NOT NULL,
			PRIMARY KEY (run_id,triggering_delivery_id,package_key,element_id))`,
		`CREATE TABLE fan_out_outcomes (
			run_id TEXT NOT NULL, triggering_delivery_id TEXT NOT NULL, package_key TEXT NOT NULL, element_id TEXT NOT NULL,
			ordinal INTEGER NOT NULL, outcome_kind TEXT NOT NULL, event_id TEXT, source_event_id TEXT, inherited_disposition TEXT, failure TEXT, created_at TIMESTAMP NOT NULL,
			PRIMARY KEY (run_id,triggering_delivery_id,package_key,element_id,ordinal))`,
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
	runID, eventID, deliveryID, elementID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	producer, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	capsule, err := json.Marshal(fanoutobligation.Capsule{
		NodeKey: "root.fan-out", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
		HandlerEventKey: "items.ready", ProducerSource: producer,
		Lineage: events.EventLineage{RunID: runID, ParentEventID: eventID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := fanoutobligation.Claim{
		Key:   fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: runtimecontracts.FanOutElementRef{PackageKey: "root", ElementID: elementID}},
		Owner: "readback-worker", Generation: 1, LeaseUntil: now.Add(time.Minute),
	}
	if _, err := db.Exec(`INSERT INTO fan_out_intents (
		run_id,triggering_delivery_id,package_key,element_id,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,
		cardinality,cursor,status,next_chunk_size,last_chunk_ms,created_at,updated_at,claim_owner,claim_generation,lease_expires_at,capsule
	) VALUES ($1,$2,$3,$4,$5,$6,'event_payload_field',$7,'items',2,0,'open',4,0,$8,$8,$9,1,$10,$11)`,
		runID, deliveryID, "root", elementID, "bundle-v1:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), eventID, now, claim.Owner, claim.LeaseUntil, string(capsule)); err != nil {
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
