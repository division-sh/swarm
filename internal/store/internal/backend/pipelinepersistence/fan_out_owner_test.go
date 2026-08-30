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
						if _, err := db.ExecContext(ctx, `DELETE FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath); err != nil {
							return err
						}
					}
					return ackLost
				}
				committed, err := commitFanOutChunk(
					context.Background(), nil, backend == "postgres", run,
					func(ctx context.Context, claim fanoutobligation.Claim, status fanoutobligation.Status, nextChunk int, lastChunkMS int64, observedAt time.Time) error {
						query := `UPDATE fan_out_intents SET next_chunk_size=$1,last_chunk_ms=$2,last_served_at=$3,updated_at=$3,claim_owner=NULL,lease_expires_at=NULL WHERE run_id=$4 AND triggering_delivery_id=$5 AND flow_path=$6 AND declaration_family=$7 AND semantic_path=$8 AND status=$9 AND claim_generation=$10 AND ((status='open' AND claim_owner=$11) OR (status='closed' AND claim_owner IS NULL))`
						result, updateErr := db.ExecContext(ctx, query, nextChunk, lastChunkMS, observedAt, claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath, string(status), claim.Generation, claim.Owner)
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
				if queryErr := db.QueryRow(`SELECT cursor,next_chunk_size,last_chunk_ms,(SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id AND o.flow_path=i.flow_path AND o.declaration_family=i.declaration_family AND o.semantic_path=i.semantic_path) FROM fan_out_intents i`).Scan(&cursor, &nextChunk, &lastChunkMS, &count); queryErr != nil {
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
			run_id TEXT NOT NULL, triggering_delivery_id TEXT NOT NULL, flow_path TEXT NOT NULL, declaration_family TEXT NOT NULL, semantic_path TEXT NOT NULL,
			bundle_hash TEXT NOT NULL, semantic_digest TEXT NOT NULL, source_kind TEXT NOT NULL,
			source_event_id TEXT, source_run_id TEXT, source_entity_id TEXT, source_field TEXT, source_mutation_id TEXT,
			source_resource_flow_path TEXT, source_resource_event_name TEXT, source_resource_version_id TEXT,
			cardinality INTEGER NOT NULL, cursor INTEGER NOT NULL, status TEXT NOT NULL, next_chunk_size INTEGER NOT NULL,
			last_chunk_ms BIGINT NOT NULL DEFAULT 0, last_served_at TIMESTAMP, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
			claim_owner TEXT, claim_generation BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMP, blocked_reason TEXT, capsule TEXT NOT NULL,
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
	capsule, err := json.Marshal(fanoutobligation.Capsule{
		NodeKey: "root.fan-out", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
		HandlerEventKey: "items.ready", ProducerSource: producer,
		Lineage: events.EventLineage{RunID: runID, ParentEventID: eventID, ExecutionMode: executionmode.Live},
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
		cardinality,cursor,status,next_chunk_size,last_chunk_ms,created_at,updated_at,claim_owner,claim_generation,lease_expires_at,capsule
	) VALUES ($1,$2,$3,$4,$5,$6,$7,'event_payload_field',$8,'items',2,0,'open',4,0,$9,$9,$10,1,$11,$12)`,
		runID, deliveryID, elementRef.FlowPath, elementRef.Family, elementRef.SemanticPath, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), eventID, now, claim.Owner, claim.LeaseUntil, string(capsule)); err != nil {
		t.Fatal(err)
	}
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "fan_out_test_item_invalid", "test", "commit", nil))
	if !ok {
		t.Fatal("construct fan-out readback failure")
	}
	failureJSON, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		t.Fatal(err)
	}
	return runtimepipeline.FanOutChunkCommand{Claim: claim, Outcomes: []runtimepipeline.FanOutChunkOutcome{{Ordinal: 0, Failure: failureJSON}}, Now: now.Add(time.Second)}
}
