package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestScenarioSetupAckStorageUsesOriginalSnapshotAndExactIdentitiesBothStores(t *testing.T) {
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if out, err := ReadScenarioSetupAckStorageForTest(context.Background(), invalid, uuid.NewString(), uuid.NewString()); err == nil || !reflect.DeepEqual(out, ScenarioSetupAckStorage{}) {
			t.Fatalf("foreign owner %T supplied acknowledgment evidence: %+v/%v", invalid, out, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			writer := fixture.store.(interface {
				WithAPIIdempotency(context.Context, apiidempotency.Request, func(context.Context) (apiidempotency.Completion, error)) (apiidempotency.Completion, bool, error)
			})
			runID, entityID, sibling := uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, run := range []string{runID, sibling} {
				req := apiidempotency.Request{Method: "test.setup_entities", Actor: apiidempotency.BearerActor("ack-proof"), IdempotencyKey: run,
					RequestHash: "exact-ack-request", ResourceID: run, Now: time.Now().UTC(), TTL: time.Hour}
				if _, replay, err := writer.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) {
					return apiidempotency.Completion{ResourceID: run, Response: json.RawMessage(`{"run_id":"` + run + `"}`)}, nil
				}); err != nil || replay {
					t.Fatalf("persist original completion: replay=%v err=%v", replay, err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			out, err := ReadScenarioSetupAckStorageForTest(ctx, fixture.store, runID, entityID)
			if err != nil || out.Runs != 0 || out.Entities != 0 || out.SetupMutations != 0 || out.Completions != 1 {
				t.Fatalf("exact completion cardinalities: %+v/%v", out, err)
			}
			var response map[string]string
			if err := json.Unmarshal(out.Response, &response); err != nil || response["run_id"] != runID {
				t.Fatalf("exact stored response: %s/%v", out.Response, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("ack evidence escaped original read snapshot: %+v", counts)
			}
			for _, id := range []string{"", "bad", uuid.Nil.String(), " " + uuid.NewString()} {
				for _, pair := range [][2]string{{id, entityID}, {runID, id}} {
					if out, err := ReadScenarioSetupAckStorageForTest(ctx, fixture.store, pair[0], pair[1]); err == nil || !reflect.DeepEqual(out, ScenarioSetupAckStorage{}) {
						t.Fatalf("invalid identity supplied evidence: %+v/%v", out, err)
					}
				}
			}
			if out, err := ReadScenarioSetupAckStorageForTest(ctx, fixture.store, uuid.NewString(), entityID); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(out, ScenarioSetupAckStorage{}) {
				t.Fatalf("absent completion retained partial counts: %+v/%v", out, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if out, err := ReadScenarioSetupAckStorageForTest(cancelled, fixture.store, runID, entityID); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(out, ScenarioSetupAckStorage{}) {
				t.Fatalf("cancelled read retained evidence: %+v/%v", out, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			if out, err := ReadScenarioSetupAckStorageForTest(ctx, fixture.store, runID, entityID); err == nil || !reflect.DeepEqual(out, ScenarioSetupAckStorage{}) {
				t.Fatalf("closed read retained evidence: %+v/%v", out, err)
			}
		})
	}
}
