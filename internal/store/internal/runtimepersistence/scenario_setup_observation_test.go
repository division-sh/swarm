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
	"github.com/division-sh/swarm/internal/runtime/correlation"
	pipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestScenarioSetupPhysicalObservationsKeepExactScopeAndIndependentReads(t *testing.T) {
	fixture, ctx := openPostgresAuthorActivityReceiptFixture(t), testAuthorActivityContext()
	requireDefaultSourceArtifactForTest(t, ctx, fixture.store)
	setup := fixture.store.(interface {
		SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
	})
	runID, entityID := uuid.NewString(), uuid.NewString()
	request := pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: time.Now().UTC(), Entities: []pipeline.ScenarioSetupEntityRequest{{
		Alias: "subject", EntityID: entityID, FlowInstance: "operating", EntityType: "product", CurrentState: "waiting",
		Fields: map[string]any{"note": "seeded"}, Gates: map[string]bool{"review_ready": true},
	}}}
	if _, err := setup.SetupScenarioEntities(ctx, request); err != nil {
		t.Fatal(err)
	}
	probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	run, err := ReadPostgresScenarioSetupRunStorageForTest(ctx, fixture.store, runID)
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || err != nil || run.Status != "running" || run.OriginKind != "scenario_setup" || run.BundleHash != fact.BundleHash() {
		t.Fatalf("exact run header: %+v/%v", run, err)
	}
	entity, err := ReadPostgresScenarioSetupEntityStorageForTest(ctx, fixture.store, runID, entityID)
	want := PostgresScenarioSetupEntityStorage{FlowInstance: "operating", EntityType: "product", CurrentState: "waiting", Note: "seeded", ReviewReady: "true"}
	if err != nil || entity != want {
		t.Fatalf("exact entity JSON text: %+v/%v", entity, err)
	}
	mutations, err := CountPostgresScenarioSetupMutationsForTest(ctx, fixture.store, runID, entityID)
	if err != nil || mutations != 3 {
		t.Fatalf("exact setup writer cardinality: %d/%v", mutations, err)
	}
	fields, err := ReadPostgresScenarioSetupFieldsStorageForTest(ctx, fixture.store, runID, entityID)
	var decoded map[string]any
	if err != nil || json.Unmarshal(fields, &decoded) != nil || decoded["note"] != "seeded" {
		t.Fatalf("separate native field bytes: %s/%v", fields, err)
	}
	if counts := probe.Snapshot(); counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
		t.Fatalf("observations lost independent original reads: %+v", counts)
	}
	for _, pair := range [][2]string{{uuid.NewString(), entityID}, {runID, uuid.NewString()}} {
		if count, err := CountPostgresScenarioSetupMutationsForTest(ctx, fixture.store, pair[0], pair[1]); err != nil || count != 0 {
			t.Fatalf("foreign coordinates supplied mutation evidence: %d/%v", count, err)
		}
		if got, err := ReadPostgresScenarioSetupEntityStorageForTest(ctx, fixture.store, pair[0], pair[1]); !errors.Is(err, sql.ErrNoRows) || got != (PostgresScenarioSetupEntityStorage{}) {
			t.Fatalf("absent entity supplied scalar evidence: %+v/%v", got, err)
		}
		if got, err := ReadPostgresScenarioSetupFieldsStorageForTest(ctx, fixture.store, pair[0], pair[1]); !errors.Is(err, sql.ErrNoRows) || got != nil {
			t.Fatalf("absent entity supplied fields: %s/%v", got, err)
		}
	}
	request.RunID, request.Entities[0].EntityID = uuid.NewString(), uuid.NewString()
	request.Entities[0].Fields, request.Entities[0].Gates = nil, nil
	if _, err := setup.SetupScenarioEntities(ctx, request); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadPostgresScenarioSetupEntityStorageForTest(ctx, fixture.store, request.RunID, request.Entities[0].EntityID); err == nil || got != (PostgresScenarioSetupEntityStorage{}) {
		t.Fatalf("missing JSON field was defaulted or retained a partial row: %+v/%v", got, err)
	}
	if count, err := CountPhysicalEntitiesForTest(ctx, fixture.store); err != nil || count != 2 {
		t.Fatalf("whole-store entity count filtered another run: %d/%v", count, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	assertScenarioSetupObservationRefusals(t, cancelled, fixture.store, runID, entityID)
	if count, err := CountPhysicalEntitiesForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) || count != 0 {
		t.Fatalf("cancelled entity count supplied evidence: %d/%v", count, err)
	}
	assertScenarioSetupObservationRefusals(t, ctx, fixture.store, "invalid", entityID)
	assertScenarioSetupObservationRefusals(t, ctx, fixture.store, runID, "invalid")
	if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	assertScenarioSetupObservationRefusals(t, ctx, fixture.store, runID, entityID)
}

func assertScenarioSetupObservationRefusals(t *testing.T, ctx context.Context, selected any, runID, entityID string) {
	t.Helper()
	if entityID != "invalid" {
		if got, err := ReadPostgresScenarioSetupRunStorageForTest(ctx, selected, runID); err == nil || got != (ScenarioSetupRunStorage{}) {
			t.Fatalf("refused run read supplied evidence: %+v/%v", got, err)
		}
	}
	if got, err := ReadPostgresScenarioSetupEntityStorageForTest(ctx, selected, runID, entityID); err == nil || got != (PostgresScenarioSetupEntityStorage{}) {
		t.Fatalf("refused entity read supplied evidence: %+v/%v", got, err)
	}
	if got, err := CountPostgresScenarioSetupMutationsForTest(ctx, selected, runID, entityID); err == nil || got != 0 {
		t.Fatalf("refused mutation count supplied evidence: %d/%v", got, err)
	}
	if got, err := ReadPostgresScenarioSetupFieldsStorageForTest(ctx, selected, runID, entityID); err == nil || got != nil {
		t.Fatalf("refused field read supplied evidence: %s/%v", got, err)
	}
}

func TestScenarioSetupPhysicalObservationsRefuseUnselectedAndWrongDialectOwners(t *testing.T) {
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}, newBootstrappedSQLiteRuntimeStoreForTest(t)} {
		assertScenarioSetupObservationRefusals(t, context.Background(), invalid, uuid.NewString(), uuid.NewString())
	}
}

func TestScenarioSetupReplayReceiptRemovalUsesWholeOriginalWriterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			setup := fixture.store.(interface {
				SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
			})
			if _, err := setup.SetupScenarioEntities(ctx, pipeline.ScenarioSetupRequest{RunID: uuid.NewString(), CreatedAt: time.Now().UTC(), Entities: []pipeline.ScenarioSetupEntityRequest{{
				Alias: "subject", EntityID: uuid.NewString(), FlowInstance: "operating", EntityType: "product", CurrentState: "waiting",
			}}}); err != nil {
				t.Fatal(err)
			}
			writer := fixture.store.(interface {
				WithAPIIdempotency(context.Context, apiidempotency.Request, func(context.Context) (apiidempotency.Completion, error)) (apiidempotency.Completion, bool, error)
			})
			for _, method := range []string{"test.setup_entities", "run.start", "event.publish"} {
				req := apiidempotency.Request{Method: method, Actor: apiidempotency.BearerActor(method), IdempotencyKey: uuid.NewString(),
					RequestHash: "receipt-removal-proof", ResourceID: uuid.NewString(), Now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), TTL: time.Hour}
				if _, replay, err := writer.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) {
					return apiidempotency.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"stored":true}`)}, nil
				}); err != nil || replay {
					t.Fatalf("persist original receipt: replay=%v err=%v", replay, err)
				}
			}
			if count, err := CountAPICommandReceiptsForTest(ctx, fixture.store); err != nil || count != 3 {
				t.Fatalf("expired and foreign-method receipts were not present: %d/%v", count, err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := RemoveScenarioSetupReplayReceiptsForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled receipt fault did not refuse: %v", err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("cancelled removal changed storage: %v", err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := RemoveScenarioSetupReplayReceiptsForTest(ctx, fixture.store); err != nil {
				t.Fatal(err)
			}
			if count, err := CountAPICommandReceiptsForTest(ctx, fixture.store); err != nil || count != 0 {
				t.Fatalf("removal narrowed the original whole-table cut: %d/%v", count, err)
			}
			after, err = ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			for table, rows := range before {
				if table != "api_idempotency" && !reflect.DeepEqual(rows, after[table]) {
					t.Fatalf("receipt removal changed unrelated history %s", table)
				}
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("receipt fault escaped original unrevisioned writer: %+v", counts)
			}
			if count, err := CountPhysicalEntitiesForTest(ctx, fixture.store); err != nil || count != 1 {
				t.Fatalf("nonzero physical entity count changed: %d/%v", count, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := RemoveScenarioSetupReplayReceiptsForTest(ctx, fixture.store); err == nil {
				t.Fatal("closed receipt removal accepted")
			}
			if count, err := CountPhysicalEntitiesForTest(ctx, fixture.store); err == nil || count != 0 {
				t.Fatalf("closed entity count supplied evidence: %d/%v", count, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if err := RemoveScenarioSetupReplayReceiptsForTest(context.Background(), invalid); err == nil {
			t.Fatalf("unselected owner %T supplied receipt-removal authority", invalid)
		}
		if count, err := CountPhysicalEntitiesForTest(context.Background(), invalid); err == nil || count != 0 {
			t.Fatalf("unselected owner %T supplied entity evidence: %d/%v", invalid, count, err)
		}
	}
}
