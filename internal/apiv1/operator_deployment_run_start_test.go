package apiv1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestDeploymentRunStartFeedOnlyAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, scanRef, scoreRef := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		candidateOwner, ok := fixture.primary.(runtimerunlifecycle.CandidateOwner)
		if !ok {
			t.Fatal("selected store lacks completion candidate authority")
		}
		scope := runtimerunlifecycle.CandidateScope{BundleHash: runStartTestBundleHash}
		executor, err := runtimerunlifecycle.NewExecutor(
			candidateOwner, scope, runCompletionTerminalCatalog(source),
			newAPITestRuntimeWorkOccurrence(t, authorActivityTestRuntimeInstanceID, runStartTestBundleHash),
			runtimerunlifecycle.ExecutorOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		registration, err := candidateOwner.RegisterCompletionCandidateSink(ctx, scope, executor)
		if err != nil {
			t.Fatal(err)
		}
		if err := executor.Start(ctx); err != nil {
			registration.Release()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := executor.Retire(ctx); err != nil {
				t.Errorf("retire completion executor: %v", err)
			}
			registration.Release()
		})
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		versions := make(map[string]durabledata.VersionID)

		for _, tc := range []struct {
			name     string
			rows     string
			rowCount int
		}{
			{name: "rows", rows: "{\"topic\":\"one\"}\n{\"topic\":\"two\"}\n", rowCount: 2},
			{name: "empty", rows: "", rowCount: 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				runID, sourceID, key := uuid.NewString(), uuid.NewString(), uuid.NewString()
				ref := scanRef
				if tc.rowCount == 0 {
					ref = scoreRef
				}
				data := map[string]any{
					"imports": []any{dataRunFusedImport(sourceID, ref, durabledata.AbsentHead(), []byte(tc.rows))},
					"pins":    []any{},
				}
				body := deploymentRunStartBody(runID, key, data)
				response := rpcCall(t, handler, body)
				if response.Error != nil {
					t.Fatalf("feed-only run.start: %#v", response.Error)
				}
				result := asMap(t, response.Result)
				if result["run_id"] != runID || asMap(t, result["data_binding"])["pin_count"] != float64(1) {
					t.Fatalf("feed-only result = %#v", result)
				}
				if got := dataRunCount(t, fixture, "events", "run_id", runID); got != 0 {
					t.Fatalf("feed-only created %d events", got)
				}
				if got := dataRunCount(t, fixture, "fan_out_intents", "run_id", runID); got != 1 {
					t.Fatalf("feed-only created %d intents, want 1", got)
				}
				query := "SELECT cardinality, status FROM fan_out_intents WHERE run_id = ?"
				if _, postgres := fixture.primary.(*store.PostgresStore); postgres {
					query = "SELECT cardinality, status FROM fan_out_intents WHERE run_id = $1::uuid"
				}
				var cardinality int
				var intentStatus string
				if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&cardinality, &intentStatus); err != nil {
					t.Fatalf("read feed intent: %v", err)
				}
				if cardinality != tc.rowCount || (tc.rowCount == 0 && intentStatus != "closed") || (tc.rowCount > 0 && intentStatus != "open") {
					t.Fatalf("feed intent cardinality/status = %d/%s, want %d", cardinality, intentStatus, tc.rowCount)
				}
				origin, err := fixture.reconstructed.LoadRunOrigin(ctx, runID)
				if err != nil || origin.Kind() != "deployment" || origin.EventID() != "" {
					t.Fatalf("deployment origin = %#v, %v", origin, err)
				}
				receipt, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
				if err != nil || receipt.Summary.EventID != "" || receipt.Summary.PinCount != 1 {
					t.Fatalf("permanent feed-only receipt = %#v, %v", receipt, err)
				}
				if err := receipt.Validate(); err != nil {
					t.Fatalf("feed-only public receipt invalid: %v", err)
				}
				sourceReceipt, err := fixture.reconstructed.LoadDataSourceOperation(ctx, sourceID)
				if err != nil || sourceReceipt.Result.Outcome != "accepted" {
					t.Fatalf("feed-only source receipt = %#v, %v", sourceReceipt, err)
				}
				versions[ref.Key()] = sourceReceipt.Result.Candidate.VersionID
				replay := rpcCall(t, handler, body)
				if replay.Error != nil || asMap(t, replay.Result)["run_id"] != runID {
					t.Fatalf("feed-only replay = %#v", replay)
				}
				cacheDelete := "DELETE FROM api_idempotency WHERE resource_id = ?"
				if _, postgres := fixture.primary.(*store.PostgresStore); postgres {
					cacheDelete = "DELETE FROM api_idempotency WHERE resource_id = $1"
				}
				if _, err := fixture.db.ExecContext(ctx, cacheDelete, runID); err != nil {
					t.Fatalf("expire transport cache for permanent-receipt proof: %v", err)
				}
				permanentReplay := rpcCall(t, handler, body)
				if permanentReplay.Error != nil || asMap(t, permanentReplay.Result)["run_id"] != runID {
					t.Fatalf("feed-only permanent replay = %#v", permanentReplay)
				}
				if got := dataRunCount(t, fixture, "fan_out_intents", "run_id", runID); got != 1 {
					t.Fatalf("replay created %d intents", got)
				}
				if tc.rowCount == 0 {
					deadline := time.Now().Add(3 * time.Second)
					for {
						header, err := fixture.reconstructed.LoadRunHeader(ctx, runID)
						if err != nil {
							t.Fatalf("empty feed run readback: %v", err)
						}
						if header.Status == "completed" {
							break
						}
						if time.Now().After(deadline) {
							query := "SELECT completion_revision, completion_due_at FROM runs WHERE run_id = ?"
							if _, postgres := fixture.primary.(*store.PostgresStore); postgres {
								query = "SELECT completion_revision, completion_due_at FROM runs WHERE run_id = $1::uuid"
							}
							var revision int64
							var due any
							_ = fixture.db.QueryRowContext(ctx, query, runID).Scan(&revision, &due)
							t.Fatalf("empty feed run remained %s (candidate revision=%d due=%v active=%d)", header.Status, revision, due, executor.ActiveCandidates())
						}
						time.Sleep(10 * time.Millisecond)
					}
				}
			})
		}
		t.Run("multiple explicit pins create independent feeds", func(t *testing.T) {
			runID := uuid.NewString()
			data := map[string]any{
				"imports": []any{},
				"pins": []any{
					map[string]any{"declaration": dataRunDeclaration(scanRef), "version_id": versions[scanRef.Key()]},
					map[string]any{"declaration": dataRunDeclaration(scoreRef), "version_id": versions[scoreRef.Key()]},
				},
			}
			response := rpcCall(t, handler, deploymentRunStartBody(runID, uuid.NewString(), data))
			if response.Error != nil || asMap(t, asMap(t, response.Result)["data_binding"])["pin_count"] != float64(2) {
				t.Fatalf("multi-pin deployment run = %#v", response)
			}
			if got := dataRunCount(t, fixture, "fan_out_intents", "run_id", runID); got != 2 {
				t.Fatalf("multi-pin intents = %d, want 2", got)
			}
			if got := dataRunCount(t, fixture, "events", "run_id", runID); got != 0 {
				t.Fatalf("multi-pin run created %d initial events", got)
			}
		})
	})
}

func deploymentRunStartBody(runID, key string, data map[string]any) string {
	raw, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": key, "method": "run.start",
		"params": map[string]any{"run_id": runID, "bundle_hash": runStartTestBundleHash, "idempotency_key": key, "data": data},
	})
	if err != nil {
		panic(err)
	}
	return string(raw)
}
