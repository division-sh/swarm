package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Retains the unexpected log-selection defect separately from #2394's approved
// incident aggregation repair. No compensating component/level or client filter.
func TestRuntimeLogFilterParityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var selected interface {
				diagnosticRuntimeLogFixtureStore
				ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
			}
			if backend == "postgres" {
				_, db, _ := testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			} else {
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			}
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
			artifactA := storeTestSourceArtifact("runtime-log-filter-a")
			artifactB := storeTestSourceArtifact("runtime-log-filter-b")
			runA, runB := uuid.NewString(), uuid.NewString()
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runA, Artifact: artifactA, StartedAt: base.Add(-time.Minute)})
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runB, Artifact: artifactB, StartedAt: base.Add(-time.Minute)})
			insert := func(run, code string, at time.Time) string {
				t.Helper()
				failure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, code, nil))
				payload := json.RawMessage(`{"log_level":"warn","message":"log filter probe","details":{"component":"workflow-runtime","action":"filter_probe","failure":` + failure + `}}`)
				id := uuid.NewString()
				event := eventtest.DiagnosticDirect(id, events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, run, "", events.EventEnvelope{}, at)
				if err := commitDiagnosticRuntimeLogFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
				return id
			}
			wantedA := insert(runA, "wanted_code", base)
			otherA := insert(runA, "other_code", base.Add(time.Second))
			wantedB := insert(runB, "wanted_code", base.Add(2*time.Second))
			wantCodes := map[string]string{wantedA: "wanted_code", otherA: "other_code", wantedB: "wanted_code"}
			wantRuns := map[string]string{wantedA: runA, otherA: runA, wantedB: runB}
			// Establish persistence and canonical decode independently of selection.
			all, err := selected.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Order: "desc", Limit: 10})
			if err != nil || len(all.Logs) != 3 {
				t.Fatalf("unfiltered fixture: %+v err=%v", all, err)
			}
			for _, log := range all.Logs {
				if code, exists := wantCodes[log.LogID]; !exists || log.ErrorCode != code || log.RunID != wantRuns[log.LogID] {
					t.Fatalf("canonical fixture identity/code: %+v", log)
				}
			}
			for _, tc := range []struct {
				name string
				opts operatorread.OperatorRuntimeLogListOptions
				want []string
			}{
				{"error_code", operatorread.OperatorRuntimeLogListOptions{ErrorCode: "wanted_code"}, []string{wantedB, wantedA}},
				{"run_and_error_code", operatorread.OperatorRuntimeLogListOptions{RunID: runA, ErrorCode: "wanted_code"}, []string{wantedA}},
				{"error_code_before_limit", operatorread.OperatorRuntimeLogListOptions{RunID: runA, ErrorCode: "wanted_code", Limit: 1}, []string{wantedA}},
				{"absent_error_code", operatorread.OperatorRuntimeLogListOptions{RunID: runA, ErrorCode: "absent_code"}, []string{}},
				{"bundle", operatorread.OperatorRuntimeLogListOptions{BundleHash: artifactA.BundleHash()}, []string{otherA, wantedA}},
				{"run_and_foreign_bundle", operatorread.OperatorRuntimeLogListOptions{RunID: runA, BundleHash: artifactB.BundleHash()}, []string{}},
				{"bundle_and_error_code", operatorread.OperatorRuntimeLogListOptions{BundleHash: artifactA.BundleHash(), ErrorCode: "wanted_code"}, []string{wantedA}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tc.opts.Order = "desc"
					if tc.opts.Limit == 0 {
						tc.opts.Limit = 10
					}
					result, err := selected.ListOperatorRuntimeLogs(ctx, tc.opts)
					if err != nil {
						t.Fatal(err)
					}
					got := make([]string, 0, len(result.Logs))
					for _, log := range result.Logs {
						got = append(got, log.LogID)
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Errorf("selected identities=%v, want %v; options=%+v logs=%+v", got, tc.want, tc.opts, result.Logs)
					}
				})
			}
		})
	}
}
