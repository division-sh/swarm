package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRuntimeIncidentFiltersAndGroupingOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var selected interface {
				diagnosticRuntimeLogFixtureStore
				ListOperatorRuntimeIncidents(context.Context, operatorread.OperatorRuntimeIncidentListOptions) (operatorread.OperatorRuntimeIncidentListResult, error)
			}
			if backend == "postgres" {
				_, db, _ := testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			} else {
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			}
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
			artifactA := storeTestSourceArtifact("reviewer-2394-a")
			artifactB := storeTestSourceArtifact("reviewer-2394-b")
			runA, runB := uuid.NewString(), uuid.NewString()
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runA, Artifact: artifactA, StartedAt: base.Add(-48 * time.Hour)})
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runB, Artifact: artifactB, StartedAt: base.Add(-48 * time.Hour)})
			insertRecord := func(run, code, component, level string, at time.Time) string {
				t.Helper()
				failure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassInternalFailure, code, nil))
				payload := json.RawMessage(`{"log_level":"` + level + `","message":"review probe","details":{"component":"` + component + `","action":"request_failed","failure":` + failure + `}}`)
				id := uuid.NewString()
				event := eventtest.DiagnosticDirect(id, events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, run, "", events.EventEnvelope{}, at)
				if err := commitDiagnosticRuntimeLogFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
				return id
			}
			insert := func(run, code string, at time.Time) {
				insertRecord(run, code, "mcp-gateway", "error", at)
			}
			insert(runA, "shared_code", base)
			insert(runA, "shared_code", base.Add(time.Second))
			insert(runA, "old_code", base.Add(-48*time.Hour))
			insert(runB, "foreign_code", base.Add(2*time.Second))
			options := operatorread.OperatorRuntimeIncidentListOptions{BundleHash: artifactA.BundleHash(), SinceHours: 2, Limit: 10}
			result, err := selected.ListOperatorRuntimeIncidents(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Incidents) != 1 || result.Incidents[0].ErrorCode != "shared_code" || result.Incidents[0].Count != 2 {
				t.Errorf("bundle/horizon/grouping: got %#v, want one shared_code aggregate of two A rows", result.Incidents)
			}
			options.Cursor = "not-a-valid-cursor"
			if _, err := selected.ListOperatorRuntimeIncidents(ctx, options); !errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
				t.Errorf("invalid incident cursor error: %v", err)
			}
			options.Cursor = ""
			insertRecord(runA, "shared_code", "runtime.pipeline", "warn", base.Add(3*time.Second))
			insertRecord(runA, "other_code", "mcp-tools", "error", base.Add(3*time.Second))
			// Equal timestamps exercise the stable ID tiebreaker; a page limit must
			// not truncate the source logs before grouping the older count-two row.
			options.Limit = 1
			seen := map[string]bool{}
			var grouped bool
			for page := 0; ; page++ {
				if page > 3 {
					t.Fatal("incident cursor did not terminate")
				}
				result, err := selected.ListOperatorRuntimeIncidents(ctx, options)
				if err != nil || len(result.Incidents) != 1 {
					t.Fatalf("page %d: %+v err=%v", page, result, err)
				}
				item := result.Incidents[0]
				if seen[item.IncidentID] {
					t.Fatalf("repeated incident: %s", item.IncidentID)
				}
				seen[item.IncidentID] = true
				if item.Component == "mcp-gateway" {
					grouped = true
					if item.Count != 2 || len(item.SampleLogIDs) != 2 || !item.FirstSeen.Equal(base) || !item.LastSeen.Equal(base.Add(time.Second)) {
						t.Fatalf("source logs truncated before grouping: %+v", item)
					}
				}
				if result.NextCursor == "" {
					break
				}
				options.Cursor = result.NextCursor
			}
			if len(seen) != 3 || !grouped {
				t.Fatalf("page traversal: %d incidents grouped=%v", len(seen), grouped)
			}
			options.Cursor, options.Limit = "", 10
			options.MCPOnly = true
			result, err = selected.ListOperatorRuntimeIncidents(ctx, options)
			if err != nil || len(result.Incidents) != 2 {
				t.Fatalf("MCP filter: %+v err=%v", result, err)
			}
			options.MCPOnly, options.Level = false, "warn"
			result, err = selected.ListOperatorRuntimeIncidents(ctx, options)
			if err != nil || len(result.Incidents) != 1 || result.Incidents[0].Component != "runtime.pipeline" {
				t.Fatalf("level filter: %+v err=%v", result, err)
			}
			options.Level, options.Component = "", "mcp-gateway"
			result, err = selected.ListOperatorRuntimeIncidents(ctx, options)
			if err != nil || len(result.Incidents) != 1 || result.Incidents[0].Count != 2 {
				t.Fatalf("component filter: %+v err=%v", result, err)
			}
		})
	}
}
