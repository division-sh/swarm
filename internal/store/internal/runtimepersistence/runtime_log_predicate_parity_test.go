package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestRuntimeLogPredicateBoundariesBothStores(t *testing.T) {
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
			artifactA, artifactB := storeTestSourceArtifact("predicate-a"), storeTestSourceArtifact("predicate-b")
			runA, runB, entity := uuid.NewString(), uuid.NewString(), uuid.NewString()
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runA, Artifact: artifactA, StartedAt: base.Add(-time.Minute)})
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runB, Artifact: artifactB, StartedAt: base.Add(-time.Minute)})
			id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
			for _, row := range []struct {
				n                                       int
				run, code, action, eventName, eventType string
				at                                      time.Time
			}{
				{1, runB, "wanted", "other", "", "", base.Add(-time.Second)},
				{2, runA, "wanted", "match", "", "", base},
				{3, runA, "wanted", "other", "match", "", base.Add(time.Second)},
				{4, runA, "wanted", "other", "", "match", base.Add(time.Second)},
				{5, runA, "other", "other", "", "", base.Add(2 * time.Second)},
				{6, runB, "wanted", "other", "", "", base.Add(3 * time.Second)},
			} {
				failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, row.code, nil)
				payload, err := json.Marshal(map[string]any{
					"log_level": "warn", "message": "predicate probe",
					"details": map[string]any{"component": "predicate", "action": row.action, "event_name": row.eventName, "event_type": row.eventType,
						"entity_id": entity, "agent_id": "agent-probe", "session_id": "session-probe", "failure": failure,
						// Deliberately false hints must never replace runs.bundle_hash or the persisted run.
						"bundle_hash": artifactB.BundleHash(), "run_id": runB},
				})
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.DiagnosticDirect(id(row.n), events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, row.run, "", events.EventEnvelope{}, row.at)
				if err := commitDiagnosticRuntimeLogFixture(ctx, selected, event); err != nil {
					t.Fatal(err)
				}
			}
			until := base.Add(time.Second)
			zonedSince := base.In(time.FixedZone("probe", 5*60*60))
			for _, tc := range []struct {
				name string
				opts operatorread.OperatorRuntimeLogListOptions
				want []int
			}{
				{"unfiltered_desc", operatorread.OperatorRuntimeLogListOptions{}, []int{6, 5, 4, 3, 2, 1}},
				{"bundle_before_limit_asc", operatorread.OperatorRuntimeLogListOptions{BundleHash: artifactA.BundleHash(), Limit: 1, Order: "asc"}, []int{2}},
				{"bundle_before_limit_desc", operatorread.OperatorRuntimeLogListOptions{BundleHash: artifactA.BundleHash(), Limit: 1}, []int{5}},
				{"action_or_event_asc", operatorread.OperatorRuntimeLogListOptions{ActionOrEventType: "match", Order: "asc"}, []int{2, 3, 4}},
				{"action_or_event_before_limit", operatorread.OperatorRuntimeLogListOptions{ActionOrEventType: "match", Limit: 1}, []int{4}},
				{"action_no_match", operatorread.OperatorRuntimeLogListOptions{ActionOrEventType: "absent"}, nil},
				{"since_exclusive_until_inclusive", operatorread.OperatorRuntimeLogListOptions{Since: &zonedSince, Until: &until, Order: "asc"}, []int{3, 4}},
				{"equal_window_empty", operatorread.OperatorRuntimeLogListOptions{Since: &base, Until: &base}, nil},
				{"until_inclusive", operatorread.OperatorRuntimeLogListOptions{RunID: runA, Until: &base}, []int{2}},
				{"entity_payload", operatorread.OperatorRuntimeLogListOptions{EntityID: entity, RunID: runA, ErrorCode: "wanted"}, []int{4, 3, 2}},
				{"entity_no_match", operatorread.OperatorRuntimeLogListOptions{EntityID: uuid.NewString()}, nil},
				{"all_predicates_before_limit", operatorread.OperatorRuntimeLogListOptions{RunID: runA, BundleHash: artifactA.BundleHash(), EntityID: entity, Component: "predicate", Level: "warn", ErrorCode: "wanted", Source: "agent-probe", SessionID: "session-probe", ActionOrEventType: "match", Since: &base, Until: &until, Limit: 1}, []int{4}},
				{"wrong_session", operatorread.OperatorRuntimeLogListOptions{SessionID: "wrong"}, nil},
				{"wrong_source", operatorread.OperatorRuntimeLogListOptions{Source: "runtime"}, nil},
				{"wrong_component", operatorread.OperatorRuntimeLogListOptions{Component: "wrong"}, nil},
				{"wrong_level", operatorread.OperatorRuntimeLogListOptions{Level: "error"}, nil},
				{"internal_level_is_exact", operatorread.OperatorRuntimeLogListOptions{Level: "WARN"}, nil},
				{"hostile_error_literal", operatorread.OperatorRuntimeLogListOptions{ErrorCode: "wanted' OR 1=1 --"}, nil},
				{"hostile_bundle_literal", operatorread.OperatorRuntimeLogListOptions{BundleHash: "' OR 1=1 --"}, nil},
				{"hostile_action_literal", operatorread.OperatorRuntimeLogListOptions{ActionOrEventType: "match' OR 1=1 --"}, nil},
				{"foreign_run_bundle", operatorread.OperatorRuntimeLogListOptions{RunID: runB, BundleHash: artifactA.BundleHash()}, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := selected.ListOperatorRuntimeLogs(ctx, tc.opts)
					if err != nil {
						t.Fatal(err)
					}
					got, want := []string{}, []string{}
					for _, log := range result.Logs {
						got = append(got, log.LogID)
						if log.EntityID != entity || log.Source != "agent-probe" {
							t.Fatalf("canonical decode: %+v", log)
						}
					}
					for _, n := range tc.want {
						want = append(want, id(n))
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("got %v want %v", got, want)
					}
					if backend == "sqlite" && result.NextCursor != "" {
						t.Fatalf("SQLite cursor support promoted: %+v", result)
					}
				})
			}
			t.Run("cursor_capability", func(t *testing.T) {
				opts := operatorread.OperatorRuntimeLogListOptions{BundleHash: artifactA.BundleHash(), ErrorCode: "wanted", ActionOrEventType: "match", Limit: 1, Order: "asc"}
				if backend == "sqlite" {
					opts.Cursor = "unsupported"
					if _, err := selected.ListOperatorRuntimeLogs(ctx, opts); !errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
						t.Fatalf("SQLite cursor refusal = %v", err)
					}
					return
				}
				for _, n := range []int{2, 3, 4} {
					result, err := selected.ListOperatorRuntimeLogs(ctx, opts)
					if err != nil || len(result.Logs) != 1 || result.Logs[0].LogID != id(n) {
						t.Fatalf("filtered page %d: %+v err=%v", n, result, err)
					}
					opts.Cursor = result.NextCursor
					if (n == 4) != (opts.Cursor == "") {
						t.Fatalf("filtered cursor at %d: %q", n, opts.Cursor)
					}
				}
			})
			t.Run("query_error_propagates", func(t *testing.T) {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := selected.ListOperatorRuntimeLogs(cancelled, operatorread.OperatorRuntimeLogListOptions{ErrorCode: "wanted"}); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled query = %v", err)
				}
			})
		})
	}
}
