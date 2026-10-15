package runtimepersistence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Real construction/setup and mutation owners feed authenticated HTTP readback.
// This is not served restart/fork or C's final directory integration evidence.
func TestQ10EntityDataCursorOrderingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":           "name: q10-cursor\nstages:\n  ready: {}\n  done: {final: true}\n",
				"entities.yaml":         "cursor_item:\n  count: integer?\n",
				"imports/schema.yaml":   "name: imports\ninstance: item_id\n",
				"imports/entities.yaml": "cursor_item:\n  item_id: text\n  count: integer?\n",
				"others/schema.yaml":    "name: others\ninstance: item_id\n",
				"others/entities.yaml":  "other_item:\n  item_id: text\n",
			}, nil)
			const (
				runA   = "91000000-0000-4000-8000-000000000001"
				runB   = "91000000-0000-4000-8000-000000000002"
				shared = "91000000-0000-4000-8000-000000000003"
				tail   = "91000000-0000-4000-8000-000000000004"
				other  = "91000000-0000-4000-8000-000000000005"
			)
			at := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
			source := semanticview.Wrap(f.bundle)
			seedRun := func(runID string, withTail bool) pipeline.WorkflowEngineStateRecord {
				t.Helper()
				ctx := correlation.WithRunID(f.ctx, runID)
				seed := pipeline.ScenarioSetupEntityRequest{
					Alias: "root", EntityID: runID, FlowInstance: runID, EntityType: "cursor_item",
					CurrentState: mutationlog.PresentLifecycleState("ready"), Fields: map[string]any{"count": int64(1)}, Gates: map[string]bool{},
				}
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
					ContractBundle: source, OccurredAt: at, ScenarioSeed: &seed,
					Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", ""),
				})
				if err != nil {
					t.Fatal(err)
				}
				entities := []pipeline.ScenarioSetupEntityRequest{
					seed, {Alias: "shared", EntityID: shared, FlowInstance: "imports/shared", EntityType: "cursor_item", Fields: map[string]any{"item_id": "shared", "count": int64(3)}},
					{Alias: "other", EntityID: other, FlowInstance: "others/other", EntityType: "other_item", Fields: map[string]any{"item_id": "other"}},
				}
				if withTail {
					entities = append(entities, pipeline.ScenarioSetupEntityRequest{Alias: "tail", EntityID: tail, FlowInstance: "imports/tail", EntityType: "cursor_item", Fields: map[string]any{"item_id": "tail", "count": int64(4)}})
				}
				command := runtimebus.ScenarioSetupCommand{
					Setup:       pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: at, Entities: entities},
					Activations: []runtimebus.FlowInstanceActivationCommand{{Plan: plan}},
				}
				if result, err := f.store.(runtimebus.ScenarioSetupCommitOwner).CommitScenarioSetup(ctx, command); err != nil || !result.Acknowledged {
					t.Fatalf("cursor cohort setup: %+v %v", result, err)
				}
				persisted, err := plan.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				return persisted.State
			}
			seedRun(runA, true)
			state := seedRun(runB, false)
			methods := apiv1.OperatorEntityHandlers(apiv1.EntityHandlerOptions{Entities: f.store.(apiv1.EntityReadStore)})
			readList := methods["entity.list"]
			readCalls := 0
			methods["entity.list"] = func(ctx context.Context, request apiv1.Request) (any, error) {
				readCalls++
				return readList(ctx, request)
			}
			handler, err := apiv1.NewHandler(apiv1.Options{
				PlatformSpecPath: contracts.DefaultPlatformSpecFile(repoRootForRuntimeWriterGuard(t)), AuthTokens: []string{"q10-cursor-token"},
				Handlers: methods,
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, authorization := range []string{"", "Basic invalid", "Bearer wrong-token"} {
				request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"denied","method":"entity.list","params":{"run_id":"`+runB+`"}}`)).WithContext(f.ctx)
				request.Header.Set("Authorization", authorization)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusUnauthorized || readCalls != 0 || !strings.Contains(recorder.Header().Get("WWW-Authenticate"), "Bearer") {
					t.Fatalf("unauthorized request reached entity reader: status=%d calls=%d body=%s", recorder.Code, readCalls, recorder.Body.String())
				}
			}
			missingScope := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"missing-scope","method":"entity.list","params":{"type":"cursor_item"}}`)).WithContext(f.ctx)
			missingScope.Header.Set("Authorization", "Bearer q10-cursor-token")
			missingRecorder := httptest.NewRecorder()
			handler.ServeHTTP(missingRecorder, missingScope)
			var missingResponse struct {
				Error struct {
					Code int `json:"code"`
					Data struct {
						Details struct {
							Field string `json:"field"`
						} `json:"details"`
					} `json:"data"`
				} `json:"error"`
			}
			if err := json.Unmarshal(missingRecorder.Body.Bytes(), &missingResponse); err != nil {
				t.Fatal(err)
			}
			if missingRecorder.Code != http.StatusOK || missingResponse.Error.Code != -32602 || missingResponse.Error.Data.Details.Field != "run_id" || readCalls != 0 {
				t.Fatalf("missing public run scope reached entity reader: calls=%d body=%s", readCalls, missingRecorder.Body.String())
			}
			publicPage := func(params map[string]any) operatorread.OperatorEntityListResult {
				t.Helper()
				body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "cursor", "method": "entity.list", "params": params})
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(string(body))).WithContext(f.ctx)
				request.Header.Set("Authorization", "Bearer q10-cursor-token")
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				var response struct {
					Result operatorread.OperatorEntityListResult `json:"result"`
					Error  json.RawMessage                       `json:"error"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != http.StatusOK || len(response.Error) != 0 {
					t.Fatalf("public cursor page: status=%d body=%s", recorder.Code, recorder.Body.String())
				}
				return response.Result
			}
			page := func(opts operatorread.OperatorEntityListOptions) operatorread.OperatorEntityListResult {
				t.Helper()
				opts.Type = "cursor_item"
				result, err := f.store.(apiv1.EntityReadStore).ListOperatorEntities(f.ctx, opts)
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			assertRows := func(rows []operatorread.OperatorEntitySummary, expected [][2]string) {
				t.Helper()
				identities := make([][2]string, 0, len(rows))
				for _, row := range rows {
					identities = append(identities, [2]string{row.EntityID, row.RunID})
					if !row.CreatedAt.Equal(at) {
						t.Fatalf("data origin changed: %+v", row)
					}
					if row.EntityID == shared || row.EntityID == tail || row.EntityID == other {
						if row.StageDefined != nil || row.CurrentState != nil || row.Revision != nil || row.EnteredStateAt != nil || row.LifecycleCreatedAt != nil || row.LifecycleUpdatedAt != nil || !row.UpdatedAt.Equal(at) {
							t.Fatalf("import acquired lifecycle or moved data clock: %+v", row)
						}
					}
				}
				if !reflect.DeepEqual(identities, expected) {
					t.Fatalf("data cursor identity order: got %v, want %v", identities, expected)
				}
			}
			initialOrder := [][2]string{{runA, runA}, {runB, runB}, {shared, runA}, {shared, runB}, {tail, runA}}
			assertRows(page(operatorread.OperatorEntityListOptions{}).Entities, initialOrder)
			// Prove the excluded type is really stored and publicly readable;
			// the item-only pages below must not leak it into either cursor.
			assertRows(publicPage(map[string]any{"type": "other_item", "run_id": runB}).Entities, [][2]string{{other, runB}})
			unfiltered, err := f.store.(apiv1.EntityReadStore).ListOperatorEntities(f.ctx, operatorread.OperatorEntityListOptions{RunID: runB})
			if err != nil || len(unfiltered.Entities) != 3 || unfiltered.Entities[2].EntityID != other || unfiltered.Entities[2].EntityType != "other_item" {
				t.Fatalf("mixed-type exclusion control is missing: %+v %v", unfiltered, err)
			}
			first := page(operatorread.OperatorEntityListOptions{Limit: 1})
			assertRows(first.Entities, initialOrder[:1])
			if first.NextCursor == "" {
				t.Fatal("first page omitted its remaining-data cursor")
			}
			publicFirst := publicPage(map[string]any{"type": "cursor_item", "run_id": runB, "limit": 1})
			assertRows(publicFirst.Entities, initialOrder[1:2])
			if publicFirst.NextCursor == "" {
				t.Fatal("run-scoped first page omitted its remaining import cursor")
			}
			mutate := func() {
				t.Helper()
				if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(correlation.WithRunID(f.ctx, runB), pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
					t.Fatal(err)
				}
			}
			state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
			state.ExpectedState, state.ExpectedRevision = "ready", 1
			state.Gates, state.UpdatedAt = []byte(`{"approved":true}`), at.Add(time.Minute)
			mutate()
			next := page(operatorread.OperatorEntityListOptions{Limit: 1, Cursor: first.NextCursor})
			assertRows(next.Entities, initialOrder[1:2])
			row := next.Entities[0]
			if !row.UpdatedAt.Equal(at) || row.Revision == nil || *row.Revision != 2 || row.LifecycleUpdatedAt == nil || !row.LifecycleUpdatedAt.Equal(state.UpdatedAt) {
				t.Fatalf("lifecycle-only mutation changed data position or lost header progress: %+v", row)
			}
			if again := page(operatorread.OperatorEntityListOptions{Limit: 1}); again.NextCursor != first.NextCursor {
				t.Fatalf("lifecycle-only mutation changed first cursor: %q != %q", again.NextCursor, first.NextCursor)
			}
			publicAgain := publicPage(map[string]any{"type": "cursor_item", "run_id": runB, "limit": 1})
			assertRows(publicAgain.Entities, initialOrder[1:2])
			if publicAgain.NextCursor != publicFirst.NextCursor || !publicAgain.Entities[0].UpdatedAt.Equal(at) || publicAgain.Entities[0].Revision == nil || *publicAgain.Entities[0].Revision != 2 {
				t.Fatalf("public lifecycle-only cursor changed or lost header revision: %+v", publicAgain)
			}
			assertRows(publicPage(map[string]any{"type": "cursor_item", "run_id": runB, "limit": 1, "cursor": publicFirst.NextCursor}).Entities, [][2]string{{shared, runB}})
			// Keyset pagination is not a run-wide snapshot: an actual data write
			// can move a not-yet-read row ahead of the already issued cursor.
			state.ExpectedRevision, state.UpdatedAt = 2, at.Add(2*time.Minute)
			state.Fields = []byte(`{"count":2}`)
			mutate()
			remaining := []operatorread.OperatorEntitySummary{}
			cursor := first.NextCursor
			for index := 0; index < len(initialOrder); index++ {
				result := page(operatorread.OperatorEntityListOptions{Limit: 1, Cursor: cursor})
				remaining = append(remaining, result.Entities...)
				cursor = result.NextCursor
				if cursor == "" {
					break
				}
			}
			if cursor != "" {
				t.Fatal("cursor traversal did not terminate within the exact data membership")
			}
			assertRows(remaining, initialOrder[2:])
			fresh := page(operatorread.OperatorEntityListOptions{})
			assertRows(fresh.Entities, [][2]string{{runB, runB}, {runA, runA}, {shared, runA}, {shared, runB}, {tail, runA}})
			if !fresh.Entities[0].UpdatedAt.Equal(state.UpdatedAt) || fresh.Entities[0].Revision == nil || *fresh.Entities[0].Revision != 3 {
				t.Fatalf("real data mutation did not move data cursor: %+v", fresh.Entities[0])
			}
			publicFresh := publicPage(map[string]any{"type": "cursor_item", "run_id": runB, "limit": 1})
			if publicFresh.NextCursor == publicFirst.NextCursor || !publicFresh.Entities[0].UpdatedAt.Equal(state.UpdatedAt) {
				t.Fatalf("public data mutation did not change data cursor: %+v", publicFresh)
			}
			for _, cursor := range []string{publicFirst.NextCursor, publicFresh.NextCursor} {
				assertRows(publicPage(map[string]any{"type": "cursor_item", "run_id": runB, "limit": 1, "cursor": cursor}).Entities, [][2]string{{shared, runB}})
			}
			assertRows(publicPage(map[string]any{"type": "cursor_item", "run_id": runA}).Entities, [][2]string{{runA, runA}, {shared, runA}, {tail, runA}})
			assertRows(publicPage(map[string]any{"type": "cursor_item", "run_id": runB}).Entities, [][2]string{{runB, runB}, {shared, runB}})
			assertRows(page(operatorread.OperatorEntityListOptions{Flow: "imports"}).Entities, initialOrder[2:])
			assertRows(publicPage(map[string]any{"type": "cursor_item", "run_id": runA, "flow": "imports"}).Entities, [][2]string{{shared, runA}, {tail, runA}})
			full, err := f.store.(apiv1.EntityReadStore).LoadOperatorEntity(f.ctx, runB, runB)
			if err != nil || full.Fields["count"] != json.Number("2") || !full.Gates["approved"] || full.Entity.EnteredStateAt == nil || !full.Entity.EnteredStateAt.Equal(at) || !full.Entity.CreatedAt.Equal(at) || !full.Entity.UpdatedAt.Equal(state.UpdatedAt) {
				t.Fatalf("cursor changes lost typed data, gate, stage entry or independent clocks: %+v %v", full, err)
			}
			groups, err := f.store.(apiv1.EntityReadStore).AggregateOperatorEntities(f.ctx, operatorread.OperatorEntityAggregateOptions{Type: "cursor_item", GroupBy: "current_state"})
			if err != nil || len(groups.Counts) != 1 || groups.Counts["ready"] != 2 || groups.AbsentCount != 3 {
				t.Fatalf("mutations changed staged/imported data membership: %+v %v", groups, err)
			}
		})
	}
}
