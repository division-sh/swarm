package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

// Native constructor/setup and actual selected read ports, not a served proof
// or the final C/Q6/oracle integration qualification.
func TestQ10OperatorAndAgentLifecycleReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":           "name: q10-readback\nstages:\n  unknown: {}\n  done:\n    final: true\n",
				"entities.yaml":         "record:\n  marker: text?\n  current_state: text?\n",
				"nostage/schema.yaml":   "name: nostage\n",
				"nostage/entities.yaml": "record:\n  marker: text?\n  current_state: text?\n  score: integer?\n",
				"fieldless/schema.yaml": "name: fieldless\n",
			}, nil)
			runID, importID := uuid.NewString(), uuid.NewString()
			ctx := correlation.WithRunID(f.ctx, runID)
			at := time.Now().UTC().Truncate(time.Microsecond)
			seed := pipeline.ScenarioSetupEntityRequest{Alias: "root", EntityID: runID, FlowInstance: runID, EntityType: "record",
				CurrentState: mutationlog.PresentLifecycleState("unknown"), Fields: map[string]any{"marker": "root", "current_state": "business"}, Gates: map[string]bool{}}
			source := semanticview.Wrap(f.bundle)
			plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
				ContractBundle: source, OccurredAt: at, ScenarioSeed: &seed,
				Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", ""),
			})
			if err != nil {
				t.Fatal(err)
			}
			command := runtimebus.ScenarioSetupCommand{Setup: pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: at, Entities: []pipeline.ScenarioSetupEntityRequest{
				seed, {Alias: "import", EntityID: importID, FlowInstance: "nostage/import", EntityType: "record", Fields: map[string]any{"marker": "import", "current_state": "business-import", "score": int64(12)}},
			}}, Activations: []runtimebus.FlowInstanceActivationCommand{{Plan: plan}}}
			result, err := f.store.(runtimebus.ScenarioSetupCommitOwner).CommitScenarioSetup(ctx, command)
			if err != nil || !result.Acknowledged {
				t.Fatalf("native setup: %+v %v", result, err)
			}
			constructed, err := f.store.(pipeline.WorkflowInstancePersistenceReader).ListWorkflowInstances(ctx, runID)
			if err != nil || len(constructed) != 3 {
				t.Fatalf("native constructed membership: %+v err=%v", constructed, err)
			}
			for _, instance := range constructed {
				want := "pending"
				if instance.EntityID == runID {
					want = "unknown"
				}
				assertQ10NativeInternalHistory(t, ctx, f.store, runID, instance.EntityID, &want)
			}
			assertQ10NativeInternalHistory(t, ctx, f.store, runID, importID, nil)
			operator := f.store.(interface {
				ListOperatorEntities(context.Context, operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error)
				LoadOperatorEntity(context.Context, string, string) (operatorread.OperatorEntityFull, error)
				AggregateOperatorEntities(context.Context, operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error)
			})
			entities, err := operator.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: runID})
			if err != nil || len(entities.Entities) != 3 {
				t.Fatalf("field-contract membership: %+v %v", entities, err)
			}
			observations, err := ReadWorkflowStateObservationRowsForTest(ctx, f.store)
			if err != nil || len(observations) != 3 {
				t.Fatalf("physical observation changed field-row membership: %+v %v", observations, err)
			}
			observed := map[string]bool{}
			for _, observation := range observations {
				if observed[observation.EntityID] {
					t.Fatalf("physical observation duplicated a field row: %+v", observations)
				}
				observed[observation.EntityID] = true
				var matched *operatorread.OperatorEntitySummary
				for i := range entities.Entities {
					if entities.Entities[i].EntityID == observation.EntityID {
						matched = &entities.Entities[i]
						break
					}
				}
				if matched == nil || observation.FlowInstance != matched.FlowInstance {
					t.Fatalf("physical observation invented or crossed a data row: %+v", observation)
				}
				if observation.EntityID == importID {
					if observation.CurrentState != nil {
						t.Fatalf("import borrowed a lifecycle or business stage: %+v", observation)
					}
				} else {
					want := "pending"
					if observation.EntityID == runID {
						want = "unknown"
					}
					if observation.CurrentState == nil || *observation.CurrentState != want {
						t.Fatalf("physical header state changed: %+v, want %q", observation, want)
					}
				}
			}
			for _, entity := range entities.Entities {
				full, err := operator.LoadOperatorEntity(ctx, entity.EntityID, runID)
				if err != nil {
					t.Fatal(err)
				}
				row, found, err := f.store.(tools.EntityPersistence).LoadEntityState(ctx, tools.EntityIdentity{RunID: runID, EntityID: entity.EntityID})
				if err != nil || !found {
					t.Fatalf("agent read: %v %t %v", row, found, err)
				}
				if entity.EntityID == importID {
					if entity.StageDefined != nil || entity.CurrentState != nil || entity.Revision != nil || full.Gates != nil || full.Bookkeeping != nil || full.Accumulated != nil || full.Loops != nil || row["current_state"] != nil || row["gates"] != nil {
						t.Fatalf("import acquired lifecycle: %+v %+v %v", entity, full, row)
					}
				} else {
					if entity.StageDefined == nil || entity.Revision == nil || *entity.Revision <= 0 || entity.LifecycleCreatedAt == nil || entity.LifecycleUpdatedAt == nil || full.Gates == nil || full.Bookkeeping == nil || full.Accumulated == nil || full.Loops == nil {
						t.Fatalf("constructed lifecycle missing: %+v %+v", entity, full)
					}
					if entity.EntityID == runID {
						if !*entity.StageDefined || entity.CurrentState == nil || *entity.CurrentState != "unknown" || row["current_state"] != "unknown" || full.Fields["current_state"] != "business" {
							t.Fatalf("actual unknown/business field collision: %+v %v", full, row)
						}
					} else if *entity.StageDefined || entity.CurrentState != nil || entity.EnteredStateAt != nil || row["current_state"] != nil {
						t.Fatalf("no-stage member fabricated named stage: %+v %v", entity, row)
					}
				}
				encoded, err := json.Marshal(full)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(encoded, &wire); err != nil {
					t.Fatal(err)
				}
				if _, ok := wire["loops"]; !ok {
					t.Fatal("nullable loop property omitted")
				}
			}
			filtered, err := operator.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: runID, CurrentState: "unknown"})
			if err != nil || len(filtered.Entities) != 1 || filtered.Entities[0].EntityID != runID {
				t.Fatalf("stage filter selected absence: %+v %v", filtered, err)
			}
			groups, err := operator.AggregateOperatorEntities(ctx, operatorread.OperatorEntityAggregateOptions{RunID: runID, GroupBy: "current_state"})
			if err != nil || len(groups.Counts) != 1 || groups.Counts["unknown"] != 1 || groups.AbsentCount != 2 {
				t.Fatalf("absence/unknown counts drift: %+v %v", groups, err)
			}
			handler, err := apiv1.NewHandler(apiv1.Options{
				PlatformSpecPath: contracts.DefaultPlatformSpecFile(repoRootForRuntimeWriterGuard(t)), AuthTokens: []string{"q10-read-token"},
				Handlers: apiv1.OperatorEntityHandlers(apiv1.EntityHandlerOptions{Entities: f.store.(apiv1.EntityReadStore)}),
			})
			if err != nil {
				t.Fatal(err)
			}
			readRPC := func(method string, params map[string]any) map[string]json.RawMessage {
				t.Helper()
				body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "q10-read", "method": method, "params": params})
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(string(body))).WithContext(ctx)
				request.Header.Set("Authorization", "Bearer q10-read-token")
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				var response struct {
					Result map[string]json.RawMessage `json:"result"`
					Error  json.RawMessage            `json:"error"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != http.StatusOK || len(response.Error) != 0 || response.Result == nil {
					t.Fatalf("public read %s: status=%d body=%s", method, recorder.Code, recorder.Body.String())
				}
				return response.Result
			}
			importWire := readRPC("entity.get", map[string]any{"run_id": runID, "entity_id": importID})
			var summaryWire map[string]json.RawMessage
			if err := json.Unmarshal(importWire["entity"], &summaryWire); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"stage_defined", "current_state", "revision", "entered_state_at", "lifecycle_created_at", "lifecycle_updated_at"} {
				if string(summaryWire[key]) != "null" {
					t.Fatalf("public imported %s must be an explicit null: %s", key, importWire["entity"])
				}
			}
			for _, key := range []string{"bookkeeping", "gates", "accumulated", "loops"} {
				if string(importWire[key]) != "null" {
					t.Fatalf("public imported %s must be an explicit null: %s", key, importWire[key])
				}
			}
			aggregateWire := readRPC("entity.aggregate", map[string]any{"run_id": runID, "group_by": "current_state"})
			if string(aggregateWire["absent_count"]) != "2" || string(aggregateWire["counts"]) != `{"unknown":1}` {
				t.Fatalf("public stage aggregate fabricated an absence bucket: %v", aggregateWire)
			}
			for cursor, seen := "", map[string]bool{}; ; {
				page, err := operator.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: runID, Limit: 1, Cursor: cursor})
				if err != nil {
					t.Fatal(err)
				}
				for _, entity := range page.Entities {
					if seen[entity.EntityID] {
						t.Fatal("cursor repeated entity")
					}
					seen[entity.EntityID] = true
				}
				cursor = page.NextCursor
				if cursor == "" {
					if len(seen) != 3 {
						t.Fatalf("cursor lost members: %v", seen)
					}
					break
				}
			}
			targets, err := f.store.(budgetspend.Store).ListBudgetProjectionTargets(ctx)
			if err != nil || len(targets) != 3 {
				t.Fatalf("budget must include fieldless/header members, not import: %+v %v", targets, err)
			}
			for _, target := range targets {
				if target.EntityID == importID || (target.EntityID == runID && target.Stage != "unknown") {
					t.Fatalf("budget uses data lifecycle: %+v", target)
				}
			}
			contract, ok := entityruntime.ResolveForFlow(source, "nostage")
			if !ok {
				t.Fatal("missing admitted no-stage entity contract")
			}
			counter := f.store.(entityquery.Reader)
			for _, tc := range []struct {
				name      string
				predicate entityquery.Predicate
				want      int
			}{
				{"real stage does not select absent metadata", entityquery.Predicate{Field: "current_state", Op: "==", Value: "unknown"}, 0},
				{"absent stage differs from real stage", entityquery.Predicate{Field: "workflow_state", Op: "!=", Value: "unknown"}, 2},
				{"declared same-name field is not metadata", entityquery.Predicate{Field: "entity.current_state", Op: "==", Value: "business-import"}, 1},
				{"numeric fields retain numeric comparison", entityquery.Predicate{Field: "score", Op: ">", Value: int64(2)}, 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					count, err := counter.CountWorkflowEntities(ctx, entityquery.Request{RunID: runID, Source: source, Contract: contract, Predicate: tc.predicate})
					if err != nil || count != tc.want {
						t.Fatalf("handler count = %d, want %d: %v", count, tc.want, err)
					}
				})
			}
			_, err = counter.CountWorkflowEntities(ctx, entityquery.Request{RunID: runID, Source: source, Contract: contract,
				Predicate: entityquery.Predicate{Field: "current_state", Op: ">", Value: "unknown"}})
			var nullComparison *entityquery.NullComparisonError
			if !errors.As(err, &nullComparison) {
				t.Fatalf("ordered absent stage must fail typed, not compare a sentinel: %v", err)
			}
			if _, err := counter.CountWorkflowEntities(ctx, entityquery.Request{RunID: runID, Source: source, Contract: contract,
				Predicate: entityquery.Predicate{Field: "current_state", Op: "==", Value: nil}}); err == nil {
				t.Fatal("handler query must retain its non-null RHS grammar")
			}
		})
	}
}
