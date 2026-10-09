package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
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
				"nostage/entities.yaml": "record:\n  marker: text?\n",
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
				seed, {Alias: "import", EntityID: importID, FlowInstance: runID + "/import", EntityType: "record", Fields: map[string]any{"marker": "import"}},
			}}, Activations: []runtimebus.FlowInstanceActivationCommand{{Plan: plan}}}
			for _, construction := range plan.ConstructionPlans() {
				command.Activations[0].RouteTopology = append(command.Activations[0].RouteTopology, runtimebus.FlowInstanceRouteRecordSet{
					Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: construction.Identity.Route()},
				})
			}
			result, err := f.store.(runtimebus.ScenarioSetupCommitOwner).CommitScenarioSetup(ctx, command)
			if err != nil || !result.Acknowledged {
				t.Fatalf("native setup: %+v %v", result, err)
			}
			operator := f.store.(interface {
				ListOperatorEntities(context.Context, operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error)
				LoadOperatorEntity(context.Context, string, string) (operatorread.OperatorEntityFull, error)
				AggregateOperatorEntities(context.Context, operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error)
			})
			entities, err := operator.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: runID})
			if err != nil || len(entities.Entities) != 3 {
				t.Fatalf("field-contract membership: %+v %v", entities, err)
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
		})
	}
}
