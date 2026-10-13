package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestHistoricalForkHeaderAndFieldsShareLifecycleCorrespondence(t *testing.T) {
	for _, shape := range []string{"root", "static", "keyed", "fieldless"} {
		for _, variant := range []string{"exact", "foreign_run", "foreign_path", "foreign_entity", "missing_target"} {
			t.Run(shape+"/"+variant, func(t *testing.T) {
				sourceRun, childRun := uuid.NewString(), uuid.NewString()
				flow, path, mode, entityType := "consumer", "consumer", "static", "receipt"
				if shape == "root" {
					flow, path = ".", sourceRun
				} else if shape == "keyed" {
					flow, path, mode = "review", "review/one", "template"
				} else if shape == "fieldless" {
					entityType = ""
				}
				entityID := flowidentity.EntityID(path)
				entry := a2ForkEntry(t, sourceRun, entityID, path, "active", "historical")
				switch variant {
				case "foreign_run":
					entry.RunID = uuid.NewString()
				case "foreign_path":
					entry.InstancePath = "unrelated/one"
				case "foreign_entity":
					entry.EntityID = uuid.NewString()
				}
				at := time.Unix(100, 0).UTC()
				sourceHash, targetHash := strings.Repeat("a", 64), strings.Repeat("b", 64)
				identity := flowidentity.Stored(nil, flow, path, flowidentity.LogicalInstanceID(path), entityID, "")
				construction := selectedWorkflowConstructionRecordFixture(t, sourceRun, "bundle-v2:sha256:"+sourceHash, identity, pipeline.WorkflowInstance{
					WorkflowVersion: "fixture", Mode: mode, Status: "active", EntityType: entityType,
					CurrentState: "active", StageDefined: true, EnteredStageAt: at, CreatedAt: at,
				}, executionmode.Mock)
				outcomes := map[string]contracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}
				topology := contracts.BuildWorkflowStageTopology(flow, "active", []string{"active", "done"}, []string{"done"}, nil, nil, nil,
					[]contracts.WorkflowGatePlan{{FlowID: flow, Stage: "active", Decision: "review", Outcomes: outcomes}})
				transition, err := topology.AdmitTransition(contracts.WorkflowTransitionSite{DecisionID: "review", Verdict: "approve"}, "active", "done")
				if err != nil {
					t.Fatal(err)
				}
				routes, err := gateruntime.FreezeRoutes(outcomes, map[string]contracts.CompiledTransition{"approve": transition})
				if err != nil {
					t.Fatal(err)
				}
				gate, err := gateruntime.New(sourceRun, path, entityID, flow, "active", "review", sourceHash, routes, "historical", at)
				if err != nil {
					t.Fatal(err)
				}
				buckets := map[string]map[string]any{}
				if err := gateruntime.Store(buckets, gate); err != nil {
					t.Fatal(err)
				}
				history := runfork.RunForkEntityState{
					EntityID: entityID, CurrentState: "active", EnteredStateAt: &at,
					Fields: map[string]any{}, Bookkeeping: map[string]any{"stage_entry": entry},
					Accumulator: engine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets(),
					MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
						Owner:        runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
						Source:       runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
						FlowInstance: path, FlowTemplate: flow, EntityType: entityType, Mode: mode, StageDefined: true,
						FlowConfig: construction.Config, InitialMaterialization: construction.InitialMaterialization,
					},
				}
				before, err := json.Marshal(history.Accumulator)
				if err != nil {
					t.Fatal(err)
				}
				if variant == "missing_target" {
					targetHash = ""
				}
				projection, err := runfork.ProjectEntityOwnership(sourceRun, childRun, entityID, path)
				if err != nil {
					t.Fatal(err)
				}
				state := selectedContractWorkflowState{
					SourceRunID: sourceRun, RunID: childRun, EntityID: projection.Fork.EntityID,
					EntityType: entityType, WorkflowName: flow, WorkflowVersion: "fixture", Mode: mode,
					Route: projection.Fork.FlowInstance, History: history,
				}
				header, headerErr := selectedContractHistoricalHeader(state, []byte(`{}`), targetHash, at)
				fields, fieldErr := projectRunForkHistoricalFields(sourceRun, childRun, projection.Fork.EntityID, targetHash, history, runForkEntityMetadata{
					FlowInstance: projection.Fork.FlowInstance, EntityType: entityType,
				}, at)
				if variant != "exact" {
					if headerErr == nil || fieldErr == nil {
						t.Fatalf("hostile source evidence accepted: header=%v fields=%v", headerErr, fieldErr)
					}
					return
				}
				if headerErr != nil || fieldErr != nil {
					t.Fatalf("exact projection refused: header=%v fields=%v", headerErr, fieldErr)
				}
				if len(fields.GateBindings) != 1 || fields.GateBindings[0].Source.BundleHash != sourceHash || fields.GateBindings[0].Fork.BundleHash != targetHash || !bytes.Equal(header.Accumulator, fields.Record.Accumulator) {
					t.Fatalf("paired gate projections lost admitted target source: %+v", fields.GateBindings)
				}
				after, err := json.Marshal(history.Accumulator)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("projection mutated source gate evidence: %v", err)
				}
				var headerBookkeeping, fieldBookkeeping map[string]any
				if err := json.Unmarshal(header.Bookkeeping, &headerBookkeeping); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(fields.Record.Bookkeeping, &fieldBookkeeping); err != nil {
					t.Fatal(err)
				}
				headerEntry, headerFound, headerErr := workflowlifecycle.LoadStageEntry(headerBookkeeping)
				fieldEntry, fieldFound, fieldErr := workflowlifecycle.LoadStageEntry(fieldBookkeeping)
				if headerErr != nil || fieldErr != nil || !headerFound || !fieldFound || headerEntry != fieldEntry {
					t.Fatalf("paired historical entries disagree: header=%+v fields=%+v errors=%v/%v", headerEntry, fieldEntry, headerErr, fieldErr)
				}
				if err := headerEntry.RequireOwner(childRun, header.Identity.Route.ScopeKey, header.Identity.Route.InstanceID, header.Identity.Route.InstancePath, header.EntityID, header.CurrentState); err != nil {
					t.Fatal(err)
				}
				if headerEntry.OriginRunID != sourceRun || headerEntry.OccurrenceID != entry.OccurrenceID || history.Bookkeeping["stage_entry"] != entry {
					t.Fatal("projection changed historical occurrence, origin or source")
				}
			})
		}
	}
}
