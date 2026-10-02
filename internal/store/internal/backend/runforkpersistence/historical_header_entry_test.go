package runforkpersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestHistoricalForkHeaderAndFieldsShareLifecycleCorrespondence(t *testing.T) {
	for _, shape := range []string{"root", "static", "keyed", "fieldless"} {
		for _, variant := range []string{"exact", "foreign_run", "foreign_path", "foreign_entity"} {
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
				history := runfork.RunForkEntityState{
					EntityID: entityID, CurrentState: "active", EnteredStateAt: &at,
					Fields: map[string]any{}, Bookkeeping: map[string]any{"stage_entry": entry},
					MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
						Source:       runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
						FlowInstance: path, FlowTemplate: flow, EntityType: entityType, Mode: mode, StageDefined: true,
					},
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
				header, headerErr := selectedContractHistoricalHeader(state, []byte(`{}`), at)
				fields, fieldErr := projectRunForkHistoricalFields(sourceRun, childRun, projection.Fork.EntityID, history, runForkEntityMetadata{
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
