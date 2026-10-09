package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func forkConstructionPlanFixture(t *testing.T, outgoing bool) runfork.RunForkPlan {
	t.Helper()
	sourceRun := uuid.NewString()
	path, at := "review/key/final", time.Unix(100, 0).UTC()
	identity := flowidentity.Instance{
		TemplateID: "review/final", ScopeKey: "review/final", InstanceID: "final", InstancePath: path,
		EntityID: flowidentity.EntityID(path), HasStoredPath: true,
		ParentEntityID: sourceRun, ParentRoute: flowidentity.ParentRoute{FlowID: ".", FlowInstance: sourceRun, EntityID: sourceRun},
	}
	record := selectedWorkflowConstructionRecordFixture(t, sourceRun, "bundle-v2:sha256:"+strings.Repeat("a", 64), identity, pipeline.WorkflowInstance{
		WorkflowVersion: "v1", Mode: "static", Status: "active", EntityType: "record",
		CurrentState: "initial", StageDefined: true, EnteredStageAt: at, CreatedAt: at,
		Fields: map[string]any{"value": int64(7), "double": float64(7)},
	}, executionmode.Mock)
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(record.InitialMaterialization, sourceRun, identity.EntityID, path, identity.TemplateID)
	if err != nil {
		t.Fatal(err)
	}
	receipt.CreatingInput = pipeline.FlowConstructionInput{EventID: uuid.NewString(), Input: "task.started"}
	if outgoing {
		receipt.CreationEvent = &pipeline.DynamicFlowRuntimeCreationEventPlan{
			EventID: uuid.NewString(), EventType: "review/final.created", RunID: sourceRun, ParentEventID: receipt.CreatingInput.EventID,
			Payload: json.RawMessage(`{"value":7,"double":7.0}`), CreatedAt: at, ExecutionMode: executionmode.Mock,
		}
	}
	raw, err := pipeline.EncodeFlowConstructionReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return (runfork.RunForkPlan{
		SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1},
		Entities: []runfork.RunForkEntityState{{
			EntityID: identity.EntityID, CurrentState: "later", EnteredStateAt: &at,
			Fields: map[string]any{"value": int64(99), "double": float64(99)},
			MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
				Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
				FlowTemplate: identity.TemplateID, FlowInstance: path, FlowConfig: record.Config, EntityType: "record", Mode: "static",
				CreatedAt: at, UpdatedAt: at, EnteredStateAt: at, InitialMaterialization: raw,
			},
		}},
	}).WithHistoricalEvents(1, nil)
}

func TestForkConstructionReceiptPreservesInitializationAndExactIdentity(t *testing.T) {
	for _, outgoing := range []bool{false, true} {
		plan := forkConstructionPlanFixture(t, outgoing)
		entity, childRun, born := plan.Entities[0], uuid.NewString(), time.Unix(200, 0).UTC()
		before := bytes.Clone(entity.MaterializationMetadata.InitialMaterialization)
		original, err := pipeline.DecodeStoredFlowConstructionReceipt(before, plan.SourceRunID, entity.EntityID,
			entity.MaterializationMetadata.FlowInstance, entity.MaterializationMetadata.FlowTemplate)
		if err != nil {
			t.Fatal(err)
		}
		child, err := projectRunForkConstructionReceipt(plan, childRun, entity, born)
		if err != nil {
			t.Fatal(err)
		}
		if child.Persisted.Fields["value"] != int64(7) || child.Persisted.Fields["double"] != float64(7) ||
			child.InitialState != "initial" || !child.OccurredAt.Equal(born) || child.Identity.InstanceID != "final" ||
			child.Identity.ParentEntityID != childRun || child.Identity.ParentRoute.FlowInstance != childRun ||
			child.CreatingInput.EventID != deterministicRunForkReplayEventID(childRun, original.CreatingInput.EventID) {
			t.Fatalf("child lost independent initialization/identity: %+v", child)
		}
		if child.Identity.EntityID != entity.EntityID || child.Identity.InstancePath != entity.MaterializationMetadata.FlowInstance {
			t.Fatal("non-root child was rehomed")
		}
		if (child.CreationEvent != nil) != outgoing {
			t.Fatal("child invented or lost the outgoing occurrence")
		}
		if outgoing && (child.CreationEvent.ParentEventID != child.CreatingInput.EventID || child.CreationEvent.RunID != childRun ||
			!bytes.Equal(child.CreationEvent.Payload, original.CreationEvent.Payload) || !bytes.Contains(child.CreationEvent.Payload, []byte(`"double":7.0`))) {
			t.Fatal("child changed frozen business data or occurrence correspondence")
		}
		repeated, err := projectRunForkConstructionReceipt(plan, childRun, entity, born)
		if err != nil || !reflect.DeepEqual(repeated, child) {
			t.Fatal("exact child receipt is not replay-stable")
		}
		child.Persisted.Fields["value"] = int64(123)
		if !bytes.Equal(before, entity.MaterializationMetadata.InitialMaterialization) || entity.Fields["value"] != int64(99) {
			t.Fatal("projection mutated source initialization or current fields")
		}
	}
}

func TestForkConstructionOccurrenceSeparatesOwedFromPublished(t *testing.T) {
	plan := forkConstructionPlanFixture(t, true)
	entity := plan.Entities[0]
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(entity.MaterializationMetadata.InitialMaterialization,
		plan.SourceRunID, entity.EntityID, entity.MaterializationMetadata.FlowInstance, entity.MaterializationMetadata.FlowTemplate)
	if err != nil {
		t.Fatal(err)
	}
	if owed, err := runForkConstructionOccurrenceOwed(plan, entity); err != nil || !owed {
		t.Fatalf("unpublished output is not owed: %v %v", owed, err)
	}
	plan = plan.WithHistoricalEvents(1, []string{receipt.CreationEvent.EventID})
	if owed, err := runForkConstructionOccurrenceOwed(plan, entity); err != nil || owed {
		t.Fatalf("published output would be re-emitted: %v %v", owed, err)
	}
	plan = plan.WithHistoricalEvents(2, nil)
	if _, err := runForkConstructionOccurrenceOwed(plan, entity); err == nil {
		t.Fatal("missing event-membership proof silently interpreted as un-emitted")
	}
	plan = forkConstructionPlanFixture(t, false)
	if owed, err := runForkConstructionOccurrenceOwed(plan, plan.Entities[0]); err != nil || owed {
		t.Fatal("explicit no-output receipt invented publication work")
	}
}

func TestForkConstructionReceiptRejectsMissingAndForeignEvidence(t *testing.T) {
	for _, variant := range []string{"missing", "foreign_run", "foreign_entity", "foreign_route", "missing_birth"} {
		t.Run(variant, func(t *testing.T) {
			plan := forkConstructionPlanFixture(t, true)
			entity, born := plan.Entities[0], time.Unix(200, 0).UTC()
			switch variant {
			case "missing":
				entity.MaterializationMetadata.InitialMaterialization = nil
			case "foreign_run":
				plan.SourceRunID = uuid.NewString()
			case "foreign_entity":
				entity.EntityID = uuid.NewString()
			case "foreign_route":
				entity.MaterializationMetadata.FlowInstance = "review/foreign/final"
			case "missing_birth":
				born = time.Time{}
			}
			if _, err := projectRunForkConstructionReceipt(plan, uuid.NewString(), entity, born); err == nil {
				t.Fatal("contradictory construction evidence accepted")
			}
		})
	}
}
