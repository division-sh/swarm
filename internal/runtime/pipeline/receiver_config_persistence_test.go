package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestReceiverConfigAndRuntimeControlsRoundTripBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var store *workflowInstanceStore
			if backend == "sqlite" {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				store = newSQLiteWorkflowInstanceStoreForTest(t, db)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store = newPostgresWorkflowInstanceStoreForTest(db)
			}
			ensurePipelineTestRun(t, store, testPipelineRunID)
			ctx := correlation.WithRunID(testAuthorActivityContext(t, context.Background()), testPipelineRunID)
			config := map[string]any{
				"status": false, "flow_path": []any{"business", "path"},
				"instance_id": int64(42), "workflow_version": "business-version",
				"storage_ref": nil, "instance_kind": "business-kind", "template_version": "business-template",
				"parent_flow_id": "business-parent", "parent_flow_instance": "business-parent/path", "parent_entity_id": "business-parent-id",
				"transition_history": "business-history", "config": map[string]any{"control": "business-control"},
				"nested": []any{map[string]any{"integer": int64(75), "double": float64(75), "null": nil, "empty": ""}},
			}
			want := cloneWorkflowSchemaValue(config).(map[string]any)
			now := time.Now().UTC().Round(time.Microsecond)
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: "ti-not-the-business-key", StorageRef: "review/ti-not-the-business-key", EntityID: uuid.NewString(),
				EntityType: "review_subject", WorkflowName: "review", WorkflowVersion: "runtime-version",
				CurrentState: "active", Status: "active", InstanceKind: "template", Config: config,
				ParentFlowID: "parent", ParentFlowInstance: "parent/one", ParentEntityID: uuid.NewString(),
				TransitionHistory: []WorkflowTransitionRecord{lifecycleTransitionRecordFixtureForTest(t, "review", "queued", "active", "evt-1", now)},
			})
			if err := store.create(ctx, instance); err != nil {
				t.Fatal(err)
			}
			config["nested"].([]any)[0].(map[string]any)["integer"] = int64(99)
			owner := testRunScopedWorkflowInstance(instance.StorageRef)
			for attempt := 0; attempt < 2; attempt++ {
				loaded, found, err := store.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("load: found=%v err=%v", found, err)
				}
				if !reflect.DeepEqual(loaded.Config, want) {
					t.Fatalf("business config changed: got %#v want %#v", loaded.Config, want)
				}
				if loaded.Status != "active" || loaded.StorageRef != instance.StorageRef || loaded.InstanceID != instance.InstanceID || loaded.WorkflowVersion != instance.WorkflowVersion || loaded.ParentFlowID != instance.ParentFlowID || loaded.ParentEntityID != instance.ParentEntityID || len(loaded.TransitionHistory) != 1 {
					t.Fatalf("business data replaced runtime controls: %#v", loaded)
				}
				projection, err := store.LoadRouteRecoveryProjection(ctx, owner)
				if err != nil {
					t.Fatal(err)
				}
				if projection.Identity.Route() != owner.Route || projection.Identity.EntityID != instance.EntityID || !reflect.DeepEqual(projection.Config, want) {
					t.Fatalf("route recovery changed exact config/identity: %#v", projection)
				}
				loaded.Config["nested"].([]any)[0].(map[string]any)["integer"] = int64(100)
				projection.Config["config"].(map[string]any)["control"] = "mutated-readback"
			}
		})
	}
}

func TestReceiverConfigCodecIsolatesNestedBusinessValues(t *testing.T) {
	config := map[string]any{"nested": []any{map[string]any{"integer": int64(4), "double": float64(4), "number": json.Number("4.0"), "null": nil}}}
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		StorageRef: "review/one", EntityID: uuid.NewString(), EntityType: "review_subject", WorkflowName: "review", Config: config,
	})
	projection, err := workflowInstancePersistedProjectionFromInstance(instance, instance.StorageRef)
	if err != nil {
		t.Fatal(err)
	}
	payload := projection.ConfigPayload("version")
	config["nested"].([]any)[0].(map[string]any)["integer"] = int64(99)
	projection.Config["nested"].([]any)[0].(map[string]any)["double"] = float64(99)
	raw, err := canonicaljson.MarshalPreservingNumberKinds(payload)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := decodeWorkflowInstanceConfigPayload(raw, workflowInstancePersistedControl{StorageRef: instance.StorageRef})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"nested": []any{map[string]any{"integer": int64(4), "double": float64(4), "number": float64(4), "null": nil}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("aliasing or numeric kind drift: %#v", got)
	}
}

func TestReceiverConfigCodecRejectsFlatAndMalformedBusinessObjects(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"instance_id":"one","business":"old-flat-value"}`, `{"config":null}`, `{"config":[]}`, `{"config":"value"}`, `{"config":{},"unknown":"value"}`} {
		t.Run(raw, func(t *testing.T) {
			if _, _, err := decodeWorkflowInstanceConfigPayload([]byte(raw), workflowInstancePersistedControl{}); err == nil || !strings.Contains(err.Error(), "flow_instances.config") {
				t.Fatalf("invalid config accepted: %v", err)
			}
		})
	}
	// An empty business object is evidence; absence or null is not.
	if config, _, err := decodeWorkflowInstanceConfigPayload([]byte(`{"config":{}}`), workflowInstancePersistedControl{}); err != nil || config == nil || len(config) != 0 {
		t.Fatalf("explicit empty config rejected: %#v %v", config, err)
	}
}

func TestHistoricalWorkflowConfigProjectsOnlyOwnership(t *testing.T) {
	parent := flowidentity.ParentRoute{FlowID: "parent", FlowInstance: "parent/one", EntityID: uuid.NewString()}
	source := materializedWorkflowInstanceForTest(WorkflowInstance{
		StorageRef: "parent/one/review", EntityID: uuid.NewString(), WorkflowName: "review",
		WorkflowVersion: "source-version", InstanceKind: "static", TemplateVersion: "source-template",
		Status: "active", ParentFlowID: parent.FlowID, ParentFlowInstance: parent.FlowInstance, ParentEntityID: parent.EntityID,
		Config: map[string]any{"workflow_version": "business-version", "parent_entity_id": "business-parent", "nested": []any{int64(3), float64(3)}},
	})
	projection, err := workflowInstancePersistedProjectionFromInstance(source, source.StorageRef)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(projection.ConfigPayload(source.WorkflowVersion))
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := DecodeWorkflowInstanceRecordedConfig(flowidentity.RouteForInstancePath(source.StorageRef), raw)
	if err != nil || recorded.WorkflowVersion() != source.WorkflowVersion || recorded.ParentRoute() != parent {
		t.Fatalf("recorded controls: %+v %v", recorded, err)
	}
	target := flowidentity.RouteForInstancePath("parent/two/review")
	projectedParent := flowidentity.ParentRoute{FlowID: parent.FlowID, FlowInstance: "parent/two", EntityID: uuid.NewString()}
	projected, err := recorded.Project(target, projectedParent)
	if err != nil {
		t.Fatal(err)
	}
	business, control, err := decodeWorkflowInstanceConfigPayload(projected, workflowInstancePersistedControl{})
	if err != nil || !reflect.DeepEqual(business, source.Config) || control.WorkflowVersion != source.WorkflowVersion ||
		control.InstanceKind != source.InstanceKind || control.TemplateVersion != source.TemplateVersion || control.Status != source.Status ||
		control.StorageRef != target.InstancePath || control.FlowPath != target.InstancePath || control.InstanceID != target.InstanceID ||
		control.ParentFlowInstance != projectedParent.FlowInstance || control.ParentEntityID != projectedParent.EntityID {
		t.Fatalf("historical projection changed non-ownership controls: business=%+v controls=%+v err=%v", business, control, err)
	}
	for _, invalid := range []flowidentity.ParentRoute{{}, {FlowID: "different", FlowInstance: "parent/two", EntityID: projectedParent.EntityID}, {FlowID: parent.FlowID, FlowInstance: "parent/two"}} {
		if _, err := recorded.Project(target, invalid); err == nil {
			t.Fatalf("accepted incomplete/changed parent %+v", invalid)
		}
	}
	if _, err := DecodeWorkflowInstanceRecordedConfig(flowidentity.RouteForInstancePath(source.StorageRef), []byte(strings.Replace(string(raw), `"workflow_version":"source-version"`, `"workflow_version":1`, 1))); err == nil {
		t.Fatal("accepted malformed recorded workflow version")
	}
	again, err := recorded.Project(target, projectedParent)
	if err != nil || string(again) != string(projected) {
		t.Fatalf("projection mutated recorded controls: %s %v", again, err)
	}
}
