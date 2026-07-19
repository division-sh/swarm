package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestFlowInstanceIdentity_DistinguishesScopeKeyInstancePathAndEntityID(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-gates-in-child-flow")

	scopeKey := strings.TrimSpace(workflowScopeKey(source, "child"))
	if scopeKey != "child" {
		t.Fatalf("workflowScopeKey(child) = %q, want child", scopeKey)
	}

	instancePath := strings.TrimSpace(DeriveFlowInstancePath(source, "child", "inst-1"))
	if instancePath != "child/inst-1" {
		t.Fatalf("DeriveFlowInstancePath(child, inst-1) = %q, want child/inst-1", instancePath)
	}
	if instancePath == scopeKey {
		t.Fatalf("instance path and scope key should differ, both = %q", instancePath)
	}

	entityID := strings.TrimSpace(FlowInstanceEntityID(instancePath))
	if entityID == "" {
		t.Fatal("expected canonical flow entity id")
	}
	if entityID == instancePath {
		t.Fatalf("canonical flow entity id should differ from instance path, both = %q", entityID)
	}
	if entityID == scopeKey {
		t.Fatalf("canonical flow entity id should differ from scope key, both = %q", entityID)
	}
}

func TestFlowInstanceIdentity_CreateEntityUsesScopeKeyForPathAndLogicalInstanceForMetadata(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-gates-in-child-flow")

	handler, ok := source.NodeEventHandler("validator", "validate.start")
	if !ok {
		t.Fatal("expected validator handler for validate.start")
	}
	state := &WorkflowState{
		EntityID: "11111111-1111-1111-1111-111111111111",
		Metadata: map[string]any{},
	}

	entityID, _, err := resolveHandlerEntityIDForFlow(source, "child", handler, state.EntityID, mustEvent("child/validate.start", state.EntityID), state)
	if err != nil {
		t.Fatalf("resolveHandlerEntityIDForFlow: %v", err)
	}

	if got := strings.TrimSpace(state.EntityID); got != entityID {
		t.Fatalf("state.EntityID = %q, want %q", got, entityID)
	}
	instanceID := strings.TrimSpace(asString(state.Metadata["instance_id"]))
	if instanceID == "" {
		t.Fatal("expected logical instance_id in state metadata")
	}
	flowPath := strings.TrimSpace(asString(state.Metadata["flow_path"]))
	if flowPath != "child/"+instanceID {
		t.Fatalf("flow_path = %q, want child/%s", flowPath, instanceID)
	}
	if got := strings.TrimSpace(asString(state.Metadata["storage_ref"])); got != flowPath {
		t.Fatalf("storage_ref = %q, want %q", got, flowPath)
	}
	if wantEntityID := FlowInstanceEntityID(flowPath); entityID != wantEntityID {
		t.Fatalf("entityID = %q, want canonical flow entity id %q", entityID, wantEntityID)
	}
}

func TestFlowInstanceIdentity_DescendantDetectionIsDepthSafe(t *testing.T) {
	cases := []struct {
		name         string
		scopeKey     string
		instancePath string
		want         bool
	}{
		{name: "same scope", scopeKey: "child", instancePath: "child", want: false},
		{name: "same flow instance", scopeKey: "child", instancePath: "child/inst-1", want: false},
		{name: "direct descendant instance", scopeKey: "child", instancePath: "child/grandchild/inst-1", want: true},
		{name: "deep descendant instance", scopeKey: "child", instancePath: "child/grandchild/great/inst-1", want: true},
		{name: "different branch", scopeKey: "child", instancePath: "other/grandchild/inst-1", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDescendantFlowInstance(tc.scopeKey, tc.instancePath); got != tc.want {
				t.Fatalf("isDescendantFlowInstance(%q, %q) = %v, want %v", tc.scopeKey, tc.instancePath, got, tc.want)
			}
		})
	}
}

func TestFlowInstanceIdentity_ResolveEmittedEntityID(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")

	childState := WorkflowState{
		EntityID: "ent-child",
		Metadata: map[string]any{
			"flow_path":        "child/inst-1",
			"parent_entity_id": "ent-parent",
		},
	}
	trigger := mustEvent("child/child.start", "ent-child")

	if got := resolveEmittedEntityID(source, "child", "child/child.internal", childState, trigger, "ent-child", "ent-child"); got != "ent-child" {
		t.Fatalf("internal emitted entity_id = %q, want ent-child", got)
	}
	if got := resolveEmittedEntityID(source, "child", "child/child.done", childState, trigger, "ent-child", "ent-child"); got != "ent-child" {
		t.Fatalf("output emitted entity_id = %q, want ent-child", got)
	}

	rootState := WorkflowState{
		EntityID: "ent-child",
		Metadata: map[string]any{
			"parent_entity_id": "ent-root",
		},
	}
	if got := resolveEmittedEntityID(source, "child", "child/child.done", rootState, trigger, "ent-child", "ent-child"); got != "ent-child" {
		t.Fatalf("non-instanced child emitted entity_id = %q, want ent-child", got)
	}

	if got := resolveEmittedEntityID(source, "scoring", "scoring/scoring.requested", WorkflowState{
		EntityID: "ent-child",
		Metadata: map[string]any{
			"parent_entity_id": "ent-root",
		},
	}, mustEvent("vertical.discovered", "ent-root"), "ent-child", "ent-root"); got != "ent-child" {
		t.Fatalf("root flow emitted entity_id = %q, want ent-child", got)
	}
}

func TestWorkflowInstanceCoordinates_SeparateStaticScopeFromGenericStorageRef(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-gates-in-child-flow")

	instance := WorkflowInstance{
		WorkflowName: "child",
		StorageRef:   "22222222-2222-2222-2222-222222222222",
		Metadata: map[string]any{
			"storage_ref": "22222222-2222-2222-2222-222222222222",
		},
	}

	if got := workflowInstanceScopeKey(source, instance); got != "child" {
		t.Fatalf("workflowInstanceScopeKey(static child) = %q, want child", got)
	}
	if got := workflowInstancePath(source, instance); got != "child" {
		t.Fatalf("workflowInstancePath(static child) = %q, want child", got)
	}
}

func TestWorkflowInstanceCoordinates_KeepNestedInstancePathDistinctFromScope(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")

	instance := WorkflowInstance{
		WorkflowName: "grandchild",
		Metadata: map[string]any{
			"flow_path": "child/grandchild/inst-1",
		},
	}

	if got := workflowInstanceScopeKey(source, instance); got != "child/grandchild" {
		t.Fatalf("workflowInstanceScopeKey(nested) = %q, want child/grandchild", got)
	}
	if got := workflowInstancePath(source, instance); got != "child/grandchild/inst-1" {
		t.Fatalf("workflowInstancePath(nested) = %q, want child/grandchild/inst-1", got)
	}
}

func TestWorkflowInstanceCoordinates_DeriveNestedStaticScopeWithoutTruncation(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")

	instance := WorkflowInstance{
		WorkflowName: "grandchild",
	}
	identity := StoredFlowInstance(source, instance)

	if identity.ScopeKey != "child/grandchild" {
		t.Fatalf("StoredFlowInstance(static nested).ScopeKey = %q, want child/grandchild", identity.ScopeKey)
	}
	if identity.InstancePath != "child/grandchild" {
		t.Fatalf("StoredFlowInstance(static nested).InstancePath = %q, want child/grandchild", identity.InstancePath)
	}
	if identity.HasStoredPath {
		t.Fatal("StoredFlowInstance(static nested).HasStoredPath = true, want derived path")
	}
}

func TestWorkflowInstanceOwnedByFlow_UsesExactSemanticScope(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")

	instance := WorkflowInstance{
		WorkflowName: "grandchild",
		Metadata: map[string]any{
			"flow_path": "child/grandchild/inst-1",
		},
	}

	if workflowInstanceOwnedByFlow(source, instance, "child") {
		t.Fatal("did not expect child to own child/grandchild/inst-1")
	}
	if !workflowInstanceOwnedByFlow(source, instance, "grandchild") {
		t.Fatal("expected grandchild to own child/grandchild/inst-1")
	}
}

func mustEvent(eventType, entityID string) Event {
	return eventtest.RunCreatingRootIngress("", events.EventType(eventType), "", "", nil, 0, "", "", events.EventEnvelope{EntityID: entityID}, time.Time{})
}
