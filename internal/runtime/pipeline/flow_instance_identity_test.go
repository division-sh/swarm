package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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

func TestFlowInstanceIdentity_ConstructorUsesTypedPathAndLogicalInstance(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-dynamic-flow-instance")
	instance := deriveFlowInstanceIdentity(source, "worker", "inst-1")
	if instance.InstancePath != "worker/inst-1" || instance.InstanceID != "inst-1" || instance.EntityID != FlowInstanceEntityID(instance.InstancePath) {
		t.Fatalf("constructor identity = %#v", instance)
	}
}

func TestFlowInstanceIdentity_RootConstructorAndExecutionShareRoute(t *testing.T) {
	source := semanticview.Wrap(compiledAdapterSource(t))
	constructed := runtimeflowidentity.Stored(source, ".", testPipelineRunID, testPipelineRunID, "", "")
	execution, err := workflowInstanceRouteForExecution(source, ".", testPipelineRunID)
	if err != nil {
		t.Fatal(err)
	}
	if execution != constructed.Route() || execution.ScopeKey != "." || execution.InstancePath != testPipelineRunID {
		t.Fatalf("root execution route %#v disagrees with constructor %#v", execution, constructed.Route())
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

func TestWorkflowInstanceOwnedByFlow_UsesExactSemanticScope(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	root := runtimeflowidentity.Stored(source, semanticview.RootExecutionFlowID(source), testPipelineRunID, testPipelineRunID, runtimeflowidentity.EntityID(testPipelineRunID), "")
	child, err := runtimeflowidentity.KeylessChild(source, root, "child")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := runtimeflowidentity.KeylessChild(source, child, "child/grandchild")
	if err != nil {
		t.Fatal(err)
	}

	instance := WorkflowInstance{
		WorkflowName: grandchild.TemplateID, StorageRef: grandchild.InstancePath,
		InstanceID: grandchild.InstanceID, EntityID: grandchild.EntityID, EntityType: "test_entity",
		ParentFlowID: grandchild.ParentRoute.FlowID, ParentFlowInstance: grandchild.ParentRoute.FlowInstance, ParentEntityID: grandchild.ParentEntityID,
	}

	if workflowInstanceOwnedByFlow(source, instance, "child", testPipelineRunID) {
		t.Fatal("did not expect child to own child/grandchild/inst-1")
	}
	if !workflowInstanceOwnedByFlow(source, instance, "child/grandchild", testPipelineRunID) {
		t.Fatal("expected grandchild to own child/grandchild/inst-1")
	}
}

func TestWorkflowInstanceOwnedByFlowPreservesConstructedParent(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleNestedTemplates(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	parent := runtimeflowidentity.Derive(source, "outer/left/sink", "same-revision")
	child, err := runtimeflowidentity.KeylessChild(source, parent, parent.TemplateID+"/final")
	if err != nil {
		t.Fatal(err)
	}
	valid := WorkflowInstance{
		WorkflowName: child.TemplateID, InstanceID: child.InstanceID, StorageRef: child.InstancePath, EntityID: child.EntityID,
		ParentFlowID: child.ParentRoute.FlowID, ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*WorkflowInstance)
		valid  bool
	}{
		{name: "exact", valid: true},
		{name: "missing_parent", mutate: func(i *WorkflowInstance) { i.ParentFlowInstance = "" }},
		{name: "crossed_parent", mutate: func(i *WorkflowInstance) {
			other := runtimeflowidentity.Derive(source, parent.TemplateID, "other-revision")
			i.ParentFlowInstance, i.ParentEntityID = other.InstancePath, other.EntityID
		}},
		{name: "foreign_parent_flow", mutate: func(i *WorkflowInstance) { i.ParentFlowID = "outer/right/sink" }},
		{name: "missing_parent_entity", mutate: func(i *WorkflowInstance) { i.ParentEntityID = "" }},
		{name: "stored_parent_entity", mutate: func(i *WorkflowInstance) { i.ParentEntityID = "33333333-3333-4333-8333-333333333333" }, valid: true},
		{name: "reconstructed_absolute_path", mutate: func(i *WorkflowInstance) {
			i.StorageRef = child.ScopeKey
			i.EntityID = runtimeflowidentity.EntityID(i.StorageRef)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance := valid
			if tc.mutate != nil {
				tc.mutate(&instance)
			}
			if got := workflowInstanceOwnedByFlow(source, instance, child.TemplateID, testPipelineRunID); got != tc.valid {
				t.Fatalf("construction admission=%v want=%v: %+v", got, tc.valid, instance)
			}
			if workflowInstanceOwnedByFlow(source, instance, parent.TemplateID, testPipelineRunID) {
				t.Fatal("parent context granted parent execution ownership")
			}
		})
	}
}

func TestRequireWorkflowInstanceIdentityRejectsMissingAndMismatchedFacts(t *testing.T) {
	route := runtimeflowidentity.RouteForInstancePath("review/instance-1")
	entityID := identity.NormalizeEntityID("11111111-1111-4111-8111-111111111111")
	valid := WorkflowInstance{
		StorageRef: "review/instance-1",
		InstanceID: "instance-1",
		EntityID:   entityID.String(),
		EntityType: "test_entity",
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, valid); err != nil {
		t.Fatalf("exact identity rejected: %v", err)
	}

	missing := valid
	missing.EntityID = ""
	if _, err := requireWorkflowInstanceIdentity(route, entityID, missing); err == nil || !strings.Contains(err.Error(), "missing entity_id") {
		t.Fatalf("missing entity error = %v", err)
	}

	mismatch := valid
	mismatch.EntityID = "22222222-2222-4222-8222-222222222222"
	if _, err := requireWorkflowInstanceIdentity(route, entityID, mismatch); err == nil || !strings.Contains(err.Error(), "disagrees with requested entity") {
		t.Fatalf("mismatched entity error = %v", err)
	}

	wrongRoute := valid
	wrongRoute.StorageRef = "review/instance-2"
	if _, err := requireWorkflowInstanceIdentity(route, entityID, wrongRoute); err == nil || !strings.Contains(err.Error(), "disagrees") {
		t.Fatalf("mismatched route error = %v", err)
	}
}

func TestWorkflowConstructionSourceIdentityRejectsNormalizedImpostors(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	root := runtimeflowidentity.Stored(source, ".", testPipelineRunID, testPipelineRunID, runtimeflowidentity.EntityID(testPipelineRunID), "")
	child, err := runtimeflowidentity.KeylessChild(source, root, "child")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(testPipelineRunID, child.Route())
	if err != nil {
		t.Fatal(err)
	}
	valid := WorkflowInstance{WorkflowName: child.TemplateID, StorageRef: child.InstancePath, InstanceID: child.InstanceID, EntityID: child.EntityID,
		ParentFlowID: child.ParentRoute.FlowID, ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID}
	if actual, err := valid.ConstructionIdentity(owner); err != nil || actual != child {
		t.Fatalf("exact construction source header: actual=%#v err=%v", actual, err)
	}
	for _, field := range []string{"workflow", "storage", "instance", "entity", "parent_flow", "parent_instance", "parent_entity"} {
		t.Run(field, func(t *testing.T) {
			bad := valid
			switch field {
			case "workflow":
				bad.WorkflowName += " "
			case "storage":
				bad.StorageRef += " "
			case "instance":
				bad.InstanceID += " "
			case "entity":
				bad.EntityID += " "
			case "parent_flow":
				bad.ParentFlowID += " "
			case "parent_instance":
				bad.ParentFlowInstance += " "
			case "parent_entity":
				bad.ParentEntityID += " "
			}
			if _, err := bad.ConstructionIdentity(owner); err == nil {
				t.Fatal("malformed source header was normalized into authority")
			}
		})
	}
}

func mustEvent(eventType, entityID string) Event {
	return eventtest.RunCreatingRootIngress("", events.EventType(eventType), "", "", nil, 0, "", "", events.EventEnvelope{EntityID: entityID}, time.Time{})
}
