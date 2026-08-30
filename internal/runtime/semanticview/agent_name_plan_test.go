package semanticview

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestAgentNamePlansPreserveScopedCoordinateAndEffectivePublicName(t *testing.T) {
	source := agentNamePlanNestedSource(false)
	plans, err := AgentNamePlans(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 {
		t.Fatalf("plans = %#v, want two nested declarations", plans)
	}
	byFlow := map[string]AgentNamePlan{}
	for _, plan := range plans {
		byFlow[plan.OwnerFlowID] = plan
	}
	if got := byFlow["parent"]; got.LocalID != "worker" || got.AgentID != "worker" || got.OwnerURI != "test://agent-name/parent/worker" {
		t.Fatalf("parent plan = %#v", got)
	}
	if got := byFlow["parent/child"]; got.LocalID != "worker" || got.AgentID != "public-worker" || got.OwnerURI != "test://agent-name/child/worker" {
		t.Fatalf("child plan = %#v", got)
	}
	parent, err := byFlow["parent"].Materialize()
	if err != nil {
		t.Fatal(err)
	}
	child, err := byFlow["parent/child"].Materialize()
	if err != nil {
		t.Fatal(err)
	}
	if parent.Owner == child.Owner || parent.AgentID == child.AgentID {
		t.Fatalf("materialized names = parent %#v child %#v, want distinct owner and effective name", parent, child)
	}
}

func TestAgentNamePlansRejectMissingOwnerAndSameScopeCollision(t *testing.T) {
	missingOwner := agentNamePlanNestedSource(false)
	bundle, _ := Bundle(missingOwner)
	bundle.URIRegistry = runtimecontracts.ContractURIRegistry{}
	if _, err := AgentNamePlans(missingOwner); err == nil || !strings.Contains(err.Error(), "missing a unique scoped owner") {
		t.Fatalf("missing-owner error = %v", err)
	}

	collision := agentNamePlanNestedSource(true)
	if _, err := AgentNamePlans(collision); err == nil || !strings.Contains(err.Error(), "derive the same effective public name") {
		t.Fatalf("collision error = %v", err)
	}
}

func TestResolveAgentRegistryEntryUsesEffectiveNameWithoutLocalAlias(t *testing.T) {
	source := agentNamePlanNestedSource(false)
	localID, _, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "public-worker", FlowID: "parent/child"})
	if !ok || localID != "worker" {
		t.Fatalf("effective-name lookup = local %q ok %v, want child worker declaration", localID, ok)
	}
	if _, _, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "worker", FlowID: "parent/child"}); ok {
		t.Fatal("explicitly overridden child declaration retained its local map key as a public-name alias")
	}
}

func TestResolveAgentRegistryEntryPreservesScopeForSharedEffectiveName(t *testing.T) {
	source := agentNamePlanNestedSource(false)
	bundle, _ := Bundle(source)
	child := bundle.FlowTree.ByID["parent/child"]
	entry := child.Agents["worker"]
	entry.ID = ""
	child.Agents["worker"] = entry

	localID, got, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "worker", FlowID: "parent/child"})
	if !ok || localID != "worker" || got.Role != "child-worker" {
		t.Fatalf("child lookup = local %q entry %#v ok %v, want child-scoped worker", localID, got, ok)
	}
	localID, got, ok = ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "worker", FlowID: "parent"})
	if !ok || localID != "worker" || got.Role != "parent-worker" {
		t.Fatalf("parent lookup = local %q entry %#v ok %v, want parent-scoped worker", localID, got, ok)
	}
	if _, _, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "worker"}); ok {
		t.Fatal("scope-free lookup accepted a public name owned by two declarations")
	}
}

func TestAgentContractProjectionPreservesScopedToolForLiteralPublicName(t *testing.T) {
	source := agentNamePlanNestedSource(false)
	child, ok := ResolveAgentContractProjection(source, models.AgentConfig{ID: "public-worker", FlowID: "parent/child"})
	if !ok || child.OwnerFlowID != "parent/child" || child.ContractSource.FlowPath != "parent/child" {
		t.Fatalf("child projection = %#v ok %v", child, ok)
	}
	tool, ok := child.ToolEntry("shared-tool")
	if !ok || tool.Description() != "child scoped tool" {
		t.Fatalf("child scoped tool = %#v ok %v", tool, ok)
	}
	parent, ok := ResolveAgentContractProjection(source, models.AgentConfig{ID: "worker", FlowID: "parent"})
	if !ok {
		t.Fatal("parent projection did not resolve")
	}
	tool, ok = parent.ToolEntry("shared-tool")
	if !ok || tool.Description() != "parent scoped tool" {
		t.Fatalf("parent scoped tool = %#v ok %v", tool, ok)
	}
}

func agentNamePlanNestedSource(collision bool) Source {
	tool := func(description string) runtimecontracts.ToolSchemaEntry {
		return runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolDescription(description),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin),
			runtimecontracts.WithToolSchemas(
				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			),
		)
	}
	parentAgents := map[string]runtimecontracts.AgentRegistryEntry{
		"worker": {Role: "parent-worker"},
	}
	childAgents := map[string]runtimecontracts.AgentRegistryEntry{
		"worker": {ID: "public-worker", Role: "child-worker"},
	}
	if collision {
		childAgents["alias"] = runtimecontracts.AgentRegistryEntry{ID: "public-worker", Role: "alias"}
	}
	child := runtimecontracts.FlowContractView{
		Path: "parent/child",
		Paths: runtimecontracts.FlowContractPaths{
			FlowPath: "parent/child",
		},
		Agents: childAgents,
		Tools:  map[string]runtimecontracts.ToolSchemaEntry{"shared-tool": tool("child scoped tool")},
	}
	parent := runtimecontracts.FlowContractView{
		Path: "parent",
		Paths: runtimecontracts.FlowContractPaths{
			FlowPath: "parent",
		},
		Agents:   parentAgents,
		Tools:    map[string]runtimecontracts.ToolSchemaEntry{"shared-tool": tool("parent scoped tool")},
		Children: []runtimecontracts.FlowContractView{child},
	}
	refs := map[string]runtimecontracts.ContractURIRef{
		"parent/worker": {Kind: "agent", FlowID: "parent", LocalID: "worker", Full: "test://agent-name/parent/worker"},
		"child/worker":  {Kind: "agent", FlowID: "parent/child", LocalID: "worker", Full: "test://agent-name/child/worker"},
	}
	if collision {
		refs["child/alias"] = runtimecontracts.ContractURIRef{Kind: "agent", FlowID: "parent/child", LocalID: "alias", Full: "test://agent-name/child/alias"}
	}
	byURI := map[string]runtimecontracts.ContractURIRef{}
	for _, ref := range refs {
		byURI[ref.Full] = ref
	}
	return Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{parent}},
			ByID: map[string]*runtimecontracts.FlowContractView{"parent": &parent, "parent/child": &child},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{Agents: refs, ByURI: byURI},
	})
}
