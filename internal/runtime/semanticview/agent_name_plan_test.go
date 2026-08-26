package semanticview

import (
	"os"
	"path/filepath"
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
	if got := byFlow["child"]; got.LocalID != "worker" || got.AgentID != "public-worker" || got.OwnerURI != "test://agent-name/child/worker" {
		t.Fatalf("child plan = %#v", got)
	}
	parent, err := byFlow["parent"].Materialize()
	if err != nil {
		t.Fatal(err)
	}
	child, err := byFlow["child"].Materialize()
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
	localID, _, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "public-worker", FlowID: "child"})
	if !ok || localID != "worker" {
		t.Fatalf("effective-name lookup = local %q ok %v, want child worker declaration", localID, ok)
	}
	if _, _, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "worker", FlowID: "child"}); ok {
		t.Fatal("explicitly overridden child declaration retained its local map key as a public-name alias")
	}
}

func TestResolveAgentRegistryEntryPreservesScopeForSharedEffectiveName(t *testing.T) {
	source := agentNamePlanNestedSource(false)
	bundle, _ := Bundle(source)
	child := bundle.FlowTree.ByID["child"]
	entry := child.Agents["worker"]
	entry.ID = ""
	child.Agents["worker"] = entry

	localID, got, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{ID: "worker", FlowID: "child"})
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
	child, ok := ResolveAgentContractProjection(source, models.AgentConfig{ID: "public-worker", FlowID: "child"})
	if !ok || child.OwnerFlowID != "child" || child.ContractSource.FlowID != "child" {
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

func TestProjectAgentNamePlanPreservesExactScopeForSharedFlowAndLocalID(t *testing.T) {
	source, projects := loadSameFlowSiblingAgentPackages(t)
	firstScope := projects["flows/support/first"]
	secondScope := projects["flows/support/second"]

	first, err := ProjectAgentNamePlan(source, firstScope, "worker")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ProjectAgentNamePlan(source, secondScope, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if first.ScopeID != "flows/support/first" || first.AgentID != "first-worker" || first.OwnerURI != firstScope.AgentURIs["worker"] {
		t.Fatalf("first plan = %#v", first)
	}
	if second.ScopeID != "flows/support/second" || second.AgentID != "second-worker" || second.OwnerURI != secondScope.AgentURIs["worker"] {
		t.Fatalf("second plan = %#v", second)
	}
}

func TestResolveAgentDeclarationUsesConcreteOwnerForSameFlowProjectScopes(t *testing.T) {
	source, projects := loadSameFlowSiblingAgentPackages(t)
	for _, scopeKey := range []string{"flows/support/first", "flows/support/second"} {
		scope := projects[scopeKey]
		plan, err := ProjectAgentNamePlan(source, scope, "worker")
		if err != nil {
			t.Fatal(err)
		}
		name, err := plan.Materialize()
		if err != nil {
			t.Fatal(err)
		}
		actor := models.AgentConfig{ID: plan.AgentID, FlowID: "support"}
		actor.Identity.Name = name
		declaration, ok := ResolveAgentDeclaration(source, actor)
		prefix := strings.TrimSuffix(strings.TrimPrefix(scopeKey, "flows/support/"), "/")
		if !ok || declaration.ScopeID != scope.Key || declaration.Entry.Role != prefix+"-role" {
			t.Fatalf("scope %q declaration = %#v ok %v", scope.Key, declaration, ok)
		}
		projection, ok := ResolveAgentContractProjection(source, actor)
		if !ok || projection.Declaration.ScopeID != scope.Key || projection.ContractSource.PackageKey != scope.Key {
			t.Fatalf("scope %q projection = %#v ok %v", scope.Key, projection, ok)
		}
	}

	firstScope := projects["flows/support/first"]
	firstPlan, err := ProjectAgentNamePlan(source, firstScope, "worker")
	if err != nil {
		t.Fatal(err)
	}
	firstName, err := firstPlan.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	wrongID := models.AgentConfig{ID: "other", FlowID: "support"}
	wrongID.Identity.Name = firstName
	if _, ok := ResolveAgentDeclaration(source, wrongID); ok {
		t.Fatal("declared identity with a mismatched public ID fell back to another declaration")
	}
	wrongFlow := models.AgentConfig{ID: "worker", FlowID: "other"}
	wrongFlow.Identity.Name = firstName
	if _, ok := ResolveAgentDeclaration(source, wrongFlow); ok {
		t.Fatal("declared identity with a mismatched owner flow bypassed scope validation")
	}
	scopeKeyFlow := models.AgentConfig{ID: firstPlan.AgentID, FlowID: firstScope.Key}
	scopeKeyFlow.Identity.Name = firstName
	if _, ok := ResolveAgentDeclaration(source, scopeKeyFlow); ok {
		t.Fatal("project scope key was accepted as the declaration owner flow")
	}
	if _, _, ok := ResolveAgentRegistryEntry(source, scopeKeyFlow); ok {
		t.Fatal("project scope key inherited a declaration registry entry across flows")
	}
	if _, ok := ResolveAgentContractProjection(source, scopeKeyFlow); ok {
		t.Fatal("project scope key inherited a declaration contract across flows")
	}
	if _, ok := ResolveAgentDeclaration(source, models.AgentConfig{ID: "worker", FlowID: firstScope.Key}); ok {
		t.Fatal("project scope key cross-bound the ID-only declaration lookup")
	}
	if _, ok := ResolveAgentDeclaration(source, models.AgentConfig{Role: "first-role", FlowID: firstScope.Key}); ok {
		t.Fatal("project scope key cross-bound the role-only declaration lookup")
	}
	firstName.Owner = "test://agent-name/packages/missing/worker"
	unknownOwner := models.AgentConfig{ID: firstPlan.AgentID, FlowID: "support"}
	unknownOwner.Identity.Name = firstName
	if _, ok := ResolveAgentDeclaration(source, unknownOwner); ok {
		t.Fatal("unknown declared owner fell back to a public ID")
	}
	if _, _, ok := ResolveAgentRegistryEntry(source, unknownOwner); ok {
		t.Fatal("unknown declared owner inherited a registry entry")
	}
	if _, ok := ResolveAgentContractProjection(source, unknownOwner); ok {
		t.Fatal("unknown declared owner inherited a scoped contract")
	}
}

func loadSameFlowSiblingAgentPackages(t *testing.T) (Source, map[string]ProjectScope) {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: same-flow-sibling-agents
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: support, flow: support, mode: static}
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: same-flow-sibling-agents\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "root-worker:\n  role: root-role\n  intent: {inline: Exercise root ownership.}\n  model: regular\n  memory: false\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
packages:
  - {path: first}
  - {path: second}
flows: []
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), "name: support\nmode: static\ninitial_state: active\nstates: [active]\n")
	for _, prefix := range []string{"first", "second"} {
		packageRoot := filepath.Join(root, "flows", "support", prefix)
		writeSemanticviewFixtureFile(t, filepath.Join(packageRoot, "package.yaml"), "name: "+prefix+"\nversion: \"1.0.0\"\nflows: []\n")
		writeSemanticviewFixtureFile(t, filepath.Join(packageRoot, "agents.yaml"), "worker:\n  id: "+prefix+"-worker\n  role: "+prefix+"-role\n  intent: {inline: Exercise exact "+prefix+" ownership.}\n  model: regular\n  memory: false\n")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)
	projects := map[string]ProjectScope{}
	for _, scope := range source.ProjectScopes() {
		projects[scope.Key] = scope
	}
	for _, key := range []string{"flows/support/first", "flows/support/second"} {
		if _, ok := projects[key]; !ok {
			t.Fatalf("missing loaded project scope %q: %#v", key, source.ProjectScopes())
		}
	}
	return source, projects
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
			ID: "child", Flow: "child",
		},
		Agents: childAgents,
		Tools:  map[string]runtimecontracts.ToolSchemaEntry{"shared-tool": tool("child scoped tool")},
	}
	parent := runtimecontracts.FlowContractView{
		Path: "parent",
		Paths: runtimecontracts.FlowContractPaths{
			ID: "parent", Flow: "parent",
		},
		Agents:   parentAgents,
		Tools:    map[string]runtimecontracts.ToolSchemaEntry{"shared-tool": tool("parent scoped tool")},
		Children: []runtimecontracts.FlowContractView{child},
	}
	refs := map[string]runtimecontracts.ContractURIRef{
		"parent/worker": {Kind: "agent", FlowID: "parent", LocalID: "worker", Full: "test://agent-name/parent/worker"},
		"child/worker":  {Kind: "agent", FlowID: "child", LocalID: "worker", Full: "test://agent-name/child/worker"},
	}
	if collision {
		refs["child/alias"] = runtimecontracts.ContractURIRef{Kind: "agent", FlowID: "child", LocalID: "alias", Full: "test://agent-name/child/alias"}
	}
	byURI := map[string]runtimecontracts.ContractURIRef{}
	for _, ref := range refs {
		byURI[ref.Full] = ref
	}
	return Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{parent}},
			ByID: map[string]*runtimecontracts.FlowContractView{"parent": &parent, "child": &child},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{Agents: refs, ByURI: byURI},
	})
}
