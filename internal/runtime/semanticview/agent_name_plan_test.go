package semanticview

import (
	"slices"
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
	firstOwner := "test://agent-name/packages/first/worker"
	secondOwner := "test://agent-name/packages/second/worker"
	refs := map[string]runtimecontracts.ContractURIRef{
		firstOwner:  {Kind: "agent", LocalID: "worker", Full: firstOwner},
		secondOwner: {Kind: "agent", LocalID: "worker", Full: secondOwner},
	}
	source := agentNameProjectScopeSource{
		Source: Wrap(&runtimecontracts.WorkflowContractBundle{URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: refs}}),
		projects: []ProjectScope{
			{Key: "packages/first", OwningFlowID: "support", Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "first-worker"}}, AgentURIs: map[string]string{"worker": firstOwner}},
			{Key: "packages/second", OwningFlowID: "support", Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "second-worker"}}, AgentURIs: map[string]string{"worker": secondOwner}},
		},
	}

	first, err := ProjectAgentNamePlan(source, source.projects[0], "worker")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ProjectAgentNamePlan(source, source.projects[1], "worker")
	if err != nil {
		t.Fatal(err)
	}
	if first.ScopeID != "packages/first" || first.AgentID != "first-worker" || first.OwnerURI != firstOwner {
		t.Fatalf("first plan = %#v", first)
	}
	if second.ScopeID != "packages/second" || second.AgentID != "second-worker" || second.OwnerURI != secondOwner {
		t.Fatalf("second plan = %#v", second)
	}
}

func TestResolveAgentDeclarationUsesConcreteOwnerForSameFlowProjectScopes(t *testing.T) {
	firstOwner := "test://agent-name/packages/first/worker"
	secondOwner := "test://agent-name/packages/second/worker"
	refs := map[string]runtimecontracts.ContractURIRef{
		firstOwner:  {Kind: "agent", LocalID: "worker", Full: firstOwner},
		secondOwner: {Kind: "agent", LocalID: "worker", Full: secondOwner},
	}
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
	source := agentNameProjectScopeSource{
		Source: Wrap(&runtimecontracts.WorkflowContractBundle{URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: refs}}),
		projects: []ProjectScope{
			{
				Key: "packages/first", OwningFlowID: "support",
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"worker": {
						Role:           "first-role",
						Permissions:    []string{"first-permission"},
						Criteria:       []string{"first-criterion"},
						FlowDataAccess: []string{"first.json"},
						EntityWrites: map[string]runtimecontracts.AgentEntityWriteDecl{
							"case": {Save: runtimecontracts.AgentEntityWriteRule{Fields: []string{"first-field"}}},
						},
					},
				},
				AgentURIs: map[string]string{"worker": firstOwner},
				Tools:     map[string]runtimecontracts.ToolSchemaEntry{"shared-tool": tool("first scoped tool")},
			},
			{
				Key: "packages/second", OwningFlowID: "support",
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"worker": {
						Role:           "second-role",
						Permissions:    []string{"second-permission"},
						Criteria:       []string{"second-criterion"},
						FlowDataAccess: []string{"second.json"},
						EntityWrites: map[string]runtimecontracts.AgentEntityWriteDecl{
							"case": {Save: runtimecontracts.AgentEntityWriteRule{Fields: []string{"second-field"}}},
						},
					},
				},
				AgentURIs: map[string]string{"worker": secondOwner},
				Tools:     map[string]runtimecontracts.ToolSchemaEntry{"shared-tool": tool("second scoped tool")},
			},
		},
	}

	prefixes := []string{"first", "second"}
	toolDescriptions := []string{"first scoped tool", "second scoped tool"}
	for index, scope := range source.projects {
		plan, err := ProjectAgentNamePlan(source, scope, "worker")
		if err != nil {
			t.Fatal(err)
		}
		name, err := plan.Materialize()
		if err != nil {
			t.Fatal(err)
		}
		actor := models.AgentConfig{ID: "worker", FlowID: "support"}
		actor.Identity.Name = name
		declaration, ok := ResolveAgentDeclaration(source, actor)
		prefix := prefixes[index]
		if !ok || declaration.ScopeID != scope.Key || declaration.Entry.Role != prefix+"-role" ||
			!slices.Equal(declaration.Entry.Permissions, []string{prefix + "-permission"}) ||
			!slices.Equal(declaration.Entry.Criteria, []string{prefix + "-criterion"}) ||
			!slices.Equal(declaration.Entry.FlowDataAccess, []string{prefix + ".json"}) ||
			!slices.Equal(declaration.Entry.EntityWrites["case"].Save.Fields, []string{prefix + "-field"}) {
			t.Fatalf("scope %q declaration = %#v ok %v", scope.Key, declaration, ok)
		}
		projection, ok := ResolveAgentContractProjection(source, actor)
		if !ok || projection.Declaration.ScopeID != scope.Key {
			t.Fatalf("scope %q projection = %#v ok %v", scope.Key, projection, ok)
		}
		entry, ok := projection.ToolEntry("shared-tool")
		if !ok || entry.Description() != toolDescriptions[index] {
			t.Fatalf("scope %q tool = %#v ok %v", scope.Key, entry, ok)
		}
	}

	firstPlan, err := ProjectAgentNamePlan(source, source.projects[0], "worker")
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
	scopeKeyFlow := models.AgentConfig{ID: "worker", FlowID: source.projects[0].Key}
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
	if _, ok := ResolveAgentDeclaration(source, models.AgentConfig{ID: "worker", FlowID: source.projects[0].Key}); ok {
		t.Fatal("project scope key cross-bound the ID-only declaration lookup")
	}
	if _, ok := ResolveAgentDeclaration(source, models.AgentConfig{Role: "first-role", FlowID: source.projects[0].Key}); ok {
		t.Fatal("project scope key cross-bound the role-only declaration lookup")
	}
	firstName.Owner = "test://agent-name/packages/missing/worker"
	unknownOwner := models.AgentConfig{ID: "worker", FlowID: "support"}
	unknownOwner.Identity.Name = firstName
	if _, ok := ResolveAgentDeclaration(source, unknownOwner); ok {
		t.Fatal("unknown declared owner disambiguated otherwise ambiguous flow/name selection")
	}
	source.projects = source.projects[:1]
	if _, ok := ResolveAgentDeclaration(source, unknownOwner); ok {
		t.Fatal("unknown declared owner fell back to the unique public ID")
	}
	if _, _, ok := ResolveAgentRegistryEntry(source, unknownOwner); ok {
		t.Fatal("unknown declared owner inherited the unique registry entry")
	}
	if _, ok := ResolveAgentContractProjection(source, unknownOwner); ok {
		t.Fatal("unknown declared owner inherited the unique scoped contract")
	}
	entry := source.projects[0].Agents["worker"]
	entry.ID = "public-worker"
	entry.Role = ""
	source.projects[0].Agents["worker"] = entry
	if !retiredLocalAgentAlias(source, "support", "worker") {
		t.Fatal("literal public name did not retire its local alias in the owner flow")
	}
	if retiredLocalAgentAlias(source, source.projects[0].Key, "worker") {
		t.Fatal("project scope key was treated as the owner flow while retiring a local alias")
	}
	runtimeRoleActor := models.AgentConfig{ID: "runtime-worker", Role: "public-worker", FlowID: "support"}
	declaration, ok := ResolveAgentDeclaration(source, runtimeRoleActor)
	if !ok || declaration.ScopeID != source.projects[0].Key {
		t.Fatalf("effective-role fallback = %#v ok %v, want exact project declaration", declaration, ok)
	}
	if _, ok := ResolveAgentContractProjection(source, runtimeRoleActor); !ok {
		t.Fatal("effective-role fallback withheld the declaration contract")
	}
	runtimeRoleActor.FlowID = source.projects[0].Key
	if _, ok := ResolveAgentDeclaration(source, runtimeRoleActor); ok {
		t.Fatal("project scope key cross-bound the effective-role fallback")
	}
}

type agentNameProjectScopeSource struct {
	Source
	projects []ProjectScope
}

func (s agentNameProjectScopeSource) ProjectScopes() []ProjectScope {
	return append([]ProjectScope(nil), s.projects...)
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
