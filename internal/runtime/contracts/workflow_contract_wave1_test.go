package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"gopkg.in/yaml.v3"
)

func TestWorkflowContractBundleNodeContractSourceUsesCanonicalRootNodeTable(t *testing.T) {
	bundle := &WorkflowContractBundle{Nodes: map[string]SystemNodeContract{
		"root-node": {ID: "root-node"},
	}}

	record, ok := bundle.ExecutableNode(identitytest.RootNode(t, "root-node"))
	if !ok {
		t.Fatal("canonical root node did not have contract source")
	}
	source := record.Source
	if source.FlowPath != "." || source.Family != "nodes" {
		t.Fatalf("root node source = %#v, want root flow nodes owner", source)
	}
}

func TestWorkflowContractBundleScopedNodeRecordsPreserveExportedTreeScopes(t *testing.T) {
	joinHandler := func(stage string) SystemNodeEventHandler {
		return SystemNodeEventHandler{Join: &JoinSpec{
			Stage:        stage,
			Members:      JoinMembersSpec{From: "payload.members", By: "payload.member_id"},
			Output:       "payload.result",
			OnComplete:   HandlerRuleEntry{AdvancesTo: "done"},
			Timeout:      JoinTimeoutSpec{After: "1h", Outcome: HandlerRuleEntry{AdvancesTo: "failed"}},
			CompleteWhen: "size(join.members) > 0",
		}}
	}
	root := FlowContractView{
		Paths: FlowContractPaths{FlowPath: ".", NodesFile: "nodes.yaml"},
		Path:  ".",
		Nodes: map[string]SystemNodeContract{
			"root-node": {
				EventHandlers: map[string]SystemNodeEventHandler{"root.received": joinHandler("root-active")},
				Timers:        []WorkflowTimerContract{{ID: "root-timer", Event: "root.tick"}},
			},
		},
		Children: []FlowContractView{
			{
				Paths: FlowContractPaths{FlowPath: "b", NodesFile: "b/nodes.yaml"},
				Path:  "b",
				Nodes: map[string]SystemNodeContract{
					"shared": {
						EventHandlers: map[string]SystemNodeEventHandler{"item.received": joinHandler("b-active")},
						Timers:        []WorkflowTimerContract{{ID: "b-timer", Event: "b.tick"}},
					},
				},
				Children: []FlowContractView{
					{
						Paths: FlowContractPaths{FlowPath: "b/child", NodesFile: "b/child/nodes.yaml"},
						Path:  "b/child",
						Nodes: map[string]SystemNodeContract{
							"shared": {
								EventHandlers: map[string]SystemNodeEventHandler{"item.received": joinHandler("b-child-active")},
								Timers:        []WorkflowTimerContract{{ID: "b-child-timer", Event: "b.child.tick"}},
							},
						},
					},
				},
			},
			{
				Paths: FlowContractPaths{FlowPath: "a", NodesFile: "a/nodes.yaml"},
				Path:  "a",
				Nodes: map[string]SystemNodeContract{
					"shared": {
						EventHandlers: map[string]SystemNodeEventHandler{"item.received": joinHandler("a-active")},
						Timers:        []WorkflowTimerContract{{ID: "a-timer", Event: "a.tick"}},
					},
				},
			},
		},
	}
	bundle := &WorkflowContractBundle{
		FlowTree: FlowTree{Root: &root},
	}

	records := bundle.ScopedNodeRecords()
	if len(records) != 4 {
		t.Fatalf("ScopedNodeRecords() = %#v, want root and three child flow records", records)
	}
	want := []struct {
		logicalID string
		flowPath  string
		file      string
	}{
		{logicalID: "root-node", flowPath: ".", file: "nodes.yaml"},
		{logicalID: "shared", flowPath: "a", file: "a/nodes.yaml"},
		{logicalID: "shared", flowPath: "b", file: "b/nodes.yaml"},
		{logicalID: "shared", flowPath: "b/child", file: "b/child/nodes.yaml"},
	}
	for index, expected := range want {
		got := records[index]
		if got.LogicalID != expected.logicalID || got.Source.FlowPath != expected.flowPath || got.Source.Family != "nodes" || got.Source.File != expected.file {
			t.Fatalf("ScopedNodeRecords()[%d] = %#v, want logical=%q flow_path=%q file=%q", index, got, expected.logicalID, expected.flowPath, expected.file)
		}
	}

	populateWorkflowSemantics(bundle)
	if len(bundle.Semantics.Joins) != 4 {
		t.Fatalf("exported-tree joins = %#v, want all four scoped handlers", bundle.Semantics.Joins)
	}
	if len(bundle.Semantics.Timers) != 4 {
		t.Fatalf("exported-tree timers = %#v, want all four scoped nodes", bundle.Semantics.Timers)
	}
	seenFlow := map[string]bool{}
	for _, join := range bundle.Semantics.Joins {
		seenFlow[join.Node.FlowPath()] = true
	}
	for _, flowPath := range []string{".", "a", "b", "b/child"} {
		if !seenFlow[flowPath] {
			t.Fatalf("exported-tree joins = %#v, missing flow %q", bundle.Semantics.Joins, flowPath)
		}
	}
	wantNodes := map[string]bool{}
	wantTimers := map[string]bool{}
	for _, record := range records {
		node, err := record.Identity()
		if err != nil {
			t.Fatal(err)
		}
		wantNodes[node.Key()] = false
		wantTimers[node.Key()] = false
	}
	for _, join := range bundle.Semantics.Joins {
		if _, ok := wantNodes[join.Node.Key()]; ok {
			wantNodes[join.Node.Key()] = true
		}
	}
	for _, timer := range bundle.Semantics.Timers {
		if timer.StageOwned {
			continue
		}
		if _, ok := wantTimers[timer.Node.Key()]; !ok {
			t.Fatalf("timer node = %q, want one of %#v", timer.Node.Key(), wantTimers)
		}
		wantTimers[timer.Node.Key()] = true
	}
	for key, found := range wantNodes {
		if !found {
			t.Fatalf("exported-tree joins missing exact node %q: %#v", key, bundle.Semantics.Joins)
		}
		if !wantTimers[key] {
			t.Fatalf("exported-tree timers missing exact node %q: %#v", key, bundle.Semantics.Timers)
		}
	}
}

func TestWorkflowContractBundleScopedNodeRecordsUseLoadedDeclarationMapKey(t *testing.T) {
	source := ContractItemSource{FlowPath: "flow", Family: "nodes", File: "flow/nodes.yaml"}
	bundle := &WorkflowContractBundle{
		scopedNodes: map[string]SystemNodeContract{
			contractScopeKey(source, "declared-node"): {ID: "non-authoritative-embedded-id"},
		},
		scopedNodeSources: map[string]ContractItemSource{
			contractScopeKey(source, "declared-node"): source,
		},
	}

	records := bundle.ScopedNodeRecords()
	if len(records) != 1 || records[0].LogicalID != "declared-node" || records[0].Source != source {
		t.Fatalf("ScopedNodeRecords() = %#v, want loaded declaration map key and exact source", records)
	}
}

func TestLoadWorkflowContractBundle_LoadsWave1TypeAndEntityDocuments(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/schema.yaml", "name: wave1-bundle\n")
	writeFixtureFile(t, root+"/types.yaml", `
scalars:
  URL: text
types:
  Brand:
    name: text
`)
	writeFixtureFile(t, root+"/events.yaml", `
root.ready:
  _note: root event
  entity_id: uuid
`)
	writeFixtureFile(t, root+"/scoring/schema.yaml", `
name: scoring
mode: static
initial_state: discovered
states: [discovered, shortlisted]
terminal_states: [shortlisted]
pins:
  inputs:
    events: [root.ready]
  outputs:
    events: [vertical.shortlisted]
`)
	writeFixtureFile(t, root+"/scoring/types.yaml", `
types:
  ScoreBreakdown:
    total: numeric
`)
	writeFixtureFile(t, root+"/scoring/entities.yaml", `
vertical:
  _description: scoring vertical
  name: text
  review_count:
    type: integer
    indexed: true
    initial: 0
`)
	writeFixtureFile(t, root+"/scoring/events.yaml", `
vertical.shortlisted:
  _note: shortlist event
  vertical_name: text
  composite_score: numeric
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	rootTypes := bundle.RootTypeCatalog()
	if got := rootTypes.Scalars["URL"].Base; got != "text" {
		t.Fatalf("RootTypeCatalog().Scalars[URL] = %q", got)
	}
	if _, ok := rootTypes.Types["Brand"]; !ok {
		t.Fatal("expected root Brand type")
	}
	flowTypes, ok := bundle.FlowTypeCatalogByID("scoring")
	if !ok {
		t.Fatal("expected scoring flow types")
	}
	if _, ok := flowTypes.Types["ScoreBreakdown"]; !ok {
		t.Fatal("expected scoring flow-local type")
	}
	entityType, entity, ok := bundle.FlowPrimaryEntityContract("scoring")
	if !ok {
		t.Fatal("expected scoring primary entity contract")
	}
	if entityType != "vertical" {
		t.Fatalf("FlowPrimaryEntityContract entity type = %q", entityType)
	}
	if got := entity.Fields["review_count"].Type; got != "integer" {
		t.Fatalf("review_count type = %q", got)
	}
	if !entity.Fields["review_count"].Indexed {
		t.Fatal("review_count Indexed = false, want true")
	}
	resolvedTypes := bundle.ResolvedTypeCatalogForFlow("scoring")
	if _, ok := resolvedTypes.Scalars["URL"]; !ok {
		t.Fatal("expected resolved flow type catalog to include root scalar")
	}
	if _, ok := resolvedTypes.Types["ScoreBreakdown"]; !ok {
		t.Fatal("expected resolved flow type catalog to include flow-local type")
	}
}

func TestMergeAgentContractsRejectsDuplicateScopedAgentID(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Agents:                map[string]AgentRegistryEntry{},
		agentSources:          map[string]ContractItemSource{},
		scopedAgents:          map[string]AgentRegistryEntry{},
		scopedAgentSources:    map[string]ContractItemSource{},
		ambiguousAgentAliases: map[string]struct{}{},
	}
	sourceA := ContractItemSource{FlowPath: "review", Family: "agents", File: "review/agents.yaml"}
	sourceB := ContractItemSource{FlowPath: "review", Family: "agents", File: "review/agents-extra.yaml"}
	if err := mergeAgentContracts(bundle, map[string]AgentRegistryEntry{
		"worker": {ID: "worker", Role: "reviewer"},
	}, sourceA); err != nil {
		t.Fatalf("mergeAgentContracts initial: %v", err)
	}
	err := mergeAgentContracts(bundle, map[string]AgentRegistryEntry{
		"worker": {ID: "worker", Role: "alternate"},
	}, sourceB)
	if err == nil {
		t.Fatal("mergeAgentContracts duplicate scoped agent error = nil")
	}
	for _, want := range []string{`duplicate scoped agent id "` + contractScopeKey(sourceA, "worker") + `"`, sourceA.File, sourceB.File} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate scoped agent error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestWorkflowContractBundleResolveFlowTemplateInstance_UsesScalarIdentity(t *testing.T) {
	field := mustTemplateInstanceField(t, "scope_id")
	bundle := &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{
			"spec_repo": {
				Name:     "spec_repo",
				Mode:     "template",
				Instance: field,
			},
		},
		flowEntities: map[string]EntityContractsDocument{
			"spec_repo": {
				"artifact_repo": {
					Fields: map[string]EntityFieldDecl{
						"artifact_type": {Type: "text"},
						"scope":         {Type: "text"},
						"scope_id":      {Type: "uuid"},
					},
				},
			},
		},
	}

	resolved, err := bundle.ResolveFlowTemplateInstance("spec_repo")
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance: %v", err)
	}
	if got, want := resolved.Field.Path(), "scope_id"; got != want {
		t.Fatalf("resolved Field = %q, want %q", got, want)
	}
	key, err := resolved.CanonicalKeyMaterial(map[string]any{
		"scope_id": "vertical-1",
	})
	if err != nil {
		t.Fatalf("CanonicalKeyMaterial: %v", err)
	}
	if got, want := keyMaterialString(key), "scope_id=vertical-1"; got != want {
		t.Fatalf("CanonicalKeyMaterial = %q, want %q", got, want)
	}
}

func TestWorkflowContractBundleResolveFlowTemplateInstance_RejectsInvalidDeclarations(t *testing.T) {
	missingField := mustTemplateInstanceField(t, "account_id")
	nonScalarField := mustTemplateInstanceField(t, "tags")
	staticField := mustTemplateInstanceField(t, "tenant_id")
	tests := []struct {
		name     string
		schema   FlowSchemaDocument
		entities EntityContractsDocument
		wantErr  string
	}{
		{
			name: "missing field",
			schema: FlowSchemaDocument{
				Mode:     "template",
				Instance: missingField,
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tenant_id": {Type: "text"}}}},
			wantErr:  "not declared",
		},
		{
			name: "non scalar key field",
			schema: FlowSchemaDocument{
				Mode:     "template",
				Instance: nonScalarField,
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tags": {Type: "[text]"}}}},
			wantErr:  "scalar or enum",
		},
		{
			name: "non template",
			schema: FlowSchemaDocument{
				Mode:     "static",
				Instance: staticField,
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tenant_id": {Type: "text"}}}},
			wantErr:  "not mode: template",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &WorkflowContractBundle{
				FlowSchemas:  map[string]FlowSchemaDocument{"worker": tc.schema},
				flowEntities: map[string]EntityContractsDocument{"worker": tc.entities},
			}
			_, err := bundle.ResolveFlowTemplateInstance("worker")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ResolveFlowTemplateInstance error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseTemplateInstanceFieldRejectsNestedAndEmptyIdentity(t *testing.T) {
	for _, raw := range []string{"", "tenant.id"} {
		if _, err := ParseTemplateInstanceField(raw); err == nil {
			t.Fatalf("ParseTemplateInstanceField(%q) error = nil", raw)
		}
	}
}

func TestWorkflowContractBundleResolveFlowSingletonCoordinator_UsesPrimaryEntityContainedState(t *testing.T) {
	bundle := &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{
			"coordinator": {
				Name: "coordinator",
				Mode: FlowModeSingleton,
			},
		},
		flowEntities: map[string]EntityContractsDocument{
			"coordinator": {
				"coordinator_state": {
					Fields: map[string]EntityFieldDecl{
						"status":    {Type: "text"},
						"verticals": {Type: "map[text]VerticalState"},
						"jobs":      {Type: "[Job]"},
					},
				},
			},
		},
		flowTypes: map[string]TypeCatalogDocument{
			"coordinator": {
				Types: map[string]NamedTypeDecl{
					"VerticalState": {
						Fields: map[string]TypeFieldSpec{
							"status":      {Type: "text"},
							"active_jobs": {Type: "[Job]"},
						},
					},
					"Job": {
						Fields: map[string]TypeFieldSpec{
							"id":    {Type: "text"},
							"title": {Type: "text"},
						},
					},
				},
			},
		},
	}

	resolved, err := bundle.ResolveFlowSingletonCoordinator("coordinator")
	if err != nil {
		t.Fatalf("ResolveFlowSingletonCoordinator: %v", err)
	}
	if got := resolved.PrimaryEntity.EntityType; got != "coordinator_state" {
		t.Fatalf("PrimaryEntity.EntityType = %q, want coordinator_state", got)
	}
	if len(resolved.ContainedState) != 2 {
		t.Fatalf("ContainedState = %#v, want verticals/jobs", resolved.ContainedState)
	}
	if got := resolved.ContainedState[0].Name + ":" + resolved.ContainedState[0].Kind; got != "jobs:list" {
		t.Fatalf("ContainedState[0] = %q, want jobs:list", got)
	}
	if got := resolved.ContainedState[1].Name + ":" + resolved.ContainedState[1].Kind; got != "verticals:map" {
		t.Fatalf("ContainedState[1] = %q, want verticals:map", got)
	}
}

func TestWorkflowContractBundleResolveFlowSingleton_AllowsEmptyAndScalarOnlyPrimaryEntity(t *testing.T) {
	for _, fields := range []map[string]EntityFieldDecl{
		{},
		{"status": {Type: "text"}},
		{"unused": {Type: "map[text]text"}},
	} {
		bundle := &WorkflowContractBundle{
			FlowSchemas: map[string]FlowSchemaDocument{"service": {Mode: FlowModeSingleton}},
			flowEntities: map[string]EntityContractsDocument{
				"service": {"service_state": {Fields: fields}},
			},
		}
		resolved, err := bundle.ResolveFlowSingleton("service")
		if err != nil {
			t.Fatalf("ResolveFlowSingleton(%#v): %v", fields, err)
		}
		if resolved.FlowID != "service" || resolved.PrimaryEntity.EntityType != "service_state" {
			t.Fatalf("ResolveFlowSingleton = %#v", resolved)
		}
	}
}

func TestWorkflowContractBundleResolveFlowSingletonCoordinator_RejectsInvalidDeclarations(t *testing.T) {
	instanceField := mustTemplateInstanceField(t, "vertical_id")
	tests := []struct {
		name     string
		schema   FlowSchemaDocument
		entities EntityContractsDocument
		wantErr  string
	}{
		{
			name: "bare static is not singleton",
			schema: FlowSchemaDocument{
				Mode: FlowModeStatic,
			},
			entities: EntityContractsDocument{"coordinator_state": {Fields: map[string]EntityFieldDecl{"verticals": {Type: "map[text]VerticalState"}}}},
			wantErr:  "not mode: singleton",
		},
		{
			name: "template instance mix",
			schema: FlowSchemaDocument{
				Mode:     FlowModeSingleton,
				Instance: instanceField,
			},
			entities: EntityContractsDocument{"coordinator_state": {Fields: map[string]EntityFieldDecl{"verticals": {Type: "map[text]VerticalState"}}}},
			wantErr:  "must not declare template instance",
		},
		{
			name: "agent memory only no contained state",
			schema: FlowSchemaDocument{
				Mode: FlowModeSingleton,
			},
			entities: EntityContractsDocument{"coordinator_state": {Fields: map[string]EntityFieldDecl{"status": {Type: "text"}}}},
			wantErr:  "agent conversation memory is not coordinator state authority",
		},
		{
			name: "unresolved map value type",
			schema: FlowSchemaDocument{
				Mode: FlowModeSingleton,
			},
			entities: EntityContractsDocument{"coordinator_state": {Fields: map[string]EntityFieldDecl{"verticals": {Type: "map[text]MissingType"}}}},
			wantErr:  "MissingType",
		},
		{
			name: "unresolved list item type",
			schema: FlowSchemaDocument{
				Mode: FlowModeSingleton,
			},
			entities: EntityContractsDocument{"coordinator_state": {Fields: map[string]EntityFieldDecl{"jobs": {Type: "[MissingType]"}}}},
			wantErr:  "MissingType",
		},
		{
			name: "schema entity restatement",
			schema: FlowSchemaDocument{
				Mode:   FlowModeSingleton,
				Entity: "coordinator_state",
			},
			entities: EntityContractsDocument{"coordinator_state": {Fields: map[string]EntityFieldDecl{"verticals": {Type: "map[text]VerticalState"}}}},
			wantErr:  "schema.yaml entity",
		},
		{
			name: "multiple entity contracts",
			schema: FlowSchemaDocument{
				Mode: FlowModeSingleton,
			},
			entities: EntityContractsDocument{
				"coordinator_state": {Fields: map[string]EntityFieldDecl{"verticals": {Type: "map[text]VerticalState"}}},
				"legacy_state":      {Fields: map[string]EntityFieldDecl{"status": {Type: "text"}}},
			},
			wantErr: "multiple entity types",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &WorkflowContractBundle{
				FlowSchemas:  map[string]FlowSchemaDocument{"coordinator": tc.schema},
				flowEntities: map[string]EntityContractsDocument{"coordinator": tc.entities},
			}
			_, err := bundle.ResolveFlowSingletonCoordinator("coordinator")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ResolveFlowSingletonCoordinator error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func keyMaterialString(values []TemplateInstanceKeyValue) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, value.Field.Path()+"="+value.Value)
	}
	return strings.Join(parts, ",")
}

func mustTemplateInstanceField(t testing.TB, raw string) TemplateInstanceField {
	t.Helper()
	field, err := ParseTemplateInstanceField(raw)
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField(%q): %v", raw, err)
	}
	return field
}

func TestTypeDiagnosticsUseAuthorFacingVocabulary(t *testing.T) {
	t.Run("scalar alias", func(t *testing.T) {
		var scalar ScalarTypeDecl
		err := yaml.Unmarshal([]byte("future_scalar\n"), &scalar)
		if err == nil || !strings.Contains(err.Error(), "supported built-in scalar") {
			t.Fatalf("scalar alias error = %v", err)
		}
		if strings.Contains(err.Error(), "Wave 1") {
			t.Fatalf("scalar alias error leaks internal rollout vocabulary: %v", err)
		}
	})

	t.Run("unsupported type form", func(t *testing.T) {
		err := validateWave1TypeRef("Optional<text>", "field")
		if err == nil || !strings.Contains(err.Error(), "current type system") {
			t.Fatalf("type error = %v", err)
		}
		if strings.Contains(err.Error(), "Wave 1") {
			t.Fatalf("type error leaks internal rollout vocabulary: %v", err)
		}
	})
}

func TestLoadWorkflowContractBundle_RejectsMultipleFlowEntityTypes(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/schema.yaml", "name: invalid-flow-entities\n")
	writeFixtureFile(t, root+"/scoring/schema.yaml", `
name: scoring
initial_state: pending
states: [pending, done]
terminal_states: [done]
`)
	writeFixtureFile(t, root+"/scoring/entities.yaml", `
vertical:
  name: text
campaign:
  title: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "INVALID-PRIMARY-ENTITY") || !strings.Contains(err.Error(), "exactly one entity type") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want INVALID-PRIMARY-ENTITY requiring exactly one entity type", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsMultipleRootEntityTypes(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/schema.yaml", `
name: invalid-root-entities
initial_state: pending
states: [pending, done]
terminal_states: [done]
`)
	writeFixtureFile(t, root+"/entities.yaml", `
vertical:
  name: text
campaign:
  title: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "INVALID-PRIMARY-ENTITY") || !strings.Contains(err.Error(), "exactly one entity type") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want INVALID-PRIMARY-ENTITY requiring exactly one entity type", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsSchemaEntitySelector(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/schema.yaml", "name: schema-entity-selector\n")
	writeFixtureFile(t, root+"/scoring/schema.yaml", `
name: scoring
entity: vertical
initial_state: pending
states: [pending, done]
terminal_states: [done]
`)
	writeFixtureFile(t, root+"/scoring/entities.yaml", `
vertical:
  name: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "schema.yaml entity") || !strings.Contains(err.Error(), "single entity authority") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want schema.yaml entity selector rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsRootSchemaEntitySelector(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/schema.yaml", `
name: root-schema-entity-selector
entity: vertical
initial_state: pending
states: [pending, done]
terminal_states: [done]
`)
	writeFixtureFile(t, root+"/entities.yaml", `
vertical:
  name: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "schema.yaml entity") || !strings.Contains(err.Error(), "single entity authority") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want root schema.yaml entity selector rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsSchemaEntitySelectorForMissingEntity(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/schema.yaml", "name: schema-entity-selector-missing\n")
	writeFixtureFile(t, root+"/scoring/schema.yaml", `
name: scoring
entity: missing
initial_state: pending
states: [pending, done]
terminal_states: [done]
`)
	writeFixtureFile(t, root+"/scoring/entities.yaml", `
vertical:
  name: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "schema.yaml entity") || !strings.Contains(err.Error(), "single entity authority") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want schema.yaml entity selector rejection", err)
	}
}

func TestEventCatalogEntryDecode_AcceptsFlatWave1PayloadGrammar(t *testing.T) {
	entry, err := admitEventCatalogEntryForTest(t, `
swarm:
  note: root handoff
  source: scoring
vertical_name: text
composite_score:
  type: numeric
  description: final score
`)
	if err != nil {
		t.Fatalf("admit event catalog entry: %v", err)
	}
	if got := entry.Note; got != "root handoff" {
		t.Fatalf("Note = %q", got)
	}
	if got := entry.Payload.Properties["vertical_name"].Type; got != "text" {
		t.Fatalf("vertical_name type = %q", got)
	}
	if got := entry.Payload.Properties["composite_score"].Type; got != "numeric" {
		t.Fatalf("composite_score type = %q", got)
	}
}

func TestEventCatalogEntryDecode_AuthorSummaryFieldIsMetadataNotPayload(t *testing.T) {
	entry, err := admitEventCatalogEntryForTest(t, `
chat_id: text
text: text
author_summary_field: text
`)
	if err != nil {
		t.Fatalf("admit event catalog entry: %v", err)
	}
	if entry.AuthorSummaryField != "text" {
		t.Fatalf("AuthorSummaryField = %q, want text", entry.AuthorSummaryField)
	}
	if _, exists := entry.Payload.Properties["author_summary_field"]; exists {
		t.Fatalf("author_summary_field leaked into payload schema: %#v", entry.Payload.Properties)
	}
	if _, exists := entry.Payload.Properties["text"]; !exists {
		t.Fatalf("text payload field missing: %#v", entry.Payload.Properties)
	}
}

func TestEventCatalogEntryDecode_RejectsMixedPayloadBlockAndFlatFields(t *testing.T) {
	_, err := admitEventCatalogEntryForTest(t, `
payload:
  entity_id: uuid
vertical_name: text
`)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED nested payload rejection", err)
	}
}

func TestEntityContractsDocumentDecode_RejectsRetiredParserLocalForms(t *testing.T) {
	var doc EntityContractsDocument
	err := yaml.Unmarshal([]byte(`
vertical:
  _state_model:
    state_field: current_state
  metadata:
    type: jsonb
`), &doc)
	if err == nil || (!strings.Contains(err.Error(), "RETIRED") && !strings.Contains(err.Error(), "jsonb")) {
		t.Fatalf("yaml.Unmarshal error = %v, want retired parser-local form rejection", err)
	}
}

func TestTypeCatalogDocumentDecode_RejectsInlineObjectField(t *testing.T) {
	var doc TypeCatalogDocument
	err := yaml.Unmarshal([]byte(`
types:
  Brand:
    palette:
      type: object
      properties:
        primary: text
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED inline object rejection", err)
	}
}

func TestCustomContractDocumentsDecodeMergeExpandedMappings(t *testing.T) {
	var entities EntityContractsDocument
	if err := yaml.Unmarshal([]byte("<<: &entities\n  item: {}\n"), &entities); err != nil {
		t.Fatalf("yaml.Unmarshal entities: %v", err)
	}
	if _, ok := entities["item"]; !ok {
		t.Fatalf("merged entities = %#v, want item", entities)
	}
	if _, ok := entities["<<"]; ok {
		t.Fatalf("merged entities published pseudo-key: %#v", entities)
	}

	var types TypeCatalogDocument
	if err := yaml.Unmarshal([]byte("<<: &catalog\n  types:\n    Item: {}\n"), &types); err != nil {
		t.Fatalf("yaml.Unmarshal types: %v", err)
	}
	if _, ok := types.Types["Item"]; !ok {
		t.Fatalf("merged types = %#v, want Item", types)
	}
	if _, ok := types.Types["<<"]; ok {
		t.Fatalf("merged types published pseudo-key: %#v", types)
	}
}
