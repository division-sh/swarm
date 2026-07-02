package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadWorkflowContractBundle_LoadsWave1TypeAndEntityDocuments(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: wave1-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
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
	writeFixtureFile(t, root+"/flows/scoring/schema.yaml", `
name: scoring
initial_state: discovered
states: [discovered, shortlisted]
terminal_states: [shortlisted]
pins:
  inputs:
    events: [root.ready]
  outputs:
    events: [vertical.shortlisted]
`)
	writeFixtureFile(t, root+"/flows/scoring/types.yaml", `
types:
  ScoreBreakdown:
    total: numeric
`)
	writeFixtureFile(t, root+"/flows/scoring/entities.yaml", `
vertical:
  _description: scoring vertical
  name: text
  review_count:
    type: integer
    indexed: true
    initial: 0
`)
	writeFixtureFile(t, root+"/flows/scoring/events.yaml", `
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
	sourceA := ContractItemSource{FlowID: "review", Layer: "flow", File: "flows/review/agents.yaml"}
	sourceB := ContractItemSource{FlowID: "review", Layer: "flow", File: "flows/review/agents-extra.yaml"}
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
	for _, want := range []string{`duplicate scoped agent id "review::worker"`, sourceA.File, sourceB.File} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate scoped agent error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestWorkflowContractBundleResolveFlowTemplateInstance_PreservesOrderedCompositeKey(t *testing.T) {
	bundle := &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{
			"spec_repo": {
				Name: "spec_repo",
				Mode: "template",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"scope", "scope_id", "artifact_type"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
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
	if got, want := strings.Join(resolved.By, ","), "scope,scope_id,artifact_type"; got != want {
		t.Fatalf("resolved By = %q, want %q", got, want)
	}
	key, err := resolved.CanonicalKeyMaterial(map[string]any{
		"artifact_type": "contract",
		"scope_id":      "vertical-1",
		"scope":         "project",
	})
	if err != nil {
		t.Fatalf("CanonicalKeyMaterial: %v", err)
	}
	if got, want := keyMaterialString(key), "scope=project,scope_id=vertical-1,artifact_type=contract"; got != want {
		t.Fatalf("CanonicalKeyMaterial = %q, want %q", got, want)
	}
}

func TestWorkflowContractBundleResolveFlowTemplateInstance_RejectsInvalidDeclarations(t *testing.T) {
	tests := []struct {
		name     string
		schema   FlowSchemaDocument
		entities EntityContractsDocument
		wantErr  string
	}{
		{
			name: "duplicate key",
			schema: FlowSchemaDocument{
				Mode: "template",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"tenant_id", "tenant_id"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tenant_id": {Type: "text"}}}},
			wantErr:  "duplicated",
		},
		{
			name: "missing field",
			schema: FlowSchemaDocument{
				Mode: "template",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"account_id"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tenant_id": {Type: "text"}}}},
			wantErr:  "not declared",
		},
		{
			name: "nested field",
			schema: FlowSchemaDocument{
				Mode: "template",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"tenant.id"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tenant": {Type: "Tenant"}}}},
			wantErr:  "top-level",
		},
		{
			name: "missing policy",
			schema: FlowSchemaDocument{
				Mode: "template",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"tenant_id"},
					OnConflict: "reject",
				},
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tenant_id": {Type: "text"}}}},
			wantErr:  "on_missing",
		},
		{
			name: "non scalar key field",
			schema: FlowSchemaDocument{
				Mode: "template",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"tags"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
			},
			entities: EntityContractsDocument{"tenant": {Fields: map[string]EntityFieldDecl{"tags": {Type: "[text]"}}}},
			wantErr:  "scalar or enum",
		},
		{
			name: "non template",
			schema: FlowSchemaDocument{
				Mode: "static",
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"tenant_id"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
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

func TestWorkflowContractBundleResolveFlowSingletonCoordinator_RejectsInvalidDeclarations(t *testing.T) {
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
				Mode: FlowModeSingleton,
				Instance: FlowTemplateInstanceDeclaration{
					By:         []string{"vertical_id"},
					OnMissing:  "create",
					OnConflict: "reject",
				},
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
		parts = append(parts, value.Field+"="+value.Value)
	}
	return strings.Join(parts, ",")
}

func TestLoadWorkflowContractBundle_RejectsLegacyPackageEntitySchema(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: compat-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
entity_schema:
  item:
    item_id: text
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: compat-bundle\n")
	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "entity_schema") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want RETIRED entity_schema rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsMixedLegacyEntitySchemaAndWave1Entities(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: mixed-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
entity_schema:
  item:
    item_id: text
flows: []
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: mixed-bundle\n")
	writeFixtureFile(t, root+"/entities.yaml", `
bundle_item:
  _owner: scoring
  name: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "entity_schema") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want RETIRED entity_schema rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsLegacySubpackageEntitySchemaAlongsideWave1Entities(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: mixed-subpackage-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
packages:
  - path: packages/legacy-child
flows: []
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: mixed-subpackage-bundle\n")
	writeFixtureFile(t, root+"/entities.yaml", `
root_entity:
  _owner: scoring
  name: text
`)
	writeFixtureFile(t, root+"/packages/legacy-child/package.yaml", `
name: legacy-child
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
entity_schema:
  child:
    legacy_id: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "entity_schema") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want RETIRED entity_schema rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsPackageScopedTypeCatalog(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: invalid-package-types
version: "1.0.0"
platform_version: ">=1.0.0"
packages:
  - path: packages/child
flows: []
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: invalid-package-types\n")
	writeFixtureFile(t, root+"/packages/child/package.yaml", `
name: child
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
`)
	writeFixtureFile(t, root+"/packages/child/types.yaml", "types:\n  Thing:\n    name: text\n")

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "package-scoped types.yaml") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want package-scoped types.yaml rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsPackageScopedEntityContracts(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: invalid-package-entities
version: "1.0.0"
platform_version: ">=1.0.0"
packages:
  - path: packages/child
flows: []
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: invalid-package-entities\n")
	writeFixtureFile(t, root+"/packages/child/package.yaml", `
name: child
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
`)
	writeFixtureFile(t, root+"/packages/child/entities.yaml", "child:\n  name: text\n")

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "package-scoped entities.yaml") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want package-scoped entities.yaml rejection", err)
	}
}

func TestLoadWorkflowContractBundle_RejectsMultipleFlowEntityTypes(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: invalid-flow-entities
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: invalid-flow-entities\n")
	writeFixtureFile(t, root+"/flows/scoring/schema.yaml", `
name: scoring
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`)
	writeFixtureFile(t, root+"/flows/scoring/entities.yaml", `
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

	writeFixtureFile(t, root+"/package.yaml", `
name: invalid-root-entities
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
`)
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

	writeFixtureFile(t, root+"/package.yaml", `
name: schema-entity-selector
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: schema-entity-selector\n")
	writeFixtureFile(t, root+"/flows/scoring/schema.yaml", `
name: scoring
entity: vertical
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`)
	writeFixtureFile(t, root+"/flows/scoring/entities.yaml", `
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

	writeFixtureFile(t, root+"/package.yaml", `
name: root-schema-entity-selector
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
`)
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

	writeFixtureFile(t, root+"/package.yaml", `
name: schema-entity-selector-missing
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: schema-entity-selector-missing\n")
	writeFixtureFile(t, root+"/flows/scoring/schema.yaml", `
name: scoring
entity: missing
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`)
	writeFixtureFile(t, root+"/flows/scoring/entities.yaml", `
vertical:
  name: text
`)

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "schema.yaml entity") || !strings.Contains(err.Error(), "single entity authority") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want schema.yaml entity selector rejection", err)
	}
}

func TestProjectPackageDocumentDecode_RejectsLegacyEntitySchema(t *testing.T) {
	var doc ProjectPackageDocument
	err := yaml.Unmarshal([]byte(`
name: test-accumulate-all
version: 1.0.0
description: Accumulate 3 items, fire on_complete when all arrive.
platform_version: ">=1.1.0"
flows: []
entity_schema:
  core:
    expected_count: integer
    received_items: [text]
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "entity_schema") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED entity_schema rejection", err)
	}
}

func TestLoadWorkflowContractBundle_InvalidLegacyPackageFieldReturnsParseError(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, root+"/package.yaml", `
name: invalid-legacy-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
entity_schema:
  core:
    metadata: jsonb
flows: []
`)
	writeFixtureFile(t, root+"/schema.yaml", "name: invalid-legacy-bundle\n")

	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "entity_schema") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want RETIRED entity_schema rejection", err)
	}
	if strings.Contains(err.Error(), "workflow.name missing") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want parse error instead of downstream semantics failure", err)
	}
}

func TestEventCatalogEntryDecode_AcceptsFlatWave1PayloadGrammar(t *testing.T) {
	var entry EventCatalogEntry
	if err := yaml.Unmarshal([]byte(`
swarm:
  note: root handoff
  source: scoring
vertical_name: text
composite_score:
  type: numeric
  description: final score
`), &entry); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
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

func TestEventCatalogEntryDecode_RejectsMixedPayloadBlockAndFlatFields(t *testing.T) {
	var entry EventCatalogEntry
	err := yaml.Unmarshal([]byte(`
payload:
  entity_id: uuid
vertical_name: text
`), &entry)
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
