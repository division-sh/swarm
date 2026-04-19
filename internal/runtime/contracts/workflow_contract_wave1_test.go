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
	entityType, entity, ok := bundle.FlowOwnedEntityContract("scoring")
	if !ok {
		t.Fatal("expected scoring owned entity contract")
	}
	if entityType != "vertical" {
		t.Fatalf("FlowOwnedEntityContract entity type = %q", entityType)
	}
	if got := entity.Fields["review_count"].Type; got != "integer" {
		t.Fatalf("review_count type = %q", got)
	}
	resolvedTypes := bundle.ResolvedTypeCatalogForFlow("scoring")
	if _, ok := resolvedTypes.Scalars["URL"]; !ok {
		t.Fatal("expected resolved flow type catalog to include root scalar")
	}
	if _, ok := resolvedTypes.Types["ScoreBreakdown"]; !ok {
		t.Fatal("expected resolved flow type catalog to include flow-local type")
	}
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
	if err == nil || !strings.Contains(err.Error(), "INVALID-ENTITY-OWNERSHIP") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want INVALID-ENTITY-OWNERSHIP", err)
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
_note: root handoff
_source: scoring
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
