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
	if len(bundle.CompatibilityUsages()) != 0 {
		t.Fatalf("CompatibilityUsages() = %#v, want none", bundle.CompatibilityUsages())
	}
}

func TestLoadWorkflowContractBundle_RecordsLegacyCompatibilityUsage(t *testing.T) {
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
	writeFixtureFile(t, root+"/events.yaml", `
root.ready:
  payload:
    entity_id: uuid
`)
	writeFixtureFile(t, root+"/flows/scoring/schema.yaml", `
name: scoring
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [root.ready]
  outputs:
    events: []
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	usages := bundle.CompatibilityUsages()
	if len(usages) != 2 {
		t.Fatalf("len(CompatibilityUsages()) = %d, want 2 (%#v)", len(usages), usages)
	}
	seen := map[string]bool{}
	for _, usage := range usages {
		seen[usage.Kind] = true
	}
	if !seen["legacy_package_entity_schema"] {
		t.Fatalf("missing legacy_package_entity_schema usage in %#v", usages)
	}
	if !seen["legacy_event_payload_block"] {
		t.Fatalf("missing legacy_event_payload_block usage in %#v", usages)
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
	if err == nil || !strings.Contains(err.Error(), "AMBIGUOUS-CONTRACT-GRAMMAR") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want AMBIGUOUS-CONTRACT-GRAMMAR", err)
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

func TestProjectPackageDocumentDecode_PreservesManifestFieldsWithLegacyEntitySchemaListField(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: test-accumulate-all
version: 1.0.0
description: Accumulate 3 items, fire on_complete when all arrive.
platform_version: ">=1.1.0"
flows: []
entity_schema:
  core:
    expected_count: integer
    received_items: [text]
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := doc.Name; got != "test-accumulate-all" {
		t.Fatalf("Name = %q", got)
	}
	if got := doc.Version; got != "1.0.0" {
		t.Fatalf("Version = %q", got)
	}
	if got := doc.PlatformVersion; got != ">=1.1.0" {
		t.Fatalf("PlatformVersion = %q", got)
	}
	if !doc.UsesLegacyEntitySchema {
		t.Fatal("expected UsesLegacyEntitySchema to be set")
	}
	if got := len(doc.EntitySchema.Groups); got != 1 {
		t.Fatalf("len(EntitySchema.Groups) = %d", got)
	}
	fields := doc.EntitySchema.Groups[0].Fields
	if got := fields[1].Type; got != "list<text>" {
		t.Fatalf("received_items type = %q", got)
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
	if !strings.Contains(err.Error(), "parse") || !strings.Contains(err.Error(), "jsonb") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want parse error mentioning jsonb", err)
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
	if entry.UsesLegacyPayload {
		t.Fatal("expected flat event grammar to avoid legacy payload compatibility flag")
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
	if err == nil || !strings.Contains(err.Error(), "AMBIGUOUS-EVENT-GRAMMAR") {
		t.Fatalf("yaml.Unmarshal error = %v, want AMBIGUOUS-EVENT-GRAMMAR", err)
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
