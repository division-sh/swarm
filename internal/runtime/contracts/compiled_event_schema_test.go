package contracts

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
)

func TestCompiledEventSchemasCurrentGrammarOwnsOptionalityAndCanonicalSchema(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := currentWorkflowContractsDirForTest(t)
	eventsFile := filepath.Join(root, "events.yaml")
	writeFixtureFile(t, eventsFile, `
item.created:
  entity_id: string?
  item_id: uuid
  note: text?
evidence.recorded:
  entity_id: string
  note: string
item.*: {}
`)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load compiled-event fixture: %v", err)
	}

	compiled, err := bundle.CompiledEventSchemas()
	if err != nil {
		t.Fatalf("CompiledEventSchemas: %v", err)
	}
	event := requireCompiledEventSchema(t, compiled, ".", "item.created")
	if event.Classification() != CompiledEventSchemaAuthored || !event.Importable() {
		t.Fatalf("classification = %q, importable=%t", event.Classification(), event.Importable())
	}
	if _, ok := event.BusinessKey(); ok {
		t.Fatal("current grammar adapter invented a business key")
	}
	if got := event.Source(); got.Layer != "project" || got.FlowID != "" || got.File != eventsFile {
		t.Fatalf("source = %#v, want project diagnostic source %s", got, eventsFile)
	}

	fields := event.Fields()
	if len(fields) != 3 || fields[0].Name() != "entity_id" || fields[1].Name() != "item_id" || fields[2].Name() != "note" {
		t.Fatalf("fields = %#v, want closed canonical field order", compiledEventFieldNames(fields))
	}
	if !fields[0].IsOptional() || fields[1].IsOptional() || !fields[2].IsOptional() {
		t.Fatalf("optionality = [%t %t %t], want [true false true]", fields[0].IsOptional(), fields[1].IsOptional(), fields[2].IsOptional())
	}
	if got := fields[1].SemanticSchema()["format"]; got != "uuid" {
		t.Fatalf("item_id admitted schema format = %#v, want uuid", got)
	}

	resolution, _, ok := EventSchemaForFlowEvent(bundle, "", "item.created")
	if !ok {
		t.Fatal("canonical event schema owner did not resolve item.created")
	}
	wantSchema := runtimeeventschema.CanonicalAcceptanceSchema(resolution.Schema)
	wantBytes, err := canonicaljson.Bytes(wantSchema)
	if err != nil {
		t.Fatalf("canonical schema owner: %v", err)
	}
	if got := event.CanonicalAcceptanceSchema(); !bytes.Equal(got, wantBytes) {
		t.Fatalf("canonical schema = %s, want %s", got, wantBytes)
	}
	if got, want := event.AcceptanceSchemaDigest(), canonicaljson.HashBytes(wantBytes); got != want {
		t.Fatalf("schema digest = %q, want %q", got, want)
	}

	for _, candidate := range compiled {
		if candidate.EventName() == "item.*" {
			t.Fatal("pattern declaration became an importable compiled event")
		}
	}

	// Every readback is detached from the immutable compiled value.
	acceptance := event.AcceptanceSchema()
	acceptance["type"] = "array"
	acceptance["properties"].(map[string]any)["item_id"] = map[string]any{"type": "number"}
	canonical := event.CanonicalAcceptanceSchema()
	canonical[0] = 'x'
	fields[1].SemanticSchema()["format"] = "changed"
	fields[0], fields[1] = fields[1], CompiledEventField{}
	againFields := event.Fields()
	if event.AcceptanceSchema()["type"] != "object" || againFields[0].Name() != "entity_id" || againFields[1].Name() != "item_id" || againFields[1].SemanticSchema()["format"] != "uuid" || !bytes.Equal(event.CanonicalAcceptanceSchema(), wantBytes) {
		t.Fatalf("compiled schema same-value readback mutation escaped into owner: schema=%#v fields=%#v field=%#v bytes=%s", event.AcceptanceSchema(), compiledEventFieldNames(againFields), againFields[1].SemanticSchema(), event.CanonicalAcceptanceSchema())
	}
}

func TestCompiledEventSchemasPreservesPackageOwnerAndRejectsRawRestatements(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeCompiledEventPackageFixture(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load package fixture: %v", err)
	}
	compiled, err := bundle.CompiledEventSchemas()
	if err != nil {
		t.Fatalf("CompiledEventSchemas: %v", err)
	}

	wantCoordinates := []string{
		".:orders/root.start",
		".:root.start",
		"child:child.ready",
		"flows/orders/child:orders/child.ready",
	}
	if got := compiledEventCoordinates(compiled); !stringSlicesEqual(got, wantCoordinates) {
		t.Fatalf("compiled coordinates = %#v, want exact physical declaration list %#v", got, wantCoordinates)
	}
	requireCompiledEventSchema(t, compiled, ".", "root.start")
	flowEvent := requireCompiledEventSchema(t, compiled, ".", "orders/root.start")
	if got := flowEvent.Source(); got.Layer != "flow" || got.FlowID != "orders" || !strings.HasSuffix(got.File, "/flows/orders/events.yaml") {
		t.Fatalf("flow source = %#v", got)
	}
	requireCompiledEventSchema(t, compiled, "child", "child.ready")
	childEvent := requireCompiledEventSchema(t, compiled, "flows/orders/child", "orders/child.ready")
	if got := childEvent.Source(); got.Layer != "project" || got.FlowID != "orders" || !strings.HasSuffix(got.File, "/flows/orders/child/events.yaml") {
		t.Fatalf("flow-owned child source = %#v", got)
	}
	childFields := childEvent.Fields()
	if len(childFields) != 1 || childFields[0].Name() != "order_code" || childFields[0].SemanticSchema()["type"] != "string" {
		t.Fatalf("flow-owned child fields = %#v/%#v, want OrderCode lowered through owning flow catalog", compiledEventFieldNames(childFields), childFields[0].SemanticSchema())
	}
	childCanonical, err := canonicaljson.Bytes(childEvent.AcceptanceSchema())
	if err != nil {
		t.Fatalf("flow-owned child canonical schema: %v", err)
	}
	if !bytes.Equal(childEvent.CanonicalAcceptanceSchema(), childCanonical) || childEvent.AcceptanceSchemaDigest() != canonicaljson.HashBytes(childCanonical) {
		t.Fatalf("flow-owned child canonical bytes/digest disagree with admitted schema")
	}
	for _, event := range compiled {
		if strings.Contains(event.EventName(), "addon-a") {
			t.Fatalf("noncanonical package restatement became declaration identity: %s:%s", event.PackageKey(), event.EventName())
		}
		if event.Classification() != CompiledEventSchemaAuthored {
			t.Fatalf("non-authored event escaped exact enumeration: %#v", event.Classification())
		}
	}
}

func TestCompiledEventSchemasExcludeGeneratedAndProviderImportedEvents(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: compiled-event-exclusions
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
provider_trigger_events:
  imports:
    - provider: telegram
      event: inbound.telegram.text_message
flows: []
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: compiled-event-exclusions\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "source.requested:\n  url: string\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
scanner:
  id: scanner
  execution_type: system_node
  subscribes_to: [source.requested]
  event_handlers:
    source.requested:
      activity:
        id: source_scrape
        tool: source_scrape
        input:
          url: {cel: payload.url}
`)
	writeFixtureFile(t, filepath.Join(root, "tools.yaml"), `
source_scrape:
  description: Read a source.
  handler_type: http
  effect_class: read_only
  http:
    method: GET
    url: https://example.test
  input_schema:
    type: object
    required: [url]
    properties:
      url: {type: string}
  output_schema:
    type: object
    properties:
      title: {type: string}
`)
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load generated/provider fixture: %v", err)
	}
	generated := bundle.GeneratedActivityEventEntries()
	if len(generated) == 0 {
		t.Fatal("fixture did not materialize generated activity events")
	}
	for generatedName, want := range generated {
		got, key, ok := bundle.ResolveFlowEventCatalogEntry("", generatedName)
		if !ok || key != generatedName || len(got.Payload.Properties) != len(want.Payload.Properties) {
			t.Fatalf("generated event effective resolution %q = key:%q ok:%t entry:%#v", generatedName, key, ok, got)
		}
	}
	if len(bundle.Package.ProviderTriggerEvents.Imports) != 1 {
		t.Fatalf("fixture provider imports = %#v", bundle.Package.ProviderTriggerEvents.Imports)
	}
	compiled, err := bundle.CompiledEventSchemas()
	if err != nil {
		t.Fatalf("CompiledEventSchemas: %v", err)
	}
	if got := compiledEventCoordinates(compiled); !stringSlicesEqual(got, []string{".:source.requested"}) {
		t.Fatalf("compiled coordinates = %#v, want authored event only", got)
	}
	for generatedName := range generated {
		for _, candidate := range compiled {
			if candidate.EventName() == generatedName {
				t.Fatalf("generated event %q entered authored compiled declarations", generatedName)
			}
		}
	}
	for _, imported := range bundle.Package.ProviderTriggerEvents.Imports {
		for _, candidate := range compiled {
			if candidate.EventName() == imported.Event {
				t.Fatalf("provider-imported event %q entered authored compiled declarations", imported.Event)
			}
		}
	}
}

func TestCompiledEventBusinessKeyAdmissionIsClosed(t *testing.T) {
	entry := EventCatalogEntry{
		Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"bool_key":     {Type: "boolean"},
			"string_key":   {Type: "string"},
			"number_key":   {Type: "numeric"},
			"integer_key":  {Type: "integer"},
			"object_key":   {Type: "object"},
			"array_key":    {Type: "list<text>"},
			"optional_key": {Type: "string"},
		}, Required: []string{"bool_key", "string_key", "number_key", "integer_key", "object_key", "array_key"}},
	}
	for _, test := range []struct {
		field        string
		semanticType string
	}{
		{field: "bool_key", semanticType: "boolean"},
		{field: "string_key", semanticType: "string"},
		{field: "number_key", semanticType: "number"},
		{field: "integer_key", semanticType: "number"},
	} {
		t.Run("accept_"+test.field, func(t *testing.T) {
			compiled, err := newCompiledEventSchema(".", "item.recorded", entry, TypeCatalogDocument{}, test.field, CompiledEventSchemaSource{})
			if err != nil {
				t.Fatalf("compile business key: %v", err)
			}
			key, ok := compiled.BusinessKey()
			if !ok || key.Field != test.field || key.SemanticType != test.semanticType {
				t.Fatalf("business key = %#v, %t, want %q/%q", key, ok, test.field, test.semanticType)
			}
		})
	}

	for _, test := range []struct {
		name  string
		field string
		want  string
	}{
		{name: "object", field: "object_key", want: "boolean, number, or string"},
		{name: "array", field: "array_key", want: "boolean, number, or string"},
		{name: "optional", field: "optional_key", want: "must be required"},
		{name: "missing", field: "unknown", want: "is not declared"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCompiledEventSchema(".", "item.recorded", entry, TypeCatalogDocument{}, test.field, CompiledEventSchemaSource{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("business key error = %v, want %q", err, test.want)
			}
		})
	}
}

func requireCompiledEventSchema(t *testing.T, schemas []CompiledEventSchema, packageKey, eventName string) CompiledEventSchema {
	t.Helper()
	for _, schema := range schemas {
		if schema.PackageKey() == packageKey && schema.EventName() == eventName {
			return schema
		}
	}
	t.Fatalf("compiled event %s:%s missing from %#v", packageKey, eventName, compiledEventCoordinates(schemas))
	return CompiledEventSchema{}
}

func compiledEventCoordinates(schemas []CompiledEventSchema) []string {
	out := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, schema.PackageKey()+":"+schema.EventName())
	}
	return out
}

func compiledEventFieldNames(fields []CompiledEventField) []string {
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, field.Name())
	}
	return out
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func writeCompiledEventPackageFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: compiled-event-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: child
flows:
  - id: orders
    flow: orders
    mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: compiled-event-package\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "root.start: {}\nroot.*: {}\n")
	for _, name := range []string{"nodes.yaml", "agents.yaml", "tools.yaml", "policy.yaml"} {
		writeFixtureFile(t, filepath.Join(root, name), "{}\n")
	}

	writeFixtureFile(t, filepath.Join(root, "child", "package.yaml"), "name: child\nversion: \"1.0.0\"\nflows: []\n")
	writeFixtureFile(t, filepath.Join(root, "child", "events.yaml"), "child.ready:\n  id: string\n")
	for _, name := range []string{"nodes.yaml", "agents.yaml", "tools.yaml", "policy.yaml"} {
		writeFixtureFile(t, filepath.Join(root, "child", name), "{}\n")
	}

	flowRoot := filepath.Join(root, "flows", "orders")
	writeFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: orders\nversion: \"1.0.0\"\npackages:\n  - path: child\nflows: []\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), `
name: orders
mode: static
initial_state: active
states: [active]
pins:
  inputs:
    events: [root.start]
`)
	writeFixtureFile(t, filepath.Join(flowRoot, "events.yaml"), "root.start: {}\naddon-a.start: {}\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "types.yaml"), "scalars:\n  OrderCode: text\n")
	for _, name := range []string{"nodes.yaml", "agents.yaml", "tools.yaml", "policy.yaml"} {
		writeFixtureFile(t, filepath.Join(flowRoot, name), "{}\n")
	}
	writeFixtureFile(t, filepath.Join(flowRoot, "child", "package.yaml"), "name: orders-child\nversion: \"1.0.0\"\nflows: []\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "child", "events.yaml"), "child.ready:\n  order_code: OrderCode\n")
	for _, name := range []string{"nodes.yaml", "agents.yaml", "tools.yaml", "policy.yaml"} {
		writeFixtureFile(t, filepath.Join(flowRoot, "child", name), "{}\n")
	}
	return root
}
