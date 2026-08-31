package contracts

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
)

func TestCompiledEventSchemasOwnOptionalityAndCanonicalSchema(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: compiled-events\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string?
  item_id: uuid
  note: text?
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
	if got := event.Source(); got.Layer != "flow" || got.FlowPath != "." || !strings.HasSuffix(filepath.ToSlash(got.File), "events.yaml") {
		t.Fatalf("source = %#v", got)
	}
	fields := event.Fields()
	if len(fields) != 3 || fields[0].Name() != "entity_id" || fields[1].Name() != "item_id" || fields[2].Name() != "note" {
		t.Fatalf("fields = %#v", compiledEventFieldNames(fields))
	}
	if !fields[0].IsOptional() || fields[1].IsOptional() || !fields[2].IsOptional() {
		t.Fatalf("optionality = [%t %t %t]", fields[0].IsOptional(), fields[1].IsOptional(), fields[2].IsOptional())
	}
	if got := fields[1].SemanticSchema()["format"]; got != "uuid" {
		t.Fatalf("item_id format = %#v", got)
	}

	resolution, _, ok := EventSchemaForFlowEvent(bundle, ".", "item.created")
	if !ok {
		t.Fatal("canonical event schema owner did not resolve item.created")
	}
	wantSchema := runtimeeventschema.CanonicalAcceptanceSchema(resolution.Schema)
	wantBytes, err := canonicaljson.Bytes(wantSchema)
	if err != nil {
		t.Fatal(err)
	}
	if got := event.CanonicalAcceptanceSchema(); !bytes.Equal(got, wantBytes) {
		t.Fatalf("canonical schema = %s, want %s", got, wantBytes)
	}
	if got, want := event.AcceptanceSchemaDigest(), canonicaljson.HashBytes(wantBytes); got != want {
		t.Fatalf("schema digest = %q, want %q", got, want)
	}
	for _, candidate := range compiled {
		if candidate.EventName() == "item.*" {
			t.Fatal("pattern declaration became importable")
		}
	}

	acceptance := event.AcceptanceSchema()
	acceptance["type"] = "array"
	canonical := event.CanonicalAcceptanceSchema()
	canonical[0] = 'x'
	fields[1].SemanticSchema()["format"] = "changed"
	again := event.Fields()
	if event.AcceptanceSchema()["type"] != "object" || again[1].SemanticSchema()["format"] != "uuid" || !bytes.Equal(event.CanonicalAcceptanceSchema(), wantBytes) {
		t.Fatal("compiled event readback was mutable")
	}
}

func TestCompileCurrentEventDeclarationRejectsNoncanonicalIdentity(t *testing.T) {
	bundle := &WorkflowContractBundle{}
	for _, name := range []string{"", " item.created ", "/item.created", "Item Ready", "item.pre*"} {
		_, ok, err := bundle.compileCurrentEventDeclaration(".", "flow", "events.yaml", name, name, EventCatalogEntry{}, TypeCatalogDocument{})
		if err == nil || ok || !strings.Contains(err.Error(), "canonical") {
			t.Fatalf("compile declaration %q = ok:%t err:%v", name, ok, err)
		}
	}
	if _, ok, err := bundle.compileCurrentEventDeclaration(".", "flow", "events.yaml", "item.*", "item.*", EventCatalogEntry{}, TypeCatalogDocument{}); err != nil || ok {
		t.Fatalf("pattern declaration = ok:%t err:%v", ok, err)
	}
}

func TestCompiledEventSchemasPreserveFilesystemFlowOwner(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: event-owner-root\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "root.started: {}\n")
	writeFixtureFile(t, filepath.Join(root, "orders", "schema.yaml"), "name: orders\n")
	writeFixtureFile(t, filepath.Join(root, "orders", "events.yaml"), "ready:\n  order_id: text\n")

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := bundle.CompiledEventSchemas()
	if err != nil {
		t.Fatal(err)
	}
	requireCompiledEventSchema(t, compiled, ".", "root.started")
	child := requireCompiledEventSchema(t, compiled, "orders", "orders/ready")
	if got := child.Source(); got.FlowPath != "orders" || !strings.HasSuffix(filepath.ToSlash(got.File), "orders/events.yaml") {
		t.Fatalf("child source = %#v", got)
	}
	if got := compiledEventCoordinates(compiled); !stringSlicesEqual(got, []string{".:root.started", "orders:orders/ready"}) {
		t.Fatalf("compiled coordinates = %#v", got)
	}
}

func TestCompiledEventSchemasExcludeGeneratedEvents(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: generated-event-exclusion\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "source.requested:\n  url: text\n")
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
  http: {method: GET, url: https://example.test}
  input_schema:
    type: object
    required: [url]
    properties: {url: {type: string}}
  output_schema: {type: object}
`)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	generated := bundle.GeneratedActivityEventEntries()
	if len(generated) == 0 {
		t.Fatal("fixture did not generate activity outcome events")
	}
	compiled, err := bundle.CompiledEventSchemas()
	if err != nil {
		t.Fatal(err)
	}
	if got := compiledEventCoordinates(compiled); !stringSlicesEqual(got, []string{".:source.requested"}) {
		t.Fatalf("compiled coordinates = %#v", got)
	}
	for name := range generated {
		for _, event := range compiled {
			if event.EventName() == name {
				t.Fatalf("generated event %q entered authored declarations", name)
			}
		}
	}
}

func TestCompiledEventBusinessKeyAdmissionIsClosed(t *testing.T) {
	entry := EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
		"bool_key": {Type: "boolean"}, "text_key": {Type: "string"}, "object_key": {Type: "object"}, "optional_key": {Type: "string"},
	}, Required: []string{"bool_key", "text_key", "object_key"}}}
	for _, field := range []string{"bool_key", "text_key"} {
		compiled, err := newCompiledEventSchema(".", "item.recorded", entry, TypeCatalogDocument{}, field, CompiledEventSchemaSource{})
		if err != nil {
			t.Fatalf("business key %s: %v", field, err)
		}
		if key, ok := compiled.BusinessKey(); !ok || key.Field != field {
			t.Fatalf("business key = %#v, %t", key, ok)
		}
	}
	for _, test := range []struct{ field, want string }{{"object_key", "boolean, number, or string"}, {"optional_key", "must be required"}, {"missing", "is not declared"}} {
		if _, err := newCompiledEventSchema(".", "item.recorded", entry, TypeCatalogDocument{}, test.field, CompiledEventSchemaSource{}); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("business key %q error = %v", test.field, err)
		}
	}
}

func requireCompiledEventSchema(t *testing.T, schemas []CompiledEventSchema, flowPath, eventName string) CompiledEventSchema {
	t.Helper()
	for _, schema := range schemas {
		if schema.FlowPath() == flowPath && schema.EventName() == eventName {
			return schema
		}
	}
	t.Fatalf("compiled event %s:%s missing from %#v", flowPath, eventName, compiledEventCoordinates(schemas))
	return CompiledEventSchema{}
}

func compiledEventCoordinates(schemas []CompiledEventSchema) []string {
	out := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, schema.FlowPath()+":"+schema.EventName())
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
