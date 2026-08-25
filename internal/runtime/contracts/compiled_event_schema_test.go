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
  entity_id: string
  item_id: uuid
  note: text
  required: [item_id]
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
	event.AcceptanceSchema()["type"] = "array"
	event.CanonicalAcceptanceSchema()[0] = 'x'
	fields[1].SemanticSchema()["format"] = "changed"
	second, err := bundle.CompiledEventSchemas()
	if err != nil {
		t.Fatalf("second CompiledEventSchemas: %v", err)
	}
	again := requireCompiledEventSchema(t, second, ".", "item.created")
	if again.AcceptanceSchema()["type"] != "object" || again.Fields()[1].SemanticSchema()["format"] != "uuid" || !bytes.Equal(again.CanonicalAcceptanceSchema(), wantBytes) {
		t.Fatalf("compiled schema readback mutation escaped into owner: schema=%#v field=%#v bytes=%s", again.AcceptanceSchema(), again.Fields()[1].SemanticSchema(), again.CanonicalAcceptanceSchema())
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

	requireCompiledEventSchema(t, compiled, ".", "root.start")
	flowEvent := requireCompiledEventSchema(t, compiled, ".", "orders/root.start")
	if got := flowEvent.Source(); got.Layer != "flow" || got.FlowID != "orders" || !strings.HasSuffix(got.File, "/flows/orders/events.yaml") {
		t.Fatalf("flow source = %#v", got)
	}
	requireCompiledEventSchema(t, compiled, "child", "child.ready")
	for _, event := range compiled {
		if strings.Contains(event.EventName(), "addon-a") {
			t.Fatalf("noncanonical package restatement became declaration identity: %s:%s", event.PackageKey(), event.EventName())
		}
		if event.Classification() != CompiledEventSchemaAuthored {
			t.Fatalf("non-authored event escaped exact enumeration: %#v", event.Classification())
		}
	}
}

func TestCompiledEventBusinessKeyAdmissionIsClosed(t *testing.T) {
	entry := EventCatalogEntry{
		Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"id":    {Type: "integer"},
			"label": {Type: "string"},
			"items": {Type: "list<text>"},
		}},
		Required: []string{"id", "items"},
	}
	compiled, err := newCompiledEventSchema(".", "item.recorded", entry, TypeCatalogDocument{}, "id", CompiledEventSchemaSource{})
	if err != nil {
		t.Fatalf("compile required numeric key: %v", err)
	}
	key, ok := compiled.BusinessKey()
	if !ok || key.Field != "id" || key.SemanticType != "number" {
		t.Fatalf("business key = %#v, %t", key, ok)
	}

	for _, test := range []struct {
		name  string
		field string
		want  string
	}{
		{name: "optional", field: "label", want: "must be required"},
		{name: "container", field: "items", want: "boolean, number, or string"},
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
	writeFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: orders\nversion: \"1.0.0\"\nflows: []\n")
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
	for _, name := range []string{"nodes.yaml", "agents.yaml", "tools.yaml", "policy.yaml"} {
		writeFixtureFile(t, filepath.Join(flowRoot, name), "{}\n")
	}
	return root
}
