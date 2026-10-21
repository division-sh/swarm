package contracts

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/platform"
	"gopkg.in/yaml.v3"
)

const platformAdmissionFixture = `platform: {name: test, version: 0.7.0}
interfaces:
  sample:
    v1:
      kind: pack_channel
      schemas: {text: {type: string}}
      operations: {deliver: {effect_class: non_idempotent_write, input: {text: {schema: text}}, context: {destination: {opaque: destination}}, output: {receipt: {opaque: receipt}}}}
      events: {received: {required_fields: {text: {schema: text}}, optional_fields: {reply: {opaque: receipt}}}}
permissions_model: {permissions: [read]}
vocabulary: {participant: {types: {agent: {execution: llm}}}}
workflow_state: {ddl: "CREATE TABLE work (id TEXT);\n", fields: {id: {type: uuid}}}
platform_tables: {tables: {work: {ddl: "CREATE TABLE work (id TEXT);\n", description: note}}}
builtin_hooks: {guards: [{id: check}]}
platform_events:
  catalog:
    sample.observed:
      description: inert annotation
      payload: {value: string, note: {type: 'string?', description: note}, nested: {flag: boolean}}
`

func TestPlatformConsumedLawAdmissionPresenceMatrix(t *testing.T) {
	rows := []struct {
		path                                     []string
		optional, emptyMap, emptyList, emptyText bool
	}{
		{[]string{"platform"}, true, false, false, false},
		{[]string{"platform", "name"}, true, false, false, true},
		{[]string{"platform", "version"}, false, false, false, false},
		{[]string{"interfaces"}, true, true, false, false},
		{[]string{"interfaces", "sample", "v1", "kind"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "schemas"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "operations"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "events"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "operations", "deliver", "effect_class"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "operations", "deliver", "input"}, true, true, false, false},
		{[]string{"interfaces", "sample", "v1", "operations", "deliver", "context"}, true, true, false, false},
		{[]string{"interfaces", "sample", "v1", "operations", "deliver", "output"}, true, true, false, false},
		{[]string{"interfaces", "sample", "v1", "events", "received", "required_fields"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "events", "received", "optional_fields"}, true, true, false, false},
		{[]string{"interfaces", "sample", "v1", "events", "received", "optional_fields", "reply", "opaque"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "operations", "deliver", "input", "text", "schema"}, false, false, false, false},
		{[]string{"interfaces", "sample", "v1", "operations", "deliver", "context", "destination", "opaque"}, false, false, false, false},
		{[]string{"permissions_model", "permissions"}, false, false, true, false},
		{[]string{"vocabulary", "participant", "types", "agent", "execution"}, false, false, false, false},
		{[]string{"workflow_state", "ddl"}, true, false, false, false},
		{[]string{"workflow_state", "fields", "id", "type"}, false, false, false, false},
		{[]string{"platform_tables", "tables", "work", "ddl"}, false, false, false, false},
		{[]string{"platform_tables", "tables", "work", "description"}, true, false, false, true},
		{[]string{"builtin_hooks", "guards"}, true, false, true, false},
		{[]string{"platform_events", "catalog", "sample.observed", "payload"}, true, true, false, false},
		{[]string{"platform_events", "catalog", "sample.observed", "payload", "note", "type"}, true, false, false, false},
		{[]string{"platform_events", "catalog", "sample.observed", "payload", "note", "description"}, true, false, false, true},
	}
	for _, row := range rows {
		for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
			t.Run(strings.Join(row.path, "/")+"/"+state, func(t *testing.T) {
				body := platformPresenceBody(t, []byte(platformAdmissionFixture), row.path, state)
				_, err := ParsePlatformSpecDocument(body, "platform-spec.yaml")
				want := state == "valid" || state == "merge" || (state == "missing" && row.optional) || (state == "empty_map" && row.emptyMap) || (state == "empty_list" && row.emptyList) || (state == "empty_text" && row.emptyText)
				if (err == nil) != want {
					t.Fatalf("want admission=%v: %v", want, err)
				}
			})
		}
	}
}

func TestPlatformEventCatalogAdmissionRejectsMalformedEntries(t *testing.T) {
	for _, value := range []string{"null", "[]", "7", "true", "''", "[string]", "{required: []}"} {
		for _, target := range []string{"entry", "payload"} {
			t.Run(target+"/"+value, func(t *testing.T) {
				entry := value
				if target == "payload" {
					entry = "{payload: " + value + "}"
				}
				_, err := ParsePlatformSpecDocument([]byte("platform_events: {catalog: {sample: "+entry+"}}\n"), "platform-spec.yaml")
				if err == nil || !strings.Contains(err.Error(), "platform-spec.yaml:") {
					t.Fatalf("malformed catalog admitted or lost source: %v", err)
				}
			})
		}
	}
	for _, inactive := range []string{"null", "''", "false", "{}", "[]", "other"} {
		body := strings.Replace(platformAdmissionFixture, "{schema: text}", "{schema: text, opaque: "+inactive+"}", 1)
		if _, err := ParsePlatformSpecDocument([]byte(body), "platform-spec.yaml"); err == nil {
			t.Fatalf("inactive interface branch admitted: %s", inactive)
		}
	}
}

func TestPlatformCatalogMaterializationIsImmutableAndPreservesExactDDL(t *testing.T) {
	spec, err := ParsePlatformSpecDocument([]byte(platformAdmissionFixture), "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if spec.WorkflowState.DDL != "CREATE TABLE work (id TEXT);\n" || spec.PlatformTables.Tables["work"].DDL != spec.WorkflowState.DDL {
		t.Fatal("SQL bytes changed")
	}
	entry, _, ok := PlatformEventCatalogEntry(spec, "sample.observed")
	if !ok {
		t.Fatal("catalog missing")
	}
	entry.Payload.Required[0] = "forged"
	delete(entry.Payload.Properties, "value")
	fresh, _, _ := PlatformEventCatalogEntry(spec, "sample.observed")
	if fresh.Payload.Properties["value"].Type != "string" || fresh.Payload.Required[0] == "forged" {
		t.Fatal("lookup mutation changed canonical authority")
	}
	before := spec.SourceValue()
	var projected map[string]any
	if err := before.Project(&projected); err != nil {
		t.Fatal(err)
	}
	projected["platform"] = nil
	if _, err := platform.PlatformVersionFromValue(spec.SourceValue()); err != nil {
		t.Fatalf("source projection mutated authority: %v", err)
	}
	// Reach: reference law survives admission/readback as reference evidence,
	// while altered producer/reference declarations cannot grant that authority.
	const referenceSource = `platform_tables:
  diagnostics_encoding:
    runtime_log_encoding:
      event_name: platform.runtime_log
      produced_by_type: platform
      payload_fields: {message: human-readable}
platform_events:
  catalog:
    platform.runtime_log:
      defined_in: platform_tables.diagnostics_encoding
      schema_authority: platform-spec.yaml#platform_tables.diagnostics_encoding
      producer_evidence: {produced_by_type: platform, produced_by: runtime}
`
	diagnostic := eventtest.DiagnosticDirect("", events.EventTypePlatformRuntimeLog, "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{})
	pinned, err := ParsePlatformSpecDocument([]byte(referenceSource), "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	declaration, authority, err := ResolvePlatformEventAuthority(pinned, diagnostic)
	if err != nil || authority.Reference() != events.RuntimeLogSchemaReference || authority.Digest() == "" || len(declaration.Payload.Properties) != 0 {
		t.Fatalf("reference declaration lost or reinterpreted: %+v err=%v", authority, err)
	}
	if _, invented := EventSchemaRegistryFromCatalog(map[string]EventCatalogEntry{string(diagnostic.Type()): declaration})[string(diagnostic.Type())]; invented {
		t.Fatal("reference-only catalog entry invented a structural schema")
	}
	for name, body := range map[string]string{
		"missing_producer":     strings.Replace(referenceSource, "producer_evidence: {produced_by_type: platform, produced_by: runtime}", "producer_evidence: {}", 1),
		"wrong_producer":       strings.Replace(referenceSource, "produced_by: runtime", "produced_by: foreign", 1),
		"wrong_class":          strings.Replace(referenceSource, "produced_by_type: platform,", "produced_by_type: external,", 1),
		"missing_reference":    strings.Replace(referenceSource, "schema_authority: platform-spec.yaml#platform_tables.diagnostics_encoding", "schema_authority: ''", 1),
		"wrong_reference":      strings.Replace(referenceSource, "schema_authority: platform-spec.yaml#platform_tables.diagnostics_encoding", "schema_authority: another.yaml#platform_tables.diagnostics_encoding", 1),
		"wrong_definition":     strings.Replace(referenceSource, "defined_in: platform_tables.diagnostics_encoding", "defined_in: another_encoding", 1),
		"inline_contradiction": strings.Replace(referenceSource, "defined_in: platform_tables.diagnostics_encoding", "payload: {}\n      defined_in: platform_tables.diagnostics_encoding", 1),
		"wrong_law_event":      strings.Replace(referenceSource, "event_name: platform.runtime_log", "event_name: platform.boot", 1),
	} {
		candidate, err := ParsePlatformSpecDocument([]byte(body), name+".yaml")
		if err != nil {
			continue
		}
		if _, _, err := ResolvePlatformEventAuthority(candidate, diagnostic); err == nil {
			t.Errorf("%s gained reference authority", name)
		}
	}
	changed, err := ParsePlatformSpecDocument([]byte(strings.Replace(referenceSource, "message: human-readable", "message: changed-law", 1)), "changed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, changedAuthority, err := ResolvePlatformEventAuthority(changed, diagnostic)
	if err != nil || changedAuthority.Digest() == authority.Digest() {
		t.Fatal("pinned referenced-law change did not change provenance")
	}
	_, stableAuthority, err := ResolvePlatformEventAuthority(pinned, diagnostic)
	if err != nil || stableAuthority != authority {
		t.Fatal("later law admission mutated earlier pinned provenance")
	}
}

func TestPackPlatformSourceAdmissionBoundsWholeDocument(t *testing.T) {
	var body bytes.Buffer
	body.WriteString("annotation: &base [a, b]\n")
	for i := 1; i < 16; i++ {
		fmt.Fprintf(&body, "annotation_%d: &a%d [*%s, *%s]\n", i, i, sourceAliasName(i-1), sourceAliasName(i-1))
	}
	body.WriteString("platform: {version: 0.7.0}\n")
	if _, err := ParsePlatformSpecDocument(body.Bytes(), "platform-spec.yaml"); err == nil {
		t.Fatal("aggregate alias expansion escaped document budget")
	}
	if _, err := ParsePlatformSpecDocument([]byte("note: &version 0.7.0\nplatform: {version: *version}\n"), "platform-spec.yaml"); err != nil {
		t.Fatal(err)
	}
}

func sourceAliasName(index int) string {
	if index == 0 {
		return "base"
	}
	return fmt.Sprintf("a%d", index)
}

func platformPresenceBody(t testing.TB, body []byte, path []string, state string) []byte {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	parent := doc.Content[0]
	for _, name := range path[:len(path)-1] {
		var next *yaml.Node
		for i := 0; i < len(parent.Content); i += 2 {
			if parent.Content[i].Value == name {
				next = parent.Content[i+1]
				break
			}
		}
		if next == nil {
			t.Fatalf("missing fixture path %v", path)
		}
		parent = next
	}
	name := path[len(path)-1]
	index := -1
	for i := 0; i < len(parent.Content); i += 2 {
		if parent.Content[i].Value == name {
			index = i
			break
		}
	}
	if index < 0 {
		t.Fatalf("missing fixture field %v", path)
	}
	valid := parent.Content[index+1]
	switch state {
	case "missing":
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
	case "merge":
		key := parent.Content[index]
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
		parent.Content = append(parent.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!merge", Value: "<<"}, &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Anchor: "base", Content: []*yaml.Node{key, valid}})
	case "valid":
	default:
		literal := map[string]string{"null": "null", "empty_text": "''", "empty_list": "[]", "empty_map": "{}", "wrong_kind": "true"}[state]
		var replacement yaml.Node
		if err := yaml.Unmarshal([]byte(literal), &replacement); err != nil {
			t.Fatal(err)
		}
		parent.Content[index+1] = replacement.Content[0]
	}
	result, err := yaml.Marshal(&doc)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
