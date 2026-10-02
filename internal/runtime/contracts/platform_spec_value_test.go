package contracts

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

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
}

func TestPackPlatformProvenanceComposesWithoutOverwrite(t *testing.T) {
	bundle, err := loadSchemaFragment(t, "name: provenance\nstages: []\n")
	if err != nil {
		t.Fatal(err)
	}
	before := bundle.EffectiveProvenance().Entries()
	for _, path := range []string{"platform.platform.version", "platform.interfaces[\"swarm.hitl-channel\"].v2.kind", "packs.platform_membership.version", "packs[\"provider.telegram\"].envelope.id", "schemas[\".\"].name"} {
		if proof, ok := bundle.EffectiveProvenance().Lookup(path); !ok || proof.SourceLine == 0 || proof.SourcePresence != "scalar" {
			t.Fatalf("source evidence missing for %s: %#v", path, proof)
		}
	}
	if err := populateEffectiveProvenance(bundle); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, bundle.EffectiveProvenance().Entries()) {
		t.Fatal("ledger overwritten or duplicated")
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
