package packmodel

import (
	"gopkg.in/yaml.v3"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

const admittedEnvelopeFixture = `id: provider.demo
version: 0.1.0
platform_version: '>=0.7.0 <0.8.0'
type: trigger
manifest_hash: derived
provenance: {source: project}
capabilities:
  can: {receive_https_route: demo, emit_events: [inbound.demo], persist_dedupe_markers: true}
  cannot: [emit_undeclared_events]
requires: {}
tests: [demo]
`

func TestEnvelopeAdmissionPreservesTypedPresence(t *testing.T) {
	for _, test := range []struct{ name, old, replacement, path string }{
		{"boolean id", "id: provider.demo", "id: true", "id"},
		{"numeric version", "version: 0.1.0", "version: 1", "version"},
		{"numeric platform version", "platform_version: '>=0.7.0 <0.8.0'", "platform_version: 1.5", "platform_version"},
		{"null provenance", "provenance: {source: project}", "provenance: null", "provenance"},
		{"missing provenance source", "provenance: {source: project}", "provenance: {}", "source"},
		{"null capabilities", "capabilities:\n  can: {receive_https_route: demo, emit_events: [inbound.demo], persist_dedupe_markers: true}\n  cannot: [emit_undeclared_events]", "capabilities: null", "capabilities"},
		{"null requires", "requires: {}", "requires: null", "requires"},
		{"null tests", "tests: [demo]", "tests: null", "tests"},
		{"missing tests", "tests: [demo]\n", "", "tests"},
		{"empty test", "tests: [demo]", "tests: ['']", "tests"},
		{"numeric test", "tests: [demo]", "tests: [7]", "tests"},
		{"text boolean", "persist_dedupe_markers: true", "persist_dedupe_markers: 'true'", "persist_dedupe_markers"},
		{"fractional boolean", "persist_dedupe_markers: true", "persist_dedupe_markers: 1.5", "persist_dedupe_markers"},
		{"inactive false member", "persist_dedupe_markers: true", "persist_dedupe_markers: true, lower_through_activity: false", "lower_through_activity"},
		{"inactive empty sequence", "persist_dedupe_markers: true", "persist_dedupe_markers: true, call_provider_actions: []", "call_provider_actions"},
		{"inactive implements", "requires: {}", "requires: {}\nimplements: []", "implements"},
		{"duplicate id", "id: provider.demo", "id: provider.demo\nid: provider.other", "id"},
		{"null optional secrets", "requires: {}", "requires: {secrets: null}", "secrets"},
		{"numeric dependency", "requires: {}", "requires: {packs: {trigger: 1}}", "trigger"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := strings.Replace(admittedEnvelopeFixture, test.old, test.replacement, 1)
			_, err := ParseEnvelopeAt([]byte(body), "packs/demo/pack.yaml")
			if err == nil || !strings.Contains(err.Error(), test.path) || !strings.Contains(err.Error(), "packs/demo/pack.yaml:") {
				t.Fatalf("want contextual rejection of %s, got %v", test.path, err)
			}
		})
	}
}

func TestPackEnvelopeAdmissionPresenceMatrix(t *testing.T) {
	body := strings.Replace(admittedEnvelopeFixture, "persist_dedupe_markers: true", "persist_dedupe_markers: true, verify_secret: header.secret", 1)
	body = strings.Replace(body, "requires: {}", "requires: {secrets: [demo], managed_credentials: [demo], packs: {trigger: provider.demo}}", 1)
	for _, row := range []struct {
		path                          string
		optional, emptyMap, emptyList bool
	}{
		{"id", false, false, false}, {"version", false, false, false}, {"platform_version", false, false, false}, {"type", false, false, false}, {"manifest_hash", false, false, false},
		{"provenance", false, false, false}, {"provenance.source", false, false, false}, {"capabilities", false, false, false}, {"capabilities.can", false, false, false}, {"capabilities.cannot", false, false, false},
		{"capabilities.can.receive_https_route", false, false, false}, {"capabilities.can.verify_secret", true, false, false}, {"capabilities.can.emit_events", false, false, false}, {"capabilities.can.persist_dedupe_markers", false, false, false},
		{"requires", false, true, false}, {"requires.secrets", true, false, true}, {"requires.managed_credentials", true, false, true}, {"requires.packs", true, true, false}, {"requires.packs.trigger", true, false, false}, {"tests", false, false, false},
	} {
		for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
			t.Run(row.path+"/"+state, func(t *testing.T) {
				_, err := ParseEnvelopeAt(envelopePresenceBody(t, []byte(body), row.path, state), "packs/demo/pack.yaml")
				want := state == "valid" || state == "merge" || state == "missing" && row.optional || state == "empty_map" && row.emptyMap || state == "empty_list" && row.emptyList
				if (err == nil) != want {
					t.Fatalf("want admission=%t: %v", want, err)
				}
			})
		}
	}
	for _, kind := range []string{TypeConnector, TypeChannel} {
		branch := strings.Replace(admittedEnvelopeFixture, "type: trigger", "type: "+kind, 1)
		can := "{call_provider_actions: [demo.call], lower_through_activity: true, journal_activity_attempts: true}"
		if kind == TypeChannel {
			can = "{}"
			branch += "implements: [sample/v1]\n"
		}
		branch = strings.Replace(branch, "{receive_https_route: demo, emit_events: [inbound.demo], persist_dedupe_markers: true}", can, 1)
		paths := []string{"capabilities.can.call_provider_actions", "capabilities.can.lower_through_activity", "capabilities.can.journal_activity_attempts"}
		if kind == TypeChannel {
			paths = []string{"implements"}
		}
		for _, field := range paths {
			for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
				t.Run(kind+"/"+field+"/"+state, func(t *testing.T) {
					_, err := ParseEnvelopeAt(envelopePresenceBody(t, []byte(branch), field, state), "packs/demo/pack.yaml")
					if (err == nil) != (state == "valid" || state == "merge") {
						t.Fatalf("branch admission differs: %v", err)
					}
				})
			}
		}
	}
}

func envelopePresenceBody(t testing.TB, body []byte, path, state string) []byte {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	parts, parent := strings.Split(path, "."), doc.Content[0]
	for _, name := range parts[:len(parts)-1] {
		var next *yaml.Node
		for i := 0; i < len(parent.Content); i += 2 {
			if parent.Content[i].Value == name {
				next = parent.Content[i+1]
				break
			}
		}
		if next == nil {
			t.Fatalf("missing fixture path %s", path)
		}
		parent = next
	}
	index := -1
	for i := 0; i < len(parent.Content); i += 2 {
		if parent.Content[i].Value == parts[len(parts)-1] {
			index = i
			break
		}
	}
	if index < 0 {
		t.Fatalf("missing fixture field %s", path)
	}
	switch state {
	case "missing":
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
	case "merge":
		key, value := parent.Content[index], parent.Content[index+1]
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
		parent.Content = append(parent.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!merge", Value: "<<"}, &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{key, value}})
	case "valid":
	default:
		var value yaml.Node
		if err := yaml.Unmarshal([]byte(map[string]string{"null": "null", "empty_text": "''", "empty_list": "[]", "empty_map": "{}", "wrong_kind": "1.5"}[state]), &value); err != nil {
			t.Fatal(err)
		}
		parent.Content[index+1] = value.Content[0]
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEnvelopeAdmissionRetainsAliasSourceAndExactText(t *testing.T) {
	body := strings.Replace(admittedEnvelopeFixture, "id: provider.demo", "id: &identity 'provider.demo'", 1)
	body = strings.Replace(body, "tests: [demo]", "tests: [*identity]", 1)
	envelope, err := ParseEnvelopeAt([]byte(body), "packs/demo/pack.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Tests[0] != "provider.demo" || envelope.Requires.Packs != nil {
		t.Fatalf("bad projection: %#v", envelope)
	}
	tests, err := envelope.SourceValue().Lookup("tests")
	if err != nil {
		t.Fatal(err)
	}
	values, err := tests.Value.Sequence()
	if err != nil {
		t.Fatal(err)
	}
	scalar, err := values[0].Scalar()
	if err != nil {
		t.Fatal(err)
	}
	if scalar.Location.File != "packs/demo/pack.yaml" || scalar.Location.Line == scalar.ResolvedLocation.Line {
		t.Fatalf("alias occurrence and definition coordinates lost: %#v", scalar)
	}
	var projected []string
	if err := tests.Value.Project(&projected); err != nil {
		t.Fatal(err)
	}
	projected[0] = "mutated"
	again, _ := tests.Value.Sequence()
	scalar, _ = again[0].Scalar()
	if scalar.Value != "provider.demo" || tests.Value.Presence() != yamlsource.PresenceSequence {
		t.Fatal("source authority is caller mutable")
	}
}
