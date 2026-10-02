package packmodel

import (
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
