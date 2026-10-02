package scenariodocument

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

const ordinary = "steps: [{publish: request, payload: {id: example}}]\n"
const derived = "name: profile\nderive: {flow: '.', input: request, payload: {generate: true}}\n"

func TestAdmissionRejectsRetiredAndConditionalForms(t *testing.T) {
	for _, value := range []string{"1", "2", "'1'", "v1", "null", "''", "{}", "[]"} {
		for _, base := range []string{ordinary, derived} {
			if _, err := Admit([]byte("version: "+value+"\n"+base), "tests/version.yaml"); err == nil || !strings.Contains(err.Error(), "remove `version`; the scenario format has one version") {
				t.Fatalf("version %s: %v", value, err)
			}
		}
	}
	for _, value := range []string{"null", "''", "7", "{}", "[]", "{tool: {ok: true}}", "[witness]"} {
		if _, err := Admit([]byte(ordinary+"connector_responses: "+value+"\n"), "tests/witness.yaml"); err == nil || !strings.Contains(err.Error(), "connector_responses requires derive") {
			t.Fatalf("non-derived witness %s: %v", value, err)
		}
	}
	for _, raw := range []string{
		"vars: {witness: &witness {tool: {ok: true}}}\n" + ordinary + "connector_responses: *witness\n",
		"vars: {fields: &fields {connector_responses: null}}\n" + ordinary + "<<: *fields\n",
	} {
		if _, err := Admit([]byte(raw), "tests/introduced-witness.yaml"); err == nil || !strings.Contains(err.Error(), "connector_responses requires derive") {
			t.Fatalf("introduced non-derived witness: %v", err)
		}
	}
	for _, row := range []struct{ raw, want string }{
		{ordinary + "connector_responses: {}\n", "connector_responses requires derive"},
		{ordinary + "derive: null\n", "mutually exclusive"},
		{derived + "expect: 7\n", "expect must be a mapping"},
		{derived + "setup: 7\n", "setup must be a mapping"},
		{derived + "vars: 7\n", "vars must be a mapping"},
		{strings.Replace(derived, "name: profile\n", "", 1), "name is required"},
		{"steps: [{mailbox.decide: {match: 7, verdict: approve}}]", "must be a mapping"},
		{"steps: [{mailbox.decide: {match: {anchor_kind: stage_gate}, verdict: approve, until: null}}]", "unsupported"},
		{"steps: [{mailbox.defer: {match: {anchor_kind: stage_gate}, until: tomorrow, fields: null}}]", "unsupported"},
		{"steps: [{publish: 7, payload: {}}]", "publish must be text"},
		{ordinary + "invalid: {base: {publish: request, payload: {}}, cases: [{expect: null}]}\n", "remove `expect`"},
		{ordinary + "invalid: {base: {publish: request, payload: {}, discarded: null}, cases: [{}]}\n", "unsupported"},
		{"steps: [{publish: request, payload: {set: {id: a, payload.id: b}}}]", "overlap"},
		{"steps: [{publish: request, payload: {set: {a: {}, a.b: b}}}]", "overlap"},
	} {
		_, err := Admit([]byte(row.raw), "tests/negative.yaml")
		if err == nil || !strings.Contains(err.Error(), row.want) {
			t.Fatalf("%s: got %v, want %s", row.raw, err, row.want)
		}
	}
}

func TestAdmissionRetainsExactValuesAndDefensiveProjections(t *testing.T) {
	document, err := Admit([]byte(`vars:
  original: &object {id: exact, ' id ': spaced, nested: [null, 4.5]}
setup: {entities: [{as: item, type: task, fields: *object, gates: {approved: true}}]}
steps:
  - publish: request
    payload: {<<: *object}
  - mailbox.decide: {match: {anchor_kind: stage_gate}, verdict: approve, fields: *object}
expect: {events: {exact: []}, entities: [{ref: item, fields: {<<: *object}}]}
`), "tests/aliased.yaml")
	if err != nil {
		t.Fatal(err)
	}
	first, err := document.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if first.Expect.Empty() || first.Expect.Events.Exact == nil {
		t.Fatal("empty exact presence erased")
	}
	want := map[string]any{"id": "exact", " id ": "spaced", "nested": []any{nil, float64(4.5)}}
	if !reflect.DeepEqual(first.Steps[0].Payload, want) {
		t.Fatalf("payload = %#v", first.Steps[0].Payload)
	}
	for name, value := range map[string]any{"vars": first.Vars["original"], "setup fields": first.Setup.Entities[0].Fields, "verdict fields": first.Steps[1].Fields, "assertion fields": first.Expect.Entities[0].Fields} {
		if !reflect.DeepEqual(value, want) {
			t.Fatalf("%s lost literal data: %#v", name, value)
		}
	}
	first.Steps[0].Payload.(map[string]any)["id"] = "mutated"
	first.Vars["original"].(map[string]any)["nested"].([]any)[1] = int64(9)
	second, err := document.Projection()
	if err != nil || !reflect.DeepEqual(second.Steps[0].Payload, want) {
		t.Fatalf("mutable document: %#v, %v", second, err)
	}
	if document.SourceValue().Location().File != "tests/aliased.yaml" {
		t.Fatalf("missing source coordinate: %#v", document.SourceValue().Location())
	}
	derivedDocument, err := Admit([]byte("name: profile\nvars: {original: &object {id: exact, ' id ': spaced, nested: [null, 4.5]}}\nderive: {flow: '.', input: request, payload: {generate: true, set: {data: *object}}}\nconnector_responses: {tool: {<<: *object}}\n"), "tests/derived-alias.yaml")
	if err != nil {
		t.Fatal(err)
	}
	projection, err := derivedDocument.Projection()
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"derived overlay": projection.Derive.Set["data"], "response witness": projection.Derive.ConnectorResponses["tool"]} {
		if !reflect.DeepEqual(value, want) {
			t.Fatalf("%s lost literal data: %#v", name, value)
		}
	}
}

func TestAdmissionUsesBoundedSourceAndSemanticNumberOwners(t *testing.T) {
	for _, raw := range []string{
		ordinary + "---\n" + ordinary,
		"steps: [{publish: request, payload: {id: one, id: two}}]",
		"vars: {a: &a [*a]}\n" + ordinary,
		"vars: {a: &a {id: one}}\nsteps: [{publish: request, payload: {<<: *a, id: two}}]",
		"steps: [{publish: request, payload: {1: value}}]",
		"steps: [{publish: request, payload: {id: 9007199254740993}}]",
		"steps: [{publish: request, payload: {id: 1e-999}}]",
		"steps: [{publish: request, payload: {id: .nan}}]",
		"steps: [{publish: request, payload: {id: -0}}]",
		"steps: [{publish: request, payload: {id: 2026-10-02}}]",
		" steps : []",
		"[]", "null", "", "steps: []", "steps: null",
	} {
		if _, err := Admit([]byte(raw), "tests/boundary.yaml"); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	fixture, err := AdmitFixture([]byte(`{"id":"json","n":9007199254740991}`), "tests/fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	value, err := workflowexpr.ProjectSemanticValue(fixture)
	if err != nil || value.(map[string]any)["n"] != int64(9007199254740991) {
		t.Fatalf("number = %#v %v", value, err)
	}
	if _, err := AdmitFixture([]byte(`{"id":1,"id":2}`), "tests/fixture.json"); err == nil {
		t.Fatal("duplicate JSON fixture accepted")
	}
}

func TestDiscoveryAndAdmissionShareCandidatePolicy(t *testing.T) {
	for _, raw := range []string{ordinary, derived} {
		_, found, err := Discover([]byte(raw), "tests/candidate.yaml")
		if err != nil || !found {
			t.Fatalf("candidate: %v %v", found, err)
		}
	}
	for _, raw := range []string{"name: fixture\n", "id: fixture\n", "[one, two]\n"} {
		_, found, err := Discover([]byte(raw), "tests/resource.yaml")
		if err != nil || found {
			t.Fatalf("resource: %v %v", found, err)
		}
	}
	for _, raw := range []string{"steps: null\n", "derive: null\n", "version: null\n", "invalid: null\n", "id: one\n---\nid: two\n"} {
		if _, _, err := Discover([]byte(raw), "tests/malformed.yaml"); err == nil {
			t.Fatalf("ignored malformed resource %s", raw)
		}
	}
}

func TestScenarioAdmissionEnforcesAggregateExpansionAndIntroducedCoordinates(t *testing.T) {
	var raw strings.Builder
	raw.WriteString("vars:\n  a0: &a0 [x, x]\n")
	for i := 1; i <= 18; i++ {
		fmt.Fprintf(&raw, "  a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
	}
	raw.WriteString(ordinary)
	if _, err := Admit([]byte(raw.String()), "tests/expanded.yaml"); err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT") {
		t.Fatalf("aggregate expansion accepted: %v", err)
	}
	document, err := Admit([]byte("vars: {base: &base {id: value}}\nsteps: [{publish: request, payload: {<<: *base}}]\n"), "tests/introduced.yaml")
	if err != nil {
		t.Fatal(err)
	}
	steps, err := document.SourceValue().Lookup("steps")
	if err != nil {
		t.Fatal(err)
	}
	items, err := steps.Value.Sequence()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := items[0].Lookup("payload")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := payload.Value.Mapping()
	if err != nil || len(fields) != 1 {
		t.Fatalf("merged fields: %#v %v", fields, err)
	}
	if fields[0].Value.Location().File != "tests/introduced.yaml" || fields[0].Value.Location().Line != 1 {
		t.Fatalf("lost original value coordinate: %#v", fields[0].Value.Location())
	}
}
