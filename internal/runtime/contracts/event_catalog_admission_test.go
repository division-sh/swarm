package contracts

import (
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func admitEventCatalogEntryForTest(t testing.TB, source string) (EventCatalogEntry, error) {
	t.Helper()
	body := strings.TrimSpace(source)
	if body == "" {
		body = "{}"
	} else {
		body = "\n  " + strings.ReplaceAll(body, "\n", "\n  ")
	}
	snapshot, err := yamlsource.Load([]byte("test.event:" + body + "\n"))
	if err != nil {
		return EventCatalogEntry{}, err
	}
	entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"))
	if err != nil {
		return EventCatalogEntry{}, err
	}
	return entries["test.event"], nil
}

func TestEventCatalogAdmissionOwnsRequiredByDefaultOptionalityAndBusinessKey(t *testing.T) {
	entry, err := admitEventCatalogEntryForTest(t, `
key: item_id
item_id: uuid
note: text?
`)
	if err != nil {
		t.Fatal(err)
	}
	if entry.BusinessKeyField != "item_id" || len(entry.Payload.Required) != 1 || entry.Payload.Required[0] != "item_id" {
		t.Fatalf("entry key/required = %q/%v", entry.BusinessKeyField, entry.Payload.Required)
	}
	if entry.Payload.Properties["note"].Type != "text" {
		t.Fatalf("optional marker leaked into effective type: %#v", entry.Payload.Properties["note"])
	}
	if provenance := entry.admissionProvenance["fields.item_id.is_optional"]; provenance.Origin != EffectiveValueOriginDerived || provenance.RuleID != eventRequiredByDefaultRule {
		t.Fatalf("required-by-default provenance = %#v", provenance)
	}
}

func TestEventCatalogAdmissionAcceptsExactlyOneFieldLevelOptionalMarkerAcrossTypeForms(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		wantType string
	}{
		{name: "scalar", field: "value: text?", wantType: "text"},
		{name: "named", field: "value: Customer?", wantType: "Customer"},
		{name: "list scalar", field: "value: '[text]?'", wantType: "[text]"},
		{name: "map scalar", field: "value: 'map[text]uuid?'", wantType: "map[text]uuid"},
		{name: "mapping scalar", field: "value:\n  type: text?", wantType: "text"},
		{name: "mapping list", field: "value:\n  type: list?\n  of: Customer", wantType: "[Customer]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, err := admitEventCatalogEntryForTest(t, tc.field)
			if err != nil {
				t.Fatal(err)
			}
			if got := entry.Payload.Properties["value"].Type; got != tc.wantType {
				t.Fatalf("type = %q, want %q", got, tc.wantType)
			}
			if slices.Contains(entry.Payload.Required, "value") {
				t.Fatalf("optional value appears in required fields: %v", entry.Payload.Required)
			}
		})
	}
}

func TestEventCatalogAdmissionRejectsEveryNonFieldLevelOptionalMarker(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "sequence element", field: "value: ['text?']"},
		{name: "list element", field: "value: '[text?]'"},
		{name: "map key", field: "value: 'map[text?]uuid'"},
		{name: "map value", field: "value: 'map[text]u?uid?'"},
		{name: "internal scalar", field: "value: te?xt"},
		{name: "mapping list element", field: "value:\n  type: list\n  of: Customer?"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitEventCatalogEntryForTest(t, tc.field)
			if err == nil || !strings.Contains(err.Error(), "exactly one trailing ?") {
				t.Fatalf("error = %v, want closed optional-marker rejection", err)
			}
		})
	}
}

func TestEventCatalogAdmissionRejectsRetiredAndAmbiguousSyntax(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{name: "retired required", source: "id: text\nrequired: [id]", wantErr: "RETIRED: events.yaml field required"},
		{name: "retired required through merge", source: "<<: &legacy\n  required: [id]\nid: text", wantErr: "RETIRED: events.yaml field required"},
		{name: "duplicate direct", source: "id: text\nid: uuid", wantErr: "duplicate effective field \"id\""},
		{name: "duplicate through merge", source: "<<: &base\n  id: text\nid: uuid", wantErr: "duplicate effective field \"id\""},
		{name: "double optional marker", source: "id: text??", wantErr: "exactly one trailing ?"},
		{name: "optional business key", source: "key: id\nid: uuid?", wantErr: "must be required"},
		{name: "missing business key field", source: "key: id\nname: text", wantErr: "is not a declared payload field"},
		{name: "null declaration", source: "null", wantErr: "want mapping"},
		{name: "unknown swarm metadata", source: "swarm:\n  producerr: external", wantErr: `event swarm metadata field "producerr" is not supported`},
		{name: "unknown swarm metadata through merge", source: "swarm:\n  <<: &metadata\n    producerr: external", wantErr: `event swarm metadata field "producerr" is not supported`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitEventCatalogEntryForTest(t, tc.source)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestEventCatalogAdmissionRetainsAliasValues(t *testing.T) {
	entry, err := admitEventCatalogEntryForTest(t, "id: &kind uuid\ncorrelation_id: *kind")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Payload.Properties["id"].Type != "uuid" || entry.Payload.Properties["correlation_id"].Type != "uuid" {
		t.Fatalf("alias projection = %#v", entry.Payload.Properties)
	}
	provenance := entry.admissionProvenance["fields.correlation_id.type"]
	if provenance.SourceLine != 3 {
		t.Fatalf("alias provenance line = %d, want authored alias use on line 3", provenance.SourceLine)
	}
}

func TestEventCatalogAdmissionOwnsDeclarationIdentityBeforeMapInsertion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		source  string
		wantErr string
	}{
		{name: "empty", source: `"": {}`, wantErr: `event declaration name ""`},
		{name: "whitespace only", source: `"   ": {}`, wantErr: `event declaration name "   "`},
		{name: "surrounding whitespace", source: "item: {}\n\" item \": {}", wantErr: `event declaration name " item "`},
		{name: "leading slash", source: `"/item": {}`, wantErr: `event declaration name "/item"`},
		{name: "uppercase and space", source: `"Item Ready": {}`, wantErr: `event declaration name "Item Ready"`},
		{name: "hyphenated event token", source: `"addon-a.start": {}`, wantErr: `event declaration name "addon-a.start"`},
		{name: "unsupported partial glob", source: `"item.pre*": {}`, wantErr: `supported wildcard pattern`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.source + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = admitEventCatalogDocument(snapshot.Document("events.yaml"))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "events.yaml:") {
				t.Fatalf("admission error = %v, want %q with key location", err, tc.wantErr)
			}
		})
	}

	snapshot, err := yamlsource.Load([]byte("item.created: {}\n'item.*': {}\n'*.completed': {}\n'*/order.completed': {}\n'**/item.processed': {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("admitted declarations = %#v", entries)
	}
}

func TestEventCatalogAdmissionRecordsExactNestedValueProvenance(t *testing.T) {
	entry, err := admitEventCatalogEntryForTest(t, `
swarm:
  note: delivery contract
value:
  type: text?
  description: Human-readable value
  pattern: ^[a-z]+$
  length:
    min: 1
    max: 20
  citation:
    criteria: source field
    allowed_classes: [public]
`)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{
		"fields.value.type",
		"fields.value.is_optional",
		"fields.value.description",
		"fields.value.refinements.pattern",
		"fields.value.refinements.length.min",
		"fields.value.refinements.length.max",
		"fields.value.citation.criteria",
		"fields.value.citation.allowed_classes",
		"metadata.swarm.note",
	}
	for _, path := range paths {
		provenance, ok := entry.admissionProvenance[path]
		if !ok || provenance.SourceFile != "events.yaml" || provenance.SourceLine <= 0 || provenance.SourceColumn <= 0 {
			t.Fatalf("provenance %s = %#v, found=%t", path, provenance, ok)
		}
	}
	typeSource := entry.admissionProvenance["fields.value.type"]
	descriptionSource := entry.admissionProvenance["fields.value.description"]
	if typeSource.SourceLine == descriptionSource.SourceLine {
		t.Fatalf("mapping type provenance points at outer field instead of nested type: type=%#v description=%#v", typeSource, descriptionSource)
	}
}

func TestEventCatalogBusinessKeyRejectsDynamicSemanticsAtBundleAdmission(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := currentWorkflowContractsDirForTest(t)
	writeFixtureFile(t, root+"/events.yaml", `
item.created:
  key: payload
  payload: json
evidence.recorded: {}
`)
	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !contractErrorContains(err, "business key field \"payload\" must have boolean, number, or string semantics") {
		t.Fatalf("load error = %v, want non-scalar business-key rejection", err)
	}
}
