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
	if body != "" {
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
_key: item_id
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
}

func TestEventCatalogAdmissionAcceptsExactlyOneFieldLevelOptionalMarkerAcrossTypeForms(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		wantType string
	}{
		{name: "scalar", field: "value: text?", wantType: "text"},
		{name: "named", field: "value: Customer?", wantType: "Customer"},
		{name: "list scalar metadata", field: "value: {type: '[text]?'}", wantType: "[text]"},
		{name: "map scalar", field: "value: 'map[text]uuid?'", wantType: "map[text]uuid"},
		{name: "mapping scalar", field: "value:\n  type: text?", wantType: "text"},
		{name: "mapping list", field: "value:\n  type: '[Customer]?'", wantType: "[Customer]"},
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
		{name: "sequence element", field: "value:\n  - text?"},
		{name: "list element", field: "value: {type: '[text?]'}"},
		{name: "map key", field: "value: 'map[text?]uuid'"},
		{name: "map value", field: "value: 'map[text]u?uid?'"},
		{name: "internal scalar", field: "value: te?xt"},
		{name: "mapping list element", field: "value:\n  type: '[Customer?]'"},
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
		{name: "malformed required metadata", source: "id: text\nrequired: {fields: [id]}", wantErr: `field "fields" is not supported`},
		{name: "malformed required metadata through merge", source: "<<: &legacy\n  required: {fields: [id]}\nid: text", wantErr: `field "fields" is not supported`},
		{name: "duplicate direct", source: "id: text\nid: uuid", wantErr: "duplicate effective field \"id\""},
		{name: "duplicate through merge", source: "<<: &base\n  id: text\nid: uuid", wantErr: "duplicate effective field \"id\""},
		{name: "double optional marker", source: "id: text??", wantErr: "exactly one trailing ?"},
		{name: "optional business key", source: "_key: id\nid: uuid?", wantErr: "must be required"},
		{name: "missing business key field", source: "_key: id\nname: text", wantErr: "is not a declared payload field"},
		{name: "null declaration", source: "null", wantErr: "an event with no fields is declared bare"},
		{name: "unknown swarm metadata", source: "swarm:\n  producerr: external", wantErr: `field "producerr" is not supported`},
		{name: "unknown swarm metadata through merge", source: "swarm:\n  <<: &metadata\n    producerr: external", wantErr: `field "producerr" is not supported`},
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
	bundle := retainedLocationFixture(t, "events.yaml", "test.event:\n  id: &kind uuid\n  correlation_id: *kind\n")
	location, found := bundle.IntroductionLocation("events.yaml", `["test.event"].correlation_id`, false)
	if !found || location.Line != 3 {
		t.Fatalf("alias provenance line = %d, want authored alias use on line 3", location.Line)
	}
	catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{
		"Record": {Fields: map[string]TypeFieldSpec{"id": {Type: "uuid"}, "note": {Type: "text", IsOptional: true}}},
		"Other":  {Fields: map[string]TypeFieldSpec{"id": {Type: "uuid"}}},
		"Empty":  {Fields: map[string]TypeFieldSpec{}},
	}, Enums: map[string]EnumTypeDecl{"Status": {Values: []string{"open"}}}}
	for _, body := range []string{"_embed: [Record]\n", "_embed: &records [Record]\n"} {
		snapshot, err := yamlsource.Load([]byte("test.event:\n  " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n  ") + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"), catalog)
		if err != nil {
			t.Fatal(err)
		}
		payload := entries["test.event"].Payload
		if len(payload.Properties) != 2 || payload.Properties["id"].Type != "uuid" || payload.Properties["note"].Type != "text" || !slices.Equal(payload.Required, []string{"id"}) {
			t.Fatalf("embedded types/optionality changed: %#v", payload)
		}
		stateSource, err := yamlsource.Load([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		state, err := projectStateContract(stateSource.Document("state.yaml").Root(), catalog)
		if err != nil || len(state.Fields) != 2 || state.Fields["id"].Type != "uuid" || !state.Fields["note"].IsOptional {
			t.Fatalf("state/event embedded semantics differ: %#v (%v)", state, err)
		}
	}
	for _, body := range []string{"_embed: [Empty]\n", "_embed: [Empty]\nown: text\n", "_embed: [Record, Empty]\n", "aliases: &records [Empty]\n_embed: *records\n", "_embed: [Record, Record]\n", "_embed: [Record, Other]\n", "_embed: [Record]\nid: text\n", "_embed: [Status]\n", "_embed: [missing]\n", "_embed: []\n", "_embed: Record\n", "_embed: null\n"} {
		for _, family := range []string{"state", "event"} {
			t.Run(family+"_"+body, func(t *testing.T) {
				snapshot, err := yamlsource.Load([]byte(body))
				if err != nil {
					t.Fatal(err)
				}
				if family == "state" {
					_, err = projectStateContract(snapshot.Document("state.yaml").Root(), catalog)
				} else {
					snapshot, err = yamlsource.Load([]byte("test.event:\n  " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n  ") + "\n"))
					if err == nil {
						_, err = admitEventCatalogDocument(snapshot.Document("events.yaml"), catalog)
					}
				}
				if err == nil || !strings.Contains(err.Error(), family+"s.yaml:") && !strings.Contains(err.Error(), "state.yaml:") {
					t.Fatalf("embedding must refuse with authored location: %v", err)
				}
			})
		}
	}
	t.Run("detached definition and use", func(t *testing.T) {
		source, err := yamlsource.Load([]byte("Record:\n  id: uuid\n  note: text?\nOther:\n  id: text\nEmpty: {}\n"))
		if err != nil {
			t.Fatal(err)
		}
		authored, err := projectTypeCatalogDocument(source.Document("child/types.yaml").Root())
		if err != nil {
			t.Fatal(err)
		}
		for _, body := range []string{"_embed: [Record, Other]\n", "_embed: [Record]\nid: text\n", "_embed: [Empty]\nown: text\n"} {
			use, err := yamlsource.Load([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			_, err = projectStateContract(use.Document("child/state.yaml").Root(), authored)
			if err == nil || !strings.Contains(err.Error(), "child/types.yaml:") || !strings.Contains(err.Error(), "child/state.yaml:") {
				t.Fatalf("definition/use missing: %v", err)
			}
			if strings.Contains(body, "Other") && (!strings.Contains(err.Error(), "child/types.yaml:2:") || !strings.Contains(err.Error(), "child/types.yaml:5:")) {
				t.Fatalf("collision lost one defining field: %v", err)
			}
		}
		use, err := yamlsource.Load([]byte("aliases: &records [Record]\n_embed: *records\n"))
		if err != nil {
			t.Fatal(err)
		}
		state, err := projectStateContract(use.Document("child/state.yaml").Root(), authored)
		if err != nil {
			t.Fatal(err)
		}
		index, definition, ok := state.Fields["id"].EmbeddedSource()
		if !ok || index != 0 || definition.File != "child/types.yaml" || definition.Line != 2 || state.Fields["id"].origin.use.Line != 2 {
			t.Fatalf("expanded provenance = %#v", state.Fields["id"].origin)
		}
		copy := cloneEntityContract(state)
		copy.Fields["id"] = EntityFieldDecl{Type: "integer"}
		if state.Fields["id"].Type != "uuid" || state.Fields["id"].origin.definition != definition {
			t.Fatal("readback changed retained field/provenance")
		}
	})
}

func TestEventCatalogAdmissionRetainsAliasAndMergeIntroductionLocations(t *testing.T) {
	for _, tc := range []struct {
		name     string
		source   string
		wantLine int
	}{
		{
			name: "nested mapping alias",
			source: `
defaults: &defaults
  type: text
value: *defaults
`,
			wantLine: 4,
		},
		{
			name: "merge alias",
			source: `
defaults: &defaults
  type: text
value:
  <<: *defaults
`,
			wantLine: 5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry, err := admitEventCatalogEntryForTest(t, tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if entry.Payload.Properties["value"].Type != "text" {
				t.Fatal("alias/merge changed the admitted field type")
			}
			body := "test.event:\n  " + strings.ReplaceAll(strings.TrimSpace(tc.source), "\n", "\n  ") + "\n"
			bundle := retainedLocationFixture(t, "events.yaml", body)
			location, found := bundle.IntroductionLocation("events.yaml", `["test.event"].value.type`, false)
			if !found || location.Line != tc.wantLine {
				t.Fatalf("provenance line = %d, want authored introduction on line %d", location.Line, tc.wantLine)
			}
		})
	}

	snapshot, err := yamlsource.Load([]byte("<<: &defaults\n  test.event:\n"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, found := entries["test.event"]; !found {
		t.Fatal("merge lost its event declaration")
	}
	bundle := retainedLocationFixture(t, "events.yaml", "<<: &defaults\n  test.event:\n")
	location, found := bundle.IntroductionLocation("events.yaml", `["test.event"]`, true)
	if !found || location.Line != 1 {
		t.Fatalf("merged declaration provenance line = %d, want authored merge occurrence on line 1", location.Line)
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
		{name: "surrounding whitespace", source: "item:\n\" item \": {}", wantErr: `event declaration name " item "`},
		{name: "leading slash", source: `"/item": {}`, wantErr: `event declaration name "/item"`},
		{name: "uppercase and space", source: `"Item Ready": {}`, wantErr: `event declaration name "Item Ready"`},
		{name: "hyphenated event token", source: `"addon-a.start":`, wantErr: `event declaration name "addon-a.start"`},
		{name: "unsupported partial glob", source: `"item.pre*":`, wantErr: `supported wildcard pattern`},
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

	snapshot, err := yamlsource.Load([]byte("item.created:\n'item.*':\n'completion.*':\n'*/order.completed':\n'**/item.processed':\n"))
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

func TestEventCatalogBusinessKeyRejectsDynamicSemanticsAtBundleAdmission(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := currentWorkflowContractsDirForTest(t)
	writeFixtureFile(t, root+"/events.yaml", `
item.created:
  _key: payload
  payload: json
evidence.recorded:
`)
	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !contractErrorContains(err, "business key field \"payload\" must have boolean, number, or string semantics") {
		t.Fatalf("load error = %v, want non-scalar business-key rejection", err)
	}
}
