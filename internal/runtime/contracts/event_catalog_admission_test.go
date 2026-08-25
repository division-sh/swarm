package contracts

import (
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
