package contracts

import (
	"strings"
	"testing"
)

func TestEventCatalogPayloadFieldSupportsEveryWave1Shape(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		wantType    string
		wantPattern string
		wantCite    string
	}{
		{name: "scalar", yaml: "payload: json\n", wantType: "json"},
		{name: "singleton list", yaml: "payload: [json]\n", wantType: "[json]"},
		{name: "mapping", yaml: "payload: {type: json, description: raw provider envelope}\n", wantType: "json"},
		{name: "mapping list", yaml: "payload: {type: list, of: json}\n", wantType: "[json]"},
		{
			name: "mapping refinements and citation",
			yaml: `payload:
  type: text
  description: provider payload
  pattern: "^[a-z]+$"
  citation:
    criteria: provider_contract
    allowed_classes: [raw]
`,
			wantType: "text", wantPattern: "^[a-z]+$", wantCite: "provider_contract",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, err := admitEventCatalogEntryForTest(t, tc.yaml)
			if err != nil {
				t.Fatalf("admit event catalog entry: %v", err)
			}
			field, ok := entry.Payload.Properties["payload"]
			if !ok {
				t.Fatal("canonical payload property was discarded")
			}
			if field.Type != tc.wantType || field.Refinements.Pattern != tc.wantPattern || field.Citation.Criteria != tc.wantCite {
				t.Fatalf("payload field = %#v", field)
			}
		})
	}
}

func TestEventCatalogPayloadFieldDistinguishesRetiredBlocksFromMalformedFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{name: "properties block", yaml: "payload:\n  properties:\n    message: json\n", wantErr: "RETIRED: nested events.yaml payload blocks"},
		{name: "child field block", yaml: "payload:\n  message: json\n", wantErr: "RETIRED: nested events.yaml payload blocks"},
		{name: "missing type", yaml: "payload:\n  description: raw provider envelope\n", wantErr: "event payload field type is required"},
		{name: "unknown metadata with type", yaml: "payload:\n  type: json\n  mystery: true\n", wantErr: "event payload field field \"mystery\" is not supported"},
		{name: "invalid refinement", yaml: "payload:\n  type: text\n  pattern: \"[\"\n", wantErr: "must compile as a regular expression"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitEventCatalogEntryForTest(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
